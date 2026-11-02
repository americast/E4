"""Static input-capability checks for generated Bank operators with signed requirements.

These declarations describe what trusted producers can supply. They catch known
missing fields/statistics and incomplete row coverage before a DAG executes; they
do not prove that arbitrary generated analysis code honors its declarations.
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Literal, Mapping

from .dag import VerifiedDAG, VerifiedNode


@dataclass(frozen=True)
class InputCapability:
    producer: str
    source_columns: Mapping[str, frozenset[str]]
    statistics: frozenset[str]
    group_by: frozenset[str]
    support_units: frozenset[str]
    coverage: str
    representation: str
    coverage_metadata: bool
    group_by_by_table: Mapping[str, frozenset[str]] = field(default_factory=dict)
    source_field_labels: Mapping[str, Mapping[str, frozenset[str]]] = field(default_factory=dict)


@dataclass(frozen=True, slots=True)
class ComputationWindowSupport:
    """Observed support for one named window; counts never imply rankability."""

    window: Literal["reference", "alert"]
    data_present: bool
    finite_unique_timestamps: int
    minimum_timestamps: int | None = None
    exact_shared_pairs: int | None = None
    independent_blocks: int | None = None
    minimum_independent_blocks: int | None = None
    constant: bool | None = None
    computable: bool | None = None
    reason_codes: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        for field_name in ("finite_unique_timestamps", "minimum_timestamps",
                           "exact_shared_pairs", "independent_blocks",
                           "minimum_independent_blocks"):
            value = getattr(self, field_name)
            if value is not None and (type(value) is not int or value < 0):
                raise ValueError(f"{field_name} must be null or a nonnegative integer")
        if self.minimum_timestamps is not None and self.finite_unique_timestamps > 0 and not self.data_present:
            raise ValueError("timestamp support cannot be present when data_present is false")
        object.__setattr__(self, "reason_codes", tuple(self.reason_codes))

    def as_dict(self) -> dict[str, Any]:
        return {"window": self.window, "data_present": self.data_present,
            "finite_unique_timestamps": self.finite_unique_timestamps,
            "minimum_timestamps": self.minimum_timestamps,
            "exact_shared_pairs": self.exact_shared_pairs,
            "independent_blocks": self.independent_blocks,
            "minimum_independent_blocks": self.minimum_independent_blocks,
            "constant": self.constant, "computable": self.computable,
            "reason_codes": list(self.reason_codes)}


@dataclass(frozen=True, slots=True)
class UpstreamAbstention:
    """First non-computable ancestor, preserved across downstream kernels."""

    code: str
    origin_kernel: str
    origin_node: str | None = None
    chain: tuple[str, ...] = ()

    def __post_init__(self) -> None:
        if not self.code or not self.origin_kernel:
            raise ValueError("upstream abstention requires an error code and origin kernel")
        object.__setattr__(self, "chain", tuple(self.chain))

    def as_dict(self) -> dict[str, Any]:
        return {"code": self.code, "origin_kernel": self.origin_kernel,
            "origin_node": self.origin_node, "chain": list(self.chain)}


@dataclass(frozen=True, slots=True)
class ComputationAvailability:
    """Three separate facts for a specific computation and input selection."""

    computation: str
    data_state: Literal["present", "partial", "absent", "unknown"]
    computability_state: Literal["computable", "not_computable", "unknown"]
    ranking_state: Literal["rankable", "not_rankable", "unknown"]
    required_inputs: tuple[str, ...] = ()
    observed_fields: tuple[str, ...] = ()
    windows: tuple[ComputationWindowSupport, ...] = ()
    selected_identity: str | None = None
    selection_reason: str | None = None
    reasons: tuple[str, ...] = ()
    upstream_abstention: UpstreamAbstention | None = None
    details: Mapping[str, Any] = field(default_factory=dict)

    def __post_init__(self) -> None:
        if not self.computation:
            raise ValueError("computation identity must be nonempty")
        if self.data_state == "absent" and self.computability_state == "computable":
            raise ValueError("absent data cannot be computable")
        if self.computability_state == "not_computable" and self.ranking_state == "rankable":
            raise ValueError("a non-computable result cannot be rankable")
        object.__setattr__(self, "required_inputs", tuple(self.required_inputs))
        object.__setattr__(self, "observed_fields", tuple(self.observed_fields))
        object.__setattr__(self, "windows", tuple(self.windows))
        object.__setattr__(self, "reasons", tuple(self.reasons))
        object.__setattr__(self, "details", dict(self.details))

    def as_dict(self) -> dict[str, Any]:
        return {"computation": self.computation, "data_state": self.data_state,
            "computability_state": self.computability_state,
            "ranking_state": self.ranking_state,
            "required_inputs": list(self.required_inputs),
            "observed_fields": list(self.observed_fields),
            "windows": [row.as_dict() for row in self.windows],
            "selected_identity": self.selected_identity,
            "selection_reason": self.selection_reason,
            "reasons": list(self.reasons),
            "upstream_abstention": (None if self.upstream_abstention is None else
                self.upstream_abstention.as_dict()), "details": dict(self.details)}


@dataclass(frozen=True, slots=True)
class OperatorIRCapabilityReport:
    """JSON-safe, case-local analysis availability for stage-three authoring."""

    primary_kpi: ComputationAvailability
    computations: tuple[ComputationAvailability, ...]
    provenance: Mapping[str, str]
    metric_selectors: tuple[Mapping[str, Any], ...] = ()
    schema_version: str = "e4-typed-ir-capability-report/1"

    def __post_init__(self) -> None:
        if self.schema_version != "e4-typed-ir-capability-report/1":
            raise ValueError("unsupported operator-IR capability report schema")
        object.__setattr__(self, "computations", tuple(self.computations))
        object.__setattr__(self, "provenance", dict(self.provenance))
        object.__setattr__(self, "metric_selectors", tuple(dict(row) for row in self.metric_selectors))

    def as_dict(self) -> dict[str, Any]:
        return {"schema_version": self.schema_version,
            "primary_kpi": self.primary_kpi.as_dict(),
            "computations": [row.as_dict() for row in self.computations],
            "metric_selectors": [dict(row) for row in self.metric_selectors],
            "metric_selector_catalog": [dict(row["selector"]) for row in self.metric_selectors],
            "metric_selector_resolutions": [{"selector_ref": row["selector"]["selector_ref"],
                "source_table": row["selector"]["source_table"],
                "status": row["resolution_status"], "match_count": row["match_count"],
                "policy_sha256": row["policy_sha256"],
                "case_data_sha256": row["case_data_sha256"],
                "matches": [dict(match) for match in row["matches"]]}
                for row in self.metric_selectors],
            "provenance": dict(sorted(self.provenance.items()))}


class InputCapabilityError(ValueError):
    def __init__(self, code: str, message: str, *, details: Mapping[str, Any] | None = None):
        super().__init__(message)
        self.code = code
        self.details = dict(details or {})

    def as_dict(self) -> dict[str, Any]:
        return {"code": self.code, "message": str(self), "details": self.details}


def input_producer_capability_schema(registry: Any) -> dict[str, Any]:
    """Compact prompt schema for trusted producers, bound to active signatures."""
    signatures = {(row["name"], row["version"]): row for row in registry.signatures()}
    producer_names = ("bank.public.query", "bank.metrics.aggregate",
        "bank.traces.aggregate", "bank.public.project", "bank.candidates.generate",
        "paper.input.prepare", "paper.backend.metric_conditions")
    rows: list[dict[str, Any]] = []
    for name in producer_names:
        signature = next((row for (registered_name, _version), row in signatures.items()
                          if registered_name == name), None)
        if signature is None:
            continue
        row: dict[str, Any] = {"name": name, "version": signature["version"],
            "output_type": signature["output_type"]}
        if name == "bank.public.query":
            row.update({"source_columns": "native rows from the selected table and fields parameter",
                "statistics": ["raw"], "group_by": [], "support_unit": "raw_row",
                "coverage": "page with explicit query coverage metadata"})
        elif name == "bank.metrics.aggregate":
            row.update({"source_columns": {
                    "metric/metric_app.csv": ["timestamp", "tc", "rr", "sr", "cnt", "mrt"],
                    "metric/metric_container.csv": ["timestamp", "cmdb_id", "kpi_name", "value"]},
                "statistics": ["median", "tail_quantile"],
                "group_by": {"metric/metric_app.csv": ["app_kpi", "source_field", "bomc_id", "item_id"],
                    "metric/metric_container.csv": ["component", "source_field", "bomc_id", "item_id"]},
                "support_unit": "timestamp", "coverage": "full_window",
                "retained_details": "median rows retain every timestamped bucket_samples entry for full-window IR analysis; source_hashes and case_data_sha256 identify the original inputs"})
        elif name == "bank.traces.aggregate":
            row.update({"source_columns": {"trace/trace_span.csv": ["cmdb_id", "trace_id", "duration"]},
                "statistics": ["per_trace_median", "per_trace_mean", "per_trace_tail_quantile"],
                "group_by": ["component"], "support_unit": "trace_id",
                "coverage": "full_window; trace_values details retain per-trace summaries, not raw spans"})
        elif name == "bank.public.project":
            row.update({"source_columns": "preserves upstream represented fields",
                "statistics": "filters existing statistic rows; does not calculate new statistics",
                "group_by": "preserves upstream result grouping; selected rows may omit groups",
                "support_unit": "preserves upstream support unit",
                "coverage": "page/selected rows with explicit projection omission metadata"})
        elif name == "paper.backend.metric_conditions":
            row.update({"source_columns": {}, "statistics": [],
                "group_by": ["component"], "support_unit": None,
                "coverage": "full_window",
                "provenance": "exact trusted paper C4 producer; exact observed component IDs with supported native metric excursions; no aliases"})
        elif name == "paper.input.prepare":
            row.update({"source_columns": {
                    "metric/metric_app.csv": ["timestamp", "tc", "rr", "sr", "cnt", "mrt"],
                    "metric/metric_container.csv": ["timestamp", "cmdb_id", "kpi_name", "value"],
                    "log/log_service.csv": ["log_id", "timestamp", "cmdb_id", "log_name", "value"]},
                "statistics": ["median", "normalized_log_pattern_count", "window_log_denominator"],
                "group_by": ["component", "app_kpi", "log_name", "window"],
                "support_unit": "timestamp_or_full_window_log_row",
                "coverage": "full_window with explicit per-capability availability and source pins",
                "log_boundary": "normalized full-window summaries only; no raw log text"})
        else:
            row.update({"source_columns": {}, "statistics": [],
                "group_by": ["component"], "support_unit": None, "coverage": "full_window"})
        rows.append(row)
    return {"schema_version": "e4-bank-input-capabilities/1",
        "source_column_semantics": "Fields actually exposed or represented in the producer output, qualified by canonical native table path; aggregate provenance does not mean raw rows are available.",
        "coverage_semantics": {"any": "no completeness guarantee required",
            "full_window": "all eligible groups in the declared source window are supplied",
            "page": "a bounded subset with explicit coverage/omission metadata; not full-window completeness"},
        "native_tables": {table: list(columns) for table, columns in _table_schemas().items()},
        "producers": rows,
        "declaration_limit": "These checks reject known schema and producer/consumer mismatches or coverage gaps; they do not prove that a registered computation is statistically appropriate or diagnostically correct."}


def _table_schemas() -> Mapping[str, tuple[str, ...]]:
    # Import locally: m4_data registers m4_generated operators during import.
    from .m4_data import TABLE_SCHEMAS
    return TABLE_SCHEMAS


def validate_input_requirements(value: Any, ports: list[Mapping[str, Any]], *,
        allow_table_specific: bool = False) -> dict[str, dict[str, Any]]:
    """Validate exact per-port source/statistic/group/support/coverage declarations."""
    table_schemas = _table_schemas()
    names = {str(port["name"]) for port in ports}
    if not isinstance(value, Mapping) or set(value) != names:
        raise InputCapabilityError("input_requirements_ports",
            "input_requirements must define exactly every declared input port",
            details={"declared_ports": sorted(names),
                     "requirement_ports": sorted(value) if isinstance(value, Mapping) else []})
    normalized: dict[str, dict[str, Any]] = {}
    required_fields = {"source_columns", "statistics", "group_by", "support_unit", "coverage"}
    table_group_field = "group_by_by_table"
    for port_name in sorted(names):
        row = value[port_name]
        allowed_shapes = ({frozenset(required_fields)} if not allow_table_specific else
            {frozenset(required_fields), frozenset(required_fields | {table_group_field})})
        if not isinstance(row, Mapping) or frozenset(row) not in allowed_shapes:
            if not allow_table_specific:
                # Preserve the legacy V1/generated input contract and message.
                raise InputCapabilityError("input_requirements_shape",
                    f"input_requirements.{port_name} must contain exactly source_columns, statistics, group_by, support_unit, and coverage")
            raise InputCapabilityError("input_requirements_shape",
                f"input_requirements.{port_name} must contain the legacy requirement fields and may add group_by_by_table")
        source_columns = row["source_columns"]
        if not isinstance(source_columns, Mapping):
            raise InputCapabilityError("input_requirements_source_columns",
                f"input_requirements.{port_name}.source_columns must map canonical table paths to column arrays")
        source_map: dict[str, list[str]] = {}
        for table, columns in source_columns.items():
            if not isinstance(table, str) or table not in table_schemas:
                raise InputCapabilityError("input_requirements_table",
                    f"input_requirements.{port_name} references an unknown public table {table!r}")
            if (not isinstance(columns, list) or len(columns) > len(table_schemas[table]) or
                    any(not isinstance(column, str) or column not in table_schemas[table]
                        for column in columns) or len(set(columns)) != len(columns)):
                raise InputCapabilityError("input_requirements_column",
                    f"input_requirements.{port_name} contains an unknown or duplicate column for {table}")
            source_map[table] = list(columns)
        list_fields: dict[str, list[str]] = {}
        for field in ("statistics", "group_by"):
            items = row[field]
            if (not isinstance(items, list) or len(items) > 32 or
                    any(not isinstance(item, str) or not item.strip() or len(item) > 128
                        for item in items) or len(set(items)) != len(items)):
                raise InputCapabilityError("input_requirements_values",
                    f"input_requirements.{port_name}.{field} must contain unique bounded nonempty strings")
            list_fields[field] = list(items)
        table_groups: dict[str, list[str]] | None = None
        if table_group_field in row:
            raw_table_groups = row[table_group_field]
            if (not allow_table_specific or not isinstance(raw_table_groups, Mapping) or
                    not raw_table_groups or list_fields["group_by"]):
                raise InputCapabilityError("input_requirements_grouping_shape",
                    f"input_requirements.{port_name}.group_by_by_table must be a nonempty table map used instead of flat group_by")
            table_groups = {}
            for table, items in raw_table_groups.items():
                if not isinstance(table, str) or table not in source_map:
                    raise InputCapabilityError("input_requirements_grouping_table",
                        f"input_requirements.{port_name}.group_by_by_table keys must be tables in source_columns")
                if (not isinstance(items, list) or not items or len(items) > 32 or
                        any(not isinstance(item, str) or not item.strip() or len(item) > 128
                            for item in items) or len(set(items)) != len(items)):
                    raise InputCapabilityError("input_requirements_grouping_values",
                        f"input_requirements.{port_name}.group_by_by_table.{table} must contain unique bounded nonempty strings")
                table_groups[table] = list(items)
        support_unit = row["support_unit"]
        if support_unit is not None and (not isinstance(support_unit, str) or
                not support_unit.strip() or len(support_unit) > 128):
            raise InputCapabilityError("input_requirements_support_unit",
                f"input_requirements.{port_name}.support_unit must be null or bounded nonempty text")
        coverage = row["coverage"]
        if not isinstance(coverage, str) or coverage not in {"any", "full_window", "page"}:
            raise InputCapabilityError("input_requirements_coverage",
                f"input_requirements.{port_name}.coverage must be any, full_window, or page")
        normalized_row = {"source_columns": source_map, **list_fields,
            "support_unit": support_unit, "coverage": coverage}
        if table_groups is not None:
            normalized_row[table_group_field] = table_groups
        normalized[port_name] = normalized_row
    return normalized


def _capability(node: VerifiedNode, upstream: Mapping[str, InputCapability]) -> InputCapability | None:
    table_schemas = _table_schemas()
    name = node.operator.name
    producer_key = node.operator.qualified_name
    if name == "case.telemetry":
        return InputCapability(producer_key,
            {table: frozenset(columns) for table, columns in table_schemas.items()},
            frozenset({"raw"}), frozenset({"component", "trace_id", "span_id", "timestamp"}),
            frozenset({"raw_row", "timestamp", "trace_id"}), "full_window", "telemetry", True,
            {})
    if name == "paper.input.prepare" and node.operator.output_type == "PaperStarterInput":
        return InputCapability(producer_key, {
            "metric/metric_app.csv": frozenset({"timestamp", "tc", "rr", "sr", "cnt", "mrt"}),
            "metric/metric_container.csv": frozenset({"timestamp", "cmdb_id", "kpi_name", "value"}),
            "log/log_service.csv": frozenset({"timestamp", "cmdb_id", "log_name", "value"})},
            frozenset({"median", "normalized_log_pattern_count", "window_log_denominator"}),
            frozenset({"component", "app_kpi", "log_name", "window"}),
            frozenset({"timestamp", "raw_log_row", "independent_time_block"}),
            "full_window", "paper_starter_input", True,
            {"metric/metric_app.csv": frozenset({"app_kpi"}),
             "metric/metric_container.csv": frozenset({"component"}),
             "log/log_service.csv": frozenset({"component", "log_name", "window"})})
    if name == "bank.public.query":
        table = node.params.get("table")
        if not isinstance(table, str) or table not in table_schemas:
            return None
        selected = node.params.get("fields", ())
        columns = frozenset(selected if selected else table_schemas[table])
        return InputCapability(producer_key, {table: columns}, frozenset({"raw"}), frozenset(),
            frozenset({"raw_row"}), "page", "native_rows", True, {})
    if name == "bank.metrics.aggregate":
        return InputCapability(producer_key, {
            # Aggregate retains timestamped bucket_samples and exact identity;
            # these represented fields are not exposed as raw rows.
            "metric/metric_app.csv": frozenset({"timestamp", "tc", "rr", "sr", "cnt", "mrt"}),
            "metric/metric_container.csv": frozenset({"timestamp", "cmdb_id", "kpi_name", "value"})},
            frozenset({"median", "tail_quantile"}),
            frozenset({"component", "app_kpi", "source_table", "source_field", "bomc_id", "item_id"}),
            frozenset({"timestamp"}), "full_window", "aggregate_rows", True,
            {"metric/metric_app.csv": frozenset({"app_kpi", "source_field", "bomc_id", "item_id"}),
             "metric/metric_container.csv": frozenset({"component", "source_field", "bomc_id", "item_id"})},
            {"metric/metric_app.csv": {field: frozenset({field})
                for field in ("rr", "sr", "cnt", "mrt")},
             # metric_container.source_field values are case data (kpi_name),
             # so the catalog does not infer a field label from the column name.
             "metric/metric_container.csv": {}})
    if name == "bank.traces.aggregate":
        return InputCapability(producer_key,
            # Rows are one summary per entity/statistic; trace_id/value pairs are
            # retained in details. Per-span timestamps/IDs are not exposed.
            {"trace/trace_span.csv": frozenset({"cmdb_id", "trace_id", "duration"})},
            frozenset({"per_trace_median", "per_trace_mean", "per_trace_tail_quantile"}),
            frozenset({"component"}), frozenset({"trace_id"}),
            "full_window", "aggregate_rows", True,
            {"trace/trace_span.csv": frozenset({"component"})},
            {"trace/trace_span.csv": {"duration": frozenset({"duration"})}})
    if name == "bank.public.project":
        references = node.inputs.get("data", ())
        sources = [upstream.get(ref) for ref in references]
        if len(sources) != 1 or sources[0] is None:
            return InputCapability(producer_key, {}, frozenset(), frozenset(),
                frozenset(), "page", "unsupported_projection_input", False)
        source = sources[0]
        if source.representation not in {"aggregate_rows", "projected_rows"}:
            return InputCapability(producer_key, {}, frozenset(), frozenset(),
                frozenset(), "page", "unsupported_projection_input", False)
        if source.representation == "projected_rows":
            return InputCapability(producer_key, {}, frozenset(), frozenset(),
                frozenset(), "page", "unsupported_projection_input", False)
        source_filter = node.params.get("source_fields", ())
        statistic_filter = node.params.get("statistics", ())
        projected_columns: dict[str, frozenset[str]] = {
            table: fields for table, fields in source.source_columns.items()}
        projected_stats: frozenset[str] = source.statistics
        projected_groups: frozenset[str] = source.group_by
        projected_labels: dict[str, dict[str, frozenset[str]]] = {table: dict(field_labels)
                  for table, field_labels in source.source_field_labels.items()}
        if source_filter:
            selected = frozenset(source_filter)
            # The projector accepts actual row.source_field labels only.
            filtered: dict[str, frozenset[str]] = {}
            filtered_labels: dict[str, dict[str, frozenset[str]]] = {}
            for table, fields in projected_columns.items():
                table_labels = projected_labels.get(table, {})
                retained = frozenset(field for field in fields
                    if table_labels.get(field, frozenset()) & selected)
                filtered[table] = retained
                filtered_labels[table] = {field: table_labels[field]
                    for field in retained if field in table_labels}
            projected_columns, projected_labels = filtered, filtered_labels
        if statistic_filter:
            projected_stats = projected_stats & frozenset(statistic_filter)
        # Projection keeps scalar row fields and provenance but omits nested
        # per-trace details such as trace_values. Those details are the only
        # representation of individual trace IDs in this aggregate.
        projected_columns = {table: frozenset(fields - ({"trace_id"} if table == "trace/trace_span.csv" else set()))
                   for table, fields in projected_columns.items()}
        return InputCapability(producer_key, projected_columns, projected_stats, projected_groups,
            source.support_units, "page", "projected_rows", True,
            source.group_by_by_table, projected_labels)
    if name == "bank.candidates.generate":
        return InputCapability(producer_key, {}, frozenset(), frozenset({"component"}),
            frozenset(), "full_window", "candidate_set", True, {})
    if _is_trusted_paper_component_candidate_producer(node):
        return InputCapability(producer_key, {}, frozenset(), frozenset({"component"}),
            frozenset(), "full_window", "candidate_set", True, {})
    if name in {"bank.metrics.contrast", "bank.traces.latency_contrast", "bank.logs.volume_contrast"}:
        return InputCapability(producer_key, {}, frozenset({"scored_candidates"}), frozenset({"component"}),
            frozenset(), "full_window", "scored_candidates", True, {})
    if name.startswith("evolved."):
        # Generated code has no trusted producer contract for its output semantics.
        return None
    return None


def _is_trusted_paper_component_candidate_producer(node: VerifiedNode) -> bool:
    """Recognize only the exact pinned paper C4 builtin CandidateSet contract."""
    operator = node.operator
    if (operator.name != "paper.backend.metric_conditions" or operator.version != "1" or
            operator.family != "backend.metric_conditions" or
            operator.output_type != "CandidateSet" or operator.external_input):
        return False
    if len(operator.inputs) != 1:
        return False
    port = operator.inputs[0]
    if (port.name, port.value_type, port.required, port.variadic,
            port.minimum_items, port.maximum_items) != (
            "data", "PaperStarterInput", True, False, 1, None):
        return False
    # Do not trust a same-named generated passthrough. The registered wrapper
    # must close over the exact host implementation and its policy-bound params.
    from .paper_starter import (_metric_conditions, _rankable_component_metric_fields,
        _rankable_component_metric_rule_refs)
    implementation = operator.implementation
    closure = getattr(implementation, "__closure__", None) or ()
    closure_values = tuple(cell.cell_contents for cell in closure)
    if not any(value is _metric_conditions for value in closure_values):
        return False
    parameter_maps = [value for value in closure_values if isinstance(value, Mapping)]
    if len(parameter_maps) != 1 or parameter_maps[0] != operator.parameters:
        return False

    from .bank_metric_policy import load_bank_metric_policy
    metric_policy = load_bank_metric_policy()
    rule_choices = ("", *tuple(_rankable_component_metric_rule_refs()))
    metric_choices = ("", *tuple(sorted(_rankable_component_metric_fields())))
    policy_sha = metric_policy.sha256
    expected_descriptions = {
        "metric": ("Exact component metric_container.kpi_name from the Bank metric catalog; "
            "empty means test allowlisted fields separately. "
            f"Catalog policy SHA-256: {policy_sha}."),
        "metric_rule_ref": ("Stable versioned source-rule reference; trusted host resolves every "
            "exact observed field in the selected case. "
            f"Catalog policy SHA-256: {policy_sha}."),
    }
    expected_parameters = {
        "minimum_samples": ("positive_integer", 2, (), {}),
        "minimum_effect": ("number", 0.0, (), {"minimum": 0.0}),
        "metric": ("string", "", metric_choices, {}),
        "metric_rule_ref": ("string", "", rule_choices, {}),
    }
    if set(operator.parameters) != set(expected_parameters):
        return False
    for name, (value_type, default, choices, constraints) in expected_parameters.items():
        parameter = operator.parameters[name]
        if (parameter.value_type != value_type or parameter.required or parameter.nullable or
                not parameter.has_default or parameter.default != default or
                parameter.runtime_field is not None or parameter.estimator is not None or
                tuple(parameter.choices) != tuple(choices) or
                dict(parameter.constraints) != constraints):
            return False
        if name in expected_descriptions and parameter.description != expected_descriptions[name]:
            return False
        if name not in expected_descriptions and parameter.description != "":
            return False
    return True


def _merge_capabilities(values: list[InputCapability]) -> InputCapability:
    if not values:
        raise ValueError("cannot merge an empty capability list")
    tables = set.intersection(*(set(value.source_columns) for value in values))
    columns = {table: frozenset.intersection(*(value.source_columns[table] for value in values))
               for table in tables}
    group_tables = set.intersection(*(set(value.group_by_by_table) for value in values))
    groups_by_table = {table: frozenset.intersection(*(value.group_by_by_table[table]
        for value in values)) for table in group_tables}
    return InputCapability(" & ".join(value.producer for value in values), columns,
        frozenset.intersection(*(value.statistics for value in values)),
        frozenset.intersection(*(value.group_by for value in values)),
        frozenset.intersection(*(value.support_units for value in values)),
        "full_window" if all(value.coverage == "full_window" for value in values) else "page",
        values[0].representation if len({value.representation for value in values}) == 1 else "mixed",
        all(value.coverage_metadata for value in values), groups_by_table,
        {table: {column: frozenset.intersection(*(value.source_field_labels.get(table, {}).get(column, frozenset())
                    for value in values)) for column in set.intersection(*(
                        set(value.source_field_labels.get(table, {})) for value in values))}
            for table in set.intersection(*(set(value.source_field_labels) for value in values))})


def check_typed_input_capabilities(verified: VerifiedDAG,
        typed_specs: list[Mapping[str, Any]]) -> list[dict[str, Any]]:
    """Check declared new-operator requirements against verified upstream producers."""
    typed_by_key = {(str(spec["name"]), str(spec["version"])): spec
                    for spec in typed_specs
                    if spec.get("authoring_profile") in {"typed-records-v1", "typed-operator-ir-v1",
                        "typed-operator-ir-v2"}
                    or isinstance(spec.get("input_requirements"), Mapping)}
    if not typed_by_key:
        return []
    capabilities: dict[str, InputCapability] = {}
    reports: list[dict[str, Any]] = []
    for node in verified.nodes:
        spec = typed_by_key.get((node.operator.name, node.operator.version))
        if spec is not None:
            requirements = spec["input_requirements"]
            for port_name, requirement in requirements.items():
                refs = list(node.inputs.get(port_name, ()))
                if not refs:
                    if (requirement["source_columns"] or requirement["statistics"] or
                            requirement["group_by"] or requirement["support_unit"] is not None or
                            requirement["coverage"] != "any"):
                        raise InputCapabilityError("input_capability_missing",
                            f"{node.node_id}.{port_name} declares data requirements but has no bound producer",
                            details={"node_id": node.node_id, "port": port_name})
                    reports.append({"node_id": node.node_id, "port": port_name,
                        "status": "unverified", "reason": "no producer bound; requirements are empty"})
                    continue
                sources = [capabilities.get(reference) for reference in refs]
                if any(source is None for source in sources):
                    if (requirement["source_columns"] or requirement["statistics"] or
                            requirement["group_by"] or requirement["support_unit"] is not None or
                            requirement["coverage"] != "any"):
                        raise InputCapabilityError("input_capability_unverified",
                            f"{node.node_id}.{port_name} is fed by a producer with no trusted capability declaration",
                            details={"node_id": node.node_id, "port": port_name,
                                     "producer_nodes": refs})
                    reports.append({"node_id": node.node_id, "port": port_name,
                        "status": "unverified", "reason": "upstream producer capability is unknown",
                        "producer_nodes": refs})
                    continue
                available = _merge_capabilities([source for source in sources if source is not None])
                if available.representation == "unsupported_projection_input":
                    raise InputCapabilityError("input_projection_unsupported",
                        f"{node.node_id}.{port_name} is fed by a projection input that the Bank projector cannot consume",
                        details={"node_id": node.node_id, "port": port_name,
                            "producer": available.producer})
                for table, columns in requirement["source_columns"].items():
                    missing = sorted(set(columns) - set(available.source_columns.get(table, ())))
                    if missing:
                        raise InputCapabilityError("input_source_column_unavailable",
                            f"{node.node_id}.{port_name} requires unavailable {table} columns {missing}",
                            details={"node_id": node.node_id, "port": port_name, "table": table,
                                "required": columns, "available": sorted(available.source_columns.get(table, ())),
                                "producer": available.producer})
                missing_statistics = sorted(set(requirement["statistics"]) - available.statistics)
                if missing_statistics:
                    raise InputCapabilityError("input_statistic_unavailable",
                        f"{node.node_id}.{port_name} requires statistics not produced upstream: {missing_statistics}",
                        details={"node_id": node.node_id, "port": port_name,
                            "required": requirement["statistics"],
                            "available": sorted(available.statistics), "producer": available.producer})
                if "group_by_by_table" in requirement:
                    # V2 can consume multiple trusted tables in one port. Keep
                    # each table's grouping contract separate (for example,
                    # container rows by component and app rows by app_kpi).
                    for table, required_groups in requirement["group_by_by_table"].items():
                        available_groups = available.group_by_by_table.get(table, frozenset())
                        missing_groups = sorted(set(required_groups) - available_groups)
                        if missing_groups:
                            raise InputCapabilityError("input_grouping_unavailable",
                                f"{node.node_id}.{port_name} requires unavailable grouping dimensions for {table}: {missing_groups}",
                                details={"node_id": node.node_id, "port": port_name,
                                    "table": table, "required": required_groups,
                                    "available": sorted(available_groups),
                                    "producer": available.producer})
                    available_groups = {table: sorted(available.group_by_by_table.get(table, frozenset()))
                        for table in requirement["group_by_by_table"]}
                else:
                    # Preserve the legacy flat-group contract unchanged. When
                    # source requirements span tables, flat groups remain the
                    # intersection of groups available from every requested table.
                    if requirement["source_columns"] and available.group_by_by_table:
                        tables = set(requirement["source_columns"])
                        per_table = [available.group_by_by_table.get(table, frozenset()) for table in tables]
                        available_groups = (frozenset.intersection(*per_table) if per_table else frozenset())
                    else:
                        available_groups = available.group_by
                    missing_groups = sorted(set(requirement["group_by"]) - available_groups)
                    if missing_groups:
                        raise InputCapabilityError("input_grouping_unavailable",
                            f"{node.node_id}.{port_name} requires unavailable grouping dimensions {missing_groups}",
                            details={"node_id": node.node_id, "port": port_name,
                                "required": requirement["group_by"], "available": sorted(available_groups),
                                "producer": available.producer})
                if requirement["support_unit"] is not None and requirement["support_unit"] not in available.support_units:
                    raise InputCapabilityError("input_support_unit_unavailable",
                        f"{node.node_id}.{port_name} requires unsupported support unit {requirement['support_unit']!r}",
                        details={"node_id": node.node_id, "port": port_name,
                            "required": requirement["support_unit"],
                            "available": sorted(available.support_units), "producer": available.producer})
                needed_coverage = requirement["coverage"]
                coverage_ok = (needed_coverage == "any" or
                    needed_coverage == "full_window" and available.coverage == "full_window" or
                    needed_coverage == "page" and available.coverage == "page")
                if not coverage_ok:
                    raise InputCapabilityError("input_coverage_unavailable",
                        f"{node.node_id}.{port_name} requires {needed_coverage} coverage but upstream is {available.coverage}",
                        details={"node_id": node.node_id, "port": port_name,
                            "required": needed_coverage, "available": available.coverage,
                            "producer": available.producer,
                            "coverage_metadata": available.coverage_metadata})
                report_row = {"node_id": node.node_id, "port": port_name,
                    "status": "verified", "producer": available.producer,
                    "coverage": available.coverage,
                    "declared_requirements": requirement,
                    "capability_check_scope": "declared producer compatibility; generated algorithm semantics are not proven"}
                if "group_by_by_table" in requirement:
                    report_row["available_group_by_by_table"] = {
                        table: sorted(available.group_by_by_table.get(table, frozenset()))
                        for table in requirement["group_by_by_table"]}
                reports.append(report_row)
        cap = _capability(node, capabilities)
        if cap is not None:
            capabilities[node.node_id] = cap
    return reports
