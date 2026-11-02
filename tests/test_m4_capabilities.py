import unittest

from e4core.dag import VerifiedDAG, VerifiedNode
from e4core.m4_capabilities import (InputCapabilityError, check_typed_input_capabilities,
                                   validate_input_requirements)
from e4core.m4_data import make_m4_registry
from e4core.registry import OperatorSpec, PortSpec


def _requirements(*, columns=None, statistics=None, group_by=None,
                  support_unit=None, coverage="any"):
    return {"reference": {"source_columns": columns or {},
        "statistics": statistics or [], "group_by": group_by or [],
        "support_unit": support_unit, "coverage": coverage}}


def _trace_graph(requirements, *, query=False, project=False):
    registry = make_m4_registry(None, [])
    telemetry = registry.require("case.telemetry", "1")
    nodes = [VerifiedNode("data", telemetry, {}, {}, {}, 1)]
    if query:
        producer = registry.require("bank.public.query", "1")
        nodes.append(VerifiedNode("reference", producer, {"data": ("data",)},
            {"table": "trace/trace_span.csv", "window": "reference",
             "fields": ["timestamp", "duration"], "offset": 0, "limit": 256}, {}, 2))
    else:
        trace = next(row for row in registry.signatures() if row["name"] == "bank.traces.aggregate")
        producer = registry.require(trace["name"], trace["version"])
        nodes.append(VerifiedNode("reference", producer, {"data": ("data",)},
            {"window": "reference"}, {}, 2))
    upstream_id = "reference"
    if project:
        projector = registry.require("bank.public.project", next(row["version"]
            for row in registry.signatures() if row["name"] == "bank.public.project"))
        nodes.append(VerifiedNode("projected", projector, {"data": ("reference",)},
            {"source_fields": ["duration"], "statistics": ["per_trace_mean"],
             "entities": [], "offset": 0, "limit": 64}, {}, 3))
        upstream_id = "projected"
    typed = OperatorSpec("evolved.capability_probe", "1",
        (PortSpec("reference", "Aggregate"),), "ScoredCandidates")
    nodes.append(VerifiedNode("probe", typed, {"reference": (upstream_id,)}, {}, {}, len(nodes) + 1))
    return VerifiedDAG(tuple(nodes), "probe", len(nodes), len(nodes)), [{
        "name": typed.name, "version": typed.version,
        "authoring_profile": "typed-records-v1", "input_requirements": requirements}]


class M4InputCapabilityTests(unittest.TestCase):
    def test_requirement_schema_uses_canonical_table_paths_and_exact_ports(self):
        ports = [{"name": "reference"}]
        valid = _requirements(columns={"trace/trace_span.csv": ["duration"]})
        self.assertEqual(validate_input_requirements(valid, ports)["reference"]["source_columns"],
            {"trace/trace_span.csv": ["duration"]})
        invalid = _requirements(columns={"trace/trace_span.csv": ["success"]})
        with self.assertRaisesRegex(InputCapabilityError, "unknown or duplicate column"):
            validate_input_requirements(invalid, ports)
        with self.assertRaisesRegex(InputCapabilityError, "exactly every declared input port"):
            validate_input_requirements({}, ports)

    def test_table_specific_grouping_is_v2_opt_in_and_keeps_legacy_flat_shape(self):
        ports = [{"name": "reference"}]
        requirement = {"reference": {"source_columns": {
                "metric/metric_container.csv": ["timestamp", "cmdb_id", "kpi_name", "value"],
                "metric/metric_app.csv": ["timestamp", "tc", "mrt"]},
            "statistics": ["median"], "group_by": [], "support_unit": "timestamp",
            "coverage": "full_window", "group_by_by_table": {
                "metric/metric_container.csv": ["component"],
                "metric/metric_app.csv": ["app_kpi"]}}}
        with self.assertRaises(InputCapabilityError):
            validate_input_requirements(requirement, ports)
        normalized = validate_input_requirements(requirement, ports, allow_table_specific=True)
        self.assertEqual(normalized["reference"]["group_by_by_table"],
            {"metric/metric_container.csv": ["component"], "metric/metric_app.csv": ["app_kpi"]})
        flat = _requirements(columns={"trace/trace_span.csv": ["duration"]},
            statistics=["per_trace_mean"], group_by=["component"],
            support_unit="trace_id", coverage="full_window")
        self.assertEqual(validate_input_requirements(flat, ports=[{"name": "reference"}]), flat)

    def test_trace_aggregator_declares_only_supported_summary_capabilities(self):
        req = _requirements(columns={"trace/trace_span.csv": ["duration"]},
            statistics=["per_trace_mean"], group_by=["component"],
            support_unit="trace_id", coverage="full_window")
        dag, specs = _trace_graph(req)
        report = check_typed_input_capabilities(dag, specs)
        self.assertEqual(report[0]["status"], "verified")
        self.assertEqual(report[0]["coverage"], "full_window")

    def test_missing_span_id_aggregated_stats_group_and_support_fail_before_execution(self):
        bad_requirements = [
            _requirements(columns={"trace/trace_span.csv": ["span_id"]}),
            _requirements(statistics=["count", "sum"]),
            _requirements(group_by=["trace_id"]),
            _requirements(support_unit="span"),
        ]
        expected = ("input_source_column_unavailable", "input_statistic_unavailable",
                    "input_grouping_unavailable", "input_support_unit_unavailable")
        for requirements, code in zip(bad_requirements, expected):
            with self.subTest(code=code):
                dag, specs = _trace_graph(requirements)
                with self.assertRaises(InputCapabilityError) as raised:
                    check_typed_input_capabilities(dag, specs)
                self.assertEqual(raised.exception.code, code)

    def test_native_query_requires_page_coverage_and_selected_columns(self):
        req = _requirements(columns={"trace/trace_span.csv": ["timestamp", "duration"]},
            statistics=["raw"], support_unit="raw_row", coverage="page")
        dag, specs = _trace_graph(req, query=True)
        self.assertEqual(check_typed_input_capabilities(dag, specs)[0]["coverage"], "page")
        full_window = _requirements(columns={"trace/trace_span.csv": ["duration"]},
            coverage="full_window")
        dag, specs = _trace_graph(full_window, query=True)
        with self.assertRaises(InputCapabilityError) as raised:
            check_typed_input_capabilities(dag, specs)
        self.assertEqual(raised.exception.code, "input_coverage_unavailable")

    def test_projection_propagates_supported_fields_but_is_page_coverage(self):
        req = _requirements(columns={"trace/trace_span.csv": ["duration"]},
            statistics=["per_trace_mean"], group_by=["component"],
            support_unit="trace_id", coverage="page")
        dag, specs = _trace_graph(req, project=True)
        report = check_typed_input_capabilities(dag, specs)
        self.assertEqual(report[0]["status"], "verified")
        self.assertEqual(report[0]["coverage"], "page")
        qualified_filter = {"reference": {**req["reference"], "coverage": "any"}}
        dag, specs = _trace_graph(qualified_filter, project=True)
        project = next(node for node in dag.nodes if node.node_id == "projected")
        altered = VerifiedNode(project.node_id, project.operator, project.inputs,
            {**project.params, "source_fields": ["trace/trace_span.csv|duration"]},
            project.bindings, project.depth)
        nodes = tuple(altered if node.node_id == "projected" else node for node in dag.nodes)
        with self.assertRaises(InputCapabilityError) as raised:
            check_typed_input_capabilities(VerifiedDAG(nodes, dag.return_node,
                dag.max_depth, dag.measured_node_count), specs)
        self.assertEqual(raised.exception.code, "input_source_column_unavailable")

    def test_projection_drops_nested_trace_ids_and_cannot_be_reprojected(self):
        trace_id_req = _requirements(columns={"trace/trace_span.csv": ["trace_id"]},
            coverage="page")
        dag, specs = _trace_graph(trace_id_req, project=True)
        with self.assertRaises(InputCapabilityError) as raised:
            check_typed_input_capabilities(dag, specs)
        self.assertEqual(raised.exception.code, "input_source_column_unavailable")

        projector = next(node.operator for node in dag.nodes if node.node_id == "projected")
        nodes = (*dag.nodes[:-1],
            VerifiedNode("projected_twice", projector, {"data": ("projected",)},
                {"source_fields": [], "statistics": [], "entities": [], "offset": 0, "limit": 64}, {}, 4),
            VerifiedNode("probe", dag.nodes[-1].operator, {"reference": ("projected_twice",)}, {}, {}, 5))
        invalid = VerifiedDAG(tuple(nodes), "probe", 5, 5)
        empty_requirements = _requirements()
        _, specs = _trace_graph(empty_requirements)
        with self.assertRaises(InputCapabilityError) as raised:
            check_typed_input_capabilities(invalid, specs)
        self.assertEqual(raised.exception.code, "input_projection_unsupported")


if __name__ == "__main__":
    unittest.main()
