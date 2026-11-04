"""Phase 4 Portability Proof: Second independent dataset adapter (OpenTelemetry style)."""

from __future__ import annotations

from typing import Any, Iterator, Sequence

from e4.data.adapter import BaseDatasetAdapter
from e4.types import (
    ColumnSchema,
    DatasetSchema,
    EntityRef,
    MetricRecord,
    SpanRecord,
    TableSchema,
    TelemetryRecord,
    Window,
)


class DemoServiceAdapter(BaseDatasetAdapter):
    """Proves E4 portability on an independent, OpenTelemetry-shaped telemetry stream."""

    def __init__(
        self,
        ref_window: tuple[int, int] = (1620000000000, 1620001800000),
        alt_window: tuple[int, int] = (1620001800000, 1620003600000),
        nodes: Sequence[str] = ("worker-node-1", "worker-node-2", "ingress-gateway"),
    ):
        self._ref_window = Window("reference", ref_window[0], ref_window[1])
        self._alt_window = Window("alert", alt_window[0], alt_window[1])
        self._nodes = tuple(nodes)

        self._schema = DatasetSchema(
            tables=(
                TableSchema(
                    name="otel/node_metrics.csv",
                    record_type="metric",
                    columns=(
                        ColumnSchema("timestamp_utc", "timestamp", unit="ms"),
                        ColumnSchema("node_name", "string"),
                        ColumnSchema("metric_type", "string"),
                        ColumnSchema("gauge_value", "number"),
                    ),
                    entity_roles=("node",),
                ),
                TableSchema(
                    name="otel/http_spans.csv",
                    record_type="span",
                    columns=(
                        ColumnSchema("start_time_utc", "timestamp", unit="ms"),
                        ColumnSchema("service_name", "string"),
                        ColumnSchema("trace_id", "string"),
                        ColumnSchema("span_id", "string"),
                        ColumnSchema("latency_ms", "number"),
                    ),
                    entity_roles=("service",),
                ),
            ),
            time_unit="ms_utc",
            description="OTel cluster metrics and HTTP traces",
        )

    def get_schema(self) -> DatasetSchema:
        return self._schema

    def get_windows(self) -> tuple[Window, Window]:
        return (self._ref_window, self._alt_window)

    def extract_suspect_entities(self) -> list[EntityRef]:
        return [EntityRef(role="node", identifier=nid) for nid in self._nodes]

    def stream_records(self, table_name: str, window: Window) -> Iterator[TelemetryRecord]:
        mid = (window.start_ms_utc + window.end_ms_utc) // 2
        if table_name == "otel/node_metrics.csv":
            for nid in self._nodes:
                val = 98.5 if (nid == "worker-node-2" and window.name == "alert") else 24.0
                yield MetricRecord(
                    source_table=table_name,
                    timestamp_ms_utc=mid,
                    entity=EntityRef("node", nid),
                    metric_name="cpu_utilization",
                    value=val,
                )
        elif table_name == "otel/http_spans.csv":
            for nid in self._nodes:
                dur = 2500.0 if (nid == "worker-node-2" and window.name == "alert") else 35.0
                yield SpanRecord(
                    source_table=table_name,
                    timestamp_ms_utc=mid,
                    trace_id="trace_001",
                    span_id=f"span_{nid}",
                    parent_id=None,
                    duration_ms=dur,
                    success=True,
                    executing_component=EntityRef("node", nid),
                )
