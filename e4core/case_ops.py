"""The operators that read a Case: the Case itself, window selection, and its candidates.

    case             (external)  -> Case
    case.select      Case        -> TelemetryWindows   the anomalous window, or some/all baselines
    case.candidates  Case        -> CandidateSet       the Case's components
"""
from __future__ import annotations

from typing import Any, Mapping

from .contracts import Candidate, CandidateSet, Case, TelemetryWindows
from .registry import OperatorRegistry, OperatorSpec, PortSpec
from .runtime import ParameterSpec


def select_windows(inputs: Mapping[str, Any], params: Mapping[str, Any], context: Any) -> TelemetryWindows:
    case: Case = inputs["case"]
    ids = tuple(params["ids"])
    if params["window"] == "anomaly":
        if ids:
            raise ValueError("ids select among baselines; the anomalous window is one window")
        return TelemetryWindows((case.anomalous(),))
    return TelemetryWindows(case.baselines(ids or None))


def case_candidates(inputs: Mapping[str, Any], params: Mapping[str, Any], context: Any) -> CandidateSet:
    case: Case = inputs["case"]
    return CandidateSet(tuple(Candidate(component) for component in case.components),
                        source="case.components")


def register_case_operators(registry: OperatorRegistry) -> OperatorRegistry:
    registry.register(OperatorSpec(
        "case", "1", (), "Case", cacheable=False, external_input=True,
        description="Executor-injected Case: telemetry windows, the anomalous one, and components."))
    registry.register(OperatorSpec(
        "case.select", "1", (PortSpec("case", "Case"),), "TelemetryWindows",
        {"window": ParameterSpec("window", "string", required=True, choices=("anomaly", "baselines")),
         "ids": ParameterSpec("ids", "string_list", default=(), has_default=True,
                              description="baseline window ids to keep; empty keeps every baseline")},
        cacheable=False, implementation=select_windows,
        description="The Case's anomalous window, or its baseline windows (all, or those named by ids)."))
    registry.register(OperatorSpec(
        "case.candidates", "1", (PortSpec("case", "Case"),), "CandidateSet",
        implementation=case_candidates,
        description="One candidate per Case component: model, harness and environment entities."))
    return registry


if __name__ == "__main__":
    from pathlib import Path

    from .cyber_adapter import load_cyber_case
    from .dag import DAGValidationError, verify_dag

    RUNS = Path(__file__).resolve().parents[1] / "data/cyber"
    case = load_cyber_case(RUNS / "dissect_k3_i_eqs_t2", [RUNS / "dissect_k3_i_eqs_t0"])
    registry = register_case_operators(OperatorRegistry())

    anomaly = select_windows({"case": case}, {"window": "anomaly", "ids": ()}, None)
    baselines = select_windows({"case": case}, {"window": "baselines", "ids": ()}, None)
    named = select_windows({"case": case}, {"window": "baselines", "ids": ("dissect_k3_i_eqs_t0:whole",)}, None)
    candidates = case_candidates({"case": case}, {}, None)
    print("anomaly   ", [w.window_id for w in anomaly.windows])
    print("baselines ", [w.window_id for w in baselines.windows])
    print("named     ", [w.window_id for w in named.windows])
    print("candidates", len(candidates.candidates))
    assert anomaly.windows[0].window_id == case.anomaly
    assert len(baselines.windows) == 1 and named.windows == baselines.windows
    assert len(candidates.candidates) == len(case.components)

    # The verifier accepts case -> select and case -> candidates edges; it stops only at the
    # missing scorer/answer, which waits on scoring.
    document = {"schema_version": "e4-dag/1", "return": {"node": "candidates"}, "nodes": [
        {"id": "case", "operator": "case", "version": "1", "inputs": {}, "params": {}, "bindings": {}},
        {"id": "anomaly", "operator": "case.select", "version": "1", "inputs": {"case": "case"},
         "params": {"window": "anomaly"}, "bindings": {}},
        {"id": "candidates", "operator": "case.candidates", "version": "1",
         "inputs": {"case": "case"}, "params": {}, "bindings": {}}]}
    try:
        verify_dag(document, registry)
    except DAGValidationError as error:
        print("verifier  ", error.code, "-", error)
