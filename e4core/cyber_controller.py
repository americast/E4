"""Run a playbook over a scope: every run in scope becomes the anomaly of its own Case, the
playbook runs on each Case, and each answer is checked before it is returned."""
from __future__ import annotations

import json
from pathlib import Path
from typing import Any, Mapping

from .contracts import Case, RankedAnswer, Telemetry
from .cyber_adapter import cyber_components, whole_run
from .dag import verify_dag
from .executor import SerialExecutor
from .registry import OperatorRegistry
from .runtime import RuntimeContext, RuntimeSchema

ROOT = Path(__file__).resolve().parents[1]
SEED_LIBRARY = ROOT / "config/cyber_seed_library.json"


def cyber_runtime() -> RuntimeContext:
    """The core runtime values. Bank's reference/alert windows are left out: a Case has its own."""
    schema_path = ROOT / "config/runtime_schema.json"
    full_schema = RuntimeSchema.from_json(schema_path)
    fields = {}
    for name, spec in full_schema.fields.items():
        if name in ("reference_window", "alert_window"):
            continue
        fields[name] = spec
    schema = RuntimeSchema(fields, full_schema.version)
    values = json.loads((ROOT / "config/core_runtime.json").read_text())
    return RuntimeContext.build(schema, values)


def load_seed_playbooks(path: Path = SEED_LIBRARY) -> dict[str, dict[str, Any]]:
    library = json.loads(path.read_text())
    return library["playbooks"]


def scope_cases(telemetries: Mapping[str, Telemetry]) -> dict[str, Case]:
    """One Case per run in scope: that run's whole window is the anomaly, every other run's
    whole window is a baseline."""
    windows = {}
    for name, telemetry in telemetries.items():
        windows[name] = whole_run(telemetry)

    cases = {}
    for name, anomalous in windows.items():
        baselines = []
        baseline_names = []
        for other, window in windows.items():
            if other != name:
                baselines.append(window)
                baseline_names.append(other)
        cases[name] = Case(case_id=f"{name}__vs__{'+'.join(baseline_names)}",
                           windows=(anomalous, *baselines), anomaly=anomalous.window_id,
                           components=cyber_components())
    return cases


def validate_cyber_answer(answer: RankedAnswer, case: Case, maximum_leads: int) -> None:
    """Every lead names one of the Case's components, and its intervals lie inside the
    anomalous window."""
    if len(answer.leads) > maximum_leads:
        raise ValueError(f"ranked answer has {len(answer.leads)} leads; at most {maximum_leads}")
    components = set()
    for component in case.components:
        components.add((component.role, component.canonical_id))
    anomalous = case.anomalous()

    for lead in answer.leads:
        if (lead.entity.role, lead.entity.canonical_id) not in components:
            raise ValueError(f"lead {lead.entity.role}:{lead.entity.identifier} is not a Case component")
        for start, end in lead.intervals or ():
            if start < anomalous.start_ms or end > anomalous.end_ms:
                raise ValueError(f"lead interval ({start}, {end}) is outside the anomalous window")


def run_scope(playbook: Mapping[str, Any], cases: Mapping[str, Case],
              registry: OperatorRegistry, runtime: RuntimeContext) -> dict[str, RankedAnswer]:
    """Verify the playbook once, run it on every run's Case, and return each checked answer."""
    verified = verify_dag(playbook, registry, runtime,
                          max_nodes=int(runtime.snapshot.values["max_nodes"]),
                          max_depth=int(runtime.snapshot.values["dag_depth"]))
    case_node = None
    for node in verified.nodes:
        if node.operator.external_input:
            case_node = node.node_id
    executor = SerialExecutor(registry)
    lead_count = int(runtime.snapshot.values["lead_count"])

    answers = {}
    for name, case in cases.items():
        result = executor.execute_verified(verified, runtime, {case_node: case})
        validate_cyber_answer(result.answer, case, lead_count)
        answers[name] = result.answer
    return answers
