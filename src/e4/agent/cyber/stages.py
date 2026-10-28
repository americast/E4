"""The three-stage cyber diagnosis: the model selects, composes, or synthesizes a playbook; each
playbook runs on every Case in scope; the judge returns one accepted bit. Stops at the first
accepted stage. Every prompt, model call, DAG and stage record is written to the run directory."""
from __future__ import annotations

import json
import time
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Mapping

from e4.synthetic.benchmark import StaticTaskError, parse_strict_json_object
from e4.contracts import Case, RankedAnswer
from e4.agent.cyber.scope import cyber_runtime, load_seed_playbooks, run_scope
from e4.agent.cyber.digest import case_digest
from e4.judge.cyber.judge import GroundTruth, judge, run_report
from e4.operators.cyber.scoring import cyber_registry
from e4.agent.cyber.prompts import (SYSTEM_PROMPT, common_prompt, compose_prompt, repair_prompt,
                                    select_prompt, synthesize_prompt)
from e4.dsl.verifier import DAGValidationError, VerifiedDAG, verify_dag
from e4.agent.bank.controller import M4ControllerError, _resolve_generated_versions
from e4.operators.generated import validate_generated_spec
from e4.agent.model_client import LocalModelClient
from e4.dsl.registry import OperatorRegistry
from e4.dsl.runtime import RuntimeContext

STAGES = ("select", "compose", "synthesize")
STATIC_REPAIRS_PER_STAGE = 2
MAXIMUM_NEW_OPERATORS = 3
HISTORY_LEADS = 3
HISTORY_INTERVALS = 5


class StaticError(ValueError):
    """A model response that fails a check before anything runs; the model may repair it."""


class CyberModelClient(LocalModelClient):
    def make_payload(self, prompt: str, *, max_tokens: int | None = None) -> dict[str, Any]:
        payload = super().make_payload(prompt, max_tokens=max_tokens)
        payload["messages"][0]["content"] = SYSTEM_PROMPT
        return payload


def write_json(path: Path, value: Any) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(value, ensure_ascii=False, indent=2, sort_keys=True, default=str) + "\n")


def append_jsonl(path: Path, value: Any) -> None:
    with path.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(value, ensure_ascii=False, sort_keys=True, default=str) + "\n")


def check_playbook(document: Mapping[str, Any], registry: OperatorRegistry, runtime: RuntimeContext,
                   new_operator_keys: set[str] | None = None) -> VerifiedDAG:
    """The DAG verifies, starts at one 'case' node, returns cyber.answers.combine, and (stage 3)
    reaches at least one new operator."""
    try:
        verified = verify_dag(document, registry, runtime,
                              max_nodes=int(runtime.snapshot.values["max_nodes"]),
                              max_depth=int(runtime.snapshot.values["dag_depth"]))
    except DAGValidationError as error:
        raise StaticError(f"dag_invalid: {error}") from error

    external = []
    for node in verified.nodes:
        if node.operator.external_input:
            external.append(node.operator.name)
    if external != ["case"]:
        raise StaticError("the DAG must contain exactly one 'case' input node")

    returned = None
    for node in verified.nodes:
        if node.node_id == verified.return_node:
            returned = node
    if returned.operator.name != "cyber.answers.combine":
        raise StaticError("the DAG must return cyber.answers.combine")

    if new_operator_keys is not None:
        used = set()
        for node in verified.nodes:
            used.add(f"{node.operator.name}@{node.operator.version}")
        if not used & new_operator_keys:
            raise StaticError("the stage 3 DAG must use at least one newly proposed operator")
    return verified


def selected_playbook(response: Mapping[str, Any], playbooks: Mapping[str, Any],
                      registry: OperatorRegistry) -> dict[str, Any]:
    """Stage 1: a stored playbook with only the parameter overrides its nodes declare."""
    if set(response) != {"playbook", "node_parameters"}:
        raise StaticError("stage 1 returns exactly {playbook, node_parameters}")
    if response["playbook"] not in playbooks:
        raise StaticError(f"unknown playbook {response['playbook']!r}")
    document = json.loads(json.dumps(playbooks[response["playbook"]]))
    nodes = {}
    for node in document["nodes"]:
        nodes[node["id"]] = node
    for node_id, overrides in response["node_parameters"].items():
        if node_id not in nodes:
            raise StaticError(f"unknown node {node_id!r}")
        operator = registry.require(nodes[node_id]["operator"], nodes[node_id]["version"])
        for name, value in overrides.items():
            if name not in operator.parameters:
                raise StaticError(f"node {node_id} declares no parameter {name!r}")
            nodes[node_id]["params"][name] = value
    return document


def synthesized_playbook(response: Mapping[str, Any]) -> tuple[dict[str, Any], list[dict[str, Any]], set[str]]:
    """Stage 3: the validated new operator specs, and the DAG with their resolved versions."""
    if set(response) != {"operators", "dag"}:
        raise StaticError("stage 3 returns exactly {operators, dag}")
    raw_specs = response["operators"]
    if not isinstance(raw_specs, list) or not 1 <= len(raw_specs) <= MAXIMUM_NEW_OPERATORS:
        raise StaticError(f"stage 3 proposes 1 to {MAXIMUM_NEW_OPERATORS} operators")
    specs = []
    versions = {}
    new_keys = set()
    for raw in raw_specs:
        spec = validate_generated_spec(raw)
        specs.append(spec)
        versions[(spec["name"], spec["declared_version"])] = spec["version"]
        new_keys.add(f"{spec['name']}@{spec['version']}")
    document = _resolve_generated_versions(response["dag"], versions)
    return document, specs, new_keys


def answer_history(answers: Mapping[str, RankedAnswer]) -> dict[str, Any]:
    """What the model may see of an attempt's answers: per run, the top leads and a few intervals."""
    per_run = {}
    for run, answer in answers.items():
        leads = []
        for lead in answer.leads[:HISTORY_LEADS]:
            intervals = list(lead.intervals or ())
            leads.append({"component": f"{lead.entity.role}:{lead.entity.identifier}",
                          "score": lead.score, "interval_count": len(intervals),
                          "first_intervals": intervals[:HISTORY_INTERVALS]})
        per_run[run] = leads
    return per_run


def stage_response(stage: str, response: Mapping[str, Any], playbooks: Mapping[str, Any],
                   registry: OperatorRegistry, runtime: RuntimeContext):
    """Turn one parsed response into (verified DAG, document, registry it runs with, new specs)."""
    if stage == "select":
        document = selected_playbook(response, playbooks, registry)
        return check_playbook(document, registry, runtime), document, registry, []
    if stage == "compose":
        if set(response) != {"dag"}:
            raise StaticError("stage 2 returns exactly {dag}")
        document = response["dag"]
        return check_playbook(document, registry, runtime), document, registry, []
    document, specs, new_keys = synthesized_playbook(response)
    stage_registry = cyber_registry(specs)
    verified = check_playbook(document, stage_registry, runtime, new_keys)
    return verified, document, stage_registry, specs


def run_cyber_diagnostic(cases: Mapping[str, Case], truth: GroundTruth, run_dir: Path,
                         client: LocalModelClient) -> dict[str, Any]:
    """Select, compose, then synthesize until the judge accepts one stage's playbook."""
    started = time.perf_counter()
    run_dir.mkdir(parents=True, exist_ok=False)
    calls_path = run_dir / "model_calls.jsonl"
    registry = cyber_registry()
    runtime = cyber_runtime()
    playbooks = load_seed_playbooks()
    first_case = next(iter(cases.values()))
    digest = case_digest(first_case)
    write_json(run_dir / "case_digest.json", digest)

    history: list[dict[str, Any]] = []
    stage_records = []
    accepted_stage = None
    for stage_number, stage in enumerate(STAGES, start=1):
        stage_name = f"stage{stage_number}_{stage}"
        common = common_prompt(digest, registry, runtime, history)
        if stage == "select":
            base_prompt = select_prompt(common, playbooks)
        elif stage == "compose":
            base_prompt = compose_prompt(common)
        else:
            base_prompt = synthesize_prompt(common)

        record: dict[str, Any] = {"stage": stage_name, "static_errors": []}
        prompt = base_prompt
        built = None
        for call_number in range(1, STATIC_REPAIRS_PER_STAGE + 2):
            (run_dir / "prompts").mkdir(exist_ok=True)
            (run_dir / "prompts" / f"{stage_name}_call{call_number}.txt").write_text(prompt)
            call = client.complete(prompt, task_id=f"cyber_{stage_name}", attempt=call_number)
            append_jsonl(calls_path, {**call.as_dict(), "stage": stage_name})
            if call.error:
                record["status"] = "model_call_failed"
                record["error"] = call.error
                break
            previous = call.content or ""
            try:
                response, _fenced = parse_strict_json_object(previous)
                built = stage_response(stage, response, playbooks, registry, runtime)
                break
            except (StaticTaskError, StaticError, M4ControllerError, ValueError, KeyError,
                    TypeError) as error:
                record["static_errors"].append({"call": call_number, "error": str(error)})
                prompt = repair_prompt(base_prompt, previous, {"error": str(error)})
        if built is None:
            record.setdefault("status", "static_validation_failed")
            stage_records.append(record)
            history.append({"stage": stage_name, "status": record["status"], "accepted": None})
            continue

        verified, document, stage_registry, specs = built
        write_json(run_dir / "dags" / f"{stage_name}.json", document)
        if specs:
            write_json(run_dir / "operators" / f"{stage_name}.json", specs)
        try:
            answers = run_scope(document, cases, stage_registry, runtime)
        except Exception as error:
            record["status"] = "execution_failed"
            record["error"] = f"{type(error).__name__}: {error}"
            stage_records.append(record)
            history.append({"stage": stage_name, "status": "execution_failed",
                            "error": record["error"][:500], "accepted": None})
            continue

        accepted = judge(truth, answers)
        record["status"] = "accepted" if accepted else "rejected"
        record["answers"] = answer_history(answers)
        # against the ground truth: kept in the result for people, never added to the history
        judgement = {}
        for run, answer in answers.items():
            judgement[run] = run_report(truth, run, answer)
        record["judgement"] = judgement
        stage_records.append(record)
        history.append({"stage": stage_name, "status": "executed", "accepted": accepted,
                        "answers": record["answers"]})
        if accepted:
            accepted_stage = stage_name
            break

    result = {"accepted": accepted_stage is not None, "accepted_stage": accepted_stage,
              "truth_component": f"{truth.component.role}:{truth.component.identifier}",
              "scope_runs": list(cases), "stages": stage_records,
              "wall_seconds": round(time.perf_counter() - started, 1),
              "finished_utc": datetime.now(timezone.utc).isoformat()}
    write_json(run_dir / "result.json", result)
    return result
