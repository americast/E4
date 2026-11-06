"""Lightweight, domain-agnostic 3-stage DiagnosisController."""

from __future__ import annotations

import copy
import json
from typing import Any, Callable, Mapping, Sequence

from ..dag import DAGValidationError, SerialExecutor, verify_dag
from ..data.adapter import TelemetrySource
from ..oracle import AcceptanceOracle, BudgetStopPolicy, Decision, DiagnosticContext
from ..prompts.templates import (
    format_compose_prompt,
    format_evolve_prompt,
    format_select_prompt,
    format_serve_prompt,
)
from ..registry import OperatorRegistry, OperatorSpec
from ..sandbox.runner import SandboxRunner, SubprocessRunner
from ..sandbox.views import TelemetryView
from ..types import DiagnosticResult, EntityRef, RankedAnswer, Window
from .library import LibraryManager, Playbook
from .repair import StaticRepairController


def _deep_copy_doc(val: Any) -> Any:
    if isinstance(val, Mapping):
        return {k: _deep_copy_doc(v) for k, v in val.items()}
    if isinstance(val, (list, tuple)):
        return [_deep_copy_doc(v) for v in val]
    return val


def _parse_json(text: str) -> dict[str, Any]:
    clean = text.strip()
    try:
        return json.loads(clean)
    except Exception:
        pass
    if "```" in clean:
        parts = clean.split("```")
        for i in range(1, len(parts), 2):
            block = parts[i].strip()
            if block.startswith("json"):
                block = block[4:].strip()
            try:
                return json.loads(block)
            except Exception:
                continue
    first_brace = clean.find("{")
    last_brace = clean.rfind("}")
    if first_brace != -1 and last_brace > first_brace:
        try:
            return json.loads(clean[first_brace : last_brace + 1])
        except Exception:
            pass
    return json.loads(clean)


def _to_ranked_answer(raw_val: Any, stage: str, entities: Sequence[EntityRef]) -> RankedAnswer | None:
    if raw_val is None:
        return None
    if isinstance(raw_val, RankedAnswer):
        if not raw_val.candidates:
            return None
        return RankedAnswer(
            candidates=raw_val.candidates,
            scores=raw_val.scores,
            stage=stage,
            evidence=raw_val.evidence,
        )

    # Reject unscored CandidateSet discovery sets
    val_type = type(raw_val).__name__
    if val_type in {"CandidateSet", "CandidateSetView"}:
        return None

    if not hasattr(raw_val, "candidates"):
        return None

    cands: list[EntityRef] = []
    scores: list[float] = []
    has_explicit_scores = False

    for c in raw_val.candidates:
        if hasattr(c, "entity"):
            ent = c.entity
            if hasattr(ent, "role") and hasattr(ent, "identifier"):
                cands.append(EntityRef(ent.role, ent.identifier))
        elif hasattr(c, "role") and hasattr(c, "identifier"):
            cands.append(EntityRef(c.role, c.identifier))
        elif isinstance(c, Mapping):
            if "entity" in c:
                ent = c["entity"]
                cands.append(EntityRef(ent.get("role", "service"), ent.get("identifier", "")))
            elif "role" in c and "identifier" in c:
                cands.append(EntityRef(c["role"], c["identifier"]))
        if hasattr(c, "score"):
            has_explicit_scores = True
            try:
                scores.append(float(c.score))
            except (ValueError, TypeError):
                scores.append(0.0)
        elif isinstance(c, Mapping) and "score" in c:
            has_explicit_scores = True
            try:
                scores.append(float(c["score"]))
            except (ValueError, TypeError):
                scores.append(0.0)

    if hasattr(raw_val, "scores") and raw_val.scores:
        scores = [float(s) for s in raw_val.scores]
        has_explicit_scores = True

    # Terminal ranking contract requires explicit scored candidates
    if not cands or not has_explicit_scores:
        return None

    if len(scores) < len(cands):
        scores.extend([0.0] * (len(cands) - len(scores)))

    ev = getattr(raw_val, "evidence", ())
    return RankedAnswer(candidates=tuple(cands), scores=tuple(scores[:len(cands)]), stage=stage, evidence=tuple(ev))


class DiagnosisController:
    """Orchestrates 3-stage diagnosis: Retrieve -> Adapt -> Evolve/Serve."""

    def __init__(
        self,
        library: LibraryManager | None = None,
        oracle: AcceptanceOracle | None = None,
        runner: SandboxRunner | None = None,
        max_attempts: int = 3,
        max_repairs: int = 2,
    ):
        self.library = library or LibraryManager()
        self.oracle = oracle or BudgetStopPolicy(max_attempts=max_attempts)
        self.runner = runner or SubprocessRunner()
        self.max_attempts = max_attempts
        self.repair_controller = StaticRepairController(max_repairs=max_repairs)

    def run_diagnostic(
        self,
        telemetry: TelemetrySource,
        model_client: Any,
        case_id: str = "case_01",
    ) -> DiagnosticResult:
        query_fn: Callable[[str], str] = (
            model_client.complete if hasattr(model_client, "complete") else model_client
        )

        eff_max_attempts = self.max_attempts
        if hasattr(self.oracle, "max_attempts"):
            try:
                eff_max_attempts = min(eff_max_attempts, int(getattr(self.oracle, "max_attempts")))
            except (ValueError, TypeError):
                pass

        schema = telemetry.get_schema()
        ref_w, alt_w = telemetry.get_windows()
        suspect_entities = telemetry.extract_suspect_entities()

        public_digest = {
            "case_id": case_id,
            "reference_window": ref_w.as_dict(),
            "alert_window": alt_w.as_dict(),
            "tables": [t.name for t in schema.tables],
            "suspect_entities": [e.as_dict() for e in suspect_entities],
        }

        # Build in-memory telemetry view for external injection into the DAG
        if hasattr(telemetry, "as_telemetry_view"):
            tel_view = telemetry.as_telemetry_view()
        else:
            records = []
            for t in schema.tables:
                for w in (ref_w, alt_w):
                    records.extend(telemetry.stream_records(t.name, w))
            tel_view = TelemetryView(
                case_id=case_id,
                reference_window=ref_w,
                alert_window=alt_w,
                records=records,
                source_tables=[t.name for t in schema.tables],
            )

        history: list[dict[str, Any]] = []
        admitted_ops: list[str] = []
        admitted_pb: str | None = None
        total_calls = 0
        last_answer: RankedAnswer | None = None

        # =====================================================================
        # STAGE 1: Playbook Retrieval (SELECT)
        # =====================================================================
        attempt = 1
        available_playbooks = self.library.list_playbooks()
        if available_playbooks:
            prompt = format_select_prompt(case_id, [pb.as_dict() for pb in available_playbooks], public_digest)
            total_calls += 1
            raw_resp = query_fn(prompt)
            sel_doc = None
            for repair_idx in range(self.repair_controller.max_repairs + 1):
                try:
                    parsed = _parse_json(raw_resp)
                    if isinstance(parsed, Mapping):
                        sel_doc = parsed
                        break
                except Exception as exc:
                    if repair_idx < self.repair_controller.max_repairs:
                        total_calls += 1
                        repair_prompt = (
                            f"Previous response was malformed JSON: {exc}\n"
                            f"Raw response: {raw_resp}\n"
                            "Please return ONLY a valid JSON object with keys 'selected_playbook' and 'parameter_bindings'."
                        )
                        raw_resp = query_fn(repair_prompt)
            if not isinstance(sel_doc, Mapping):
                history.append({
                    "stage": "select",
                    "status": "failed_parse",
                    "raw_response": raw_resp,
                })
                sel_doc = {}
            pb_name = sel_doc.get("selected_playbook") if isinstance(sel_doc, Mapping) else None
            chosen_pb = self.library.get_playbook(pb_name) if pb_name else available_playbooks[0]

            if chosen_pb:
                dag_doc = _deep_copy_doc(chosen_pb.dag)
                bindings = sel_doc.get("parameter_bindings", {}) if isinstance(sel_doc, Mapping) else {}
                for node in dag_doc.get("nodes", []):
                    nid = node.get("id")
                    if nid in bindings:
                        node.setdefault("params", {}).update(bindings[nid])

                verified = None
                answer = None
                try:
                    verified = verify_dag(dag_doc, self.library.registry)
                    executor = SerialExecutor(self.library.registry)
                    exec_res = executor.execute_verified(
                        verified,
                        runtime={"dag_depth": verified.max_depth},
                        external_values={"data": tel_view, "telemetry": tel_view},
                        runner=self.runner,
                    )
                    answer = _to_ranked_answer(exec_res.return_value, "select", suspect_entities)
                except Exception as exc:
                    history.append({
                        "stage": "select",
                        "playbook": chosen_pb.name,
                        "status": "failed_execution",
                        "error": str(exc),
                    })

                if answer is not None:
                    last_answer = answer
                    decision = self.oracle.evaluate(
                        answer,
                        DiagnosticContext(case_id, "select", attempt, eff_max_attempts, tuple(history)),
                    )
                    history.append({
                        "stage": "select",
                        "playbook": chosen_pb.name,
                        "dag": dag_doc,
                        "top_candidates": [c.as_dict() for c in answer.candidates[:3]],
                        "decision": decision.as_dict(),
                    })

                    if decision.action == "confirm":
                        return DiagnosticResult(
                            case_id=case_id,
                            status="confirmed",
                            stage="select",
                            top_candidates=answer.candidates[:3],
                            answer=answer,
                            total_calls=total_calls,
                            history=tuple(history),
                        )
                    if decision.action == "stop_unverified":
                        return DiagnosticResult(
                            case_id=case_id,
                            status="unverified",
                            stage="select",
                            top_candidates=answer.candidates[:3],
                            answer=answer,
                            total_calls=total_calls,
                            history=tuple(history),
                        )
                else:
                    if not any(h.get("stage") == "select" and h.get("status") == "failed_execution" for h in history):
                        history.append({
                            "stage": "select",
                            "playbook": chosen_pb.name,
                            "status": "empty_answer",
                        })

                if attempt >= eff_max_attempts:
                    return DiagnosticResult(
                        case_id=case_id,
                        status="unverified",
                        stage="select",
                        top_candidates=answer.candidates[:3] if answer else (),
                        answer=answer,
                        total_calls=total_calls,
                        history=tuple(history),
                    )

        # =====================================================================
        # STAGE 2: DAG Adaptation & Composition (COMPOSE)
        # =====================================================================
        attempt += 1
        prompt = format_compose_prompt(
            case_id,
            history,
            self.library.registry.signatures(),
            public_digest,
        )
        total_calls += 1
        raw_resp = query_fn(prompt)
        compose_doc = None
        for repair_idx in range(self.repair_controller.max_repairs + 1):
            try:
                parsed = _parse_json(raw_resp)
                if isinstance(parsed, Mapping):
                    compose_doc = parsed
                    break
            except Exception as exc:
                if repair_idx < self.repair_controller.max_repairs:
                    total_calls += 1
                    repair_prompt = (
                        f"Previous response was malformed JSON: {exc}\n"
                        f"Raw response: {raw_resp}\n"
                        "Please return ONLY a valid JSON object with key 'dag'."
                    )
                    raw_resp = query_fn(repair_prompt)
        if not isinstance(compose_doc, Mapping):
            history.append({
                "stage": "compose",
                "status": "failed_parse",
                "raw_response": raw_resp,
            })
            compose_doc = {}
        candidate_dag = compose_doc.get("dag", compose_doc) if isinstance(compose_doc, Mapping) else {}

        verified_compose = None
        answer_compose = None
        try:
            candidate_dag_copy = _deep_copy_doc(candidate_dag)
            verified_compose = verify_dag(candidate_dag_copy, self.library.registry)
            executor = SerialExecutor(self.library.registry)
            exec_res = executor.execute_verified(
                verified_compose,
                runtime={"dag_depth": verified_compose.max_depth},
                external_values={"data": tel_view, "telemetry": tel_view},
                runner=self.runner,
            )
            answer_compose = _to_ranked_answer(exec_res.return_value, "compose", suspect_entities)
        except Exception as exc:
            history.append({
                "stage": "compose",
                "status": "failed_execution",
                "error": str(exc),
            })

        if answer_compose is not None and verified_compose is not None:
            last_answer = answer_compose
            decision = self.oracle.evaluate(
                answer_compose,
                DiagnosticContext(case_id, "compose", attempt, eff_max_attempts, tuple(history)),
            )
            history.append({
                "stage": "compose",
                "dag": candidate_dag,
                "top_candidates": [c.as_dict() for c in answer_compose.candidates[:3]],
                "decision": decision.as_dict(),
            })

            if decision.action == "confirm":
                self.library.admit_after_accepted(f"playbook_adapted_{case_id}", "Adapted from Stage 2", verified_compose)
                return DiagnosticResult(
                    case_id=case_id,
                    status="confirmed",
                    stage="compose",
                    top_candidates=answer_compose.candidates[:3],
                    answer=answer_compose,
                    admitted_playbook=f"playbook_adapted_{case_id}",
                    total_calls=total_calls,
                    history=tuple(history),
                )
            if decision.action == "stop_unverified":
                return DiagnosticResult(
                    case_id=case_id,
                    status="unverified",
                    stage="compose",
                    top_candidates=answer_compose.candidates[:3],
                    answer=answer_compose,
                    total_calls=total_calls,
                    history=tuple(history),
                )
        else:
            if not any(h.get("stage") == "compose" and h.get("status") == "failed_execution" for h in history):
                history.append({
                    "stage": "compose",
                    "status": "empty_answer",
                })

        if attempt >= eff_max_attempts:
            return DiagnosticResult(
                case_id=case_id,
                status="unverified",
                stage="compose",
                top_candidates=answer_compose.candidates[:3] if answer_compose else (last_answer.candidates[:3] if last_answer else ()),
                answer=answer_compose or last_answer,
                total_calls=total_calls,
                history=tuple(history),
            )

        # =====================================================================
        # STAGE 3: Operator Evolution & Pipeline Serving (EVOLVE + SERVE)
        # =====================================================================
        attempt += 1
        evolve_prompt = format_evolve_prompt(
            case_id,
            history,
            public_digest,
            schema.as_dict(),
            available_operators=self.library.registry.signatures(),
        )
        total_calls += 1
        raw_resp = query_fn(evolve_prompt)

        # Run bounded repair loop
        ok, new_specs, errors, repairs_used = self.repair_controller.attempt_repairs(
            raw_resp,
            query_fn,
            dataset_schema=schema,
            existing_library=self.library.registry,
        )
        total_calls += repairs_used

        if not ok or not new_specs:
            history.append({
                "stage": "evolve",
                "status": "failed_repair",
                "errors": errors,
            })
            return DiagnosticResult(
                case_id=case_id,
                status="unverified",
                stage="evolve",
                top_candidates=last_answer.candidates[:3] if last_answer else (),
                answer=last_answer,
                total_calls=total_calls,
                history=tuple(history),
            )

        # Candidate registry: clone active registry, do NOT mutate active registry yet!
        candidate_registry = self.library.registry.clone()
        registered_evolved_docs = []
        for spec in new_specs:
            if candidate_registry.has(spec.name, spec.version):
                existing = candidate_registry.require(spec.name, spec.version)
                if existing.source != spec.source:
                    history.append({
                        "stage": "evolve",
                        "status": "conflicting_source_identity",
                        "operator": spec.qualified_name,
                    })
                    return DiagnosticResult(
                        case_id=case_id,
                        status="unverified",
                        stage="evolve",
                        top_candidates=last_answer.candidates[:3] if last_answer else (),
                        answer=last_answer,
                        total_calls=total_calls,
                        history=tuple(history),
                    )
            else:
                candidate_registry.register(spec)
            registered_evolved_docs.append(spec.as_dict())

        # Phase 3B: SERVE call to compose serving DAG
        serve_prompt = format_serve_prompt(
            case_id,
            registered_evolved_docs,
            candidate_registry.signatures(),
            public_digest,
        )
        total_calls += 1
        verified_serve = None
        answer_serve = None
        serving_dag_doc = {}
        try:
            raw_resp = query_fn(serve_prompt)
            serve_doc = _parse_json(raw_resp)
            serving_dag_doc = serve_doc.get("dag", serve_doc) if isinstance(serve_doc, Mapping) else {}

            serving_dag_copy = _deep_copy_doc(serving_dag_doc)
            verified_serve = verify_dag(serving_dag_copy, candidate_registry)
            executor = SerialExecutor(candidate_registry)
            exec_res = executor.execute_verified(
                verified_serve,
                runtime={"dag_depth": verified_serve.max_depth},
                external_values={"data": tel_view, "telemetry": tel_view},
                runner=self.runner,
            )
            answer_serve = _to_ranked_answer(exec_res.return_value, "serve", suspect_entities)
        except Exception as exc:
            history.append({
                "stage": "serve",
                "status": "failed_execution",
                "error": str(exc),
            })

        if answer_serve is None or verified_serve is None:
            return DiagnosticResult(
                case_id=case_id,
                status="unverified",
                stage="serve",
                top_candidates=last_answer.candidates[:3] if last_answer else (),
                answer=last_answer,
                total_calls=total_calls,
                history=tuple(history),
            )

        last_answer = answer_serve
        decision = self.oracle.evaluate(
            answer_serve,
            DiagnosticContext(case_id, "serve", attempt, eff_max_attempts, tuple(history)),
        )
        history.append({
            "stage": "serve",
            "dag": serving_dag_doc,
            "top_candidates": [c.as_dict() for c in answer_serve.candidates[:3]],
            "decision": decision.as_dict(),
        })

        if decision.action == "confirm":
            nodes_by_id = {}
            for node in serving_dag_doc.get("nodes", []):
                if isinstance(node, Mapping) and "id" in node:
                    nodes_by_id[node["id"]] = node

            ret_node_id = serving_dag_doc.get("return_node")
            reachable_node_ids = set()
            if ret_node_id in nodes_by_id:
                queue = [ret_node_id]
                reachable_node_ids.add(ret_node_id)
                while queue:
                    curr_id = queue.pop(0)
                    curr_node = nodes_by_id.get(curr_id)
                    if not curr_node:
                        continue
                    inputs = curr_node.get("inputs", {})
                    if isinstance(inputs, Mapping):
                        for _, src in inputs.items():
                            if isinstance(src, str) and src in nodes_by_id and src not in reachable_node_ids:
                                reachable_node_ids.add(src)
                                queue.append(src)
                            elif isinstance(src, (list, tuple)):
                                for item in src:
                                    if isinstance(item, str) and item in nodes_by_id and item not in reachable_node_ids:
                                        reachable_node_ids.add(item)
                                        queue.append(item)

            reachable_ops = {
                nodes_by_id[nid].get("operator")
                for nid in reachable_node_ids
                if nodes_by_id[nid].get("operator")
            }
            actually_admitted_specs = [
                s for s in new_specs if s.name in reachable_ops
            ]
            pb_name = f"playbook_evolved_{case_id}"
            self.library.admit_after_accepted(
                pb_name, "Admitted from Stage 3", verified_serve, actually_admitted_specs
            )
            admitted_pb = pb_name
            admitted_ops = [s.qualified_name for s in actually_admitted_specs]
            status = "confirmed"
        else:
            status = "unverified"

        return DiagnosticResult(
            case_id=case_id,
            status=status,
            stage="serve",
            top_candidates=answer_serve.candidates[:3],
            answer=answer_serve,
            admitted_operators=tuple(admitted_ops),
            admitted_playbook=admitted_pb,
            total_calls=total_calls,
            history=tuple(history),
        )
