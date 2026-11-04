"""Lightweight, domain-agnostic 3-stage DiagnosisController."""

from __future__ import annotations

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


def _parse_json(text: str) -> dict[str, Any]:
    clean = text.strip()
    if clean.startswith("```"):
        clean = clean.strip("`")
        if clean.startswith("json"):
            clean = clean[4:].strip()
    return json.loads(clean)


def _to_ranked_answer(raw_val: Any, stage: str, entities: Sequence[EntityRef]) -> RankedAnswer:
    cands: list[EntityRef] = []
    scores: list[float] = []
    if hasattr(raw_val, "candidates"):
        for c in raw_val.candidates:
            if hasattr(c, "entity"):
                ent = c.entity
                cands.append(EntityRef(ent.role, ent.identifier))
            elif hasattr(c, "role") and hasattr(c, "identifier"):
                cands.append(EntityRef(c.role, c.identifier))
            if hasattr(c, "score"):
                scores.append(float(c.score))
        if hasattr(raw_val, "scores") and raw_val.scores:
            scores = [float(s) for s in raw_val.scores]
        return RankedAnswer(candidates=tuple(cands), scores=tuple(scores), stage=stage)
    return RankedAnswer(candidates=tuple(entities[:3]), scores=tuple([1.0] * min(3, len(entities))), stage=stage)


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
        tel_view = (
            telemetry.as_telemetry_view()
            if hasattr(telemetry, "as_telemetry_view")
            else TelemetryView(
                case_id=case_id,
                reference_window=ref_w,
                alert_window=alt_w,
                records=list(telemetry.stream_records(schema.tables[0].name, alt_w)),
                source_tables=[t.name for t in schema.tables],
            )
        )

        history: list[dict[str, Any]] = []
        admitted_ops: list[str] = []
        admitted_pb: str | None = None
        total_calls = 0

        # =====================================================================
        # STAGE 1: Playbook Retrieval (SELECT)
        # =====================================================================
        attempt = 1
        available_playbooks = self.library.list_playbooks()
        if available_playbooks:
            prompt = format_select_prompt(case_id, [pb.as_dict() for pb in available_playbooks], public_digest)
            total_calls += 1
            raw_resp = query_fn(prompt)
            sel_doc = _parse_json(raw_resp)
            pb_name = sel_doc.get("selected_playbook")
            chosen_pb = self.library.get_playbook(pb_name) if pb_name else available_playbooks[0]

            if chosen_pb:
                dag_doc = dict(chosen_pb.dag)
                # Apply parameter bindings if provided
                bindings = sel_doc.get("parameter_bindings", {})
                for node in dag_doc.get("nodes", []):
                    nid = node.get("id")
                    if nid in bindings:
                        node.setdefault("params", {}).update(bindings[nid])

                verified = verify_dag(dag_doc, self.library.registry)
                executor = SerialExecutor(self.library.registry)
                exec_res = executor.execute_verified(
                    verified,
                    runtime={"dag_depth": verified.max_depth},
                    external_values={"data": tel_view, "telemetry": tel_view},
                    runner=self.runner,
                )
                answer = _to_ranked_answer(exec_res.return_value, "select", suspect_entities)
                decision = self.oracle.evaluate(
                    answer,
                    DiagnosticContext(case_id, "select", attempt, self.max_attempts, tuple(history)),
                )
                history.append({
                    "stage": "select",
                    "playbook": chosen_pb.name,
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
        compose_doc = _parse_json(raw_resp)
        candidate_dag = compose_doc.get("dag", compose_doc)

        try:
            verified = verify_dag(candidate_dag, self.library.registry)
            executor = SerialExecutor(self.library.registry)
            exec_res = executor.execute_verified(
                verified,
                runtime={"dag_depth": verified.max_depth},
                external_values={"data": tel_view, "telemetry": tel_view},
                runner=self.runner,
            )
            answer = _to_ranked_answer(exec_res.return_value, "compose", suspect_entities)
        except Exception as exc:
            answer = RankedAnswer(candidates=tuple(suspect_entities[:3]), stage="compose")

        decision = self.oracle.evaluate(
            answer,
            DiagnosticContext(case_id, "compose", attempt, self.max_attempts, tuple(history)),
        )
        history.append({
            "stage": "compose",
            "top_candidates": [c.as_dict() for c in answer.candidates[:3]],
            "decision": decision.as_dict(),
        })

        if decision.action == "confirm":
            self.library.admit_after_accepted(f"playbook_adapted_{case_id}", "Adapted from Stage 2", verified)
            return DiagnosticResult(
                case_id=case_id,
                status="confirmed",
                stage="compose",
                top_candidates=answer.candidates[:3],
                answer=answer,
                admitted_playbook=f"playbook_adapted_{case_id}",
                total_calls=total_calls,
                history=tuple(history),
            )
        if decision.action == "stop_unverified":
            return DiagnosticResult(
                case_id=case_id,
                status="unverified",
                stage="compose",
                top_candidates=answer.candidates[:3],
                answer=answer,
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
        )
        total_calls += 1
        raw_resp = query_fn(evolve_prompt)
        initial_bundle = _parse_json(raw_resp)

        # Run bounded repair loop
        ok, new_specs, errors, repairs_used = self.repair_controller.attempt_repairs(
            initial_bundle,
            query_fn,
            dataset_schema=schema,
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
                top_candidates=answer.candidates[:3],
                answer=answer,
                total_calls=total_calls,
                history=tuple(history),
            )

        # Register newly authored operators
        registered_evolved_docs = []
        for spec in new_specs:
            if not self.library.registry.has(spec.name, spec.version):
                self.library.registry.register(spec)
            admitted_ops.append(spec.qualified_name)
            registered_evolved_docs.append(spec.as_dict())

        # Phase 3B: SERVE call to compose serving DAG
        serve_prompt = format_serve_prompt(
            case_id,
            registered_evolved_docs,
            self.library.registry.signatures(),
            public_digest,
        )
        total_calls += 1
        raw_resp = query_fn(serve_prompt)
        serve_doc = _parse_json(raw_resp)
        serving_dag_doc = serve_doc.get("dag", serve_doc)

        verified = verify_dag(serving_dag_doc, self.library.registry)
        executor = SerialExecutor(self.library.registry)
        exec_res = executor.execute_verified(
            verified,
            runtime={"dag_depth": verified.max_depth},
            external_values={"data": tel_view, "telemetry": tel_view},
            runner=self.runner,
        )
        answer = _to_ranked_answer(exec_res.return_value, "serve", suspect_entities)

        decision = self.oracle.evaluate(
            answer,
            DiagnosticContext(case_id, "serve", attempt, self.max_attempts, tuple(history)),
        )
        history.append({
            "stage": "serve",
            "top_candidates": [c.as_dict() for c in answer.candidates[:3]],
            "decision": decision.as_dict(),
        })

        if decision.action == "confirm":
            pb_name = f"playbook_evolved_{case_id}"
            self.library.admit_after_accepted(pb_name, "Admitted from Stage 3", verified, new_specs)
            admitted_pb = pb_name
            status = "confirmed"
        else:
            status = "unverified"

        return DiagnosticResult(
            case_id=case_id,
            status=status,
            stage="serve",
            top_candidates=answer.candidates[:3],
            answer=answer,
            admitted_operators=tuple(admitted_ops),
            admitted_playbook=admitted_pb,
            total_calls=total_calls,
            history=tuple(history),
        )
