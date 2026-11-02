"""Deterministic synthetic execution fixtures for the reconstructed starter freeze."""

from __future__ import annotations

import csv
import hashlib
import json
import shutil
import tempfile
from pathlib import Path
from typing import Any, Mapping

from .bank_adapter import BankCase
from .cache import CacheStore
from .contracts import (EntityRef, MetricPoint, PaperSessionObservation, PaperStarterInput,
    PaperURLObservation, Span, Telemetry, Window)
from .dag import verify_dag
from .executor import SerialExecutor
from .m4_controller import validate_bank_answer
from .paper_starter import (PROFILE, bank_profile_playbooks, canonical_paper_library_source,
    make_paper_bank_registry, make_paper_starter_registry)
from .registry import OperatorRegistry
from .m4_fixtures import synthetic_case_runtime


ROOT = Path(__file__).resolve().parents[1]
_START = 1_800_000_000_000
_REFERENCE = Window("reference", _START, _START + 1_800_000)
_ALERT = Window("alert", _START + 1_800_000, _START + 3_600_000)


def _digest(value: Any) -> str:
    return hashlib.sha256(json.dumps(value, sort_keys=True, separators=(",", ":"),
        ensure_ascii=False, allow_nan=False).encode("utf-8")).hexdigest()


def _rich_fixture() -> tuple[Telemetry, PaperStarterInput]:
    metrics: list[MetricPoint] = []
    spans: list[Span] = []
    catalog = {name: {"role": "component"} for name in ("component-a", "component-b", "root-service")}
    for window, base_minute in ((_REFERENCE, 0), (_ALERT, 30)):
        for minute in range(30):
            ts = window.start_ms_utc + minute * 60_000
            x = float((minute + (2 if window.name == "alert" else 0)) % 2)
            y = float((minute * 3 + 1) % 2)
            # A metric excursion fixture for backend books.
            for component, value in (("component-a", 10 + 8 * x if window.name == "alert" else 10 + 8 * x),
                                     ("component-b", 9 + y)):
                value = value + (5.0 if window.name == "alert" and component == "component-a" else 0.0)
                metrics.append(MetricPoint("bank/metric_container.csv", ts,
                    EntityRef("component", component), "OSLinux-CPU_CPU_CPUCpuUtil", "bank.metric_container",
                    "value", value, str(value), f"{window.name}:{minute}:{component}"))
            target = 100.0 + 20.0 * x + (1.0 if window.name == "alert" else 0.0)
            metrics.append(MetricPoint("bank/metric_app.csv", ts, EntityRef("app_kpi", "opaque-kpi"),
                "mrt", "bank.metric_app", "mrt", target, str(target), f"{window.name}:{minute}:mrt"))
            # Root duration depends on two independent child-inclusive durations.
            child_a = 5.0 + (float(4 + minute % 4) if window.name == "alert" else float(minute % 8))
            child_b = 3.0 + float((minute * 2) % 5)
            root_duration = 20.0 + 2.0 * child_a + child_b + (1.0 if window.name == "alert" else 0.0)
            trace_id = f"trace-{window.name}-{minute}"
            root_span = Span(trace_id, "root", None, ts, str(root_duration), None,
                "missing", "", "unprovided", EntityRef("component", "root-service"),
                None, None, False, False)
            child_span_a = Span(trace_id, "child-a", "root", ts + 1, str(child_a), None,
                "missing", "", "unprovided", EntityRef("component", "component-a"),
                None, None, True, False)
            child_span_b = Span(trace_id, "child-b", "root", ts + 2, str(child_b), None,
                "missing", "", "unprovided", EntityRef("component", "component-b"),
                None, None, True, False)
            spans.extend((root_span, child_span_a, child_span_b))

    sessions: list[PaperSessionObservation] = []
    urls: list[PaperURLObservation] = []
    for window in (_REFERENCE, _ALERT):
        for index in range(30):
            session_id = f"session-{index}"
            minute_position = (index * 7) % 30
            west = index < 12
            linux = index % 2 == 1
            device = "tablet" if index % 3 == 0 else "phone"
            activity_count = (4 + index % 3) if device == "tablet" else (1 + index % 3)
            attr = {"region": "west" if west else "east", "os": "linux" if linux else "windows",
                    "device": device}
            primary_duration = (100.0 + 30.0 * west + 20.0 * linux + 15.0 * (device == "tablet") +
                2.0 * activity_count + (200.0 if west and window.name == "alert" else 0.0) +
                (30.0 if activity_count >= 4 and window.name == "alert" else 0.0))
            backup_duration = 90.0 + 25.0 * (device == "tablet") + 10.0 * linux
            kpi = 80.0 + 0.2 * primary_duration + 0.1 * backup_duration
            failed = bool(west and window.name == "alert")
            events = ("login", "transfer", "declined") if failed else ("login", "transfer", "complete")
            events = events + tuple("view" for _ in range(activity_count))
            sessions.append(PaperSessionObservation(session_id,
                window.start_ms_utc + minute_position * 60_000, attr, events, kpi, not failed))
            urls.append(PaperURLObservation(session_id, window.start_ms_utc + index * 60_000,
                "https://api.example/transfer", not failed,
                primary_duration,
                "ms"))
            urls.append(PaperURLObservation(session_id, window.start_ms_utc + index * 60_000,
                "https://api.example/backup", True, backup_duration, "ms"))
    data_hash = _digest({"metrics": [(p.table, p.entity.identifier, p.metric_name,
        p.timestamp_ms_utc, p.value) for p in metrics],
        "spans": [(s.trace_id, s.span_id, s.parent_id, s.elapsed_raw) for s in spans]})
    telemetry = Telemetry("synthetic-paper-fixture", _REFERENCE, _ALERT, tuple(metrics), tuple(spans),
        catalog, data_hash, {}, {}, {}, "paper-fixture/1")
    starter = PaperStarterInput(telemetry, tuple(sessions), tuple(urls), {
        "sessions": "available", "urls": "available", "native_logs": "unsupported",
        "native_log_patterns": "unsupported", "component_metrics": "available",
        "app_mrt": "available", "trace_dependencies": "available",
        "trace_root_duration": "available"}, 60_000)
    return telemetry, starter


def _write_bank_log_fixture(root: Path, *, scenario: str) -> Telemetry:
    """Write hash-pinned native CSVs for positive and volume-only log controls."""
    case_id = f"synthetic-bank-{scenario}"
    case_dir = root / "data/prepared/bank/public/cases" / case_id
    relative = "source_dates/20990101/log/log_service.csv"
    path = case_dir / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    alert = {"incident_id": case_id, "telemetry_dates": ["20990101"],
        "source_date_timestamp_units": {"20990101": {"log/log_service.csv": "seconds"}}}
    alert_path = case_dir / "alert.json"
    alert_path.write_text(json.dumps(alert, sort_keys=True), encoding="utf-8")
    component_ids = ("component-a", "component-b", "root-service")
    rows = []
    log_id = 0
    for window_name, window in (("reference", _REFERENCE), ("alert", _ALERT)):
        for minute in range(30):
            timestamp = (window.start_ms_utc + minute * 60_000) // 1000
            block_minute = minute % 5
            for component in component_ids:
                total = 4 if scenario != "volume_only" else (4 if window_name == "reference" else 8)
                motif_count = 0
                if component == "component-a" and window_name == "alert" and scenario == "positive":
                    motif_count = 3
                elif component == "component-a" and scenario == "volume_only":
                    motif_count = total // 4
                for index in range(total):
                    text = "connection refused 503" if index < motif_count else "request completed 200"
                    rows.append({"log_id": f"{log_id}", "timestamp": str(timestamp),
                        "cmdb_id": component, "log_name": "service_log", "value": text})
                    log_id += 1
    with path.open("w", newline="", encoding="utf-8") as stream:
        writer = csv.DictWriter(stream, fieldnames=("log_id", "timestamp", "cmdb_id", "log_name", "value"))
        writer.writeheader()
        writer.writerows(rows)
    hashes = {"alert.json": hashlib.sha256(alert_path.read_bytes()).hexdigest(),
        relative: hashlib.sha256(path.read_bytes()).hexdigest()}
    data_hash = _digest({"hashes": hashes, "scenario": scenario})
    spans = []
    metrics = []
    catalog = {name: {"role": "component"} for name in component_ids}
    for window in (_REFERENCE, _ALERT):
        for minute in range(30):
            ts = window.start_ms_utc + minute * 60_000
            # Metric feature support for the Bank tree adaptation.
            x = float(minute % 8)
            if window.name == "alert" and scenario != "kpi_global_only":
                x = float(4 + (minute % 4))
            for component, value in (("component-a", x), ("component-b", float((minute * 3) % 8))):
                if scenario == "staggered_sampling" and component == "component-b" and minute % 2:
                    continue
                metrics.append(MetricPoint("bank/metric_container.csv", ts,
                    EntityRef("component", component), "OSLinux-CPU_CPU_CPUCpuUtil", "bank.metric_container",
                    "value", value, str(value), f"{window.name}:{minute}:{component}"))
            target = 100.0 + (10.0 if window.name == "alert" and scenario != "kpi_global_only" else 0.0) + 2.0 * x
            metrics.append(MetricPoint("bank/metric_app.csv", ts, EntityRef("app_kpi", "opaque-kpi"),
                "mrt", "bank.metric_app", "mrt", target, str(target), f"{window.name}:{minute}:mrt"))
            # A unique root with two inclusive child spans; scenario controls collinearity.
            child_a = 5.0 + (float(4 + minute % 4)
                if window.name == "alert" and scenario not in {"collinear_trace", "kpi_global_only"}
                else float(minute % 8))
            child_b = child_a if scenario == "collinear_trace" else 3.0 + float((minute * 3) % 7)
            root_duration = 20.0 + 2.0 * child_a + child_b
            tid = f"{window.name}-{minute}"
            spans.extend((
                Span(tid, "root", None, ts, str(root_duration), None, "missing", "", "unprovided",
                    EntityRef("component", "root-service"), None, None, False, False),
                Span(tid, "child-a", "root", ts + 1, str(child_a), None, "missing", "", "unprovided",
                    EntityRef("component", "component-a"), None, None, True, False),
                Span(tid, "child-b", "root", ts + 2, str(child_b), None, "missing", "", "unprovided",
                    EntityRef("component", "component-b"), None, None, True, False)))
    return Telemetry(case_id, _REFERENCE, _ALERT, tuple(metrics), tuple(spans), catalog,
        data_hash, hashes, {}, {}, "paper-bank-fixture/1", {key: (alert_path if key == "alert.json" else path).stat().st_size
            for key in hashes})


def _copy_profile_config(root: Path) -> None:
    (root / "config").mkdir(parents=True, exist_ok=True)
    for name in ("paper_starter_runtime.json", "paper_starter_catalog.json",
                 "paper_bank_adaptations.json", "bank_metric_kind_policy.json",
                 "bank_dev_kpi_policy.json", "m4_runtime.json"):
        shutil.copyfile(ROOT / "config" / name, root / "config" / name)


def _execute_book(registry: OperatorRegistry, book: Mapping[str, Any], telemetry: Telemetry,
                  starter: PaperStarterInput, runtime, cache_root: Path) -> dict[str, Any]:
    verified = verify_dag(book["document"], registry, runtime=runtime,
        max_nodes=40, max_depth=12)
    source = next(node.node_id for node in verified.nodes if node.operator.external_input)
    result = SerialExecutor(registry, CacheStore(cache_root, enabled=False)).execute_verified(
        verified, runtime, {source: telemetry}, cache_enabled=False)
    return {"status": "ok", "lead_roles": [item.entity.role for item in result.answer.leads],
        "lead_ids": [item.entity.identifier for item in result.answer.leads[:3]],
        "lead_scores": [item.score for item in result.answer.leads[:3]],
        "lead_descriptors": [next((dict(evidence.details.get("candidate_descriptor", {}))
            for evidence in item.evidence if evidence.details.get("candidate_descriptor")), {})
            for item in result.answer.leads[:3]],
        "node_count": len(result.records), "starter_input_checked": starter.telemetry.data_sha256 == telemetry.data_sha256}


def validate_starter_fixture_suite() -> dict[str, Any]:
    """Execute 11 canonical books and Bank adaptation controls in temporary roots."""
    result: dict[str, Any] = {"profile": PROFILE, "passed": False,
        "canonical_book_count": 0, "book_results": {}, "bank_adaptation_results": {}}
    telemetry, starter = _rich_fixture()
    full_registry = make_paper_starter_registry(starter_input=starter)
    source = canonical_paper_library_source(full_registry)
    if source["operator_count"] != 19 or source["playbook_count"] != 11:
        raise AssertionError("canonical source inventory differs from 19/11");
    with tempfile.TemporaryDirectory(prefix="paper-starter-fixtures-") as temp:
        root = Path(temp)
        _copy_profile_config(root)
        runtime = synthetic_case_runtime(telemetry)
        canonical_rows = []
        for book in source["playbooks"]:
            row = _execute_book(full_registry, book, telemetry, starter, runtime, root / "cache" / book["name"])
            result["book_results"][book["name"]] = row
            canonical_rows.append(row)
        result["canonical_book_count"] = len(canonical_rows)
        if len(canonical_rows) != 11:
            raise AssertionError("not all 11 canonical playbooks executed")

        for scenario in ("positive", "volume_only", "collinear_trace", "kpi_global_only", "staggered_sampling"):
            case_root = root / scenario
            _copy_profile_config(case_root)
            bank_telemetry = _write_bank_log_fixture(case_root, scenario=scenario)
            registry, bank_input = make_paper_bank_registry(bank_telemetry, root=case_root)
            bank_books = bank_profile_playbooks(registry, bank_input)
            runtime_for_case = synthetic_case_runtime(bank_telemetry)
            book_rows = {}
            for book in bank_books:
                row = _execute_book(registry, book, bank_telemetry, bank_input,
                    runtime_for_case, root / "bank-cache" / scenario / book["name"])
                book_rows[book["name"]] = row
            result["bank_adaptation_results"][scenario] = {
                "capabilities": dict(bank_input.capabilities), "books": book_rows,
                "diagnostic_count": len(bank_profile_playbooks(registry, bank_input))}
            if bank_input.native_log_summaries:
                result["bank_adaptation_results"][scenario]["source_rows_scanned"] = max(
                    row.source_rows_scanned for row in bank_input.native_log_summaries)
            result["bank_adaptation_results"][scenario]["normalized_log_templates"] = [
                {"tokens": row.tokens, "pattern_sha256": row.pattern_sha256}
                for row in bank_input.native_log_patterns[:3]]
            result["bank_adaptation_results"][scenario]["bank_answer_validation"] = []
            for book in bank_books:
                row = book_rows[book["name"]]
                # Validate every nonempty terminal answer against exact observed Bank IDs
                # and source-tagged eligible evidence.
                from .dag import verify_dag
                verified = verify_dag(book["document"], registry, runtime=runtime_for_case,
                    max_nodes=40, max_depth=12)
                source_id = next(node.node_id for node in verified.nodes if node.operator.external_input)
                execution = SerialExecutor(registry, CacheStore(root / "answer-cache", enabled=False)).execute_verified(
                    verified, runtime_for_case, {source_id: bank_telemetry}, cache_enabled=False)
                case = BankCase(bank_telemetry, {}, {}, bank_telemetry.deployment_catalog, {}, {})
                validate_bank_answer(execution.answer, case)
                result["bank_adaptation_results"][scenario]["bank_answer_validation"].append(book["name"])
        result["fixture_source_rows_beyond_64"] = all(
            row.get("source_rows_scanned", 0) > 64
            for row in result["bank_adaptation_results"].values())
        result["raw_log_text_in_typed_input"] = any(
            hasattr(item, "value") and getattr(item, "value", None) for item in starter.native_log_summaries)
        canonical_positive_names = ("attribute_cohort_shift", "joint_cohort_interaction",
            "activity_cohort_explanation", "backend_mean_shift", "backend_location_shift",
            "backend_tail_degradation", "url_failure_diagnosis", "url_delay_flow_attribution",
            "failed_session_event_patterns", "combined_client_backend",
            "dependency_adjusted_backend_consensus")
        canonical_positive = all(result["book_results"][name]["lead_ids"] for name in canonical_positive_names)
        descriptor_expectations = {
            "attribute_cohort_shift": lambda row: row.get("attributes", {}).get("region") == "west",
            "joint_cohort_interaction": lambda row: row.get("attributes", {}).get("region") == "west",
            "activity_cohort_explanation": lambda row: row.get("attributes", {}).get("activity") in {"high_activity", "low_activity"},
            "url_failure_diagnosis": lambda row: row.get("url") == "https://api.example/transfer",
            "url_delay_flow_attribution": lambda row: row.get("url") == "https://api.example/transfer",
            "failed_session_event_patterns": lambda row: "declined" in row.get("tokens", ()),
        }
        canonical_semantics = all(
            bool(result["book_results"][name]["lead_descriptors"]) and
            descriptor_expectations[name](result["book_results"][name]["lead_descriptors"][0])
            for name in descriptor_expectations)
        positive_bank = result["bank_adaptation_results"]["positive"]["books"]
        positive_expectations = {
            "bank_component_metric_to_app_mrt": "component-a",
            "bank_trace_root_duration_flow": "component-a",
            "bank_log_template_composition": "component-a"}
        bank_positive = all(positive_bank.get(name, {}).get("lead_ids", [None])[0] == expected
            for name, expected in positive_expectations.items())
        negatives = (
            "component-a" not in result["bank_adaptation_results"]["volume_only"]["books"].get(
                "bank_log_template_composition", {}).get("lead_ids", ()),
            "component-a" not in result["bank_adaptation_results"]["collinear_trace"]["books"].get(
                "bank_trace_root_duration_flow", {}).get("lead_ids", ()),
            "component-a" not in result["bank_adaptation_results"]["kpi_global_only"]["books"].get(
                "bank_component_metric_to_app_mrt", {}).get("lead_ids", ()))
        result["canonical_positive_gates_passed"] = canonical_positive
        result["canonical_semantic_gates_passed"] = canonical_semantics
        result["bank_positive_gates_passed"] = bank_positive
        result["bank_negative_gates_passed"] = all(negatives)
        result["staggered_metric_case_book_count"] = result["bank_adaptation_results"]["staggered_sampling"]["diagnostic_count"]
        result["passed"] = (result["canonical_book_count"] == 11 and canonical_positive and canonical_semantics and
            bank_positive and all(negatives) and result["fixture_source_rows_beyond_64"] and
            not result["raw_log_text_in_typed_input"] and
            all(len(v["bank_answer_validation"]) == len(v["books"])
                for v in result["bank_adaptation_results"].values()))
    return result


__all__ = ["validate_starter_fixture_suite"]
