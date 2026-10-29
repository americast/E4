import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

from e4.synthetic.benchmark import (ALLOWED_CATEGORIES, DEFAULT_SUITE, SemanticTaskError, StaticTaskError, _DAG_TASKS, _validate_dag_task,
                                    build_calibration_prompt, composition_telemetry,
                                    parse_strict_json_object, run_suite, runtime_context, run_task,
                                    telemetry_from_spec, task_prompt, validate_operator_source, _validate_operator_task,
                                    _operator_output_matches,
                                    validate_suite)
from e4.models.analysis_model import model_from_document
from e4.models.client import ModelCall, ModelClient
from e4.operators.telecom.operators import telecom_registry


ROOT = Path(__file__).resolve().parents[2]


class FakeResponse:
    def __init__(self, status, value):
        self.status = status
        self.data = json.dumps(value).encode()

    def read(self):
        return self.data

    def __enter__(self):
        return self

    def __exit__(self, *_args):
        return False


def fake_model_call(task_id, attempt, content, *, error=None, finish="stop"):
    return ModelCall(
        call_id=f"{task_id}-{attempt}", task_id=task_id, attempt=attempt,
        started_utc="2026-01-01T00:00:00+00:00", finished_utc="2026-01-01T00:00:01+00:00",
        elapsed_seconds=1.0, generation_elapsed_seconds=0.8, prompt_token_count=25,
        context_tokens=32768, preflight_calls=(), request_payload={"model": "fake"},
        prompt_sha256="a" * 64, http_status=200,
        raw_response_body=json.dumps({"choices": [{"message": {"content": content}}]}),
        raw_response={"choices": [{"message": {"content": content}}]},
        content=content, reasoning="private reasoning", usage={"prompt_tokens": 25,
            "completion_tokens": 5}, response_metadata={"finish_reason": finish}, error=error)


class FakeClient:
    def __init__(self, calls):
        self.responses = list(calls)
        self.prompts = []
        self.base_url = "http://127.0.0.1:8080"
        self.context_tokens = 32768
        self.max_generation_tokens = 8192

    def complete(self, prompt, *, task_id, attempt, max_tokens=None):
        self.prompts.append(prompt)
        return self.responses.pop(0)


class M3BenchmarkTests(unittest.TestCase):
    def profile(self):
        return {"base_url": "http://127.0.0.1:8080", "model_alias": "fixture",
                "context_tokens": 30, "max_generation_tokens": 10,
                "request_timeout_seconds": 2, "temperature": 0.1,
                "top_p": 0.9, "top_k": 16, "seed": 42,
                "reasoning_mode": "off"}

    def test_frozen_suite_shape_and_held_out_operator_cases(self):
        suite = json.loads((ROOT / "config/synthetic/suite_v3.json").read_text())
        tasks = validate_suite(suite)
        counts = {}
        for task in tasks:
            counts[task["kind"]] = counts.get(task["kind"], 0) + 1
        self.assertEqual(counts, {"dag": 8, "operator": 6, "diagnosis": 4, "schema": 2})
        for task in tasks:
            if task["kind"] == "operator":
                self.assertGreaterEqual(len(task["checks"]), 2)
                self.assertTrue(task.get("require_nonconstant"))
        self.assertEqual(len({task["id"] for task in tasks}), 20)

    def test_v3_corrects_only_the_independently_proven_median_oracle(self):
        self.assertEqual(DEFAULT_SUITE, ROOT / "config/synthetic/suite_v3.json")
        old = json.loads((ROOT / "config/synthetic/suite_v2.json").read_text())
        new = json.loads((ROOT / "config/synthetic/suite_v3.json").read_text())
        projected = json.loads(json.dumps(old))
        projected["schema_version"] = "e4-m3-synthetic-suite/3"
        projected["suite_id"] = "m3-synthetic-v3"
        task = next(item for item in projected["tasks"]
                    if item["id"] == "operator_04_median_window_contrast")
        task["checks"][1]["expected"]["delta"] = 2.0
        self.assertEqual(
            json.dumps(projected, sort_keys=True, separators=(",", ":"), allow_nan=False),
            json.dumps(new, sort_keys=True, separators=(",", ":"), allow_nan=False))

    def test_operator_prompts_publish_input_parameter_runtime_and_output_schemas(self):
        suite = json.loads((ROOT / "config/synthetic/suite_v3.json").read_text())
        operators = [task for task in validate_suite(suite) if task["kind"] == "operator"]
        self.assertEqual(suite["suite_id"], "m3-synthetic-v3")
        self.assertEqual(len(operators), 6)
        registry, runtime = telecom_registry(), runtime_context()
        for task in operators:
            contract = task["public_contract"]
            prompt = task_prompt(task, registry, runtime)
            encoded_contract = json.dumps(contract, ensure_ascii=False, sort_keys=True,
                                          separators=(",", ":"))
            self.assertIn(encoded_contract, prompt, task["id"])
            self.assertNotIn('"checks"', prompt)
            self.assertNotIn('"expected"', prompt)
            for case in task["checks"]:
                self.assertNotIn(json.dumps(case, ensure_ascii=False, sort_keys=True,
                                            separators=(",", ":")), prompt)

    def test_diagnosis_prompts_publish_category_enum_without_answer_checks(self):
        suite = json.loads((ROOT / "config/synthetic/suite_v3.json").read_text())
        diagnoses = [task for task in validate_suite(suite) if task["kind"] == "diagnosis"]
        for task in diagnoses:
            prompt = task_prompt(task, telecom_registry(), runtime_context())
            self.assertIn("Diagnosis category output schema", prompt)
            for category in ALLOWED_CATEGORIES:
                self.assertIn(json.dumps(category), prompt)
            self.assertIn("or null", prompt)
            self.assertNotIn('"expected_category"', prompt)
            self.assertNotIn(json.dumps(task["check"], ensure_ascii=False, sort_keys=True,
                                        separators=(",", ":")), prompt)
            self.assertNotIn("confounders", prompt)
            self.assertNotIn("connection limit reached", prompt)
            self.assertNotIn("server closed connection", prompt)
            self.assertNotIn("likely source", prompt)
            self.assertNotIn("trace evidence", prompt)
            self.assertIn("telemetry recipe mapping", prompt)

    def test_operator_schema_allows_integer_float_for_numbers_but_not_integer_fields(self):
        schema = {"scaled_sum": "finite number", "depth": "integer", "seed": "integer"}
        expected = {"scaled_sum": 24, "depth": 12, "seed": 42}
        self.assertTrue(_operator_output_matches(
            {"scaled_sum": 24.0, "depth": 12, "seed": 42}, expected, schema))
        self.assertFalse(_operator_output_matches(
            {"scaled_sum": 24.0, "depth": 12.0, "seed": 42}, expected, schema))
        self.assertFalse(_operator_output_matches(
            {"scaled_sum": 24.0, "depth": 12, "seed": 42.0}, expected, schema))
        self.assertFalse(_operator_output_matches(
            {"scaled_sum": True, "depth": 12, "seed": 42}, expected, schema))
        arrays = {"block_indices": "array of integers, sorted ascending",
                  "medians": "array of finite numbers; same length as block_indices"}
        self.assertTrue(_operator_output_matches(
            {"block_indices": [0, 1], "medians": [2, 5.0]},
            {"block_indices": [0, 1], "medians": [2.0, 5.0]}, arrays))
        self.assertFalse(_operator_output_matches(
            {"block_indices": [0.0, 1], "medians": [2, 5.0]},
            {"block_indices": [0, 1], "medians": [2.0, 5.0]}, arrays))

    def test_json_parser_allows_one_outer_fence_and_rejects_duplicate_keys(self):
        parsed, fenced = parse_strict_json_object("```json\n{\"ok\": true}\n```")
        self.assertEqual(parsed, {"ok": True})
        self.assertTrue(fenced)
        with self.assertRaises(StaticTaskError):
            parse_strict_json_object('{"ok":true,"ok":false}')
        with self.assertRaises(StaticTaskError):
            parse_strict_json_object('before {"ok":true}')

    def test_operator_static_gate_matches_three_argument_sandbox_api(self):
        source, _ = validate_operator_source(
            "def operator(inputs, params, runtime):\n    return {'v': runtime.dag_depth}\n")
        self.assertIn("def operator(inputs, params, runtime)", source)
        for invalid, expected_code in (
            ("def operator(data):\n    return data\n", "python_signature"),
            ("def operator(inputs, params, runtime):\n    return (\n", "python_syntax"),
            ("def operator(inputs, params, runtime):\n    return inputs.__class__\n", "python_ast"),
        ):
            with self.assertRaises(StaticTaskError) as caught:
                validate_operator_source(invalid)
            self.assertEqual(caught.exception.code, expected_code)

    def test_operator_sandbox_records_preserve_stdout_on_success_and_failure(self):
        task = {"output_type": "object", "public_contract": {"output": {"value": "integer"}}, "checks": [
            {"inputs": {}, "params": {}, "runtime": {}, "expected": {"value": 3}}]}
        success = {"ok": True, "result": {"value": 3}, "stdout": "operator ran\n",
                   "stderr": "", "source": "def operator...", "isolation": {"network": "isolated"}}
        with patch("e4.synthetic.benchmark.run_generated_operator", return_value=success):
            checked = _validate_operator_task(task, "source")
        self.assertEqual(checked["independent_checks"][0]["sandbox_execution"], success)
        self.assertEqual(checked["independent_checks"][0]["sandbox_execution"]["stdout"],
                         "operator ran\n")

        failure = {"ok": False, "error": {"code": "runtime_error", "message": "bad"},
                   "stdout": "printed before failure\n", "stderr": "trace\n",
                   "source": "def operator...", "isolation": {"network": "isolated"}}
        with patch("e4.synthetic.benchmark.run_generated_operator", return_value=failure):
            with self.assertRaises(SemanticTaskError) as caught:
                _validate_operator_task(task, "source")
        self.assertEqual(caught.exception.details["failures"][0]["sandbox_execution"], failure)
        self.assertEqual(caught.exception.details["held_out_checks"][0]["sandbox_execution"]["stdout"],
                         "printed before failure\n")

    def test_worker_output_type_failure_is_not_repaired_or_echoed(self):
        suite = json.loads((ROOT / "config/synthetic/suite_v3.json").read_text())
        task = next(item for item in suite["tasks"]
                    if item["id"] == "operator_01_metric_mean_support")
        source = "def operator(inputs, params, runtime):\n    return []\n"
        client = FakeClient([fake_model_call(task["id"], 1, source)])
        worker_record = {"ok": False, "error": {"code": "output_type_mismatch",
                                                 "message": "wrong output type"},
                         "stdout": "heldout-secret", "stderr": "", "source": source,
                         "isolation": {"network": "isolated"}}
        with tempfile.TemporaryDirectory() as tmp, patch(
                "e4.synthetic.benchmark.run_generated_operator", return_value=worker_record):
            outcome = run_task(task, registry=telecom_registry(), runtime=runtime_context(),
                               client=client, run_dir=Path(tmp))
        self.assertFalse(outcome.passed)
        self.assertEqual(outcome.repairs, 0)
        self.assertEqual(len(client.prompts), 1)
        self.assertNotIn("heldout-secret", client.prompts[0])
        self.assertEqual(outcome.error["details"]["failures"][0]["sandbox_execution"],
                         worker_record)

    def test_repair_prompt_omits_untrusted_static_error_details(self):
        from e4.synthetic.benchmark import _repair_prompt
        error = StaticTaskError("python_syntax", "invalid syntax",
                                {"debug": {"heldout": "secret"}})
        prompt = _repair_prompt("task", "bad source", error)
        self.assertIn('"code": "python_syntax"', prompt)
        self.assertNotIn("secret", prompt)

    def test_context_budget_uses_rendered_chat_template_and_actual_tokenizer(self):
        outputs = [
            FakeResponse(200, {"prompt": "rendered chat template"}),
            FakeResponse(200, {"tokens": list(range(20))}),
            FakeResponse(200, {"choices": [{"message": {
                "content": "{\"status\":\"ok\"}", "reasoning_content": "separate"},
                "finish_reason": "stop"}], "usage": {"prompt_tokens": 20,
                "completion_tokens": 3}}),
        ]
        with patch("e4.models.client.urllib.request.urlopen", side_effect=outputs) as request:
            client = ModelClient(model_from_document("fixture", self.profile()))
            call = client.complete("full prompt", task_id="test", attempt=1)
        self.assertIsNone(call.error)
        self.assertEqual(call.prompt_token_count, 20)
        self.assertEqual(call.context_tokens, 30)
        self.assertEqual(call.content, '{"status":"ok"}')
        self.assertEqual(call.reasoning, "separate")
        self.assertEqual(len(call.preflight_calls), 2)
        self.assertEqual(request.call_count, 3)
        self.assertEqual(call.request_payload["max_tokens"], 10)

    def test_context_overflow_fails_before_generation(self):
        outputs = [FakeResponse(200, {"prompt": "rendered"}),
                   FakeResponse(200, {"tokens": list(range(21))})]
        with patch("e4.models.client.urllib.request.urlopen", side_effect=outputs) as request:
            call = ModelClient(model_from_document("fixture", self.profile())).complete("large prompt", task_id="test", attempt=1)
        self.assertEqual(call.error["code"], "context_budget_exceeded")
        self.assertEqual(call.prompt_token_count, 21)
        self.assertEqual(request.call_count, 2)
        self.assertEqual(call.generation_elapsed_seconds, 0.0)

    def test_client_rejects_host_confusion_urls(self):
        for url in ("http://127.0.0.1.evil:8080", "http://127.0.0.1@evil.test:8080",
                    "http://127.0.0.1:8080/path"):
            with self.assertRaises(ValueError):
                model_from_document("fixture", {**self.profile(), "base_url": url})

    def test_static_repair_and_semantic_mismatch_have_distinct_policies(self):
        schema_task = next(task for task in validate_suite(json.loads(
            (ROOT / "config/synthetic/suite_v3.json").read_text()))
                           if task["id"] == "schema_01_strict_json_answer")
        from e4.synthetic.benchmark import run_task
        repaired = FakeClient([
            fake_model_call(schema_task["id"], 1, "not json"),
            fake_model_call(schema_task["id"], 2,
                            '{"status":"ok","count":3,"category":null}')])
        with tempfile.TemporaryDirectory() as tmp:
            outcome = run_task(schema_task, registry=telecom_registry(), runtime=runtime_context(),
                               client=repaired, run_dir=Path(tmp))
        self.assertTrue(outcome.passed)
        self.assertFalse(outcome.initial_passed)
        self.assertEqual(outcome.repairs, 1)

        wrong_value = FakeClient([fake_model_call(schema_task["id"], 1,
            '{"status":"bad","count":3,"category":null}')])
        with tempfile.TemporaryDirectory() as tmp:
            mismatch = run_task(schema_task, registry=telecom_registry(), runtime=runtime_context(),
                                client=wrong_value, run_dir=Path(tmp))
        self.assertFalse(mismatch.passed)
        self.assertEqual(mismatch.repairs, 0)
        self.assertEqual(len(wrong_value.prompts), 1)

    def test_suite_appends_each_raw_call_and_refuses_to_overwrite(self):
        task_id = "schema_01_strict_json_answer"
        content = '{"status":"ok","count":3,"category":null}'
        fake = FakeClient([fake_model_call(task_id, 1, content)])
        with tempfile.TemporaryDirectory() as tmp:
            run_dir = Path(tmp) / "run"
            summary = run_suite(ROOT / "config/synthetic/suite_v3.json", run_dir,
                                model_from_document("fixture", self.profile()), client=fake,
                                task_ids={task_id})
            self.assertEqual(summary["task_count"], 1)
            call_rows = [json.loads(line) for line in
                         (run_dir / "m3_model_calls.jsonl").read_text().splitlines()]
            task_rows = [json.loads(line) for line in
                         (run_dir / "m3_task_results.jsonl").read_text().splitlines()]
            self.assertEqual(len(call_rows), 1)
            self.assertEqual(call_rows[0]["raw_response"]["choices"][0]["message"]["content"], content)
            self.assertEqual(len(task_rows), 1)
            old = (run_dir / "m3_benchmark_summary.json").read_bytes()
            with self.assertRaises(FileExistsError):
                run_suite(ROOT / "config/synthetic/suite_v3.json", run_dir,
                          model_from_document("fixture", self.profile()), client=FakeClient([]),
                          task_ids={task_id})
            self.assertEqual((run_dir / "m3_benchmark_summary.json").read_bytes(), old)

    def test_synthetic_telemetry_recipes_expand_without_external_case_data(self):
        suite = json.loads((ROOT / "config/synthetic/suite_v3.json").read_text())
        diagnoses = [task for task in validate_suite(suite) if task["kind"] == "diagnosis"]
        for task in diagnoses:
            telemetry = telemetry_from_spec(task["telemetry"])
            self.assertEqual(telemetry.case_id, task["telemetry"]["case_id"])
            self.assertEqual(telemetry.reference_window.end_ms_utc - telemetry.reference_window.start_ms_utc,
                             1_800_000)
            self.assertEqual(telemetry.alert_window.end_ms_utc - telemetry.alert_window.start_ms_utc,
                             1_800_000)
            self.assertTrue(all(telemetry.reference_window.contains(point.timestamp_ms_utc)
                                or telemetry.alert_window.contains(point.timestamp_ms_utc)
                                for point in telemetry.metrics))
            self.assertEqual(len(telemetry.data_sha256), 64)

    def test_calibration_prompt_uses_full_registry_runtime_and_seed_library(self):
        prompt = build_calibration_prompt(telecom_registry(), runtime_context())
        self.assertIn("Registered operator signatures", prompt)
        self.assertIn("Published runtime schema", prompt)
        self.assertIn("Reviewed generic seed DAG examples", prompt)
        self.assertIn("dependencies.propagation", prompt)

    def test_eight_composition_templates_verify_and_execute(self):
        # These are deterministic checker fixtures, not model responses.
        registry = telecom_registry()
        runtime = runtime_context()
        with tempfile.TemporaryDirectory() as tmp:
            for task_id in _DAG_TASKS:
                doc = self._dag_fixture(task_id)
                checked = _validate_dag_task(task_id, doc, registry, runtime, Path(tmp))
                self.assertTrue(checked["valid"], task_id)
                self.assertIn("executed_answer_top3", checked)

    def test_run_task_executes_a_valid_model_dag_end_to_end(self):
        from e4.synthetic.benchmark import run_task
        task_id = "dag_08_runtime_bound_three_family"
        doc = self._dag_fixture(task_id)
        task = {"id": task_id, "kind": "dag", "prompt": "Return the requested DAG."}
        client = FakeClient([fake_model_call(task_id, 1, json.dumps(doc))])
        with tempfile.TemporaryDirectory() as tmp:
            outcome = run_task(task, registry=telecom_registry(), runtime=runtime_context(),
                               client=client, run_dir=Path(tmp))
        self.assertTrue(outcome.passed, outcome.error)
        self.assertTrue(outcome.initial_passed)
        self.assertEqual(outcome.repairs, 0)
        self.assertEqual(outcome.result["rank_runtime_binding"], "runtime.lead_count")
        self.assertEqual(outcome.result["rank_explicit_override"], 3)

    @staticmethod
    def _dag_fixture(task_id):
        requirements = _DAG_TASKS[task_id]
        nodes = []
        def add(node_id, op, inputs=None, params=None, bindings=None):
            nodes.append({"id": node_id, "operator": op, "version": "1",
                          "inputs": inputs or {}, "params": params or {},
                          "bindings": bindings or {}})
        add("data", "case.telemetry")
        aggregates = {}
        for index, item in enumerate(requirements["aggregates"]):
            operator, window = item.split(":")
            node_id = f"aggregate_{index}"
            add(node_id, operator, {"data": "data"}, {"window": window})
            aggregates[item] = node_id
        aggregate_refs = [node_id for _, node_id in sorted(aggregates.items())]
        add("candidates", "candidates.generate",
            {"data": "data", "aggregates": aggregate_refs})
        score_ids = []
        for index, scorer in enumerate(requirements["scorers"]):
            node_id = f"score_{index}"
            if scorer == "metrics.contrast":
                inputs = {"reference": aggregates["metrics.aggregate:reference"],
                          "alert": aggregates["metrics.aggregate:alert"],
                          "candidates": "candidates"}
            elif scorer in {"traces.error_contrast", "traces.latency_contrast"}:
                inputs = {"reference": aggregates["traces.aggregate:reference"],
                          "alert": aggregates["traces.aggregate:alert"],
                          "candidates": "candidates"}
            else:
                inputs = {"data": "data", "candidates": "candidates"}
            add(node_id, scorer, inputs)
            score_ids.append(node_id)
        binding = requirements.get("rank_binding")
        params = {"limit": requirements["rank_override"]} if binding else {}
        bindings = {"limit": binding} if binding else {}
        add("rank", "answers.combine", {"scores": score_ids}, params, bindings)
        return {"schema_version": "e4-dag/1", "nodes": nodes, "return": {"node": "rank"}}


if __name__ == "__main__":
    unittest.main()
