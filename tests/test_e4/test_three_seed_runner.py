from __future__ import annotations

import contextlib
import io
import json
import tempfile
import unittest
import urllib.error
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from benchmarks import bank_benchmark
from scripts import run_bank_three_seed as runner


class ThreeSeedRunnerTest(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.addCleanup(self.tmp.cleanup)
        self.root = Path(self.tmp.name)
        self.ids = [f"bank-case-{i}" for i in range(10)]
        sample = {"selection": {"scored_ids": self.ids},
                  "scored_cases": [{"incident_id": cid} for cid in self.ids]}
        self.sample_path = self.root / "bank_sample.json"
        self.sample_path.write_text(json.dumps(sample), encoding="utf-8")
        self.cfg_path = self.root / "gemma.json"
        self.cfg_path.write_text(json.dumps({"model_alias": "pinned", "temperature": 1.0,
            "top_p": .95, "top_k": 64, "max_generation_tokens": 8192,
            "context_tokens": 32768, "request_timeout_seconds": 600,
            "reasoning_mode": "on", "reasoning_budget": 2048}), encoding="utf-8")
        self.gt = self.root / "truth.jsonl"
        self.gt.write_text("".join(json.dumps({"incident_id": cid,
            "ground_truth": [{"component": "component"}]}) + "\n" for cid in self.ids), encoding="utf-8")
        self.cases = self.root / "cases"
        for cid in self.ids:
            (self.cases / cid).mkdir(parents=True)
            (self.cases / cid / "alert.json").write_text(json.dumps({
                "analysis_window_start_epoch_ms_utc": 0,
                "alert_window_start_epoch_ms_utc": 1000,
                "alert_window_end_epoch_ms_utc": 2000,
            }), encoding="utf-8")

    def _patch_inputs(self):
        stack = contextlib.ExitStack()
        stack.enter_context(patch.object(runner, "SAMPLE_PATH", self.sample_path))
        stack.enter_context(patch.object(runner, "CONFIG_PATH", self.cfg_path))
        stack.enter_context(patch.object(runner, "GT_PATH", self.gt))
        stack.enter_context(patch.object(runner, "CASES_DIR", self.cases))
        stack.enter_context(patch.object(runner, "load_pinned_inference_config",
                                         lambda: json.loads(self.cfg_path.read_text())))
        self.addCleanup(stack.close)

    def test_cli_main_runs_fixed_shard_and_checkpoints(self):
        self._patch_inputs()
        created = []

        class Client:
            def __init__(self, **kwargs):
                self.kwargs = kwargs
                self.usage = {"total_requests": 0, "prompt_tokens": 0,
                              "completion_tokens": 0, "total_tokens": 0}
                created.append(self)

        def fake_benchmark(cases_dir, ground_truth_path, client, *, case_ids,
                           max_attempts, profile, on_result, deadline_epoch=None,
                           operator_timeout_seconds=None, operator_memory_limit_mb=None):
            self.assertEqual(case_ids, self.ids[5:])
            self.assertEqual(client.kwargs["seed"], 43)
            self.assertEqual(max_attempts, 3)
            self.assertEqual(operator_timeout_seconds, 120)
            self.assertEqual(operator_memory_limit_mb, 512)
            rows = []
            for cid in case_ids:
                row = {"case_id": cid, "status": "unverified", "confirmed": False}
                rows.append(row)
                on_result(row)
            return {"results": rows, "accuracy": 0.0, "hits": 0, "total_cases": 5}

        out = self.root / "run"
        with patch.object(runner, "LocalLlamaClient", Client), \
             patch.object(runner, "run_benchmark", fake_benchmark), \
             patch("sys.argv", ["runner", "--seed", "43", "--shard", "1",
                                "--run-root", str(out), "--model-url", "http://127.0.0.1:19406"]), \
             contextlib.redirect_stdout(io.StringIO()) as stdout:
            runner.main()
        self.assertEqual(created[0].kwargs["temperature"], 1.0)
        self.assertEqual(created[0].kwargs["top_p"], .95)
        self.assertEqual(created[0].kwargs["top_k"], 64)
        self.assertEqual(created[0].kwargs["reasoning_budget"], 2048)
        self.assertEqual(created[0].kwargs["max_tokens"], 8192)
        self.assertEqual(created[0].kwargs["context_tokens"], 32768)
        self.assertEqual(created[0].kwargs["response_format"], {"type": "json_object"})
        self.assertEqual(created[0].kwargs["call_journal_path"], out / "model_calls.jsonl")
        self.assertIn('"status": "complete"', stdout.getvalue())
        checkpoint = json.loads((out / "checkpoint.json").read_text())
        self.assertEqual(checkpoint["status"], "complete")
        self.assertEqual([row["case_id"] for row in checkpoint["results"]], self.ids[5:])

    def test_benchmark_preserves_failure_then_stops_after_backend_outage(self):
        calls = []

        class Model:
            def __init__(self):
                self.calls = []
                self.usage = {"total_requests": 0, "prompt_tokens": 0,
                              "completion_tokens": 0, "total_tokens": 0}

            def complete(self, prompt):
                calls.append(prompt)
                self.calls.append({"status": "request_error"})
                raise urllib.error.URLError("backend offline")

        class FakeEngine:
            def __init__(self, **kwargs):
                self.model = kwargs["model"]

            def diagnose(self, adapter, case_id):
                self.model.complete("synthetic prompt")

        checkpoints = []
        with patch.object(bank_benchmark, "DiagnosisEngine", FakeEngine):
            result = bank_benchmark.run_benchmark(self.cases, self.gt, Model(),
                case_ids=self.ids[:3], on_result=checkpoints.append)
        self.assertEqual(len(calls), 1)
        self.assertEqual(result["results"][0]["error_category"], "backend_outage")
        self.assertEqual([row["status"] for row in result["results"]],
                         ["error", "not_started", "not_started"])
        self.assertEqual(len(checkpoints), 3)
        self.assertEqual(result["hits"], 0)
        self.assertEqual(result["accuracy"], 0.0)

    def test_caught_stage_outage_stops_but_unverified_semantic_result_continues(self):
        class Model:
            def __init__(self, fails=False):
                self.fails = fails
                self.calls = []
                self.usage = {"total_requests": 0, "prompt_tokens": 0,
                    "completion_tokens": 0, "total_tokens": 0, "usage_complete": True}

            def complete(self, prompt):
                self.usage["total_requests"] += 1
                if self.fails:
                    self.calls.append({"status": "request_error", "error_type": "URLError",
                                       "error": "offline", "usage_complete": False})
                    self.usage["usage_complete"] = False
                    raise RuntimeError("backend exception was caught by the stage controller")
                self.calls.append({"status": "ok", "response": "{}", "usage_complete": True})
                return "{}"

        class CaughtEngine:
            def __init__(self, **kwargs):
                self.model = kwargs["model"]

            def diagnose(self, adapter, case_id):
                try:
                    self.model.complete("select or compose prompt")
                except RuntimeError:
                    pass  # Matches controller behavior that records and returns unverified.
                return SimpleNamespace(status="unverified", stage="compose", top_candidates=(),
                    total_calls=1, history=({"stage": "compose", "status": "failed_parse"},))

        with patch.object(bank_benchmark, "DiagnosisEngine", CaughtEngine):
            outage = bank_benchmark.run_benchmark(self.cases, self.gt, Model(fails=True),
                case_ids=self.ids[:2])
            semantic = bank_benchmark.run_benchmark(self.cases, self.gt, Model(fails=False),
                case_ids=self.ids[:2])
        self.assertEqual(outage["results"][0]["status"], "error")
        self.assertEqual(outage["results"][0]["error_category"], "backend_outage")
        self.assertEqual(outage["results"][0]["partial_diagnosis"]["status"], "unverified")
        self.assertFalse(outage["results"][0]["usage_delta"]["usage_complete"])
        self.assertEqual(outage["results"][1]["status"], "not_started")
        self.assertEqual([row["status"] for row in semantic["results"]],
                         ["unverified", "unverified"])
        self.assertEqual(semantic["hits"], 0)
        self.assertEqual(semantic["stage_prefix_counts"],
                         {"select": 0, "compose": 0, "serve": 0})

        class RecoveringModel(Model):
            def complete(self, prompt):
                self.usage["total_requests"] += 1
                if self.usage["total_requests"] == 1:
                    self.calls.append({"status": "request_error", "error_type": "URLError",
                                       "error": "transient", "usage_complete": False})
                    self.usage["usage_complete"] = False
                    raise RuntimeError("transient request error")
                self.calls.append({"status": "ok", "response": "{}", "usage_complete": True})
                return "{}"

        class RecoveryEngine(CaughtEngine):
            def diagnose(self, adapter, case_id):
                for _ in range(2):
                    try:
                        self.model.complete("stage prompt")
                    except RuntimeError:
                        continue
                return SimpleNamespace(status="unverified", stage="compose", top_candidates=(),
                    total_calls=2, history=())

        with patch.object(bank_benchmark, "DiagnosisEngine", RecoveryEngine):
            recovered = bank_benchmark.run_benchmark(self.cases, self.gt, RecoveringModel(),
                case_ids=self.ids[:2])
        self.assertEqual([row["status"] for row in recovered["results"]],
                         ["unverified", "unverified"])
        self.assertTrue(recovered["results"][0]["usage_delta"]["usage_complete"] is False)

    def test_benchmark_prefix_counts_only_confirmed_stages(self):
        specs = [("confirmed", "select"), ("confirmed", "compose"),
                 ("confirmed", "serve"), ("unverified", "serve"),
                 ("unverified", "evolve")]

        class FakeEngine:
            def __init__(self, **kwargs):
                pass

            def diagnose(self, adapter, case_id):
                status, stage = specs[int(case_id.rsplit("-", 1)[1])]
                return SimpleNamespace(status=status, stage=stage, top_candidates=(),
                    total_calls=0, history=())

        class NoCallModel:
            calls = []
            usage = {}

        with patch.object(bank_benchmark, "DiagnosisEngine", FakeEngine):
            result = bank_benchmark.run_benchmark(self.cases, self.gt, NoCallModel(),
                case_ids=self.ids[:5])
        self.assertEqual(result["stage_breakdown"], {"select": 1, "compose": 1, "serve": 1})
        self.assertEqual(result["stage_prefix_counts"], {"select": 1, "compose": 2, "serve": 3})
        self.assertEqual(result["terminal_stage_breakdown"]["serve"], 2)
        self.assertEqual(result["terminal_stage_breakdown"]["evolve"], 1)

    def test_benchmark_applies_explicit_operator_resource_limits_only_when_requested(self):
        created = []
        engine_runners = []

        class FakeRunner:
            def __init__(self, *, timeout_seconds, memory_limit_mb):
                self.timeout_seconds = timeout_seconds
                self.memory_limit_mb = memory_limit_mb
                created.append(self)

        class FakeEngine:
            def __init__(self, **kwargs):
                self.runner = kwargs["runner"]
                engine_runners.append(self.runner)

            def diagnose(self, adapter, case_id):
                return SimpleNamespace(status="unverified", stage="select", top_candidates=(),
                    total_calls=0, history=())

        with patch.object(bank_benchmark, "SubprocessRunner", FakeRunner), \
             patch.object(bank_benchmark, "DiagnosisEngine", FakeEngine):
            bank_benchmark.run_benchmark(self.cases, self.gt, object(), case_ids=self.ids[:1],
                operator_timeout_seconds=120, operator_memory_limit_mb=512)
            bank_benchmark.run_benchmark(self.cases, self.gt, object(), case_ids=self.ids[:1])
        self.assertEqual((created[0].timeout_seconds, created[0].memory_limit_mb), (120, 512))
        self.assertIsNone(engine_runners[1])

    def test_existing_run_root_is_never_overwritten(self):
        self._patch_inputs()
        out = self.root / "exists"
        out.mkdir()
        marker = out / "checkpoint.json"
        marker.write_text('{"first_case":"preserve me"}', encoding="utf-8")
        with self.assertRaises(FileExistsError):
            runner.run_chain(seed=42, shard=0, run_root=out,
                             model_url="http://127.0.0.1:19250")
        self.assertEqual(json.loads(marker.read_text())["first_case"], "preserve me")

    def test_aggregator_keeps_all_seed_slots_and_separates_endpoints(self):
        self._patch_inputs()
        roots = []
        for seed in runner.SEEDS:
            for shard in runner.SHARDS:
                root = self.root / f"s{seed}-h{shard}"
                root.mkdir()
                roots.append(root)
                ids = self.ids[shard * 5:(shard + 1) * 5]
                if seed == 42 and shard == 0:
                    specs = [("confirmed", "select"), ("confirmed", "compose"),
                             ("confirmed", "serve"), ("unverified", "serve"),
                             ("unverified", "evolve")]
                    rows = [{"case_id": cid, "status": status, "stage": stage}
                            for cid, (status, stage) in zip(ids, specs)]
                else:
                    rows = [{"case_id": cid,
                        "status": "confirmed" if cid == ids[0] and shard == 0 else "unverified",
                        "stage": "select" if cid == ids[0] and shard == 0 else "select"}
                        for cid in ids]
                endpoint = "http://127.0.0.1:19250" if shard == 0 else "http://127.0.0.1:19406"
                (root / "result.json").write_text(json.dumps({
                    "profile": {"seed": seed, "shard": shard, "model_endpoint": endpoint},
                    "usage": {"prompt_tokens": 10, "completion_tokens": 5,
                              "total_tokens": 15,
                              "usage_complete": not (seed == 42 and shard == 1)},
                    "results": rows}), encoding="utf-8")
        report = runner.aggregate_run_roots(roots)
        self.assertEqual(report["status"], "complete")
        seed42 = report["per_seed"]["42"]
        self.assertEqual(seed42["hits"], 3)
        self.assertEqual(report["per_seed"]["43"]["planned_cases"], 10)
        self.assertEqual(len(report["per_endpoint"]), 2)
        self.assertEqual(seed42["stage_prefix_successes"],
                         {"select": 1, "compose": 2, "serve": 3})
        self.assertEqual(seed42["terminal_stage_counts"]["serve"], 2)
        self.assertEqual(seed42["terminal_stage_counts"]["evolve"], 1)
        self.assertEqual(seed42["usage_sum"]["total_tokens"], 30)
        self.assertEqual(seed42["mean_reported_tokens_per_planned_case"], 3)
        self.assertFalse(seed42["usage_complete"])

    def test_campaign_cli_runs_fixed_assignments_and_preserves_crashed_chain(self):
        self._patch_inputs()
        calls = []
        campaign_root = self.root / "campaign"

        def fake_chain(*, seed, shard, run_root, model_url, deadline_utc=None, **_kwargs):
            calls.append((model_url, seed, shard))
            run_root.mkdir(parents=True, exist_ok=False)
            if (model_url, seed, shard) == ("http://local", 42, 0):
                marker = run_root / "checkpoint.json"
                marker.write_text('{"first_case":"preserved"}', encoding="utf-8")
                raise RuntimeError("simulated chain crash")
            rows = [{"case_id": cid, "status": "unverified", "stage": "select"}
                    for cid in runner._case_ids(shard)]
            result = {"status": "complete", "profile": {"seed": seed, "shard": shard,
                "model_endpoint": model_url}, "results": rows,
                "usage": {"prompt_tokens": 0, "completion_tokens": 0,
                    "total_tokens": 0, "usage_complete": True}}
            (run_root / "result.json").write_text(json.dumps(result), encoding="utf-8")
            return result

        with patch.object(runner, "run_chain", side_effect=fake_chain), \
             patch("sys.argv", ["runner", "--campaign-root", str(campaign_root),
                                "--local-url", "http://local", "--wingtip-url", "http://wingtip"]), \
             contextlib.redirect_stdout(io.StringIO()) as stdout:
            runner.main()
        expected = {
            ("http://local", 42, 0), ("http://local", 43, 1), ("http://local", 44, 0),
            ("http://wingtip", 42, 1), ("http://wingtip", 43, 0), ("http://wingtip", 44, 1),
        }
        self.assertEqual(set(calls), expected)
        self.assertEqual(len(calls), 6)
        self.assertIn('"campaign_status": "incomplete"', stdout.getvalue())
        manifest = json.loads((campaign_root / "campaign_manifest.json").read_text())
        failed_root = Path(manifest["chains"]["local-seed42-shard0"]["run_root"])
        self.assertEqual(json.loads((failed_root / "checkpoint.json").read_text())[
                         "first_case"], "preserved")
        self.assertFalse((failed_root / "result.json").exists())
        combined = json.loads((campaign_root / "combined_result.json").read_text())
        self.assertEqual(combined["per_seed"]["42"]["reported_slots"], 5)
        self.assertEqual(combined["per_seed"]["42"]["accuracy_over_planned"], 0.0)
        self.assertEqual(len(combined["campaign_errors"]), 1)

    def test_client_serializes_json_envelope_and_verified_tokenizer_flags(self):
        journal = self.root / "model_calls.jsonl"
        client = bank_benchmark.LocalLlamaClient(seed=44, reasoning_budget=2048,
                                                 call_journal_path=journal)
        default_client = bank_benchmark.LocalLlamaClient()
        self.assertEqual(default_client.reasoning_budget, 2048)
        self.assertEqual(default_client.response_format, {"type": "json_object"})
        preflight_payloads = []

        def fake_post(path, payload):
            preflight_payloads.append((path, payload))
            if path == "/apply-template":
                return {"prompt": "<rendered prompt>"}
            return {"tokens": [1, 2, 3]}

        class Response:
            def __enter__(self):
                return self

            def __exit__(self, *_args):
                return False

            def read(self):
                return json.dumps({"choices": [{"message": {"content": "{}"},
                    "finish_reason": "stop"}], "usage": {"prompt_tokens": 3,
                    "completion_tokens": 1}}).encode()

        captured = {}

        def fake_urlopen(request, timeout):
            captured["request"] = request
            captured["timeout"] = timeout
            return Response()

        with patch.object(client, "_post", side_effect=fake_post), \
             patch("urllib.request.urlopen", side_effect=fake_urlopen):
            self.assertEqual(client.complete("return JSON"), "{}")
        body = json.loads(captured["request"].data.decode())
        self.assertEqual(body["response_format"], {"type": "json_object"})
        self.assertEqual(body["seed"], 44)
        self.assertEqual(body["temperature"], 1.0)
        self.assertEqual(preflight_payloads[1][1]["add_special"], True)
        self.assertEqual(preflight_payloads[1][1]["parse_special"], True)
        self.assertTrue(client.usage["protocol_constrained_json"])
        self.assertEqual(client.calls[0]["usage"]["completion_tokens"], 1)
        durable_call = json.loads(journal.read_text(encoding="utf-8").splitlines()[0])
        self.assertEqual(durable_call["status"], "ok")
        self.assertNotIn("reasoning", durable_call)

    def test_preflight_errors_are_journaled_immediately(self):
        journal = self.root / "model_calls.jsonl"
        client = bank_benchmark.LocalLlamaClient(call_journal_path=journal)
        with patch.object(client, "_post", side_effect=ValueError("render error")):
            with self.assertRaisesRegex(RuntimeError, "token preflight failed"):
                client.complete("request that must be durable")
        durable = json.loads(journal.read_text(encoding="utf-8"))
        self.assertEqual(durable["status"], "preflight_error")
        self.assertEqual(client.usage["total_requests"], 0)

    def test_http_client_error_is_not_misclassified_as_endpoint_outage(self):
        client = bank_benchmark.LocalLlamaClient()
        client.calls.append({"status": "request_error", "error_type": "HTTPError",
                             "http_status": 400, "error": "bad request"})
        http_error = urllib.error.HTTPError("http://test", 400, "bad request", {}, None)
        wrapped = RuntimeError("request failed")
        wrapped.__cause__ = http_error
        self.assertEqual(bank_benchmark.classify_case_error(wrapped, client, 0),
                         "model_request_error")


if __name__ == "__main__":
    unittest.main()
