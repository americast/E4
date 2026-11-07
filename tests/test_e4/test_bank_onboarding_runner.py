"""Mocked resume, endpoint-cost, and library-lineage tests for the Bank runner."""

from __future__ import annotations

import json
import hashlib
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from e4.controller.library import LibraryManager
from e4.registry import OperatorRegistry, OperatorSpec
from e4.types import ColumnSchema, DatasetSchema, TableSchema
from scripts import run_bank_onboarding as runner


def _schema() -> DatasetSchema:
    return DatasetSchema((
        TableSchema("metrics.csv", "metric", (ColumnSchema("timestamp", "timestamp"), ColumnSchema("value", "number"))),
    ))


def _library(mode: str, names: tuple[str, ...] = ()) -> LibraryManager:
    registry = OperatorRegistry()
    registry.register(OperatorSpec(
        name=f"baseline.{mode}", version="1", inputs=(), output_type="Aggregate",
    ))
    for name in names:
        registry.register(OperatorSpec(name=name, version="1", inputs=(), output_type="Aggregate"))
    return LibraryManager(registry=registry, mode=mode, origin=f"fixture-{mode}")


def _read_library_names(directory: Path) -> set[str]:
    payload = json.loads((directory / "operators.json").read_text(encoding="utf-8"))
    return {item["name"] for item in payload}


class _MemoryBaseClient:
    def __init__(self, response: str = "fresh-response"):
        self.seed = 42
        self.response = response
        self.calls = []

    def complete(self, prompt: str) -> str:
        self.calls.append({
            "prompt": prompt,
            "response": self.response,
            "status": "ok",
            "usage": {"prompt_tokens": 5, "completion_tokens": 3},
            "usage_complete": True,
            "request_context": dict(getattr(self, "request_context", {})),
        })
        return self.response


class BankOnboardingRunnerTests(unittest.TestCase):
    def test_journal_clients_replay_exact_prompts_per_endpoint_and_account_usage(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            paper_path = root / "paper.jsonl"
            clean_path = root / "clean.jsonl"
            paper_path.write_text(json.dumps({
                "prompt": "paper prompt", "response": "paper response", "status": "ok",
                "usage": {"prompt_tokens": 10, "completion_tokens": 4}, "usage_complete": True,
            }) + "\n", encoding="utf-8")
            clean_path.write_text(json.dumps({
                "prompt": "clean prompt", "response": "clean response", "status": "ok",
                "usage": {"prompt_tokens": 2, "completion_tokens": 3}, "usage_complete": True,
            }) + "\n", encoding="utf-8")
            paper_base = _MemoryBaseClient()
            clean_base = _MemoryBaseClient()
            paper = runner._JournalClient(paper_base, "paper", paper_path)
            clean = runner._JournalClient(clean_base, "clean_slate", clean_path)

            self.assertEqual(paper.complete("paper prompt"), "paper response")
            self.assertEqual(clean.complete("clean prompt"), "clean response")
            self.assertEqual(paper_base.calls, [])
            self.assertEqual(clean_base.calls, [])
            self.assertEqual(paper.usage["total_tokens"], 14)
            self.assertEqual(clean.usage["total_tokens"], 5)
            self.assertEqual(paper.calls[0]["journal_index"], 0)
            self.assertEqual(clean.calls[0]["journal_index"], 0)

            self.assertEqual(paper.complete("new paper prompt"), "fresh-response")
            self.assertEqual(len(paper_base.calls), 1)
            self.assertEqual(len(clean_base.calls), 0)
            self.assertEqual(paper.usage["total_tokens"], 22)
            self.assertEqual(clean.usage["total_tokens"], 5)

    def test_fresh_repeated_prompt_is_consumed_and_causes_two_physical_calls(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            journal = Path(directory) / "paper.jsonl"
            base = _MemoryBaseClient("fresh")
            client = runner._JournalClient(base, "paper", journal)
            context = {"phase": "inference", "mode": "paper", "seed": 42, "shard": 0, "case_id": "case-a"}
            client.set_context(context)

            self.assertEqual(client.complete("same exact prompt"), "fresh")
            self.assertEqual(client.used, {0})
            self.assertFalse(client.calls[-1].get("replayed", False))
            self.assertEqual(client.complete("same exact prompt"), "fresh")
            self.assertEqual(len(base.calls), 2)
            self.assertEqual(client.used, {0, 1})
            self.assertEqual([item["request_context"] for item in base.calls], [context, context])

    def test_pending_response_replay_requires_exact_seed_shard_case_and_is_once(self) -> None:
        owner = {"phase": "inference", "mode": "paper", "seed": 42, "shard": 0, "case_id": "case-a"}
        with tempfile.TemporaryDirectory() as directory:
            journal = Path(directory) / "paper.jsonl"
            journal.write_text(json.dumps({
                "prompt": "same exact prompt",
                "response": "seed42 pending response",
                "status": "ok",
                "usage": {"prompt_tokens": 2, "completion_tokens": 3},
                "usage_complete": True,
                "request_context": owner,
            }) + "\n", encoding="utf-8")

            base = _MemoryBaseClient("fresh other-context response")
            client = runner._JournalClient(base, "paper", journal)
            for mismatch in (
                {**owner, "seed": 43},
                {**owner, "shard": 1},
                {**owner, "case_id": "case-b"},
            ):
                client.set_context(mismatch)
                self.assertEqual(client.complete("same exact prompt"), "fresh other-context response")
                self.assertFalse(client.calls[-1].get("replayed", False))
            self.assertEqual(len(base.calls), 3)
            self.assertEqual({1, 2, 3}, client.used)

            restarted_base = _MemoryBaseClient("fresh after replay consumed")
            restarted = runner._JournalClient(restarted_base, "paper", journal)
            restarted.set_context(owner)
            self.assertEqual(restarted.complete("same exact prompt"), "seed42 pending response")
            self.assertTrue(restarted.calls[-1]["replayed"])
            self.assertEqual(restarted.used, {0})
            self.assertEqual(restarted.complete("same exact prompt"), "fresh after replay consumed")
            self.assertEqual(len(restarted_base.calls), 1)
            self.assertEqual(restarted.used, {0, 1})

    def test_replay_does_not_replace_first_request_owner_and_raw_client_journals_context(self) -> None:
        first_owner = {"phase": "inference", "mode": "paper", "seed": 42, "shard": 0, "case_id": "case-a"}
        replay_owner = {"phase": "inference", "mode": "paper", "seed": 42, "shard": 0, "case_id": "case-a", "replayed_by": "later resume"}
        state = {"accounted_calls": {"paper": [0]}, "call_owners": {"paper": {"0": first_owner}}}
        runner._mark_calls(state, "paper", [{"journal_index": 0, "replayed": True}], replay_owner)
        self.assertEqual(state["call_owners"]["paper"]["0"], first_owner)

        with tempfile.TemporaryDirectory() as directory:
            journal = Path(directory) / "raw.jsonl"
            raw = runner.LocalLlamaClient(base_url="http://mock.invalid", preflight=False, call_journal_path=journal)
            raw.request_context = first_owner
            raw._record_call({"status": "ok", "prompt": "p", "response": "r"})
            record = json.loads(journal.read_text(encoding="utf-8").strip())
            self.assertEqual(record["request_context"], first_owner)
            self.assertEqual(raw.calls[0]["request_context"], first_owner)

    def test_terminal_state_status_and_digest_match_final_result(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            run_root = Path(directory)
            runner._atomic_json(run_root / "state.json", {"status": "running"})
            runner._write_final(run_root, {"status": "complete", "modes": {}, "token_costs": {}})
            state = json.loads((run_root / "state.json").read_text(encoding="utf-8"))
            result_bytes = (run_root / "result.json").read_bytes()
            result = json.loads(result_bytes)
            manifest = json.loads((run_root / "manifest.json").read_text(encoding="utf-8"))

        self.assertEqual(state["status"], result["status"])
        self.assertEqual(manifest["status"], result["status"])
        self.assertEqual(state["result_sha256"], hashlib.sha256(result_bytes).hexdigest())
        self.assertEqual(manifest["result_sha256"], hashlib.sha256(result_bytes).hexdigest())

    def test_parallel_mode_failure_does_not_block_other_mode_or_claim_accuracy(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            run_root = Path(directory) / "run"
            schema = _schema()
            dev_ids = ["dev-a", "dev-b"]
            scored_ids = [f"case-{index}" for index in range(10)]
            gate = {"status": "conforming", "adapter_source_sha256": "source-hash", "schema": schema.as_dict()}
            ready_response = json.dumps({
                "status": "ready",
                "source": "trusted source fixture",
                "config_schema": {
                    "$schema": "https://json-schema.org/draft/2020-12/schema",
                    "type": "object",
                    "properties": {"case_dir": {"type": "string"}},
                    "required": ["case_dir"],
                    "additionalProperties": False,
                },
            })
            barrier = threading.Barrier(2, timeout=3)
            mode_started: set[str] = set()
            endpoint_clients = []

            class DummyClient:
                def __init__(self, endpoint):
                    self.endpoint = endpoint
                    self.seed = 42

            def fake_prepare_mode(mode, **kwargs):
                barrier.wait()
                mode_started.add(mode)
                if mode == "paper":
                    raise RuntimeError("paper generation failed")
                kwargs["state"].setdefault("modes", {})[mode] = {"status": "frozen"}
                return kwargs["state"]["modes"][mode]

            def fake_client(endpoint, base_url, *args, **kwargs):
                journal = args[0] / "model_calls" / f"{endpoint}.jsonl"
                journal.parent.mkdir(parents=True, exist_ok=True)
                token_usage = {"paper": (100, 20), "clean_slate": (7, 11)}[endpoint]
                journal.write_text(json.dumps({
                    "status": "ok",
                    "usage": {"prompt_tokens": token_usage[0], "completion_tokens": token_usage[1]},
                    "usage_complete": True,
                }) + "\n", encoding="utf-8")
                endpoint_clients.append((endpoint, base_url, journal))
                return DummyClient(endpoint)

            def fake_attempt_stage(**kwargs):
                self.assertEqual(kwargs["stage"], "adapter")
                return gate, ready_response, {"status": "conforming", "attempts_used": 1}

            with (
                patch.object(runner, "_load_plan", return_value=(scored_ids, dev_ids, scored_ids)),
                patch.object(runner, "_client", side_effect=fake_client),
                patch.object(runner, "_attempt_stage", side_effect=fake_attempt_stage),
                patch("e4.data.submissions.parse_adapter_submission", return_value=json.loads(ready_response)),
                patch.object(runner, "normalize_dataset_schema", return_value=schema),
                patch.object(runner, "_ensure_public_caches", return_value={}),
                patch.object(runner, "_prepare_mode", side_effect=fake_prepare_mode),
                patch.object(runner, "build_bank_adapter_prompt", return_value="fixture adapter prompt"),
            ):
                result = runner.run_campaign(
                    run_root,
                    paper_url="mock://paper",
                    clean_slate_url="mock://clean",
                    prepare_only=True,
                )

            self.assertEqual(mode_started, {"paper", "clean_slate"})
            self.assertEqual({row[0] for row in endpoint_clients}, {"paper", "clean_slate"})
            self.assertNotEqual(endpoint_clients[0][2], endpoint_clients[1][2])
            self.assertEqual(result["modes"]["paper"]["onboarding_status"], "failed")
            self.assertEqual(result["modes"]["clean_slate"]["onboarding_status"], "frozen")
            self.assertIsNone(result["modes"]["paper"]["accuracy"])
            self.assertIsNone(result["modes"]["clean_slate"]["accuracy"])
            self.assertEqual(result["modes"]["paper"]["planned"], 30)
            self.assertEqual(result["token_costs"]["by_endpoint"]["paper"]["total_tokens"], 120)
            self.assertEqual(result["token_costs"]["by_endpoint"]["clean_slate"]["total_tokens"], 18)

    def test_clean_slate_only_campaign_never_initializes_paper_endpoint(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            run_root = root / "run"
            response = {
                "status": "ready",
                "source": "prevalidated adapter source",
                "config_schema": {"type": "object"},
            }
            response_path = root / "adapter-response.json"
            response_path.write_text(json.dumps(response), encoding="utf-8")
            gate_path = root / "adapter-gate.json"
            client_endpoints = []
            prepare_modes = []
            schema = _schema()
            gate_path.write_text(json.dumps({
                "status": "conforming",
                "adapter_source_sha256": "source-hash",
                "schema": schema.as_dict(),
                "config_schema": response["config_schema"],
                "source_manifest": {},
                "cases": [],
                "timestamp_unit_probes": [],
                "errors": [],
            }), encoding="utf-8")
            scored_ids = [f"case-{index}" for index in range(10)]
            dev_ids = ["dev-a", "dev-b"]

            class DummyClient:
                def __init__(self, endpoint):
                    self.endpoint = endpoint
                    self.seed = 42

            def fake_client(endpoint, base_url, *args, **kwargs):
                client_endpoints.append(endpoint)
                return DummyClient(endpoint)

            def fake_prepare(mode, **kwargs):
                prepare_modes.append(mode)
                kwargs["state"].setdefault("modes", {})[mode] = {"status": "frozen"}
                return kwargs["state"]["modes"][mode]

            with (
                patch.object(runner, "_load_plan", return_value=(scored_ids, dev_ids, scored_ids)),
                patch.object(runner, "_client", side_effect=fake_client),
                patch.object(runner, "_reuse_adapter_gate_receipt", return_value=True),
                patch.object(runner, "normalize_dataset_schema", return_value=schema),
                patch.object(runner, "_ensure_public_caches", return_value={}),
                patch.object(runner, "_prepare_mode", side_effect=fake_prepare),
                patch.object(runner, "_attempt_stage", side_effect=AssertionError("prevalidated gate must avoid generation")),
                patch.object(runner, "build_bank_adapter_prompt", return_value="should not render prompt"),
            ):
                result = runner.run_campaign(
                    run_root,
                    modes=("clean_slate",),
                    adapter_response_file=response_path,
                    adapter_gate_file=gate_path,
                    prepare_only=True,
                )

            self.assertEqual(client_endpoints, ["clean_slate"])
            self.assertEqual(prepare_modes, ["clean_slate"])
            self.assertEqual(result["profile"]["modes"], ["clean_slate"])
            self.assertEqual(set(result["modes"]), {"clean_slate"})
            self.assertIsNone(result["modes"]["clean_slate"]["accuracy"])

    def test_mode_chains_keep_isolated_libraries_and_resume_from_latest_snapshot(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            run_root = Path(directory)
            scored_ids = [f"case-{index}" for index in range(10)]
            clients = {
                "paper": SimpleNamespace(seed=42),
                "clean_slate": SimpleNamespace(seed=42),
            }
            state = {
                "profile": {},
                "modes": {"paper": {"status": "frozen"}, "clean_slate": {"status": "frozen"}},
                "accounted_calls": {"paper": [], "clean_slate": []},
                "call_owners": {"paper": {}, "clean_slate": {}},
            }
            opened_libraries: dict[str, list[LibraryManager]] = {"paper": [], "clean_slate": []}
            resume_counts: list[int] = []

            def fake_prepare(schema, *, mode):
                return _library(mode)

            def fake_load(path, *, base_registry=None, mode=None):
                path = Path(path)
                if path.name == "frozen_library":
                    library = _library(mode or "custom")
                else:
                    registry = OperatorRegistry()
                    for item in json.loads((path / "operators.json").read_text(encoding="utf-8")):
                        registry.register(OperatorSpec.from_dict(item))
                    library = LibraryManager(registry=registry, mode=mode or "custom", origin="snapshot")
                opened_libraries[mode].append(library)
                return library

            def fake_benchmark(*args, **kwargs):
                profile = kwargs["profile"]
                mode = profile["mode"]
                library = kwargs["library"]
                resume = list(kwargs.get("resume_results") or [])
                resume_counts.append(len(resume))
                if not resume:
                    for index, case_id in enumerate(kwargs["case_ids"]):
                        library.registry.register(OperatorSpec(
                            name=f"learned.{mode}.{index}", version="1", inputs=(), output_type="Aggregate",
                        ))
                        row = {"case_id": case_id, "status": "unverified", "call_metadata": []}
                        kwargs["on_result"](row)
                    resume = [
                        {"case_id": case_id, "status": "unverified", "call_metadata": []}
                        for case_id in kwargs["case_ids"]
                    ]
                return {"results": resume, "hits": 0, "total_cases": len(kwargs["case_ids"]), "accuracy": 0.0}

            common = {
                "run_root": run_root,
                "state": state,
                "schema": _schema(),
                "scored_ids": scored_ids,
                "caches": {},
                "clients": clients,
                "urls": {"paper": "mock://paper", "clean_slate": "mock://clean"},
                "deadline": None,
            }
            with (
                patch.object(runner, "prepare_library", side_effect=fake_prepare),
                patch.object(LibraryManager, "load_from_dir", side_effect=fake_load),
                patch.object(runner, "run_benchmark", side_effect=fake_benchmark),
            ):
                runner._run_mode_seed("paper", 42, **common)
                runner._run_mode_seed("clean_slate", 42, **common)

                paper_chain = run_root / "modes/paper/chains/seed42/shard0"
                (paper_chain / "result.json").unlink()
                runner._run_mode_seed("paper", 42, **common)

            paper_names = {op.name for op in opened_libraries["paper"][-1].registry.all_operators()}
            clean_names = {op.name for op in opened_libraries["clean_slate"][0].registry.all_operators()}
            self.assertIsNot(opened_libraries["paper"][0], opened_libraries["paper"][1])
            self.assertIsNot(opened_libraries["clean_slate"][0], opened_libraries["clean_slate"][1])
            self.assertIn("baseline.paper", paper_names)
            self.assertIn("learned.paper.4", paper_names)
            self.assertNotIn("baseline.clean_slate", paper_names)
            self.assertIn("baseline.clean_slate", clean_names)
            self.assertIn("learned.clean_slate.4", clean_names)
            self.assertNotIn("baseline.paper", clean_names)
            self.assertEqual(resume_counts, [0, 0, 0, 0, 5])
            self.assertTrue((paper_chain / "checkpoint.json").is_file())
            self.assertIn("library_snapshots", json.loads((paper_chain / "checkpoint.json").read_text())["library_snapshot"])

    def test_campaign_seed_denominators_separate_repeated_case_ids(self) -> None:
        case_ids = [f"scored-{index}" for index in range(10)]
        hit_counts = {42: 7, 43: 6, 44: 8}
        with tempfile.TemporaryDirectory() as directory:
            run_root = Path(directory)
            for seed, hits in hit_counts.items():
                for shard in (0, 1):
                    shard_ids = case_ids[shard * 5:(shard + 1) * 5]
                    first_hit_count = max(0, min(5, hits - shard * 5))
                    results = [
                        {
                            "case_id": case_id,
                            "status": "confirmed" if index < first_hit_count else "unverified",
                            "stage": "serve" if index < first_hit_count else "select",
                        }
                        for index, case_id in enumerate(shard_ids)
                    ]
                    path = run_root / "modes/paper/chains" / f"seed{seed}" / f"shard{shard}" / "result.json"
                    path.parent.mkdir(parents=True, exist_ok=True)
                    path.write_text(json.dumps({
                        "seed": seed,
                        "shard": shard,
                        "status": "complete",
                        "hits": sum(row["status"] == "confirmed" for row in results),
                        "total_cases": len(results),
                        "planned_case_ids": shard_ids,
                        "results": results,
                    }), encoding="utf-8")

            state = {
                "profile": {},
                "modes": {"paper": {"status": "frozen"}, "clean_slate": {"status": "failed"}},
                "accounted_calls": {"paper": [], "clean_slate": []},
                "call_owners": {"paper": {}, "clean_slate": {}},
            }
            result = runner._build_campaign_result(run_root, state, None)

        seeds = result["modes"]["paper"]["seeds"]
        self.assertEqual(
            {seed: (item["confirmed"], item["planned"], item["accuracy"])
             for seed, item in seeds.items()},
            {"42": (7, 10, 0.7), "43": (6, 10, 0.6), "44": (8, 10, 0.8)},
        )


if __name__ == "__main__":
    unittest.main()
