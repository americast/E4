import hashlib
import json
import os
import shutil
import subprocess
import struct
import sys
import tempfile
import threading
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from e4core.contracts import EntityRef, RankedAnswer, ScoredCandidate
from e4core.bank_adapter import load_bank_case
from scripts.run_m4_bank import (BinaryFeedbackProxy, SupervisorError,
    _bwrap_command, _validate_feedback_request, create_restricted_project_view,
    fresh_run_directory, serve_feedback_request, _run_with_feedback, _run_outer)
from tests.test_bank_adapter import make_bank_fixture


ROOT = Path(__file__).resolve().parents[1]


def _sha(path):
    return hashlib.sha256(path.read_bytes()).hexdigest()


class M4BankSupervisorTests(unittest.TestCase):
    def _source_root(self, root):
        case_id, manifest_path = make_bank_fixture(root)
        config_dir = root / "config"
        for name in ("m4_runtime.json", "runtime_schema.json", "core_runtime.json",
                     "core_decisions.json", "metric_kind_policy.json", "dev_kpi_policy.json",
                     "bank_seed_library.json", "gemma_m3_inference.json", "model_manifest.json"):
            source = ROOT / "config" / name
            if source.exists():
                shutil.copyfile(source, config_dir / name)
        for name in ("pyproject.toml", "requirements-m3.lock"):
            shutil.copyfile(ROOT / name, root / name)
        profile = config_dir / "gemma_m3_inference.json"

        sample = {"dataset": "OpenRCA Bank synthetic test fixture",
            "development_cases": [{"incident_id": case_id, "alert_text": "synthetic public fixture"}],
            "scored_cases": [{"incident_id": case_id, "alert_text": "synthetic public fixture"}]}
        sample_path = config_dir / "bank_sample.json"
        sample_path.write_text(json.dumps(sample, sort_keys=True) + "\n", encoding="utf-8")

        dev_path = root / "data/prepared/bank/dev_incidents.jsonl"
        selected = json.loads(dev_path.read_text(encoding="utf-8").splitlines()[0])
        other = {**selected, "incident_id": "bank-20210305T0100-utc8",
                 "alert_text": "synthetic unrelated case fixture"}
        dev_path.write_text("".join(json.dumps(row, sort_keys=True) + "\n"
                                    for row in (selected, other)), encoding="utf-8")
        scored_path = root / "data/prepared/bank/scored_incidents.jsonl"
        scored_path.write_text(json.dumps(selected, sort_keys=True) + "\n", encoding="utf-8")
        extra_case = root / "data/prepared/bank/public/cases" / other["incident_id"]
        extra_case.mkdir(parents=True)
        (extra_case / "synthetic_secret_marker.txt").write_text("other synthetic case", encoding="utf-8")

        clip_path = root / "data/prepared/bank/public/clip_manifest.jsonl"
        clip_rows = [json.loads(line) for line in clip_path.read_text(encoding="utf-8").splitlines()]
        extra_clips = [{**row, "case_id": other["incident_id"]} for row in clip_rows]
        clip_path.write_text("".join(json.dumps(row, sort_keys=True) + "\n"
                                    for row in clip_rows + extra_clips), encoding="utf-8")

        manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        selected_manifest = manifest["cases"][case_id]
        manifest["cases"][other["incident_id"]] = selected_manifest
        manifest["sample_config_sha256"] = _sha(sample_path)
        manifest["dev_manifest_sha256"] = _sha(dev_path)
        manifest["scored_manifest_sha256"] = _sha(scored_path)
        manifest["clip_manifest_sha256"] = _sha(clip_path)
        manifest_path.write_text(json.dumps(manifest, sort_keys=True) + "\n", encoding="utf-8")
        catalog_path = root / "data/prepared/bank/public/deployment_catalog.json"
        catalog = json.loads(catalog_path.read_text(encoding="utf-8"))
        catalog["entities"]["synthetic-other-case-entity"] = {"role": "component",
            "relationship": "unknown"}
        catalog_path.write_text(json.dumps(catalog, sort_keys=True), encoding="utf-8")
        manifest["deployment_catalog_file_sha256"] = _sha(catalog_path)
        manifest_path.write_text(json.dumps(manifest, sort_keys=True) + "\n", encoding="utf-8")
        (root / "data/prepared/bank/evaluator").mkdir(parents=True)
        (root / "data/prepared/bank/evaluator/private_marker.txt").write_text(
            "synthetic evaluator marker", encoding="utf-8")
        return case_id, other["incident_id"], profile

    def test_restricted_view_contains_only_selected_public_dev_case(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            case_id, other_id, profile = self._source_root(root)
            view = root / "view"
            pins = create_restricted_project_view(root, view, profile_path=profile,
                                                  case_id=case_id, code_root=ROOT)
            self.assertEqual(pins["view_kind"], "one_dev_case")
            self.assertTrue((view / "data/prepared/bank/public/cases" / case_id).is_dir())
            self.assertFalse((view / "data/prepared/bank/public/cases" / other_id).exists())
            self.assertFalse((view / "data/prepared/bank/public/cases" / other_id /
                              "synthetic_secret_marker.txt").exists())
            self.assertFalse((view / "data/prepared/bank/evaluator").exists())
            self.assertFalse((view / "data/prepared/bank/scored_incidents.jsonl").exists())
            self.assertFalse((view / "e4core/m4_feedback.py").exists())
            self.assertFalse((view / "agent_plan").exists())
            self.assertFalse((view / "docs").exists())
            sample = json.loads((view / "config/bank_sample.json").read_text(encoding="utf-8"))
            self.assertEqual([row["incident_id"] for row in sample["development_cases"]], [case_id])
            manifest = json.loads((view / "config/bank_input_manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(set(manifest["cases"]), {case_id})
            original_manifest = json.loads((root / "config/bank_input_manifest.json").read_text())
            self.assertEqual(pins["original_input_manifest_sha256"],
                             _sha(root / "config/bank_input_manifest.json"))
            self.assertEqual(pins["selected_clip_hashes"],
                {relative: doc["sha256"] for relative, doc in
                 original_manifest["cases"][case_id]["files"].items()})
            filtered_catalog = json.loads((view / "data/prepared/bank/public/deployment_catalog.json").read_text())
            self.assertNotIn("synthetic-other-case-entity", filtered_catalog["entities"])
            self.assertEqual(pins["restricted_view_sha256"],
                hashlib.sha256(json.dumps(pins["view_file_inventory"], ensure_ascii=False,
                    sort_keys=True, separators=(",", ":")).encode()).hexdigest())
            dev_rows = [json.loads(line) for line in
                        (view / "data/prepared/bank/dev_incidents.jsonl").read_text().splitlines()]
            self.assertEqual([row["incident_id"] for row in dev_rows], [case_id])
            clip_rows = [json.loads(line) for line in
                         (view / "data/prepared/bank/public/clip_manifest.jsonl").read_text().splitlines()]
            self.assertEqual({row["case_id"] for row in clip_rows}, {case_id})

    def test_restricted_scored_view_keeps_single_case_manifest_hash_loadable(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            case_id, _other_id, profile = self._source_root(root)
            view = root / "scored-view"
            pins = create_restricted_project_view(root, view, profile_path=profile,
                case_id=case_id, split="scored", code_root=ROOT)
            manifest = json.loads((view / "config/bank_input_manifest.json").read_text())
            scored_path = view / "data/prepared/bank/scored_incidents.jsonl"
            self.assertEqual(manifest["scored_manifest_sha256"], _sha(scored_path))
            self.assertEqual(pins["view_kind"], "one_scored_case")
            loaded = load_bank_case(case_id, split="scored", root=view)
            self.assertEqual(loaded.public_manifest_row["incident_id"], case_id)

    def test_manifest_tampering_is_rejected_before_filtered_pins_are_created(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            case_id, _other_id, profile = self._source_root(root)
            dev_path = root / "data/prepared/bank/dev_incidents.jsonl"
            dev_path.write_text(dev_path.read_text(encoding="utf-8") + "\n", encoding="utf-8")
            view = root / "view"
            with self.assertRaisesRegex(SupervisorError, "original dev manifest"):
                create_restricted_project_view(root, view, profile_path=profile,
                    case_id=case_id, code_root=ROOT)

    def test_nested_mount_plan_uses_only_view_library_and_fresh_output(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            case_id, _other_id, profile = self._source_root(root)
            view = root / "view"
            pins = create_restricted_project_view(root, view, profile_path=profile,
                                                  case_id=case_id, code_root=ROOT)
            output = root / "outputs/run-one"
            output.mkdir(parents=True)
            library = root / "libraries/snapshot-one"
            library.mkdir(parents=True)
            command = _bwrap_command(view_root=view, run_dir=output,
                library_root=library, profile_relative=pins["profile_relative_path"]
                    if "profile_relative_path" in pins else "config/gemma_m3_inference.json",
                mode="diagnostic", case_id=case_id, endpoint="http://127.0.0.1:8080",
                feedback_socket="test-address", implementation_fingerprint_value="a" * 64)
            self.assertIn("--unshare-all", command)
            self.assertIn("--share-net", command)
            self.assertIn(str(view), command)
            self.assertIn(str(library), command)
            self.assertIn(str(output), command)
            self.assertNotIn(str(root), command)
            self.assertFalse(any("evaluator" in item or "agent_plan" in item or "docs" in item
                                 for item in command))

    def test_fresh_run_path_must_stay_under_launcher_mount_and_be_new(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            base = root / "model-run"
            base.mkdir()
            target, resolved_base = fresh_run_directory(Path("m4/trial"), base)
            self.assertTrue(target.is_relative_to(resolved_base))
            self.assertTrue(target.is_dir())
            with self.assertRaisesRegex(SupervisorError, "child of E4_RUN_DIR"):
                fresh_run_directory(root / "escape", base)
            with self.assertRaisesRegex(FileExistsError, "must be fresh"):
                fresh_run_directory(target, base)

    def test_bootstrap_supervisor_manifest_records_no_staged_task_view(self):
        with tempfile.TemporaryDirectory() as temp:
            run_root = Path(temp) / "model-run"
            run_root.mkdir()
            profile = ROOT / "config/gemma_m3_inference.json"
            args = SimpleNamespace(root=ROOT, profile=profile, run_dir=Path("m5_bootstrap"),
                mode="bootstrap", library_root=None, case_id=None, split="dev",
                experiment_manifest=None, trusted_evaluator_socket=None, task_view_root=None)
            view_pins = {"restricted_view_sha256": "a" * 64,
                         "profile_relative_path": "config/gemma_m3_inference.json",
                         "view_kind": "schema_only_bootstrap", "view_file_inventory": {}}
            with patch.dict(os.environ, {"E4_RUN_DIR": str(run_root),
                    "E4_MODEL_PROFILE": str(profile), "E4_BASE_URL": "http://127.0.0.1:8080"}), \
                    patch("scripts.run_m4_bank.create_restricted_project_view",
                          return_value=view_pins), \
                    patch("scripts.run_m4_bank._bwrap_command", return_value=["mock-bwrap"]), \
                    patch("scripts.run_m4_bank.subprocess.run",
                          return_value=SimpleNamespace(returncode=0)) as run_child, \
                    patch("e4core.cache.implementation_fingerprint", return_value="b" * 64):
                self.assertEqual(_run_outer(args), 0)

            run_child.assert_called_once()
            manifest = json.loads((run_root / "m5_bootstrap" /
                                   "supervisor_manifest.json").read_text(encoding="utf-8"))
            self.assertEqual(manifest["mode"], "bootstrap")
            self.assertIsNone(manifest["staged_task_view_sha256"])
            self.assertIsNone(manifest["attempt_slots"])

    def test_binary_feedback_ipc_sends_only_components_and_returns_one_bool(self):
        parent_sock, child_sock = __import__("socket").socketpair()
        with tempfile.TemporaryDirectory() as temp:
            audit = Path(temp) / "trusted_audit" / "feedback.jsonl"
            received = {}

            def fake_evaluator(case_id, components):
                received["case_id"] = case_id
                received["components"] = list(components)
                return bool(components and components[0] == "fixture-cause")

            def serve():
                serve_feedback_request(parent_sock, case_id="bank-20210323T0630-utc8",
                    evaluate_ids=fake_evaluator, audit_path=audit)

            thread = threading.Thread(target=serve)
            thread.start()
            proxy = BinaryFeedbackProxy(child_sock)
            answer = RankedAnswer((ScoredCandidate(EntityRef("component", "fixture-cause"),
                1.0, None, ()), ScoredCandidate(EntityRef("component", "fixture-other"),
                0.5, None, ())))
            response = proxy.evaluate("bank-20210323T0630-utc8", answer)
            thread.join(timeout=3)
            self.assertFalse(thread.is_alive())
            self.assertEqual(response, {"accepted": True})
            self.assertEqual(received, {"case_id": "bank-20210323T0630-utc8",
                                        "components": ["fixture-cause", "fixture-other"]})
            audit_row = json.loads(audit.read_text(encoding="utf-8"))
            self.assertEqual(set(audit_row), {"case_id", "components", "accepted", "updated_utc"})
            self.assertTrue(audit_row["accepted"])
            with self.assertRaisesRegex(SupervisorError, "unexpected fields"):
                _validate_feedback_request({"case_id": "bank-20210323T0630-utc8",
                    "components": [], "root_cause": "fixture-only"}, "bank-20210323T0630-utc8")
            proxy.close()
            parent_sock.close()

    @unittest.skipUnless(os.environ.get("E4_TEST_BWRAP") == "1",
                         "requires the approved local abstract-socket IPC environment")
    def test_feedback_subprocess_connects_abstract_socket_without_parent_fd3_remap(self):
        source = '''import json,os,struct,sys
import socket
sock=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
sock.connect("\\0"+sys.argv[1])
fd=sock.fileno()
def readn(n):
    data=b""
    while len(data)<n:
        part=os.read(fd,n-len(data))
        if not part: raise SystemExit(2)
        data+=part
    return data
def send(value):
    raw=json.dumps(value,separators=(",",":")).encode()
    os.write(fd,struct.pack("!I",len(raw))+raw)
send({"case_id":"bank-20210323T0630-utc8","components":["fixture-cause"]})
size=struct.unpack("!I",readn(4))[0]
response=json.loads(readn(size))
raise SystemExit(0 if response=={"accepted":True} else 3)
'''
        saved_fd3 = None
        try:
            try:
                saved_fd3 = os.dup(3)
            except OSError:
                saved_fd3 = None
            sentinel = os.open(os.devnull, os.O_RDONLY)
            if sentinel != 3:
                os.dup2(sentinel, 3)
                os.close(sentinel)
            sentinel_info = os.fstat(3)

            class FakeEvaluator:
                def evaluate(self, case_id, answer):
                    self.case_id = case_id
                    self.components = [row.entity.identifier for row in answer.leads]
                    return {"accepted": True}

            evaluator = FakeEvaluator()
            with tempfile.TemporaryDirectory() as temp:
                audit = Path(temp) / "trusted" / "feedback.jsonl"
                output = Path(temp) / "stdout.log"
                error = Path(temp) / "stderr.log"
                status = _run_with_feedback(lambda address: [sys.executable, "-c", source, address],
                    case_id="bank-20210323T0630-utc8", evaluator=evaluator,
                    audit_path=audit, stdout_path=output, stderr_path=error,
                    timeout_seconds=10)
                self.assertEqual(status, 0, error.read_text(encoding="utf-8"))
                self.assertEqual(evaluator.components, ["fixture-cause"])
                self.assertEqual(os.fstat(3).st_ino, sentinel_info.st_ino)
                self.assertTrue(json.loads(audit.read_text(encoding="utf-8"))["accepted"])
        finally:
            if saved_fd3 is not None:
                os.dup2(saved_fd3, 3)
                os.close(saved_fd3)
            else:
                try:
                    os.close(3)
                except OSError:
                    pass

    @unittest.skipUnless(os.environ.get("E4_TEST_BWRAP") == "1",
                         "requires approved bubblewrap namespace execution")
    def test_nested_bwrap_cannot_see_evaluator_other_case_or_source_tree(self):
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            case_id, other_id, profile = self._source_root(root)
            view = root / "view"
            create_restricted_project_view(root, view, profile_path=profile,
                                           case_id=case_id, code_root=ROOT)
            output = root / "run"
            output.mkdir()
            code = (
                "import json,pathlib; r=pathlib.Path('/m4project'); "
                "print(json.dumps({'selected':(r/'data/prepared/bank/public/cases/" + case_id + "').is_dir(),"
                "'other':(r/'data/prepared/bank/public/cases/" + other_id + "').exists(),"
                "'evaluator':(r/'data/prepared/bank/evaluator').exists(),"
                "'feedback_code':(r/'e4core/m4_feedback.py').exists(),"
                "'source_root':pathlib.Path('" + str(root) + "').exists()}))"
            )
            command = _bwrap_command(view_root=view, run_dir=output, library_root=None,
                profile_relative="config/gemma_m3_inference.json", mode="bootstrap",
                case_id=None, endpoint="http://127.0.0.1:8080", feedback_socket=None,
                implementation_fingerprint_value="a" * 64,
                command_override=["/usr/bin/python3.10", "-c", code])
            result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.PIPE,
                                    text=True, timeout=20, env={})
            self.assertEqual(result.returncode, 0, result.stderr)
            observed = json.loads(result.stdout)
            self.assertEqual(observed, {"selected": True, "other": False,
                "evaluator": False, "feedback_code": False, "source_root": False})

    @unittest.skipUnless(os.environ.get("E4_TEST_BWRAP") == "1",
                         "requires approved bubblewrap namespace execution")
    def test_bwrap_abstract_socket_returns_only_the_synthetic_binary_feedback(self):
        child_source = '''import json,os,struct,sys,socket
sock=socket.socket(socket.AF_UNIX,socket.SOCK_STREAM)
sock.connect("\\0"+sys.argv[1])
fd=sock.fileno()
payload=json.dumps({"case_id":"bank-20210323T0630-utc8",
    "components":["fixture-cause"]},separators=(",",":")).encode()
os.write(fd,struct.pack("!I",len(payload))+payload)
def readn(n):
    data=b""
    while len(data)<n:
        part=os.read(fd,n-len(data))
        if not part: raise SystemExit(2)
        data+=part
    return data
size=struct.unpack("!I",readn(4))[0]
result=json.loads(readn(size))
raise SystemExit(0 if result=={"accepted":True} else 3)
'''
        with tempfile.TemporaryDirectory() as temp:
            root = Path(temp)
            case_id, _other_id, profile = self._source_root(root)
            view = root / "view"
            pins = create_restricted_project_view(root, view, profile_path=profile,
                                                  case_id=case_id, code_root=ROOT)
            output = root / "run"
            output.mkdir()
            trusted_audit = root / "trusted_feedback" / "feedback.jsonl"
            class FakeEvaluator:
                def evaluate(self, case, answer):
                    ids = [row.entity.identifier for row in answer.leads]
                    return {"accepted": ids == ["fixture-cause"]}

            def command(address):
                return _bwrap_command(view_root=view, run_dir=output, library_root=None,
                    profile_relative=pins["profile_relative_path"], mode="bootstrap",
                    case_id=None, endpoint="http://127.0.0.1:8080", feedback_socket=address,
                    implementation_fingerprint_value="a" * 64,
                    command_override=["/usr/bin/python3.10", "-c", child_source, address])

            status = _run_with_feedback(command, case_id="bank-20210323T0630-utc8",
                evaluator=FakeEvaluator(), audit_path=trusted_audit,
                stdout_path=output / "stdout.log", stderr_path=output / "stderr.log",
                timeout_seconds=20)
            self.assertEqual(status, 0, (output / "stderr.log").read_text(encoding="utf-8"))
            self.assertTrue(trusted_audit.is_file())
            self.assertFalse(trusted_audit.is_relative_to(output))
            audit = json.loads(trusted_audit.read_text(encoding="utf-8"))
            self.assertEqual(set(audit), {"case_id", "components", "accepted", "updated_utc"})
            self.assertEqual(audit["components"], ["fixture-cause"])
            self.assertTrue(audit["accepted"])



if __name__ == "__main__":
    unittest.main()
