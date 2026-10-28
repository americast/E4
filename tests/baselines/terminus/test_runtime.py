import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
import uuid
from unittest.mock import patch
from pathlib import Path


ROOT = Path(__file__).resolve().parents[3]


class TerminusManifestRuntimeTests(unittest.TestCase):
    def test_development_manifest_revalidation_uses_pinned_e4_python(self):
        from scripts.comparison import terminus_case

        with tempfile.TemporaryDirectory() as directory:
            manifest = Path(directory) / "development_manifest.json"
            manifest.write_text(json.dumps({"schema_version": "e4-m5-development-manifest/1",
                "status": "development_pending_validation"}), encoding="utf-8")
            expected_hash = terminus_case._sha256(manifest)
            with patch.object(terminus_case,
                              "_validate_development_manifest_in_e4_runtime") as validate:
                checked = terminus_case._freeze_guard(
                    manifest, case_id="bank-20210323T0630-utc8", split="dev",
                    validated_sha256=expected_hash)
        validate.assert_called_once_with(manifest)
        self.assertEqual(checked["status"], "development_pending_validation")

    def test_development_manifest_validator_launches_project_python(self):
        from scripts.comparison import terminus_case

        result = subprocess.CompletedProcess(args=[], returncode=0, stdout="", stderr="")
        manifest = ROOT / "some-provisional-manifest.json"
        with patch("scripts.comparison.terminus_case.subprocess.run", return_value=result) as run:
            terminus_case._validate_development_manifest_in_e4_runtime(
                manifest)
        command = run.call_args.args[0]
        self.assertEqual(Path(command[0]), ROOT / ".venv/bin/python")
        self.assertIn("validate_development_manifest", command[2])
        self.assertEqual(run.call_args.kwargs["cwd"], ROOT)
        self.assertEqual(command[3], str(manifest))


class ActualDockerIsolationTests(unittest.TestCase):
    def test_pinned_harbor_import_precedes_docker_and_cost_map_never_connects(self):
        python = ROOT / "artifacts/m5/runtime/harbor-python/bin/python"
        source = ROOT / "artifacts/m5/runtime/harbor-source"
        if not python.is_file() or not source.is_dir():
            self.skipTest("pinned Harbor runtime/source is unavailable")
        with tempfile.TemporaryDirectory() as directory:
            snippet = "\n".join([
                "import inspect,os,socket,sys",
                f"sys.path.insert(0, {str(ROOT / 'src')!r})",
                "os.environ['LITELLM_LOCAL_MODEL_COST_MAP']='True'",
                "attempts=[]",
                "def deny_network(self,address):",
                "    attempts.append(address)",
                "    raise AssertionError('unexpected network connect: %r' % (address,))",
                "socket.socket.connect=deny_network",
                "from e4.baselines.terminus.terminus2 import load_terminus2",
                f"source={str(source)!r}",
                "agent_type,agent=load_terminus2(source,root=" + repr(str(ROOT)) +
                    ",logs_dir=__import__('pathlib').Path(sys.argv[1]),max_turns=1,"
                    "api_base='http://127.0.0.1:8080',enable_summarize=False)",
                "from harbor.environments.docker.docker import DockerEnvironment",
                "expected=__import__('pathlib').Path(source)/'src/harbor/agents/terminus_2/terminus_2.py'",
                "assert __import__('pathlib').Path(inspect.getfile(agent_type)).resolve()==expected.resolve()",
                "assert __import__('pathlib').Path(inspect.getfile(DockerEnvironment)).resolve().is_relative_to(__import__('pathlib').Path(source).resolve())",
                "assert not attempts, attempts",
            ])
            env = dict(os.environ)
            env["LITELLM_LOCAL_MODEL_COST_MAP"] = "True"
            result = subprocess.run([str(python), "-c", snippet, directory],
                                    cwd=ROOT, env=env, capture_output=True, text=True,
                                    timeout=120)
        self.assertEqual(result.returncode, 0, result.stderr)

    def test_offline_task_container_has_readonly_case_no_socket_and_no_devices(self):
        docker = shutil.which("docker")
        pin_path = ROOT / "artifacts/m5/runtime/terminus_task_image_pin.json"
        if not docker or not pin_path.is_file():
            self.skipTest("Docker CLI or pinned Terminus task image is unavailable")
        pin = json.loads(pin_path.read_text())
        image = pin["terminus_task_image_id"]
        case_code = "\n".join([
            "import pathlib, socket",
            "assert not pathlib.Path('/var/run/docker.sock').exists()",
            "assert socket.if_nameindex() == [(1,'lo')], socket.if_nameindex()",
            "p=pathlib.Path('/data/case.json')",
            "assert p.read_text() == '{\\\"case\\\":1}'",
            "try:",
            "    p.write_text('changed')",
            "except OSError:",
            "    pass",
            "else:",
            "    raise AssertionError('selected case mount is writable')",
        ])
        with tempfile.TemporaryDirectory() as directory:
            case_root = Path(directory)
            (case_root / "case.json").write_text('{"case":1}')
            name = "e4-terminus-isolation-" + uuid.uuid4().hex[:12]
            create = subprocess.run([
                docker, "create", "--name", name, "--network", "none",
                "--cpus", "1", "--memory", "16g",
                "--tmpfs", "/workspace:rw,size=21474836480",
                "--mount", f"type=bind,source={case_root},target=/data,readonly",
                "--entrypoint", "python", image, "-c", case_code,
            ], capture_output=True, text=True, timeout=60)
            if create.returncode:
                if "permission denied" in create.stderr.lower():
                    self.skipTest("Docker daemon access is unavailable")
                self.fail(create.stderr)
            try:
                inspected = subprocess.run([docker, "inspect", name], capture_output=True,
                                            text=True, timeout=30)
                self.assertEqual(inspected.returncode, 0, inspected.stderr)
                document = json.loads(inspected.stdout)[0]
                host = document["HostConfig"]
                mounts = {mount["Destination"]: mount for mount in document["Mounts"]}
                self.assertEqual(host["NetworkMode"], "none")
                self.assertFalse(host["Privileged"])
                self.assertEqual(host.get("DeviceRequests") or [], [])
                self.assertEqual(host.get("Devices") or [], [])
                self.assertFalse(mounts["/data"]["RW"])
                started = subprocess.run([docker, "start", "--attach", name],
                                          capture_output=True, text=True, timeout=60)
                self.assertEqual(started.returncode, 0, started.stderr + started.stdout)
            finally:
                subprocess.run([docker, "rm", "--force", name], capture_output=True,
                               text=True, timeout=30)


if __name__ == "__main__":
    unittest.main()
