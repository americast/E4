import hashlib
import os
import shutil
import subprocess
import sys
import tempfile
import time
import unittest
from pathlib import Path

from scripts.run_m3_model import (LaunchError, _bwrap_command, _terminate_group, build_server_command,
                                  preflight_server, resolve_server_libraries, verify_model)


ROOT = Path(__file__).resolve().parents[1]


class M3LauncherTests(unittest.TestCase):
    def test_model_file_must_match_pinned_manifest(self):
        with tempfile.TemporaryDirectory(dir=ROOT / "artifacts") as tmp:
            model = Path(tmp) / "fixture.gguf"
            model.write_bytes(b"pinned fixture model")
            digest = hashlib.sha256(model.read_bytes()).hexdigest()
            profile = {"model_path": str(model.relative_to(ROOT))}
            manifest = {"filename": model.name, "revision": "abc123", "sha256": digest,
                        "size_bytes": model.stat().st_size}
            verified, actual = verify_model(profile, manifest)
            self.assertEqual(verified, model.resolve())
            self.assertEqual(actual, digest)
            with self.assertRaisesRegex(LaunchError, "SHA-256 mismatch"):
                verify_model(profile, {**manifest, "sha256": "0" * 64})

    def test_server_command_uses_profile_context_and_reasoning(self):
        profile = {"model_alias": "fixture", "reasoning_mode": "on",
                   "reasoning_format": "deepseek", "reasoning_budget": 2048}
        command = build_server_command(Path("/bin/llama-server"), Path("/models/m.gguf"),
                                       profile, 9080, 32768)
        self.assertIn("32768", command)
        self.assertEqual(command[command.index("--reasoning") + 1], "on")
        self.assertEqual(command[command.index("--reasoning-format") + 1], "deepseek")
        self.assertEqual(command[command.index("--reasoning-budget") + 1], "2048")
        self.assertIn("--offline", command)

        off = build_server_command(Path("/bin/llama-server"), Path("/models/m.gguf"),
                                   {"model_alias": "fixture", "reasoning_mode": "off"},
                                   9080, 32768)
        self.assertEqual(off[off.index("--reasoning") + 1], "off")

    def test_client_and_server_share_network_private_namespace(self):
        with tempfile.TemporaryDirectory(dir=ROOT / "artifacts") as tmp:
            run_dir = Path(tmp)
            command = _bwrap_command(ROOT / "config/inference.json", "/usr/bin/true",
                                     run_dir, "fixture-run", 9080, 32768, 60, 120,
                                     ["/usr/lib/x86_64-linux-gnu"],
                                     ["/usr/bin/true", "--case", "independent"])
        self.assertIn("--unshare-net", command)
        self.assertIn("--unshare-pid", command)
        self.assertIn("--die-with-parent", command)
        self.assertIn("E4_BWRAPPED", command)
        self.assertIn("--", command)
        self.assertEqual(command[-3:], ["/usr/bin/true", "--case", "independent"])

    def test_timeout_cleanup_terminates_exact_process_group(self):
        child = subprocess.Popen([sys.executable, "-c", "import time; time.sleep(60)"],
                                 start_new_session=True)
        started = time.monotonic()
        _terminate_group(child, grace=1)
        self.assertIsNotNone(child.returncode)
        self.assertLess(time.monotonic() - started, 3)

    def test_real_pid_namespace_kills_detached_descendant_when_wrapper_is_terminated(self):
        bwrap = shutil.which("bwrap")
        if not bwrap:
            self.skipTest("bubblewrap is unavailable")
        with tempfile.TemporaryDirectory(dir=ROOT / "artifacts") as tmp:
            root = Path(tmp).resolve()
            pid_file = root / "detached-child.pid"
            child_code = (
                "import os,pathlib,sys,time; "
                "ids=next(line for line in open('/proc/self/status') if line.startswith('NSpid:')).split()[1:]; "
                "pathlib.Path(sys.argv[1]).write_text(str(os.stat('/proc/self/ns/pid').st_ino)+' '+ids[-1]); "
                "time.sleep(120)"
            )
            supervisor_code = (
                "import subprocess,sys,time; "
                "subprocess.Popen(['/usr/bin/python3.10','-c',sys.argv[2],sys.argv[1]], "
                "start_new_session=True); "
                "time.sleep(120)"
            )
            command = [bwrap, "--unshare-pid", "--die-with-parent", "--ro-bind", "/usr", "/usr",
                       "--ro-bind", "/lib", "/lib"]
            if Path("/lib64").exists():
                command += ["--ro-bind", "/lib64", "/lib64"]
            command += ["--proc", "/proc", "--dev", "/dev", "--bind", str(root), str(root),
                        "--", "/usr/bin/python3.10", "-c", supervisor_code,
                        str(pid_file), child_code]
            wrapper = subprocess.Popen(command, stdin=subprocess.DEVNULL,
                                       stdout=subprocess.DEVNULL, stderr=subprocess.PIPE,
                                       start_new_session=True, env={})
            namespace_inode = inner_pid = None
            try:
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline and wrapper.poll() is None:
                    if pid_file.exists():
                        namespace_inode, inner_pid = map(int, pid_file.read_text().split())
                        break
                    time.sleep(0.05)
                if namespace_inode is None:
                    stderr = wrapper.stderr.read().decode(errors="replace") if wrapper.stderr else ""
                    if wrapper.poll() is not None and "Operation not permitted" in stderr:
                        self.skipTest("bubblewrap PID namespace unavailable in this environment")
                    self.fail(f"detached child did not start; wrapper={wrapper.poll()} stderr={stderr}")
                _terminate_group(wrapper, grace=1)
                deadline = time.monotonic() + 10
                while time.monotonic() < deadline:
                    live_members = []
                    for proc_path in Path("/proc").glob("[0-9]*"):
                        try:
                            if os.stat(proc_path / "ns/pid").st_ino != namespace_inode:
                                continue
                            status = (proc_path / "status").read_text().splitlines()
                            nspid_line = next(line for line in status if line.startswith("NSpid:"))
                            if int(nspid_line.split()[-1]) == inner_pid:
                                process_state = (proc_path / "stat").read_text().split()[2]
                                if process_state != "Z":
                                    live_members.append(proc_path.name)
                        except (FileNotFoundError, PermissionError, StopIteration, ValueError):
                            continue
                    if not live_members:
                        break
                    time.sleep(0.05)
                else:
                    self.fail(f"detached namespace child survived wrapper termination: {live_members}")
            finally:
                if wrapper.poll() is None:
                    _terminate_group(wrapper, grace=1)
                if wrapper.stderr:
                    wrapper.stderr.close()

    def test_real_llama_server_dependencies_resolve_without_host_alias(self):
        server = ROOT / "third_party/llama.cpp/build-v100/bin/llama-server"
        if not server.is_file():
            self.skipTest("pinned llama-server build is not present")
        directories, dependencies, ldd_output = resolve_server_libraries(server)
        self.assertIn(str(server.parent.resolve()), directories)
        self.assertIn("libllama-server-impl.so", ldd_output)
        self.assertTrue(any(item["soname"] == "libllama-server-impl.so" for item in dependencies))
        self.assertNotIn("/localscratch/", os.pathsep.join(directories))
        env = {"PATH": "/usr/bin:/bin", "HOME": "/tmp",
               "LD_LIBRARY_PATH": os.pathsep.join(directories)}
        result = preflight_server(server, env)
        version_text = result["stdout"] + result["stderr"]
        self.assertIn("version:", version_text)
        self.assertIn("d812350", version_text)

    def test_real_llama_server_preflight_inside_launcher_namespace(self):
        server = ROOT / "third_party/llama.cpp/build-v100/bin/llama-server"
        if not server.is_file():
            self.skipTest("pinned llama-server build is not present")
        directories, _, _ = resolve_server_libraries(server)
        with tempfile.TemporaryDirectory(dir=ROOT / "artifacts") as tmp:
            wrapper = _bwrap_command(ROOT / "config/inference.json",
                                     "third_party/llama.cpp/build-v100/bin/llama-server",
                                     Path(tmp), "dependency-preflight", None, None, None, None,
                                     directories,
                                     ["/usr/bin/true"])
            separator = wrapper.index("--")
            wrapper[separator:separator] = ["--setenv", "LD_LIBRARY_PATH",
                                             os.pathsep.join(directories)]
            separator = wrapper.index("--")
            wrapper[separator + 1:] = [str(server.resolve()), "--version"]
            result = subprocess.run(wrapper, capture_output=True, text=True, timeout=30, env={})
        version_text = result.stdout + result.stderr
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertIn("version:", version_text)
        self.assertIn("d812350", version_text)


if __name__ == "__main__":
    unittest.main()
