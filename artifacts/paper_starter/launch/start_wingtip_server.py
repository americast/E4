#!/usr/bin/env python3
"""Start the pinned Wingtip Gemma server for one fresh paper-starter run root."""

from __future__ import annotations

import argparse
import base64
import hashlib
import json
import os
import shlex
import subprocess
import sys
from datetime import datetime, timezone
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[3]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

REMOTE_HOST = "ssinha97@wingtip-gpu2.cc.gatech.edu"
REMOTE_BASE = "/tmp/ssinha97-e4-v7-20261020"
MODEL_RELATIVE = "model/gemma-4-31B_q4_0-it.gguf"
SERVER_RELATIVE = "bin-sm75/llama-server"
CUDA_RELATIVE = "bin-sm75/libggml-cuda.so.0.26.0"
MODEL_SHA256 = "179cfb99212709597eae5929112cfca677e1bbf566178b479ae1da0c4772874b"
SERVER_SHA256 = "e9858760f9bfd39c2e14868901f72d56f7abadbb8c9ca53e62f24f69bee3c03d"
CUDA_SHA256 = "c999f2cbb66edcf8d7a5b63754025c47924b3a53e06598fd91d5cf325278c7a4"
MODEL_SIZE = 17651001568
LLAMA_CPP_REVISION = "d81235049384534c167caea52b85a694f6103d14"
LIFETIME_SECONDS = 40000


class WingtipStartError(RuntimeError):
    pass


def _sha(path: Path) -> str:
    h = hashlib.sha256()
    with path.open("rb") as stream:
        for block in iter(lambda: stream.read(1024 * 1024), b""):
            h.update(block)
    return h.hexdigest()


def _write_exclusive(path: Path, data: bytes) -> None:
    fd = os.open(path, os.O_WRONLY | os.O_CREAT | os.O_EXCL, 0o644)
    with os.fdopen(fd, "wb") as stream:
        stream.write(data)
        stream.flush()
        os.fsync(stream.fileno())


def _remote_script() -> str:
    return r'''import hashlib,json,os,signal,socket,subprocess,sys,time,urllib.request
base,port,expected_model,expected_server,expected_cuda,model_size=sys.argv[1],int(sys.argv[2]),sys.argv[3],sys.argv[4],sys.argv[5],int(sys.argv[6])
model=base+"/model/gemma-4-31B_q4_0-it.gguf"
server=base+"/bin-sm75/llama-server"
cuda=base+"/bin-sm75/libggml-cuda.so.0.26.0"
libpath=base+"/bin-sm75:"+base+"/lib"
def emit(row): print(json.dumps(row,sort_keys=True,separators=(",",":")),flush=True)
def sha(path):
 h=hashlib.sha256()
 with open(path,"rb") as f:
  for block in iter(lambda:f.read(8*1024*1024),b""): h.update(block)
 return h.hexdigest()
def hprobe():
 with urllib.request.urlopen(f"http://127.0.0.1:{port}/health",timeout=3) as r:
  body=json.loads(r.read(4096).decode()); return {"http_status":r.status,"body":body,"checked_utc":time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime())}
def hardware():
 cmd=["nvidia-smi","--query-gpu=name,driver_version,memory.total,memory.used,compute_cap","--format=csv,noheader,nounits"]
 p=subprocess.run(cmd,capture_output=True,text=True,timeout=15,check=True)
 rows=[x.strip() for x in p.stdout.splitlines() if x.strip()]
 if len(rows)!=1: raise RuntimeError("expected exactly one visible GPU")
 fields=[x.strip() for x in rows[0].split(",")]
 if len(fields)!=5: raise RuntimeError("nvidia-smi GPU identity row is malformed")
 return {"name":fields[0],"driver_version":fields[1],"total_mib":int(fields[2]),"used_mib":int(fields[3]),"compute_capability":fields[4]}
def occupied_compute_apps():
 p=subprocess.run(["nvidia-smi","--query-compute-apps=pid,used_memory","--format=csv,noheader,nounits"],capture_output=True,text=True,timeout=15,check=True)
 return [line.strip() for line in p.stdout.splitlines() if line.strip()]
started=time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime())
state={"schema_version":"e4-paper-starter-wingtip-start/1","phase":"validating","verified":False,"started_utc":started,"remote_base":base,"port":port}
emit(state)
supervisor=None
try:
 for path in (model,server,cuda):
  if not os.path.isfile(path): raise RuntimeError("pinned remote file missing: "+path)
 if os.path.getsize(model)!=model_size: raise RuntimeError("remote model size differs from pinned size")
 model_sha,server_sha,cuda_sha=sha(model),sha(server),sha(cuda)
 if (model_sha,server_sha,cuda_sha)!=(expected_model,expected_server,expected_cuda): raise RuntimeError("remote model/server/CUDA SHA pin mismatch")
 gpu=hardware()
 if "Quadro RTX 8000" not in gpu["name"] or gpu["compute_capability"]!="7.5" or gpu["total_mib"]<46000: raise RuntimeError("remote GPU differs from pinned Quadro RTX 8000")
 apps=occupied_compute_apps()
 if apps: raise RuntimeError("remote GPU already has compute processes: "+repr(apps))
 if gpu["used_mib"]>3072: raise RuntimeError("remote GPU has unexpected memory use before launch")
 probe=socket.socket();
 try: probe.bind(("127.0.0.1",port))
 finally: probe.close()
 state.update({"phase":"preflight_complete","server_sha256":server_sha,"checkpoint_sha256":model_sha,"cuda_sha256":cuda_sha,"hardware":gpu})
 emit(state)
 command=[server,"--model",model,"--alias","e4-gemma-m3","--host","127.0.0.1","--port",str(port),"--ctx-size","32768","--n-gpu-layers","99","--parallel","1","--offline","--reasoning","on","--reasoning-budget","2048"]
 log_path=base+"/logs/server-paperstarter-"+str(port)+".log"
 log=open(log_path,"xb")
 timeout=["/usr/bin/timeout","--signal=TERM","--kill-after=10s","40000",*command]
 supervisor=subprocess.Popen(timeout,stdout=log,stderr=subprocess.STDOUT,env={**os.environ,"LD_LIBRARY_PATH":libpath},start_new_session=True)
 timeout_pid=supervisor.pid; pgid=os.getpgid(timeout_pid)
 server_pid=None
 deadline=time.monotonic()+180
 while time.monotonic()<deadline:
  if supervisor.poll() is not None: raise RuntimeError("timeout/server process exited during startup: "+str(supervisor.returncode))
  for entry in os.listdir("/proc"):
   if not entry.isdigit(): continue
   pid=int(entry)
   try:
    raw=open(f"/proc/{pid}/cmdline","rb").read().split(b"\0"); argv=[x.decode() for x in raw if x]
    if argv==command and os.getuid()==os.stat(f"/proc/{pid}").st_uid and os.getpgid(pid)==pgid:
     server_pid=pid; break
   except (OSError,ProcessLookupError): pass
  if server_pid is not None:
   try:
    health=hprobe()
    if health["http_status"]==200 and health["body"].get("status")=="ok": break
   except Exception: pass
  time.sleep(.5)
 else: raise TimeoutError("remote llama-server did not become healthy within 180 seconds")
 if server_pid is None: raise RuntimeError("healthy server PID was not found in timeout process group")
 stat=os.stat(f"/proc/{server_pid}"); exe=os.readlink(f"/proc/{server_pid}/exe")
 raw=open(f"/proc/{server_pid}/cmdline","rb").read().split(b"\0"); actual=[x.decode() for x in raw if x]
 if (stat.st_uid!=os.getuid() or exe!=server or actual!=command or os.getpgid(server_pid)!=pgid): raise RuntimeError("live llama-server identity differs from launch command")
 ready=time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime())
 gpu_after=hardware()
 provenance={"schema_version":"e4-paper-starter-wingtip-provenance/1","verified":True,
  "remote_host":"ssinha97@wingtip-gpu2.cc.gatech.edu","remote_base":base,
  "bind_host":"127.0.0.1","bind_port":port,"server_pid":server_pid,"timeout_pid":timeout_pid,
  "process_group_id":pgid,"server_uid":stat.st_uid,"server_owner_uid":stat.st_uid,
  "server_started_utc":started,"server_ready_utc":ready,"server_lifetime_limit_seconds":40000,
  "server_command":command,"process_command_verified":True,"server_log_path":log_path,
  "llama_cpp_revision":"d81235049384534c167caea52b85a694f6103d14",
  "llama_server_sha256":server_sha,"model_sha256":model_sha,"model_path":model,
  "model_size_bytes":model_size,"build_architecture":"75",
  "hardware":gpu_after,"health_check":health,
  "libraries":{"runtime_search_path":libpath,"cuda_runtime":"CUDA 12.3","cuda_sha256":cuda_sha},
  "verified_files":{"gemma-4-31B_q4_0-it.gguf":{"path":model,"sha256":model_sha,"size_bytes":model_size},
   "llama-server":{"path":server,"sha256":server_sha,"size_bytes":os.path.getsize(server)},
   "libggml-cuda.so.0.26.0":{"path":cuda,"sha256":cuda_sha,"size_bytes":os.path.getsize(cuda)}},
  "checkpoint_and_binary_independently_rehashed":True,
  "limitations":["V100 and RTX 8000 differ; no hardware-matched latency claim.","Independent five-case chains start from the same frozen starter."]}
 emit(provenance)
except Exception as exc:
 state.update({"phase":"failed","verified":False,"error":f"{type(exc).__name__}: {str(exc)[:1000]}","failed_utc":time.strftime("%Y-%m-%dT%H:%M:%SZ",time.gmtime())})
 if supervisor is not None:
  state["timeout_pid"]=supervisor.pid; state["process_group_id"]=os.getpgid(supervisor.pid) if supervisor.poll() is None else None
 emit(state); sys.exit(1)
'''


def start(run_root: Path, port: int) -> dict[str, Any]:
    run_root = run_root.resolve(strict=True)
    if not run_root.is_dir() or not run_root.is_relative_to(ROOT):
        raise WingtipStartError("run root must be an existing project directory")
    if not 1 <= port <= 65535:
        raise WingtipStartError("port must be within 1..65535")
    provenance_path = run_root / "wingtip_server_provenance.json"
    stdout_path = run_root / "wingtip_server_startup.stdout.log"
    stderr_path = run_root / "wingtip_server_startup.stderr.log"
    if any(path.exists() or path.is_symlink() for path in (provenance_path, stdout_path, stderr_path)):
        raise WingtipStartError("preserving existing Wingtip startup/provenance artifacts")
    model_manifest = json.loads((ROOT / "config/model_manifest.json").read_text(encoding="utf-8"))
    remote_script = _remote_script()
    remote_args = ["/usr/bin/python3", "-", REMOTE_BASE, str(port), model_manifest["sha256"],
        SERVER_SHA256, CUDA_SHA256, str(MODEL_SIZE)]
    remote_command = shlex.join(remote_args)
    ssh = ["ssh", "-T", "-S", "none", "-o", "BatchMode=yes",
        "-o", "StrictHostKeyChecking=yes", "-o", "ExitOnForwardFailure=yes",
        REMOTE_HOST, remote_command]
    stdout = b""
    stderr = b""
    returncode = -1
    error = None
    try:
        process = subprocess.run(ssh, input=remote_script.encode("utf-8"),
            capture_output=True, timeout=300, check=False)
        stdout, stderr, returncode = process.stdout, process.stderr, process.returncode
    except Exception as exc:
        error = f"{type(exc).__name__}: {str(exc)[:1000]}"
    _write_exclusive(stdout_path, stdout)
    _write_exclusive(stderr_path, stderr)
    records = []
    for line in stdout.decode("utf-8", "replace").splitlines():
        try:
            value = json.loads(line)
            if isinstance(value, dict): records.append(value)
        except json.JSONDecodeError:
            continue
    final = next((row for row in reversed(records)
                  if row.get("schema_version") == "e4-paper-starter-wingtip-provenance/1"), None)
    if error is None and returncode == 0 and final is not None and final.get("verified") is True:
        provenance = final
    else:
        provenance = next((row for row in reversed(records)
            if row.get("schema_version") == "e4-paper-starter-wingtip-start/1"), {
                "schema_version": "e4-paper-starter-wingtip-provenance/1"})
        provenance = {**provenance, "schema_version": "e4-paper-starter-wingtip-provenance/1",
            "verified": False, "ssh_exit_code": returncode,
            "startup_error": error or (final or {}).get("error") or "remote startup did not return verified provenance",
            "partial_startup_records": records,
            "startup_stdout_path": str(stdout_path.relative_to(ROOT)),
            "startup_stderr_path": str(stderr_path.relative_to(ROOT))}
    _write_exclusive(provenance_path,
        (json.dumps(provenance, ensure_ascii=False, sort_keys=True, indent=2, allow_nan=False) + "\n").encode())
    if provenance.get("verified") is not True:
        raise WingtipStartError(f"Wingtip server startup failed; partial provenance preserved at {provenance_path}")
    from artifacts.e4_improvement.remote_wingtip import remote_inference as transport
    attestation = transport.verify_remote_attestation(provenance_path, remote_port=port)
    return {"status": "ready", "run_root": str(run_root.relative_to(ROOT)),
        "provenance_path": str(provenance_path.relative_to(ROOT)),
        "provenance_sha256": _sha(provenance_path),
        "startup_stdout_path": str(stdout_path.relative_to(ROOT)),
        "startup_stderr_path": str(stderr_path.relative_to(ROOT)),
        "remote_attestation": attestation}


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--run-root", type=Path, required=True)
    parser.add_argument("--port", type=int, required=True)
    args = parser.parse_args()
    result = start(args.run_root, args.port)
    print(json.dumps(result, ensure_ascii=False, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
