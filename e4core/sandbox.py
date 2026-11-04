"""Fail-closed bubblewrap runner for model-generated Python operators."""

from __future__ import annotations

import json
import base64
import hashlib
import os
import selectors
import shutil
import signal
import socket
import subprocess
import tempfile
import time
import zlib
from pathlib import Path
from typing import Any, Mapping


DEFAULT_LIMITS = {
    "wall_seconds": 5.0,
    "cpu_seconds": 2,
    "memory_bytes": 512 * 1024 * 1024,
    "stdout_bytes": 16 * 1024,
    "stderr_bytes": 16 * 1024,
    "protocol_bytes": 64 * 1024,
    "input_bytes": 2 * 1024 * 1024,
    "file_bytes": 1024 * 1024,
    "open_files": 32,
}

_WORKER = Path(__file__).with_name("sandbox_worker.py")
_IR_WORKER = Path(__file__).with_name("m4_operator_ir_worker.py")
_IR_V2_WORKER = Path(__file__).with_name("m4_operator_ir_v2_worker.py")
_OPERATOR_CODE_API = Path(__file__).with_name("operator_code_api.py")
_IR_RESULT_TRANSPORT_VERSION = "e4-typed-ir-result/1"
_IR_RESULT_TRANSPORT_CODEC = "zlib+base64"
_IR_RESULT_MAX_BYTES = 1024 * 1024


class _IRTransportError(ValueError):
    pass


def _reject_duplicate_json_keys(pairs):
    result = {}
    for key, value in pairs:
        if key in result:
            raise _IRTransportError("typed IR transport JSON contains a duplicate key")
        result[key] = value
    return result


def _bounded_json_size(value: Any, maximum_bytes: int) -> tuple[int, bool]:
    """Measure a parsed result incrementally, without retaining another JSON copy."""
    encoder = json.JSONEncoder(allow_nan=False, separators=(",", ":"), ensure_ascii=True)
    total = 0
    for chunk in encoder.iterencode(value):
        for offset in range(0, len(chunk), 8192):
            total += len(chunk[offset:offset + 8192].encode("utf-8"))
            if total > maximum_bytes:
                return total, False
    return total, True


def _decode_ir_result_transport(response: Any, *, authoring_profile: str | None,
                                decoded_limit_bytes: int = _IR_RESULT_MAX_BYTES) -> Any:
    """Decode a compressed result envelope only for the trusted typed-IR invocation."""
    if authoring_profile not in {"typed-operator-ir-v1", "typed-operator-ir-v2"} or type(response) is not dict:
        return response
    if response.get("ok") is not True:
        return response
    if set(response) != {"ok", "result", "stdout", "stderr"}:
        raise _IRTransportError("typed IR worker success response has an unexpected outer shape")
    envelope = response.get("result")
    if not isinstance(envelope, dict) or "$transport" not in envelope:
        try:
            measured, fits = _bounded_json_size(envelope, decoded_limit_bytes)
        except (TypeError, ValueError, RecursionError) as exc:
            raise _IRTransportError("typed IR plain result is not finite JSON data") from exc
        if not fits:
            raise _IRTransportError(
                f"typed IR plain result exceeds the {decoded_limit_bytes}-byte decoded limit")
        return response
    expected_keys = {"$transport", "codec", "decoded_bytes", "sha256", "payload"}
    if set(envelope) != expected_keys:
        raise _IRTransportError("typed IR result envelope has missing or extra fields")
    if (envelope.get("$transport") != _IR_RESULT_TRANSPORT_VERSION or
            envelope.get("codec") != _IR_RESULT_TRANSPORT_CODEC):
        raise _IRTransportError("typed IR result envelope version or codec is unsupported")
    decoded_size = envelope.get("decoded_bytes")
    digest = envelope.get("sha256")
    encoded = envelope.get("payload")
    if (type(decoded_size) is not int or decoded_size < 0 or decoded_size > decoded_limit_bytes or
            not isinstance(digest, str) or len(digest) != 64 or
            any(char not in "0123456789abcdef" for char in digest) or
            not isinstance(encoded, str)):
        raise _IRTransportError("typed IR result envelope length, digest, or payload is malformed")
    try:
        compressed = base64.b64decode(encoded.encode("ascii"), validate=True)
    except (UnicodeEncodeError, ValueError) as exc:
        raise _IRTransportError("typed IR result payload is not strict ASCII base64") from exc
    try:
        decompressor = zlib.decompressobj()
        decoded = decompressor.decompress(compressed, decoded_limit_bytes + 1)
    except zlib.error as exc:
        raise _IRTransportError("typed IR result zlib stream is invalid") from exc
    if len(decoded) > decoded_limit_bytes:
        raise _IRTransportError("typed IR result exceeds the decoded byte limit")
    if decompressor.unconsumed_tail:
        raise _IRTransportError("typed IR result exceeds the bounded decompression allowance")
    if not decompressor.eof:
        raise _IRTransportError("typed IR result zlib stream is incomplete")
    if decompressor.unused_data:
        raise _IRTransportError("typed IR result has trailing data or a concatenated stream")
    if len(decoded) != decoded_size or hashlib.sha256(decoded).hexdigest() != digest:
        raise _IRTransportError("typed IR result decoded length or SHA-256 does not match its envelope")

    def reject_constant(token):
        raise _IRTransportError(f"typed IR result contains non-finite JSON value {token}")
    try:
        value = json.loads(decoded.decode("utf-8"), object_pairs_hook=_reject_duplicate_json_keys,
            parse_constant=reject_constant)
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as exc:
        raise _IRTransportError("typed IR result payload is not valid UTF-8 JSON") from exc
    return {**response, "result": value}


def _ir_statistic(program: Mapping[str, Any]) -> str:
    nodes = program.get("nodes", [])
    def has_kernel(name: str) -> bool:
        return any(isinstance(row, Mapping) and row.get("kernel") == name and
            str(row.get("version")) == "1" for row in nodes)

    correlation = has_kernel("series.correlate")
    reducer = next((row for row in nodes if isinstance(row, Mapping) and
        row.get("kernel") == "series.reduce" and str(row.get("version")) == "1"), None)
    compare = next((row for row in nodes if isinstance(row, Mapping) and
        row.get("kernel") == "numbers.window_compare" and str(row.get("version")) == "1"), None)
    magnitude = has_kernel("numbers.absolute")
    if correlation and reducer is None and compare is None:
        return "component_kpi_association_magnitude" if magnitude else "component_kpi_association"
    if correlation:
        statistic = reducer.get("params", {}).get("statistic", "metric") if reducer else "metric"
        operation = compare.get("params", {}).get("operation", "score") if compare else "score"
        return f"component_kpi_association_weighted_{statistic}_{operation}"
    if reducer is not None:
        statistic = reducer.get("params", {}).get("statistic", "metric")
        operation = compare.get("params", {}).get("operation", "summary") if compare else "summary"
        return f"component_{statistic}_{operation}"
    return "component_numeric_analysis"


def _limits(overrides: Mapping[str, Any] | None) -> dict[str, Any]:
    result = dict(DEFAULT_LIMITS)
    for key, value in (overrides or {}).items():
        if key not in result:
            raise ValueError(f"unknown sandbox limit: {key}")
        result[key] = value
    if (type(result["wall_seconds"]) not in (int, float) or result["wall_seconds"] <= 0 or
            type(result["cpu_seconds"]) is not int or result["cpu_seconds"] <= 0):
        raise ValueError("wall_seconds and cpu_seconds must be positive")
    for name in ("memory_bytes", "stdout_bytes", "stderr_bytes", "protocol_bytes",
                 "input_bytes", "file_bytes", "open_files"):
        if type(result[name]) is not int or result[name] <= 0:
            raise ValueError(f"{name} must be a positive integer")
    return result


def _bwrap_base() -> list[str]:
    bwrap = shutil.which("bwrap")
    python = "/usr/bin/python3.10"
    if (not bwrap or not os.path.exists(python) or not _WORKER.is_file() or
            not _IR_WORKER.is_file() or not _IR_V2_WORKER.is_file() or not _OPERATOR_CODE_API.is_file()):
        raise RuntimeError("bubblewrap or Python 3.10 worker runtime is unavailable")
    args = [bwrap, "--unshare-all", "--die-with-parent",
            "--clearenv", "--setenv", "PATH", "/usr/bin", "--setenv", "LANG", "C.UTF-8",
            "--setenv", "OMP_NUM_THREADS", "1", "--setenv", "OPENBLAS_NUM_THREADS", "1",
            "--setenv", "MKL_NUM_THREADS", "1",
            "--ro-bind", "/usr", "/usr", "--ro-bind", "/lib", "/lib",
            "--proc", "/proc", "--dev", "/dev", "--tmpfs", "/tmp",
            "--dir", "/sandbox", "--chdir", "/tmp"]
    if os.path.exists("/lib64"):
        args += ["--ro-bind", "/lib64", "/lib64"]
    if os.path.exists("/etc/ld.so.cache"):
        args += ["--ro-bind", "/etc/ld.so.cache", "/etc/ld.so.cache"]
    args += ["--ro-bind", str(_WORKER), "/sandbox/worker.py",
             "--ro-bind", str(_IR_WORKER), "/sandbox/m4_operator_ir_worker.py",
             "--ro-bind", str(_IR_V2_WORKER), "/sandbox/m4_operator_ir_v2_worker.py",
             "--ro-bind", str(_OPERATOR_CODE_API), "/sandbox/operator_code_api.py", "--", python, "-I", "-S",
             "/sandbox/worker.py"]
    return args


def _json_safe(value: Any, where: str) -> None:
    try:
        json.dumps(value, allow_nan=False, separators=(",", ":"))
    except (TypeError, ValueError) as exc:
        raise ValueError(f"{where} must be finite JSON data: {exc}") from exc


def _check_output_type(value: Any, output_type: str) -> str:
    types = {
        "json": lambda v: True,
        "object": lambda v: type(v) is dict,
        "array": lambda v: type(v) is list,
        "string": lambda v: type(v) is str,
        "boolean": lambda v: type(v) is bool,
        "integer": lambda v: type(v) is int,
        "number": lambda v: type(v) in (int, float),
    }
    predicate = types.get(output_type.lower())
    if predicate is None:
        if output_type in {"Telemetry", "Aggregate", "CandidateSet", "ScoredCandidates", "RankedAnswer"}:
            from .cache import deserialize_contract_json
            deserialize_contract_json(value, output_type)
            return "checked_typed_contract"
        raise LookupError(f"unsupported operator output type {output_type}")
    if not predicate(value):
        raise ValueError(f"result does not satisfy output type {output_type}")
    return "checked"


def _validate_worker_response(response: Any) -> dict[str, Any]:
    if type(response) is not dict:
        raise ValueError("worker response must be a JSON object")
    if type(response.get("ok")) is not bool:
        raise ValueError("worker response must contain a boolean ok field")
    for name in ("stdout", "stderr"):
        if not isinstance(response.get(name, ""), str):
            raise ValueError(f"worker response {name} must be text")
    if response["ok"]:
        if "result" not in response:
            raise ValueError("successful worker response is missing result")
    else:
        error = response.get("error")
        if (type(error) is not dict or not isinstance(error.get("code"), str) or
                not isinstance(error.get("message"), str)):
            raise ValueError("failed worker response has malformed error")
    return response


def run_generated_operator(source: str | None, inputs: Any, params: Any, runtime: Any,
                           output_type: str, limits: Mapping[str, Any] | None = None,
                           case_dir: str | Path | None = None,
                           catalog_path: str | Path | None = None, *,
                           authoring_profile: str | None = None,
                           typecheck_spec: Mapping[str, Any] | None = None,
                           ir_program: Mapping[str, Any] | None = None,
                           trusted_context: Mapping[str, Any] | None = None) -> dict[str, Any]:
    """Run one three-argument generated operator in a fresh isolated process.

    Explicit arguments must be JSON excerpts; filesystem paths are never
    accepted through them. Optional mounts are fixed at /case and /catalog.json.
    """
    limit = _limits(limits)
    _json_safe(inputs, "inputs")
    _json_safe(params, "params")
    _json_safe(runtime, "runtime")
    if authoring_profile in {"typed-operator-ir-v1", "typed-operator-ir-v2"}:
        if (source is not None or not isinstance(typecheck_spec, Mapping) or
                typecheck_spec.get("authoring_profile") != authoring_profile or
                not isinstance(ir_program, Mapping) or typecheck_spec.get("program") != ir_program or
                not isinstance(trusted_context, Mapping)):
            return _failure("typed_ir_spec_required", "typed operator IR requires its checked source-free spec and trusted context", "")
        try:
            from .m4_generated import validate_generated_spec
            checked = validate_generated_spec(typecheck_spec)
            if checked != dict(typecheck_spec):
                raise ValueError("normalized typed operator IR identity differs")
            _json_safe(trusted_context, "trusted_context")
        except Exception as exc:
            return _failure("typed_ir_spec_mismatch", str(exc), "")
        # This is a fixed bounded result allowance, declared to the worker and
        # independently enforced by the host decoder; it does not raise the
        # existing 64 KiB response-wire cap.
        limit["ir_result_bytes"] = _IR_RESULT_MAX_BYTES
    elif not isinstance(source, str):
        raise ValueError("source must be text")
    if not isinstance(output_type, str) or not output_type:
        raise ValueError("output_type must be a non-empty string")
    if authoring_profile is not None and authoring_profile not in {
            "typed-operator-ir-v1", "typed-operator-ir-v2"}:
        from .m4_typecheck import PROFILE, OPERATOR_CODE_PROFILE, check_generated_spec_types, TypedAuthoringError
        if authoring_profile not in {PROFILE, OPERATOR_CODE_PROFILE}:
            return _failure("unsupported_authoring_profile",
                            f"unsupported authoring_profile {authoring_profile!r}", source)
        if not isinstance(typecheck_spec, Mapping) or typecheck_spec.get("authoring_profile") != authoring_profile:
            return _failure("typed_spec_required", f"{authoring_profile} execution requires its checked normalized spec", source)
        if typecheck_spec.get("source") != source or typecheck_spec.get("output_type") != output_type:
            return _failure("typed_spec_mismatch",
                "typed execution source and output type must match the checked spec", source)
        try:
            check_generated_spec_types(typecheck_spec)
        except TypedAuthoringError as exc:
            return _failure(exc.code, str(exc), source)
    elif authoring_profile is None and typecheck_spec is not None:
        return _failure("typed_spec_without_profile", "typecheck_spec requires authoring_profile", source)
    if output_type.lower() not in {"json", "object", "array", "string", "boolean", "integer", "number"} and output_type not in {
            "Telemetry", "Aggregate", "CandidateSet", "ScoredCandidates", "RankedAnswer"}:
        return _failure("unsupported_output_type", f"unsupported operator output type {output_type}", source)
    mounts: list[tuple[Path, str]] = []
    for raw, target, is_dir in ((case_dir, "/case", True), (catalog_path, "/catalog.json", False)):
        if raw is None:
            continue
        path = Path(raw).resolve(strict=True)
        if is_dir and not path.is_dir():
            raise ValueError("case_dir must be a directory")
        if not is_dir and not path.is_file():
            raise ValueError("catalog_path must be a file")
        mounts.append((path, target))
    request = {"source": source, "inputs": inputs, "params": params,
               "runtime": runtime, "output_type": output_type,
               "limits": limit}
    if authoring_profile is not None:
        request["authoring_profile"] = authoring_profile
    if authoring_profile in {"typed-operator-ir-v1", "typed-operator-ir-v2"}:
        request["typed_operator_ir"] = ir_program
        request["trusted_context"] = dict(trusted_context or {})
        request["ir_family"] = typecheck_spec.get("family")
        request["ir_statistic"] = _ir_statistic(ir_program)
    payload = json.dumps(request, allow_nan=False, separators=(",", ":")).encode()
    if len(payload) > limit["input_bytes"]:
        return _failure("input_limit", "serialized operator request exceeds input limit", source)
    try:
        command = _bwrap_base()
        for path, target in mounts:
            command.insert(command.index("--"), "--ro-bind")
            command.insert(command.index("--"), str(path))
            command.insert(command.index("--"), target)
        # File size limits do not constrain pipes, so parent drains and caps both streams.
        proc = subprocess.Popen(command, stdin=subprocess.PIPE, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, start_new_session=True, close_fds=True,
                                env={})
    except Exception as exc:
        return _failure("sandbox_unavailable", str(exc), source)

    selector = selectors.DefaultSelector()
    assert proc.stdin and proc.stdout and proc.stderr
    selector.register(proc.stdout, selectors.EVENT_READ, "stdout")
    selector.register(proc.stderr, selectors.EVENT_READ, "stderr")
    out = bytearray()
    err = bytearray()
    input_view = memoryview(payload)
    input_pos = 0
    # Make all three pipes nonblocking; writes and reads proceed together.
    for stream in (proc.stdin, proc.stdout, proc.stderr):
        os.set_blocking(stream.fileno(), False)
    selector.register(proc.stdin, selectors.EVENT_WRITE, "stdin")
    deadline = time.monotonic() + limit["wall_seconds"]
    overflow: str | None = None
    while selector.get_map() or proc.poll() is None:
        remaining = deadline - time.monotonic()
        if remaining <= 0:
            overflow = "timeout"
            break
        events = selector.select(min(remaining, 0.1))
        for key, _ in events:
            stream, label = key.fileobj, key.data
            try:
                if label == "stdin":
                    if input_pos == len(input_view):
                        selector.unregister(stream)
                        stream.close()
                    else:
                        sent = os.write(stream.fileno(), input_view[input_pos:input_pos + 65536])
                        input_pos += sent
                else:
                    chunk = os.read(stream.fileno(), 65536)
                    if not chunk:
                        selector.unregister(stream)
                        continue
                    target = out if label == "stdout" else err
                    cap = limit["protocol_bytes"] if label == "stdout" else limit["stderr_bytes"]
                    if len(target) + len(chunk) > cap:
                        target.extend(chunk[:max(0, cap - len(target))])
                        overflow = "output_limit"
                        break
                    target.extend(chunk)
            except BrokenPipeError:
                selector.unregister(stream)
                stream.close()
        if overflow:
            break
    if overflow:
        try:
            os.killpg(proc.pid, signal.SIGKILL)
        except ProcessLookupError:
            pass
    try:
        proc.wait(timeout=1)
    except subprocess.TimeoutExpired:
        proc.kill()
        proc.wait()
    selector.close()
    for stream in (proc.stdin, proc.stdout, proc.stderr):
        if stream is not None:
            try:
                stream.close()
            except OSError:
                pass
    stdout_text = out.decode("utf-8", "replace")
    stderr_text = err.decode("utf-8", "replace")
    if overflow:
        code = "timeout" if overflow == "timeout" else "output_limit"
        return {"ok": False, "result": None,
                "error": {"code": code, "message": "operator exceeded " +
                          ("wall-time limit" if code == "timeout" else "protocol output limit")},
                "stdout": stdout_text, "stderr": stderr_text, "source": source,
                "isolation": {"backend": "bubblewrap", "requested_namespaces":
                              ["user", "pid", "ipc", "uts", "network", "mount"], "active": False}}
    try:
        parsed_response = json.loads(stdout_text,
            object_pairs_hook=(_reject_duplicate_json_keys
                if authoring_profile in {"typed-operator-ir-v1", "typed-operator-ir-v2"} else None))
        parsed_response = _decode_ir_result_transport(parsed_response,
            authoring_profile=authoring_profile,
            decoded_limit_bytes=_IR_RESULT_MAX_BYTES)
        response = _validate_worker_response(parsed_response)
    except _IRTransportError as exc:
        response = {"ok": False, "result": None,
            "error": {"code": "ir_transport_invalid", "message": str(exc)[:500]},
            "stdout": "", "stderr": ""}
    except Exception:
        if proc.returncode not in (0, None):
            return {"ok": False, "result": None,
                    "error": {"code": "resource_limit" if proc.returncode < 0 or proc.returncode in (137, 152)
                              else "worker_exit",
                              "message": "worker exited before returning a result",
                              "exit_code": proc.returncode},
                    "stdout": stdout_text, "stderr": stderr_text, "source": source,
                    "isolation": {"backend": "bubblewrap", "active": True,
                                  "exit_code": proc.returncode}}
        return {"ok": False, "result": None,
                "error": {"code": "worker_protocol_error", "message": "worker returned invalid JSON",
                          "exit_code": proc.returncode},
                "stdout": stdout_text, "stderr": stderr_text, "source": source,
                "isolation": {"backend": "bubblewrap", "active": proc.returncode == 0}}
    if response.get("ok"):
        try:
            validation = _check_output_type(response.get("result"), output_type)
        except LookupError as exc:
            response = {"ok": False, "result": None,
                        "error": {"code": "unsupported_output_type", "message": str(exc)}}
        except (TypeError, ValueError, RecursionError) as exc:
            response = {"ok": False, "result": None,
                        "error": {"code": "output_type_mismatch", "message": str(exc)}}
        else:
            response["output_validation"] = validation
            if validation == "checked_typed_contract":
                # Preserve the worker's JSON wire value; parent code may decode it
                # independently through the same trusted contract decoder.
                response["validated_contract_type"] = output_type
    return {**response, "stdout": response.get("stdout", ""),
            "stderr": (response.get("stderr", "") + stderr_text)[:limit["stderr_bytes"]],
            "source": source,
            "isolation": {"backend": "bubblewrap", "requested_namespaces":
                          ["user", "pid", "ipc", "uts", "network", "mount"],
                          "active": proc.returncode == 0, "exit_code": proc.returncode,
                          "mounts": [target for _, target in mounts]}}


def _failure(code: str, message: str, source: str) -> dict[str, Any]:
    return {"ok": False, "result": None, "error": {"code": code, "message": message[:1000]},
            "stdout": "", "stderr": "", "source": source,
            "isolation": {"backend": "bubblewrap", "active": False}}


def probe_os_isolation(marker_path: str | Path) -> dict[str, Any]:
    """Trusted probe bypassing generated-code AST guards but using namespace builder."""
    path = Path(marker_path).resolve(strict=True)
    python = "/usr/bin/python3.10"
    host_listener = socket.socket()
    host_listener.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    host_listener.bind(("127.0.0.1", 0))
    host_listener.listen(1)
    host_listener.settimeout(0.2)
    listener_port = host_listener.getsockname()[1]
    probe = (
        "import os,socket; checks={}; "
        f"checks['host_marker_hidden']=not os.path.exists({str(path)!r}); "
        "checks['etc_passwd_hidden']=not os.path.exists('/etc/passwd'); "
        "checks['visible_interfaces']=[n for _,n in socket.if_nameindex()]; "
        "checks['network_isolated']=all(n == 'lo' for n in checks['visible_interfaces']); "
        "s=socket.socket(); s.settimeout(.25); "
        f"\ntry:\n s.connect(('127.0.0.1',{listener_port})); checks['host_listener_hidden']=False\n"
        "except OSError: checks['host_listener_hidden']=True\n"
        "print(__import__('json').dumps(checks))"
    )
    try:
        command = _bwrap_base()
        command[command.index("--") + 1:] = [python, "-I", "-S", "-c", probe]
        # Permit trusted probe's stdlib imports while preserving the same mounts/namespaces.
        result = subprocess.run(command, stdin=subprocess.DEVNULL, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, timeout=5, env={}, text=True)
        observed = json.loads(result.stdout)
        return {"ok": result.returncode == 0 and all(observed.values()),
                "observed": observed, "exit_code": result.returncode,
                "stderr": result.stderr[:2000]}
    except Exception as exc:
        return {"ok": False, "observed": {}, "error": str(exc)}
    finally:
        host_listener.close()
