"""A model E4 analyzes with: where it is served, how to call it, and, for a model served on this
machine, how to serve it. Each model is one JSON file in config/models/<name>.json."""
from __future__ import annotations

import json
import os
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any
from urllib.parse import urlsplit

from e4.paths import ROOT

MODELS = ROOT / "config/models"
ENV_FILE = ROOT / ".env"
TOKEN_COUNTING = ("llama_server", "none")


class ModelConfigError(ValueError):
    """A model file is missing a field, has a wrong value, or its API key cannot be found."""


@dataclass(frozen=True)
class LocalServer:
    """How scripts/model/run.py serves the model with llama-server before running E4."""
    model_path: str
    model_manifest_path: str
    reasoning_budget: int | None = None
    reasoning_format: str | None = None
    server_args: tuple[str, ...] = ()
    parallel_requests: int = 1
    minimum_free_vram_mib: int = 3072
    init_timeout_seconds: int = 1800


@dataclass(frozen=True)
class AnalysisModel:
    """One model: its endpoint, the sampling and token limits sent with every call, and an optional
    local server. A field set to null in the file is left out of the request."""
    name: str
    path: Path | None
    base_url: str
    model_alias: str
    context_tokens: int
    max_generation_tokens: int
    request_timeout_seconds: float = 600
    temperature: float | None = None
    top_p: float | None = None
    top_k: int | None = None
    seed: int | None = None
    reasoning_mode: str | None = None
    token_counting: str = "llama_server"
    api_key_env: str | None = None
    local_server: LocalServer | None = None
    document: dict[str, Any] = field(default_factory=dict, compare=False, repr=False)

    @property
    def is_local(self) -> bool:
        return self.local_server is not None


def model_path(name: str) -> Path:
    return MODELS / f"{name}.json"


def load_model(name: str) -> AnalysisModel:
    """config/models/<name>.json -> its AnalysisModel."""
    path = model_path(name)
    if not path.is_file():
        raise ModelConfigError(f"no model file {path}")
    return model_from_document(name, json.loads(path.read_text(encoding="utf-8")), path)


def model_from_environment() -> AnalysisModel:
    """The model a run was started with: scripts/model/run.py exports its name as E4_MODEL."""
    name = os.environ.get("E4_MODEL")
    if not name:
        raise ModelConfigError("E4_MODEL is not set; start the run with scripts/model/run.py --model <name>")
    return load_model(name)


def model_from_document(name: str, document: dict[str, Any], path: Path | None = None) -> AnalysisModel:
    """A model file's parsed JSON -> its AnalysisModel."""
    for required in ("base_url", "model_alias", "context_tokens", "max_generation_tokens"):
        if required not in document:
            raise ModelConfigError(f"{name} is missing {required}")
    server = document.get("local_server")
    local_server = None
    if server is not None:
        local_server = LocalServer(
            model_path=server["model_path"], model_manifest_path=server["model_manifest_path"],
            reasoning_budget=server.get("reasoning_budget"),
            reasoning_format=server.get("reasoning_format"),
            server_args=tuple(server.get("server_args", ())),
            parallel_requests=server.get("parallel_requests", 1),
            minimum_free_vram_mib=server.get("minimum_free_vram_mib", 3072),
            init_timeout_seconds=server.get("init_timeout_seconds", 1800))
    model = AnalysisModel(
        name=name, path=path, base_url=str(document["base_url"]).rstrip("/"),
        model_alias=document["model_alias"], context_tokens=int(document["context_tokens"]),
        max_generation_tokens=int(document["max_generation_tokens"]),
        request_timeout_seconds=float(document.get("request_timeout_seconds", 600)),
        temperature=document.get("temperature"), top_p=document.get("top_p"),
        top_k=document.get("top_k"), seed=document.get("seed"),
        reasoning_mode=document.get("reasoning_mode"),
        token_counting=document.get("token_counting", "llama_server"),
        api_key_env=document.get("api_key_env"), local_server=local_server, document=document)
    check_model(model)
    return model


def check_model(model: AnalysisModel) -> None:
    """A local model has a plain 127.0.0.1 HTTP address; a remote one has HTTPS and a key."""
    if model.token_counting not in TOKEN_COUNTING:
        raise ModelConfigError(f"{model.name}: token_counting must be one of {TOKEN_COUNTING}")
    if model.context_tokens <= 0 or model.max_generation_tokens <= 0:
        raise ModelConfigError(f"{model.name}: token limits must be positive")
    address = urlsplit(model.base_url)
    if (address.username is not None or address.password is not None or address.path not in {"", "/"}
            or address.query or address.fragment):
        raise ModelConfigError(f"{model.name}: base_url must be a plain scheme://host[:port]")
    if model.api_key_env is None:
        if address.scheme != "http" or address.hostname != "127.0.0.1" or address.port is None:
            raise ModelConfigError(f"{model.name}: a model without api_key_env must use "
                                   "http://127.0.0.1:<port>")
    elif address.scheme != "https":
        raise ModelConfigError(f"{model.name}: a model with an API key must use https")
    if model.is_local and model.api_key_env is not None:
        raise ModelConfigError(f"{model.name}: a locally served model takes no API key")


def read_env_file(path: Path) -> dict[str, str]:
    """KEY=VALUE lines of a .env file; blank lines and # comments are skipped, quotes stripped."""
    values = {}
    if not path.is_file():
        return values
    for line in path.read_text(encoding="utf-8").splitlines():
        line = line.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, value = line.split("=", 1)
        value = value.strip()
        if len(value) >= 2 and value[0] == value[-1] and value[0] in "'\"":
            value = value[1:-1]
        values[key.strip()] = value
    return values


def api_key(model: AnalysisModel, env_file: Path = ENV_FILE) -> str:
    """The model's key: from the environment variable it names, else from the repo's .env file."""
    if model.api_key_env is None:
        raise ModelConfigError(f"{model.name} takes no API key")
    key = os.environ.get(model.api_key_env) or read_env_file(env_file).get(model.api_key_env)
    if not key:
        raise ModelConfigError(f"{model.api_key_env} is not set; copy .env.example to .env and fill it in")
    return key
