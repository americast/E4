#!/usr/bin/env python3
"""Resolve and resumably fetch the pinned official GGUF without third-party packages."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parents[2]
MANIFEST = ROOT / "config/models/gemma_manifest.json"


def read_manifest() -> dict:
    return json.loads(MANIFEST.read_text())


def write_manifest(data: dict) -> None:
    tmp = MANIFEST.with_suffix(".json.tmp")
    tmp.write_text(json.dumps(data, indent=2) + "\n")
    tmp.replace(MANIFEST)


def resolve(data: dict) -> dict:
    repo = urllib.parse.quote(data["repository"], safe="/")
    url = f"https://huggingface.co/api/models/{repo}?blobs=true"
    req = urllib.request.Request(url, headers={"User-Agent": "E4-infrastructure/0.1"})
    with urllib.request.urlopen(req, timeout=45) as response:
        info = json.load(response)
    siblings = {item["rfilename"]: item for item in info.get("siblings", [])}
    if data["filename"] not in siblings:
        raise RuntimeError(f"requested model file missing from repository listing: {data['filename']}")
    item = siblings[data["filename"]]
    lfs = item.get("lfs") or {}
    sha = lfs.get("sha256")
    if not sha or len(sha) != 64:
        raise RuntimeError("Hub metadata did not provide a 64-character LFS SHA-256")
    data.update({"revision": info.get("sha"), "sha256": sha,
                 "size_bytes": lfs.get("size") or item.get("size")})
    if not data["revision"]:
        raise RuntimeError("Hub metadata did not provide a repository commit")
    return data


def digest(path: Path) -> str:
    result = hashlib.sha256()
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(8 * 1024 * 1024), b""):
            result.update(chunk)
    return result.hexdigest()


def fetch(data: dict, destination: Path) -> None:
    revision, expected = data.get("revision"), data.get("sha256")
    if not revision or not expected:
        raise RuntimeError("manifest must contain pinned revision and SHA-256; run --resolve first")
    destination.parent.mkdir(parents=True, exist_ok=True)
    if destination.exists():
        actual = digest(destination)
        if actual == expected:
            print(f"verified existing artifact: {destination} sha256={actual}")
            return
        raise RuntimeError(f"existing destination has wrong SHA-256 ({actual}); move it aside to retry")

    encoded_repo = urllib.parse.quote(data["repository"], safe="/")
    encoded_name = urllib.parse.quote(data["filename"], safe="/")
    url = f"https://huggingface.co/{encoded_repo}/resolve/{revision}/{encoded_name}?download=true"
    partial = destination.with_name(destination.name + ".part")
    offset = partial.stat().st_size if partial.exists() else 0
    headers = {"User-Agent": "E4-infrastructure/0.1"}
    if offset:
        headers["Range"] = f"bytes={offset}-"
    request = urllib.request.Request(url, headers=headers)
    try:
        response = urllib.request.urlopen(request, timeout=120)
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"Hub download failed with HTTP {exc.code}; partial file retained") from exc
    with response:
        append = offset > 0 and response.status == 206
        if offset and not append:
            offset = 0
        mode = "ab" if append else "wb"
        transferred = offset
        with partial.open(mode) as stream:
            while True:
                block = response.read(8 * 1024 * 1024)
                if not block:
                    break
                stream.write(block)
                transferred += len(block)
                print(f"downloaded_bytes={transferred}", file=sys.stderr, end="\r", flush=True)
    print(file=sys.stderr)
    if data.get("size_bytes") and partial.stat().st_size != data["size_bytes"]:
        raise RuntimeError(f"size mismatch: got {partial.stat().st_size}, expected {data['size_bytes']}; partial retained")
    actual = digest(partial)
    if actual != expected:
        raise RuntimeError(f"SHA-256 mismatch: got {actual}, expected {expected}; partial retained")
    os.replace(partial, destination)
    print(f"verified model: {destination} bytes={destination.stat().st_size} sha256={actual}")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--resolve", action="store_true", help="query Hub metadata and pin commit/checksum")
    parser.add_argument("--output", type=Path, default=None)
    args = parser.parse_args()
    data = read_manifest()
    if args.resolve:
        data = resolve(data)
        write_manifest(data)
        # Print only provenance fields; model metadata may include large/private templates.
        print(json.dumps({key: data[key] for key in ("repository", "filename", "revision", "sha256", "size_bytes")}, indent=2))
    fetch(data, args.output or ROOT / "data/models" / data["filename"])
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
