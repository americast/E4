#!/usr/bin/env python3
"""Fetch the six pinned OpenRCA Telecom metric/trace CSVs for selected alert dates."""

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
from urllib.parse import urljoin

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))
from e4.prep.telecom import sha256  # noqa: E402

EXPECTED = {
    "metric/metric_app.csv", "metric/metric_container.csv", "metric/metric_middleware.csv",
    "metric/metric_node.csv", "metric/metric_service.csv", "trace/trace_span.csv",
}


def api_tree(repo: str, revision: str, date: str = "") -> list[dict]:
    repo_q = urllib.parse.quote(repo, safe="/")
    path_q = urllib.parse.quote(f"Telecom/telemetry/{date}" if date else "", safe="/")
    suffix = f"/{path_q}" if path_q else ""
    url = (f"https://huggingface.co/api/datasets/{repo_q}/tree/{revision}{suffix}"
           "?recursive=true&expand=true")
    result = []
    while url:
        request = urllib.request.Request(url, headers={"User-Agent": "E4-OpenRCA-data/0.1"})
        with urllib.request.urlopen(request, timeout=60) as response:
            payload = json.load(response)
            if not isinstance(payload, list):
                raise RuntimeError(f"Unexpected Hugging Face tree response for {date}")
            result.extend(payload)
            next_link = response.headers.get("Link", "")
        url = None
        for item in next_link.split(","):
            if 'rel="next"' in item:
                url = urljoin(request.full_url, item[item.find("<") + 1:item.find(">")])
                break
    return result


def remote_sha(entry: dict) -> str | None:
    lfs = entry.get("lfs") or {}
    value = lfs.get("oid") or lfs.get("sha256")
    if isinstance(value, str):
        value = value.removeprefix("sha256:").removeprefix("sha256=")
        if len(value) == 64 and all(c in "0123456789abcdefABCDEF" for c in value):
            return value.lower()
    return None


def verify_entry_bytes(path: Path, entry: dict) -> tuple[str, str]:
    """Check the Hub LFS SHA-256 or Git blob SHA-1 plus declared size."""
    size = entry.get("size")
    if size is not None and path.stat().st_size != size:
        raise RuntimeError(f"size mismatch for {path}: {path.stat().st_size} != {size}")
    actual_sha256 = sha256(path)
    expected_sha256 = remote_sha(entry)
    if expected_sha256:
        if actual_sha256 != expected_sha256:
            raise RuntimeError(f"remote SHA-256 mismatch for {path}")
        return actual_sha256, "hub_lfs_sha256"
    oid = entry.get("oid")
    if isinstance(oid, str) and len(oid) == 40 and all(c in "0123456789abcdefABCDEF" for c in oid):
        digest = hashlib.sha1()
        blob_size = path.stat().st_size
        digest.update(f"blob {blob_size}\0".encode())
        with path.open("rb") as stream:
            for block in iter(lambda: stream.read(4 * 1024 * 1024), b""):
                digest.update(block)
        if digest.hexdigest().lower() != oid.lower():
            raise RuntimeError(f"Hub Git blob SHA-1 mismatch for {path}")
        return actual_sha256, "hub_git_blob_sha1"
    raise RuntimeError(f"pinned tree entry has no supported integrity digest: {entry.get('path')}")


def transfer(entry: dict, repo: str, revision: str, target: Path) -> dict:
    path = entry["path"]
    size = entry.get("size")
    expected = remote_sha(entry)
    if not isinstance(revision, str) or len(revision) != 40 or any(c not in "0123456789abcdefABCDEF" for c in revision):
        raise ValueError("revision must be a full 40-character commit hash; branch names are not accepted")
    target.parent.mkdir(parents=True, exist_ok=True)
    if target.exists():
        actual, integrity = verify_entry_bytes(target, entry)
        return {"path": path, "local_path": str(target), "bytes": target.stat().st_size,
                "sha256": actual, "remote_sha256": expected, "integrity_method": integrity,
                "status": "already_present_verified"}

    repo_q = urllib.parse.quote(repo, safe="/")
    file_q = urllib.parse.quote(path, safe="/")
    url = f"https://huggingface.co/datasets/{repo_q}/resolve/{revision}/{file_q}?download=true"
    partial = target.with_name(target.name + ".part")
    offset = partial.stat().st_size if partial.exists() else 0
    headers = {"User-Agent": "E4-OpenRCA-data/0.1"}
    if offset:
        headers["Range"] = f"bytes={offset}-"
    request = urllib.request.Request(url, headers=headers)
    try:
        response = urllib.request.urlopen(request, timeout=120)
    except urllib.error.HTTPError as exc:
        raise RuntimeError(f"download HTTP {exc.code} for {path}; partial kept") from exc
    with response:
        append = offset > 0 and response.status == 206
        if offset and not append:
            offset = 0
        downloaded = offset
        with partial.open("ab" if append else "wb") as output:
            while True:
                block = response.read(8 * 1024 * 1024)
                if not block:
                    break
                output.write(block)
                downloaded += len(block)
                print(f"{path}: {downloaded} bytes", file=sys.stderr, end="\r", flush=True)
    print(file=sys.stderr)
    try:
        actual, integrity = verify_entry_bytes(partial, entry)
    except RuntimeError as exc:
        raise RuntimeError(f"integrity check failed for {path}; partial kept: {exc}") from exc
    os.replace(partial, target)
    return {"path": path, "local_path": str(target), "bytes": target.stat().st_size,
            "sha256": actual, "remote_sha256": expected, "integrity_method": integrity,
            "status": "downloaded_verified"}


def fetch_dates(dates: list[str], destination: Path, audit_path: Path,
                repo: str, revision: str) -> list[dict]:
    records = []
    for date in dates:
        entries = api_tree(repo, revision, date)
        by_relative = {}
        prefix = f"Telecom/telemetry/{date}/"
        for entry in entries:
            if entry.get("type") != "file" or not entry.get("path", "").startswith(prefix):
                continue
            relative = entry["path"][len(prefix):]
            if relative in EXPECTED:
                if relative in by_relative:
                    raise RuntimeError(f"duplicate Hub tree path for {date}/{relative}")
                by_relative[relative] = entry
        missing = EXPECTED - set(by_relative)
        if missing:
            raise RuntimeError(f"pinned mirror lacks required files for {date}: {sorted(missing)}")
        for relative in sorted(EXPECTED):
            result = transfer(by_relative[relative], repo, revision,
                              destination / date / relative)
            result.update({"repository": repo, "revision": revision, "date": date})
            records.append(result)
            audit_path.parent.mkdir(parents=True, exist_ok=True)
            with audit_path.open("a", encoding="utf-8") as stream:
                stream.write(json.dumps(result) + "\n")
    return records


def fetch_bootstrap(destination: Path, mapping_path: Path, audit_path: Path,
                    repo: str, revision: str) -> list[dict]:
    """Fetch label metadata, mapping workbook and all available Telecom app KPI files."""
    if len(revision) != 40 or any(c not in "0123456789abcdefABCDEF" for c in revision):
        raise ValueError("revision must be a full 40-character commit hash")
    entries = api_tree(repo, revision)
    metadata_config = json.loads((ROOT / "config/telecom/source.json").read_text())
    expected_metadata = metadata_config["metadata_sha256"]
    expected_mapping = metadata_config["telecom_deployment_mapping"]["sha256"]
    selected = {}
    for entry in entries:
        path = entry.get("path", "")
        if path in {"Telecom/record.csv", "Telecom/query.csv"}:
            selected[path] = (destination / Path(path).name, expected_metadata[Path(path).name])
        elif path.endswith("/metric/metric_app.csv") and "/Telecom/telemetry/" in f"/{path}":
            selected[path] = (destination / "telemetry" / Path(path).parent.parent.name / "metric/metric_app.csv", None)
        elif Path(path).name == "AppTelecomDeploymentList.xlsx":
            selected[path] = (mapping_path, expected_mapping)
    expected_paths = {"Telecom/record.csv", "Telecom/query.csv"}
    if not expected_paths.issubset(selected):
        raise RuntimeError(f"pinned source is missing metadata paths: {sorted(expected_paths - set(selected))}")
    if not any(Path(path).name == "AppTelecomDeploymentList.xlsx" for path in selected):
        raise RuntimeError("pinned source is missing AppTelecomDeploymentList.xlsx")
    metrics = [path for path in selected if path.endswith("/metric/metric_app.csv")]
    if not metrics:
        raise RuntimeError("pinned source has no Telecom metric_app files")
    work = {entry["path"]: entry for entry in entries if entry.get("path") in selected}
    records = []
    for path in sorted(selected):
        target, expected_hash = selected[path]
        record = transfer(work[path], repo, revision, target)
        if expected_hash and record["sha256"] != expected_hash:
            raise RuntimeError(f"pinned source SHA-256 mismatch for {path}")
        record.update({"repository": repo, "revision": revision,
                       "expected_sha256": expected_hash, "status": record["status"]})
        records.append(record)
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        with audit_path.open("a", encoding="utf-8") as stream:
            stream.write(json.dumps(record) + "\n")
    return records


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--scored", type=Path, default=ROOT / "data/prepared/telecom/scored_incidents.jsonl")
    parser.add_argument("--dev", type=Path, default=ROOT / "data/prepared/telecom/dev_incidents.jsonl")
    parser.add_argument("--destination", type=Path,
                        help="raw Telecom root for --bootstrap, or telemetry directory for scored/dev fetch")
    parser.add_argument("--audit", type=Path, default=ROOT / "artifacts/data_audit/telecom_downloads.jsonl")
    parser.add_argument("--repo", default="tracer-cloud/opensre")
    parser.add_argument("--revision", default="07714872ea2cec77c13f9dec17a688e9df9621d1")
    parser.add_argument("--bootstrap", action="store_true",
                        help="fetch pinned metadata, mapping workbook and all Telecom metric_app coverage files")
    parser.add_argument("--mapping-output", type=Path,
                        default=ROOT / "data/raw/downloads/AppTelecomDeploymentList.xlsx")
    args = parser.parse_args()
    if args.destination is None:
        args.destination = (ROOT / "data/raw/openrca/Telecom" if args.bootstrap else
                            ROOT / "data/raw/openrca/Telecom/telemetry")
    if args.bootstrap:
        result = fetch_bootstrap(args.destination, args.mapping_output, args.audit,
                                 args.repo, args.revision)
        print(json.dumps({"repository": args.repo, "revision": args.revision,
                          "bootstrap_files": len(result),
                          "total_bytes": sum(item["bytes"] for item in result),
                          "audit": str(args.audit)}, indent=2))
        return 0
    dates = sorted({json.loads(line)["telemetry_date"]
                    for source in (args.scored, args.dev)
                    for line in source.read_text().splitlines() if line.strip()})
    if not dates:
        parser.error("scored/dev manifests contain no telemetry dates")
    result = fetch_dates(dates, args.destination, args.audit, args.repo, args.revision)
    print(json.dumps({"repository": args.repo, "revision": args.revision,
                      "dates": dates, "files": len(result),
                      "total_bytes": sum(item["bytes"] for item in result),
                      "audit": str(args.audit)}, indent=2))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
