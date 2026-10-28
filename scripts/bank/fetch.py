#!/usr/bin/env python3
"""Fetch pinned OpenRCA Bank files with Git-blob/LFS integrity checks."""

from __future__ import annotations

import argparse
import hashlib
import json
import os
import re
import sys
import time
from concurrent.futures import ThreadPoolExecutor, as_completed
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[2]
sys.path.insert(0, str(ROOT / "src"))

from e4.prep.bank import SOURCE_CONFIG, git_blob_sha1, prepare_metadata, sha256_file


AUDIT = ROOT / "artifacts/data_audit"
RAW = ROOT / "data/raw/openrca/Bank"
USER_AGENT = "E4-Bank-Data-Preparation/1"


class FetchError(RuntimeError):
    pass


def _http_json(url: str) -> Any:
    request = urllib.request.Request(url, headers={"User-Agent": USER_AGENT,
                                                   "Accept": "application/json"})
    with urllib.request.urlopen(request, timeout=120) as response:
        if response.status != 200:
            raise FetchError(f"unexpected HTTP status {response.status} for metadata endpoint")
        return json.load(response)


def remote_tree(*, root: Path = ROOT, save: bool = True) -> dict[str, dict[str, Any]]:
    source = json.loads((root / "config/bank/source.json").read_text())
    mirror = source["working_mirror"]
    repo = mirror["repository"]
    revision = mirror["revision"]
    encoded_repo = urllib.parse.quote(repo, safe="/")
    records = {}
    urls = []
    # The HF tree API returns only part of deeply nested trees in some responses.
    # Query each small pinned date directory directly instead of assuming a
    # recursive root listing is complete.
    for date in mirror["telemetry_dates"]:
        url = (f"https://huggingface.co/api/datasets/{encoded_repo}/tree/{revision}/"
               f"Bank/telemetry/{date}?recursive=true&expand=true")
        urls.append(url)
        payload = _http_json(url)
        for item in payload:
            if item.get("type") == "file" and item.get("path", "").endswith(".csv"):
                records[item["path"]] = item
    if not records:
        raise FetchError("pinned Bank telemetry tree contained no CSV files")
    if save:
        (root / "artifacts/data_audit").mkdir(parents=True, exist_ok=True)
        target = root / "artifacts/data_audit/bank_hf_remote_tree.json"
        target.write_text(json.dumps({"repository": repo, "revision": revision,
                                      "api_urls": urls, "files": records},
                                     indent=2, sort_keys=True) + "\n")
    return records


def _file_hashes(path: Path) -> tuple[int, str, str]:
    size = 0
    sha = hashlib.sha256()
    git_blob = hashlib.sha1()
    total = path.stat().st_size
    git_blob.update(f"blob {total}\0".encode("ascii"))
    with path.open("rb") as stream:
        for chunk in iter(lambda: stream.read(4 * 1024 * 1024), b""):
            size += len(chunk)
            sha.update(chunk)
            git_blob.update(chunk)
    return size, sha.hexdigest(), git_blob.hexdigest()


def _expected(entry: dict[str, Any]) -> tuple[int, str | None, str | None]:
    size = int(entry["size"])
    lfs = entry.get("lfs")
    lfs_sha = lfs.get("oid") if isinstance(lfs, dict) else None
    return size, lfs_sha, str(entry["oid"])


def verify_file(path: Path, entry: dict[str, Any]) -> dict[str, Any]:
    expected_size, expected_lfs, expected_git = _expected(entry)
    size, sha, git_blob = _file_hashes(path)
    if size != expected_size:
        raise FetchError(f"size mismatch for {entry['path']}: local {size}, remote {expected_size}")
    if expected_lfs:
        if sha != expected_lfs:
            raise FetchError(f"LFS SHA-256 mismatch for {entry['path']}")
        integrity = "hub_lfs_sha256"
    else:
        if git_blob != expected_git:
            raise FetchError(f"Git blob SHA-1 mismatch for {entry['path']}")
        integrity = "hub_git_blob_sha1"
    return {"path": entry["path"], "bytes": size, "sha256": sha,
            "hub_git_blob_sha1": expected_git, "hub_lfs_sha256": expected_lfs,
            "integrity": integrity}


def fetch_file(entry: dict[str, Any], *, root: Path = ROOT, retries: int = 3,
               refresh: bool = False) -> dict[str, Any]:
    source = json.loads((root / "config/bank/source.json").read_text())
    revision = source["working_mirror"]["revision"]
    relative = entry["path"]
    dest = root / "data/raw/openrca" / relative
    if dest.is_file() and not refresh:
        return verify_file(dest, entry)
    dest.parent.mkdir(parents=True, exist_ok=True)
    part = dest.with_suffix(dest.suffix + ".part")
    expected_size, expected_lfs, expected_git = _expected(entry)
    url = (f"https://huggingface.co/datasets/{source['working_mirror']['repository']}"
           f"/resolve/{revision}/{urllib.parse.quote(relative, safe='/')}")
    last_error = None
    for attempt in range(1, retries + 1):
        try:
            offset = part.stat().st_size if part.exists() else 0
            if offset > expected_size:
                part.unlink()
                offset = 0
            if offset == expected_size and part.is_file():
                try:
                    verified = verify_file(part, entry)
                except FetchError:
                    part.unlink()
                    offset = 0
                else:
                    os.replace(part, dest)
                    return verified
            headers = {"User-Agent": USER_AGENT}
            if offset:
                headers["Range"] = f"bytes={offset}-"
            request = urllib.request.Request(url, headers=headers)
            with urllib.request.urlopen(request, timeout=300) as response:
                if offset:
                    content_range = response.headers.get("Content-Range", "")
                    match = re.fullmatch(r"bytes (\d+)-(\d+)/(\d+)", content_range)
                    remaining = expected_size - offset
                    content_length = response.headers.get("Content-Length")
                    if (response.status != 206 or match is None or int(match[1]) != offset
                            or int(match[2]) != expected_size - 1 or int(match[3]) != expected_size
                            or (content_length is not None and int(content_length) != remaining)):
                        part.unlink(missing_ok=True)
                        raise FetchError(f"server returned an inconsistent Content-Range/Length for resume: {relative}")
                    mode = "ab"
                else:
                    if response.status not in (200, 206):
                        raise FetchError(f"unexpected HTTP {response.status} for {relative}")
                    content_length = response.headers.get("Content-Length")
                    if content_length is not None and int(content_length) != expected_size:
                        raise FetchError(f"server Content-Length differs from pinned size for {relative}")
                    remaining = expected_size
                    mode = "wb"
                with part.open(mode) as stream:
                    written = 0
                    while True:
                        block = response.read(4 * 1024 * 1024)
                        if not block:
                            break
                        if written + len(block) > remaining:
                            part.unlink(missing_ok=True)
                            raise FetchError(f"server sent more bytes than pinned size for {relative}")
                        stream.write(block)
                        written += len(block)
            verified = verify_file(part, entry)
            os.replace(part, dest)
            return verified
        except (OSError, urllib.error.URLError, FetchError) as exc:
            last_error = exc
            if attempt < retries:
                time.sleep(min(2 ** (attempt - 1), 8))
    raise FetchError(f"download failed for {relative} after {retries} attempts: {last_error}")


def _append_download_log(result: dict[str, Any], *, root: Path = ROOT) -> None:
    log = root / "artifacts/data_audit/bank_downloads.jsonl"
    log.parent.mkdir(parents=True, exist_ok=True)
    with log.open("a", encoding="utf-8") as stream:
        stream.write(json.dumps(result, ensure_ascii=False, sort_keys=True) + "\n")


def fetch_bootstrap(*, root: Path = ROOT, refresh: bool = False) -> list[dict[str, Any]]:
    source = json.loads((root / "config/bank/source.json").read_text())
    repo = source["working_mirror"]["repository"]
    revision = source["working_mirror"]["revision"]
    metadata_paths = {"record.csv": f"Bank/record.csv", "query.csv": f"Bank/query.csv"}
    results = []
    root_listing_url = (f"https://huggingface.co/api/datasets/{repo}/tree/{revision}/Bank"
                        "?recursive=false&expand=true")
    root_listing = _http_json(root_listing_url)
    for name, remote_path in metadata_paths.items():
        item = next((row for row in root_listing if row.get("path") == remote_path), None)
        if item is None:
            raise FetchError(f"pinned mirror lacks {remote_path}")
        expected = source["working_mirror"]["metadata_files"][name]
        if (item.get("size") != expected["size_bytes"] or item.get("oid") != expected["git_blob_sha1"]):
            raise FetchError(f"pinned metadata remote identity changed for {name}")
        # fetch_file expects paths relative to the Bank root tree.
        item = dict(item)
        item["path"] = remote_path
        output = fetch_file(item, root=root, refresh=refresh)
        if output["sha256"] != expected["sha256"]:
            raise FetchError(f"frozen SHA-256 mismatch for metadata {name}")
        results.append(output)
        _append_download_log({**output, "revision": revision}, root=root)
    return results


def fetch_metric_app(*, root: Path = ROOT, refresh: bool = False) -> list[dict[str, Any]]:
    source = json.loads((root / "config/bank/source.json").read_text())
    dates = source["working_mirror"]["telemetry_dates"]
    tree = remote_tree(root=root)
    results = []
    for date in dates:
        relative = f"Bank/telemetry/{date}/metric/metric_app.csv"
        entry = tree.get(relative)
        if entry is None:
            results.append({"path": relative, "status": "missing_remote"})
            continue
        result = fetch_file(entry, root=root, refresh=refresh)
        _append_download_log({**result, "revision": json.loads((root / "config/bank/source.json").read_text())["working_mirror"]["revision"]}, root=root)
        results.append(result)
    return results


def fetch_selected_telemetry(*, root: Path = ROOT, refresh: bool = False,
                             workers: int = 3) -> list[dict[str, Any]]:
    frozen_path = root / "config/bank/sample.json"
    if not frozen_path.is_file():
        raise FileNotFoundError("freeze Bank sample before fetching full selected-date telemetry")
    frozen = json.loads(frozen_path.read_text())
    dev_dates = sorted({date for case in frozen["development_cases"] for date in case["telemetry_dates"]})
    scored_dates = sorted({date for case in frozen["scored_cases"] for date in case["telemetry_dates"]})
    dates = dev_dates + [date for date in scored_dates if date not in set(dev_dates)]
    tree = remote_tree(root=root)
    required = ("metric/metric_app.csv", "metric/metric_container.csv",
                "trace/trace_span.csv", "log/log_service.csv")
    if type(workers) is not int or not 1 <= workers <= 3:
        raise ValueError("selected telemetry downloads support 1 to 3 workers")
    jobs = []
    for date in dates:
        for table in required:
            relative = f"Bank/telemetry/{date}/{table}"
            entry = tree.get(relative)
            if entry is None:
                raise FetchError(f"required Bank source table absent from pinned revision: {relative}")
            jobs.append(entry)
    revision = json.loads((root / "config/bank/source.json").read_text())["working_mirror"]["revision"]
    result_by_path = {}
    with ThreadPoolExecutor(max_workers=workers, thread_name_prefix="bank-fetch") as pool:
        futures = {pool.submit(fetch_file, entry, root=root, refresh=refresh): entry for entry in jobs}
        for future in as_completed(futures):
            result = future.result()
            _append_download_log({**result, "revision": revision}, root=root)
            result_by_path[result["path"]] = result
    return [result_by_path[entry["path"]] for entry in jobs]


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    bootstrap = sub.add_parser("bootstrap", help="Fetch pinned record/query metadata only")
    bootstrap.add_argument("--root", type=Path, default=ROOT)
    bootstrap.add_argument("--refresh", action="store_true")
    metric = sub.add_parser("metric-app", help="Fetch small metric_app files for every candidate date")
    metric.add_argument("--root", type=Path, default=ROOT)
    metric.add_argument("--refresh", action="store_true")
    selected = sub.add_parser("selected-telemetry", help="Fetch four raw Bank tables for frozen scored/dev dates")
    selected.add_argument("--root", type=Path, default=ROOT)
    selected.add_argument("--refresh", action="store_true")
    selected.add_argument("--workers", type=int, default=3)
    args = parser.parse_args()
    if args.command == "bootstrap":
        results = fetch_bootstrap(root=args.root, refresh=args.refresh)
    elif args.command == "metric-app":
        results = fetch_metric_app(root=args.root, refresh=args.refresh)
    else:
        results = fetch_selected_telemetry(root=args.root, refresh=args.refresh, workers=args.workers)
    print(json.dumps({"command": args.command, "files": len(results),
                      "bytes": sum(int(row.get("bytes", 0)) for row in results),
                      "results": results}, indent=2, sort_keys=True))
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
