#!/usr/bin/env python3
# =============================================================================
# upload-problem.py — operator CLI for registering a problem + test cases.
#
# What it does (in order):
#   1. Parses a problem.yaml file (title, time_limit_ms, memory_limit_mb, points).
#   2. POSTs to the Problem Service `POST /api/v1/problems` to create the row;
#      captures the returned problem UUID.
#   3. Walks the --tests-dir for paired `*.in` / `*.out` files (sorted), uploads
#      each to GCS under `problems/<id>/tests/<ordinal>/{input,expected}` using
#      the google-cloud-storage Python SDK.
#   4. POSTs to `POST /api/v1/problems/{id}/test-cases` with the GCS key list.
#
# Test files are paired by base name. Files <name>.in / <name>.out must exist
# together; ordinal is the 1-indexed position in sorted order. Pretests are
# ordinals 1..10; system tests are 11+.
#
# Required env vars:
#   PROBLEM_SERVICE_URL    Base URL of the Problem Service (e.g. http://oj-control-plane:8089)
#   GCS_BUCKET             Target GCS bucket (e.g. oj-test-cases)
#   GOOGLE_APPLICATION_CREDENTIALS
#                          Path to a service-account JSON with Storage Object Admin
#                          on the bucket. Standard GCP env var, read by the SDK.
#
# Optional:
#   PROBLEM_SERVICE_TIMEOUT_SECONDS  Default 30.
#
# Example:
#   PROBLEM_SERVICE_URL=http://localhost:8089 \
#   GCS_BUCKET=oj-test-cases \
#   GOOGLE_APPLICATION_CREDENTIALS=~/keys/oj-uploader.json \
#   python3 upload-problem.py \
#       --problem-yaml two-sum.yaml \
#       --tests-dir ./two-sum-tests
#
# problem.yaml shape:
#   title: "Two Sum"
#   time_limit_ms: 1000
#   memory_limit_mb: 256
#   points: 100
#
# Dependencies: google-cloud-storage, requests, pyyaml. No other deps.
# =============================================================================

from __future__ import annotations

import argparse
import os
import sys
from pathlib import Path

import requests
import yaml
from google.cloud import storage


def env(name: str, *, required: bool = True, default: str | None = None) -> str:
    val = os.environ.get(name, default)
    if required and not val:
        sys.exit(f"error: required env var {name} is not set")
    return val or ""


def load_problem_yaml(path: Path) -> dict:
    with path.open("r") as f:
        data = yaml.safe_load(f)
    for key in ("title", "time_limit_ms", "memory_limit_mb", "points"):
        if key not in data:
            sys.exit(f"error: problem yaml missing key '{key}'")
    return data


def discover_test_pairs(tests_dir: Path) -> list[tuple[Path, Path]]:
    """Pair up *.in / *.out files by base name. Sorted for deterministic ordinals."""
    in_files = {p.stem: p for p in sorted(tests_dir.glob("*.in"))}
    out_files = {p.stem: p for p in sorted(tests_dir.glob("*.out"))}
    pairs: list[tuple[Path, Path]] = []
    for stem in sorted(in_files.keys()):
        if stem not in out_files:
            sys.exit(f"error: input '{in_files[stem]}' has no matching '.out' file")
        pairs.append((in_files[stem], out_files[stem]))
    if not pairs:
        sys.exit(f"error: no .in/.out test pairs found in {tests_dir}")
    return pairs


def create_problem(base_url: str, timeout: int, meta: dict) -> str:
    payload = {
        "title": meta["title"],
        "timeLimitMs": int(meta["time_limit_ms"]),
        "memoryLimitMb": int(meta["memory_limit_mb"]),
        "points": int(meta["points"]),
    }
    r = requests.post(f"{base_url}/api/v1/problems", json=payload, timeout=timeout)
    r.raise_for_status()
    body = r.json()
    return body["id"]


def upload_test_file(bucket: storage.Bucket, key: str, source: Path) -> None:
    blob = bucket.blob(key)
    blob.upload_from_filename(str(source))


def register_test_cases(base_url: str, timeout: int, problem_id: str, entries: list[dict]) -> None:
    r = requests.post(
        f"{base_url}/api/v1/problems/{problem_id}/test-cases",
        json={"testCases": entries},
        timeout=timeout,
    )
    r.raise_for_status()


def main() -> int:
    parser = argparse.ArgumentParser(description="Upload a problem + test cases to the Problem Service.")
    parser.add_argument("--problem-yaml", type=Path, required=True, help="Path to the problem.yaml metadata file.")
    parser.add_argument("--tests-dir", type=Path, required=True, help="Directory containing paired *.in / *.out test files.")
    args = parser.parse_args()

    if not args.problem_yaml.is_file():
        sys.exit(f"error: --problem-yaml not found: {args.problem_yaml}")
    if not args.tests_dir.is_dir():
        sys.exit(f"error: --tests-dir not found: {args.tests_dir}")

    base_url = env("PROBLEM_SERVICE_URL").rstrip("/")
    bucket_name = env("GCS_BUCKET")
    env("GOOGLE_APPLICATION_CREDENTIALS")  # presence check; SDK reads it itself
    timeout = int(env("PROBLEM_SERVICE_TIMEOUT_SECONDS", required=False, default="30"))

    meta = load_problem_yaml(args.problem_yaml)
    pairs = discover_test_pairs(args.tests_dir)

    print(f"[1/4] Creating problem '{meta['title']}' ({len(pairs)} test cases) ...")
    problem_id = create_problem(base_url, timeout, meta)
    print(f"       problem_id = {problem_id}")

    print(f"[2/4] Connecting to GCS bucket gs://{bucket_name} ...")
    client = storage.Client()
    bucket = client.bucket(bucket_name)

    entries: list[dict] = []
    print(f"[3/4] Uploading {len(pairs)} test pairs to GCS ...")
    for ordinal, (in_path, out_path) in enumerate(pairs, start=1):
        in_key = f"problems/{problem_id}/tests/{ordinal}/input.txt"
        out_key = f"problems/{problem_id}/tests/{ordinal}/expected.txt"
        upload_test_file(bucket, in_key, in_path)
        upload_test_file(bucket, out_key, out_path)
        entries.append({
            "ordinal": ordinal,
            "inputGcsKey": in_key,
            "expectedOutputGcsKey": out_key,
        })
        print(f"       ordinal={ordinal:2d} {in_path.name} -> {in_key}")

    print(f"[4/4] Registering {len(entries)} test cases with the Problem Service ...")
    register_test_cases(base_url, timeout, problem_id, entries)

    print(f"done. problem_id = {problem_id}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
