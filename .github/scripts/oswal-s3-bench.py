#!/usr/bin/env python3
# Copyright 2023 Greptime Team
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

# /// script
# requires-python = ">=3.10"
# dependencies = ["boto3>=1.35,<2"]
# ///

"""Plan, image tag and bucket helpers of the object store WAL S3 benchmark.

`plan` and `tag` need only the standard library; `probe` and `cleanup` need
boto3 and run under `uv run`.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.request

SMOKE_ROWS = 50_000
# The o11ybench data profiles; a matching row count generates the canonical dataset.
DATA_PROFILES = {5_000: "S", 2_000_000: "D", 10_000_000: "P", 100_000_000: "M"}
# Identical [storage] for every configuration; only [wal] differs.
WAL_CONFIGS = {
    "R": {"PROVIDER": "raft_engine", "SYNC_WRITE": "true", "DIR": "/wal"},
    "D": {"PROVIDER": "experimental_object_store", "ACK_MODE": "durable"},
    "E": {"PROVIDER": "experimental_object_store", "ACK_MODE": "enqueued"},
}
ORDERS = (("R", "D", "E"), ("E", "D", "R"))
NIGHTLY = re.compile(r"v\d+\.\d+\.\d+(?:-[0-9A-Za-z.]+)?-nightly-(\d{8})")
TAG = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}")


def append_output(name: str, value: str) -> None:
    if path := os.environ.get("GITHUB_OUTPUT"):
        with open(path, "a") as output:
            output.write(f"{name}={value}\n")


def positive(name: str, value: str, upper: int) -> int:
    if not re.fullmatch(r"[1-9][0-9]*", value) or int(value) > upper:
        raise ValueError(f"{name} must be an integer in 1..{upper}, got {value!r}")
    return int(value)


def build_plan(concurrency: str, rows: str, batch_rows: str, rounds: str, smoke: bool) -> dict:
    levels = [positive("concurrency", level.strip(), 256) for level in concurrency.split(",")]
    if len(set(levels)) != len(levels):
        raise ValueError(f"concurrency repeats a level: {concurrency!r}")
    row_count = SMOKE_ROWS if smoke else positive("rows", rows, 100_000_000)
    round_count = 1 if smoke else positive("rounds", rounds, 10)
    profile = DATA_PROFILES.get(row_count)
    cells = []
    for round_number in range(1, round_count + 1):
        # Alternate the order between rounds so no configuration always runs first.
        for level in levels:
            for config in ORDERS[(round_number - 1) % 2]:
                cells.append({
                    "id": f"{round_number}-{level}-{config}", "round": round_number,
                    "concurrency": level, "config": config, "wal": WAL_CONFIGS[config],
                })
    return {
        "smoke": smoke, "rows": row_count, "rounds": round_count,
        "batch_rows": positive("batch_rows", batch_rows, 100_000), "concurrency": levels,
        "data_profile": profile or "custom",
        "generator_args": ["--data-profile", profile] if profile else ["--line-count", str(row_count)],
        "cells": cells,
    }


def newest_nightly(tags: list[str]) -> str:
    dated = [(match.group(1), tag) for tag in tags if (match := NIGHTLY.fullmatch(tag))]
    if not dated:
        raise ValueError("no v*-nightly-* tag found")
    return max(dated)[1]


def resolve_tag(requested: str) -> str:
    if requested != "nightly":
        if not TAG.fullmatch(requested):
            raise ValueError(f"invalid image tag: {requested!r}")
        return requested
    url = ("https://hub.docker.com/v2/repositories/greptime/greptimedb/tags"
           "?page_size=100&name=nightly&ordering=last_updated")
    with urllib.request.urlopen(url, timeout=30) as response:
        return newest_nightly([tag["name"] for tag in json.load(response)["results"]])


def s3_client(args):
    import boto3
    from botocore.config import Config

    return boto3.client("s3", region_name=args.region, endpoint_url=args.endpoint or None,
                        config=Config(retries={"mode": "standard", "max_attempts": 5}))


def probe(args) -> None:
    client, key = s3_client(args), f"{args.prefix}/probe/object"
    deadline = time.monotonic() + 60
    while True:
        try:
            client.put_object(Bucket=args.bucket, Key=key, Body=b"probe")
            break
        except Exception:
            # A fresh MinIO creates its bucket after the container starts.
            if time.monotonic() > deadline:
                raise
            time.sleep(3)
    listed = client.list_objects_v2(Bucket=args.bucket, Prefix=f"{args.prefix}/probe/")
    if [item["Key"] for item in listed.get("Contents", [])] != [key]:
        raise RuntimeError(f"listing {args.prefix}/probe/ did not return exactly {key}")
    client.delete_object(Bucket=args.bucket, Key=key)
    print(f"put, list and delete succeeded under s3://{args.bucket}/{args.prefix}/probe/")


def cleanup(args) -> None:
    client, deleted = s3_client(args), 0
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=args.bucket, Prefix=f"{args.prefix}/"):
        keys = [{"Key": item["Key"]} for item in page.get("Contents", [])]
        if keys:
            result = client.delete_objects(Bucket=args.bucket, Delete={"Objects": keys, "Quiet": True})
            if result.get("Errors"):
                raise RuntimeError(f"delete failed for {len(result['Errors'])} objects")
            deleted += len(keys)
    print(f"deleted {deleted} objects under s3://{args.bucket}/{args.prefix}/")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    plan = commands.add_parser("plan")
    for name in ("concurrency", "rows", "batch-rows", "rounds"):
        plan.add_argument(f"--{name}", required=True)
    plan.add_argument("--smoke", choices=("true", "false"), required=True)
    plan.add_argument("--output")
    commands.add_parser("tag").add_argument("requested")
    for name in ("probe", "cleanup"):
        bucket = commands.add_parser(name)
        bucket.add_argument("--bucket", required=True)
        bucket.add_argument("--region", required=True)
        bucket.add_argument("--endpoint", default="")
        bucket.add_argument("--prefix", required=True)
    args = parser.parse_args()
    if args.command == "plan":
        result = build_plan(args.concurrency, args.rows, args.batch_rows, args.rounds, args.smoke == "true")
        if args.output:
            with open(args.output, "w") as output:
                json.dump(result, output, indent=2)
        print(f"{len(result['cells'])} cells, {result['rows']} rows, profile {result['data_profile']}")
    elif args.command == "tag":
        tag = resolve_tag(args.requested)
        append_output("greptimedb_tag", tag)
        print(f"GreptimeDB image tag: {tag}")
    else:
        if not re.fullmatch(r"oswal-bench/[0-9]+-[0-9]+", args.prefix):
            raise ValueError(f"refusing a prefix outside oswal-bench/<run>: {args.prefix!r}")
        (probe if args.command == "probe" else cleanup)(args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
