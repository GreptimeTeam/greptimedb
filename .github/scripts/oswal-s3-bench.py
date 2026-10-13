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

"""Helpers of the object store WAL S3 benchmark; only `probe` and `cleanup` need boto3 (`uv run`)."""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.request
from pathlib import Path

SMOKE_ROWS = 50_000
# The o11ybench data profiles; a matching row count generates the canonical dataset.
DATA_PROFILES = {5_000: "S", 2_000_000: "D", 10_000_000: "P", 100_000_000: "M"}
# Identical [storage] for every configuration; only [wal] differs. R0 is the standalone default.
WAL_CONFIGS = {
    "R": {"PROVIDER": "raft_engine", "SYNC_WRITE": "true", "DIR": "/wal"},
    "R0": {"PROVIDER": "raft_engine", "SYNC_WRITE": "false", "DIR": "/wal"},
    "D": {"PROVIDER": "experimental_object_store", "ACK_MODE": "durable"},
    "E": {"PROVIDER": "experimental_object_store", "ACK_MODE": "enqueued"},
}
BASELINES = ("R", "R0")
# The pipeline depths an image built with the overrides reads; their defaults on main.
DEPTHS = {"GREPTIMEDB_OSWAL_MAX_IN_FLIGHT_CREATES": 4, "GREPTIMEDB_OSWAL_MAX_SEALED_BATCHES": None,
          "GREPTIMEDB_OSWAL_RECOVERY_CONCURRENCY": 8}
# Restart cells seal ten times as often as the default so recovery has thousands of objects.
RESTART_FLUSH_INTERVAL = "10ms"
RESERVED_ENV = ("GREPTIMEDB_STANDALONE__STORAGE__", "GREPTIMEDB_STANDALONE__WAL__")
ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
IMAGE = re.compile(r"[a-z0-9]+(?:[._-][a-z0-9]+)*(?::[0-9]+)?(?:/[a-z0-9]+(?:[._-][a-z0-9]+)*)+"
                   r"(?::[A-Za-z0-9_][A-Za-z0-9_.-]{0,127})?(?:@sha256:[0-9a-f]{64})?")
NIGHTLY = re.compile(r"v\d+\.\d+\.\d+(?:-[0-9A-Za-z.]+)?-nightly-(\d{8})")
TAG = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}")
DIGEST = re.compile(r".+@sha256:[0-9a-f]{64}")


def append_github(variable: str, name: str, value: str) -> None:
    if path := os.environ.get(variable):
        with open(path, "a") as output:
            output.write(f"{name}={value}\n")


def positive(name: str, value: str, upper: int) -> int:
    if not re.fullmatch(r"[1-9][0-9]*", value) or int(value) > upper:
        raise ValueError(f"{name} must be an integer in 1..{upper}, got {value!r}")
    return int(value)


def validate_budgets(timeout_minutes: str, ttl_hours: str) -> None:
    timeout, ttl = positive("timeout", timeout_minutes, 360), positive("ttl", ttl_hours, 168)
    # Provisioning, teardown and queueing need two hours beyond the benchmark job.
    if ttl * 60 < timeout + 120:
        raise ValueError(f"janitor TTL {ttl}h must cover the {timeout}-minute timeout plus 2 hours")


def check_regions(bucket: str, runner: str) -> None:
    if not bucket or bucket != runner:
        raise ValueError(f"bucket region {bucket!r} differs from runner region {runner!r}; "
                         "a cross-region bucket measures the WAN, not S3")


def build_manifest(plan: dict, greptimedb_inspect: list, runtime_image: str,
                   instance_type: str, region: str, prefix: str) -> dict:
    image = next((d for d in greptimedb_inspect[0].get("RepoDigests") or [] if DIGEST.fullmatch(d)), None)
    if image is None:
        raise ValueError("the GreptimeDB image has no sha256 repository digest")
    return {"smoke": plan["smoke"], "rows": plan["rows"], "rounds": plan["rounds"],
            "concurrency": plan["concurrency"], "batch_rows": plan["batch_rows"],
            "configs": plan["configs"], "extra_env": plan["extra_env"], "sweep": plan["sweep"],
            "restart": plan["restart"],
            "greptimedb_image": image, "runtime_image": runtime_image,
            "instance_type": instance_type, "region": region, "prefix": prefix}


def parse_extra_env(text: str) -> dict:
    """A JSON object or K=V lines; the storage and WAL settings that define a cell stay the benchmark's own."""
    if text.strip().startswith("{"):
        parsed = json.loads(text)
        if not all(isinstance(v, (str, int)) and not isinstance(v, bool) for v in parsed.values()):
            raise ValueError("extra_env values must be strings or integers")
        env = {k: str(v) for k, v in parsed.items()}
    else:
        env = {}
        for line in filter(None, (line.strip() for line in text.splitlines())):
            name, sep, value = line.partition("=")
            if not sep or name.strip() in env:
                raise ValueError(f"extra_env line is not a new K=V: {line!r}")
            env[name.strip()] = value.strip()
    for name, value in env.items():
        # GreptimeDB matches its environment keys without regard to case.
        if not ENV_NAME.fullmatch(name) or name.upper().startswith(RESERVED_ENV) or "\n" in value:
            raise ValueError(f"extra_env may not set {name!r}")
        if name in DEPTHS:
            positive(name, value, 4096)
    return env


def parse_sweep(text: str) -> tuple:
    """NAME=v1,v2,... over one depth; the GREPTIMEDB_OSWAL_ prefix may be left out."""
    if not text.strip():
        return None, [None]
    name, _, values = text.partition("=")
    name = name.strip() if name.strip().startswith("GREPTIMEDB_OSWAL_") else f"GREPTIMEDB_OSWAL_{name.strip()}"
    if name not in DEPTHS:
        raise ValueError(f"sweep must name one of {', '.join(DEPTHS)}, got {text!r}")
    points = [positive(name, value.strip(), 4096) for value in values.split(",")]
    if len(set(points)) != len(points):
        raise ValueError(f"sweep repeats a value: {text!r}")
    return name, points


def check_depths(env: dict) -> None:
    """Fail before provisioning on the sealed batch limit the store would reject."""
    creates = int(env.get("GREPTIMEDB_OSWAL_MAX_IN_FLIGHT_CREATES", DEPTHS["GREPTIMEDB_OSWAL_MAX_IN_FLIGHT_CREATES"]))
    sealed = env.get("GREPTIMEDB_OSWAL_MAX_SEALED_BATCHES")
    if sealed is not None and int(sealed) < creates + 2:
        raise ValueError(f"MAX_SEALED_BATCHES {sealed} is below MAX_IN_FLIGHT_CREATES {creates} plus 2")


def build_plan(concurrency: str, rows: str, batch_rows: str, rounds: str, smoke: bool,
               configs: str = "R,D,E", extra_env: str = "", sweep: str = "", restart: bool = False) -> dict:
    levels = [positive("concurrency", level.strip(), 256) for level in concurrency.split(",")]
    if len(set(levels)) != len(levels):
        raise ValueError(f"concurrency repeats a level: {concurrency!r}")
    chosen = [config.strip() for config in configs.split(",")]
    if not chosen or len(set(chosen)) != len(chosen) or not set(chosen) <= set(WAL_CONFIGS):
        raise ValueError(f"configs must be distinct names among {', '.join(WAL_CONFIGS)}, got {configs!r}")
    env = parse_extra_env(extra_env)
    swept, points = parse_sweep(sweep)
    if swept in env:
        raise ValueError(f"{swept} is both swept and set in extra_env")
    row_count = SMOKE_ROWS if smoke else positive("rows", rows, 100_000_000)
    round_count = 1 if smoke else positive("rounds", rounds, 10)
    profile = DATA_PROFILES.get(row_count)
    # Raft Engine ignores the depths, so each baseline runs once per round and level.
    series = [(config, point) for config in WAL_CONFIGS if config in chosen
              for point in (points if config not in BASELINES else [None])]
    cells = []
    for round_number in range(1, round_count + 1):
        # Reverse the order every other round so no configuration or value always runs first.
        for level in levels:
            for config, point in series if round_number % 2 else reversed(series):
                cell_env = env | ({swept: str(point)} if point is not None else {})
                if config not in BASELINES:
                    check_depths(cell_env)
                wal = WAL_CONFIGS[config] | ({"FLUSH_INTERVAL": RESTART_FLUSH_INTERVAL} if restart and config not in BASELINES else {})
                cells.append({
                    "id": f"{round_number}-{level}-{config}" + (f"-{point}" if point is not None else ""),
                    "round": round_number, "concurrency": level, "config": config, "wal": wal,
                    "env": cell_env, "sweep_value": point, "restart": restart,
                })
    return {
        "smoke": smoke, "rows": row_count, "rounds": round_count,
        "batch_rows": positive("batch_rows", batch_rows, 100_000), "concurrency": levels,
        "configs": chosen, "extra_env": env, "sweep": {"name": swept, "values": points} if swept else None,
        "restart": restart, "data_profile": profile or "custom",
        "generator_args": ["--data-profile", profile] if profile else ["--line-count", str(row_count)],
        "cells": cells,
    }


def newest_nightly(tags: list[str]) -> str:
    dated = [(match.group(1), tag) for tag in tags if (match := NIGHTLY.fullmatch(tag))]
    if not dated:
        raise ValueError("no v*-nightly-* tag found")
    return max(dated)[1]


def resolve_image(image: str, tag: str) -> str:
    """The full image reference when given, else greptime/greptimedb at the resolved tag."""
    if image:
        if not IMAGE.fullmatch(image) or not re.search(r"(:[^/]+|@sha256:[0-9a-f]{64})$", image):
            raise ValueError(f"greptimedb_image must be a full reference with a tag or digest: {image!r}")
        return image
    return f"greptime/greptimedb:{resolve_tag(tag)}"


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
    # One DeleteObject per key: DeleteObjects needs a checksum header the MinIO fixture rejects.
    client, deleted = s3_client(args), 0
    for page in client.get_paginator("list_objects_v2").paginate(Bucket=args.bucket, Prefix=f"{args.prefix}/"):
        for item in page.get("Contents", []):
            client.delete_object(Bucket=args.bucket, Key=item["Key"])
            deleted += 1
    print(f"deleted {deleted} objects under s3://{args.bucket}/{args.prefix}/")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    commands = parser.add_subparsers(dest="command", required=True)
    plan = commands.add_parser("plan")
    for name in ("concurrency", "rows", "batch-rows", "rounds"):
        plan.add_argument(f"--{name}", required=True)
    plan.add_argument("--smoke", choices=("true", "false"), required=True)
    plan.add_argument("--restart", choices=("true", "false"), default="false")
    plan.add_argument("--configs", default="R,D,E")
    for name in ("extra-env", "sweep"):
        plan.add_argument(f"--{name}", default="")
    for name in ("timeout-minutes", "ttl-hours", "output"):
        plan.add_argument(f"--{name}")
    image = commands.add_parser("image")
    image.add_argument("--tag", required=True)
    image.add_argument("--image", default="")
    regions = commands.add_parser("regions")
    for name in ("bucket", "runner"):
        regions.add_argument(f"--{name}", required=True)
    manifest = commands.add_parser("manifest")
    for name in ("results", "runtime-image", "instance-type", "region", "prefix"):
        manifest.add_argument(f"--{name}", required=True)
    for name in ("probe", "cleanup"):
        bucket = commands.add_parser(name)
        bucket.add_argument("--endpoint", default="")
        for option in ("bucket", "region", "prefix"):
            bucket.add_argument(f"--{option}", required=True)
    args = parser.parse_args()
    if args.command == "plan":
        if args.timeout_minutes or args.ttl_hours:
            validate_budgets(args.timeout_minutes or "", args.ttl_hours or "")
        result = build_plan(args.concurrency, args.rows, args.batch_rows, args.rounds, args.smoke == "true",
                            args.configs, args.extra_env, args.sweep, args.restart == "true")
        if args.output:
            with open(args.output, "w") as output:
                json.dump(result, output, indent=2)
        print(f"{len(result['cells'])} cells, {result['rows']} rows, profile {result['data_profile']}")
    elif args.command == "image":
        reference = resolve_image(args.image, args.tag)
        append_github("GITHUB_OUTPUT", "greptimedb_ref", reference)
        print(f"GreptimeDB image: {reference}")
    elif args.command == "regions":
        check_regions(args.bucket, args.runner)
        print(f"bucket and runner both in {args.runner}")
    elif args.command == "manifest":
        results = Path(args.results)
        manifest = build_manifest(json.loads((results / "plan.json").read_text()),
                                  json.loads((results / "images" / "greptimedb.json").read_text()),
                                  args.runtime_image, args.instance_type, args.region, args.prefix)
        (results / "run.json").write_text(json.dumps(manifest, indent=2) + "\n")
        append_github("GITHUB_ENV", "GREPTIMEDB_IMAGE", manifest["greptimedb_image"])
        print(f"GreptimeDB image: {manifest['greptimedb_image']}")
    else:
        if not re.fullmatch(r"oswal-bench/[0-9]+-[0-9]+", args.prefix):
            raise ValueError(f"refusing a prefix outside oswal-bench/<run>: {args.prefix!r}")
        (probe if args.command == "probe" else cleanup)(args)
    return 0


if __name__ == "__main__":
    try:
        sys.exit(main())
    except ValueError as error:
        print(f"::error::{error}", file=sys.stderr)
        sys.exit(1)
