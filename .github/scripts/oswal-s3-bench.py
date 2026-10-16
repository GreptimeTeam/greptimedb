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
from datetime import datetime
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
# Restart cells seal ten times as often as the default so recovery has thousands of objects.
RESTART_FLUSH_INTERVAL = "10ms"
# The loader's CREATE TABLE for the agent observability workload at the pinned runtime image.
AGENT_TABLE = """CREATE TABLE `agent_observations` (
  `event_time` TIMESTAMP(3) NOT NULL,
  `biz_date` DATE NOT NULL,
  `trace_id` STRING NOT NULL SKIPPING INDEX,
  `session_id` STRING NOT NULL,
  `observation_id` STRING NOT NULL SKIPPING INDEX,
  `parent_observation_id` STRING NULL SKIPPING INDEX,
  `seq_no` BIGINT NOT NULL,
  `type` STRING NOT NULL INVERTED INDEX,
  `status` STRING NOT NULL INVERTED INDEX,
  `tenant` STRING NOT NULL,
  `app` STRING NOT NULL,
  `environment` STRING NOT NULL,
  `task_category` STRING NULL,
  `trace_archetype` STRING NULL,
  `model` STRING NULL,
  `tool_name` STRING NULL,
  `input` STRING NULL FULLTEXT INDEX WITH(analyzer = 'English', backend = 'bloom', case_sensitive = 'false', granularity = '10240', false_positive_rate = '0.01'),
  `output` STRING NULL FULLTEXT INDEX WITH(analyzer = 'English', backend = 'bloom', case_sensitive = 'false', granularity = '10240', false_positive_rate = '0.01'),
  `input_tokens` BIGINT NULL,
  `output_tokens` BIGINT NULL,
  `total_cost` DOUBLE NULL,
  `latency_ms` BIGINT NULL,
  `payload` JSON2 NOT NULL,
  TIME INDEX (`event_time`),
  PRIMARY KEY (`tenant`, `app`, `environment`)
)
{partitions}ENGINE=mito
WITH(
  append_mode = 'true',
  'compaction.type' = 'twcs',
  'compaction.twcs.time_window' = '2h'
)"""
GROUP_NAME = re.compile(r"[a-z][a-z0-9]{0,11}")
RESERVED_ENV = ("GREPTIMEDB_STANDALONE__STORAGE__", "GREPTIMEDB_STANDALONE__WAL__")
ENV_NAME = re.compile(r"[A-Za-z_][A-Za-z0-9_]*")
# Docker's reference grammar: path components separated by ".", "_", "__" or runs of "-".
IMAGE = re.compile(r"[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*(?::[0-9]+)?(?:/[a-z0-9]+(?:(?:[._]|__|-+)[a-z0-9]+)*)+"
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
            "configs": plan["configs"], "extra_env": plan["extra_env"],
            "restart": plan["restart"], "groups": plan["groups"],
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
    return env


def partitioned_table(summary: dict, partitions: int) -> str:
    """The loader's table split into equal event_time ranges over the corpus window. The
    generator draws event times uniformly from that window, so every range gets an equal
    share of the rows and every batch writes to every region."""
    def millis(text):
        return int(datetime.fromisoformat(text.replace("Z", "+00:00")).timestamp() * 1000)
    start, end = millis(summary["start"]), millis(summary["end"])
    bounds = [start + (end - start) * i // partitions for i in range(1, partitions)]
    if len(set(bounds)) != len(bounds) or not start < bounds[0]:
        raise ValueError(f"the corpus window {summary['start']}..{summary['end']} is too short for {partitions} ranges")
    ranges = [f"`event_time` < {bounds[0]}"]
    ranges += [f"`event_time` >= {low} AND `event_time` < {high}" for low, high in zip(bounds, bounds[1:])]
    ranges.append(f"`event_time` >= {bounds[-1]}")
    clause = "PARTITION ON COLUMNS (`event_time`) (\n  " + ",\n  ".join(ranges) + "\n)\n"
    return AGENT_TABLE.format(partitions=clause)


def group_cells(group: dict, smoke: bool) -> list:
    """The cells of one group, in run order; a group without a name is the top-level inputs."""
    name, mode = group.get("name", ""), group.get("mode", "load")
    if mode not in ("load", "crash", "drop"):
        raise ValueError(f"group {name!r}: mode must be load, crash or drop, got {mode!r}")
    concurrency = str(group["concurrency"])
    levels = [positive("concurrency", level.strip(), 256) for level in concurrency.split(",")]
    if len(set(levels)) != len(levels):
        raise ValueError(f"concurrency repeats a level: {concurrency!r}")
    chosen = [config.strip() for config in str(group["configs"]).split(",")]
    allowed = set(WAL_CONFIGS) - (set(BASELINES) if mode != "load" else set())
    if not chosen or len(set(chosen)) != len(chosen) or not set(chosen) <= allowed:
        raise ValueError(f"group {name!r}: configs must be distinct names among {', '.join(sorted(allowed))}, "
                         f"got {group['configs']!r}")
    extra = group.get("extra_env", "")
    env = parse_extra_env(json.dumps(extra) if isinstance(extra, dict) else str(extra))
    restart = group.get("restart", False) is True
    rows = SMOKE_ROWS if smoke else positive("rows", str(group["rows"]), 100_000_000)
    rounds = 1 if smoke else positive("rounds", str(group["rounds"]), 10)
    idle = int(group.get("idle_seconds", 0))
    common = {"group": name, "mode": mode, "restart": restart, "idle_seconds": idle}
    partitions = int(group.get("partitions", 1))
    if partitions != 1 and (mode != "load" or not 2 <= partitions <= 64):
        raise ValueError(f"group {name!r}: partitions must be in 2..64 and needs mode load")
    if mode == "load":
        if not 0 <= idle <= 600:
            raise ValueError(f"group {name!r}: idle_seconds must be in 0..600")
        # The objects a restart cell must leave indexed at its kill; a smoke run is too small for any.
        min_objects = int(group.get("min_objects", 0))
        if min_objects < 0 or (min_objects and not restart):
            raise ValueError(f"group {name!r}: min_objects needs restart and must not be negative")
        common |= {"rows": rows, "data_profile": DATA_PROFILES.get(rows, "custom"),
                   "min_objects": 0 if smoke else min_objects, "partitions": partitions}
    elif mode == "drop":
        # Writes one table for write_seconds, drops it, kills and restarts the server, then
        # writes and flushes another table and watches the dropped table's objects go.
        write_seconds = int(group.get("write_seconds", 30))
        if not 1 <= write_seconds <= 600 or restart or idle:
            raise ValueError(f"group {name!r}: write_seconds must be in 1..600, without restart or idle")
        common |= {"write_seconds": min(write_seconds, 10) if smoke else write_seconds}
    else:
        cycles = int(group.get("cycles", 5))
        kill_after = [int(v) for v in group.get("kill_after", [3, 10])]
        if not 1 <= cycles <= 20 or len(kill_after) != 2 or not 1 <= kill_after[0] <= kill_after[1] <= 3600:
            raise ValueError(f"group {name!r}: cycles must be in 1..20 and kill_after a [min, max] in 1..3600")
        if restart or idle:
            raise ValueError(f"group {name!r}: a crash group restarts after every kill and has no idle window")
        if smoke:
            cycles, kill_after = min(cycles, 2), [min(v, 60) for v in kill_after]
        common |= {"cycles": cycles, "kill_after": kill_after, "pin": group.get("pin", False) is True}
    series = [config for config in WAL_CONFIGS if config in chosen]
    cells = []
    for round_number in range(1, rounds + 1):
        # Reverse the order every other round so no configuration always runs first.
        for level in levels:
            for config in series if round_number % 2 else reversed(series):
                wal = WAL_CONFIGS[config] | ({"FLUSH_INTERVAL": RESTART_FLUSH_INTERVAL} if restart and config not in BASELINES else {})
                cells.append(common | {
                    "id": (f"{name}-" if name else "") + f"{round_number}-{level}-{config}",
                    "round": round_number, "concurrency": level, "config": config, "wal": wal, "env": env,
                })
    return cells


def parse_groups(text: str, defaults: dict) -> list:
    """A JSON list of groups; every key a group leaves out takes the top-level input's value."""
    if not text.strip():
        return [defaults]
    groups = json.loads(text)
    if not isinstance(groups, list) or not groups or not all(isinstance(g, dict) for g in groups):
        raise ValueError("groups must be a non-empty JSON list of objects")
    known = set(defaults) | {"name", "mode", "idle_seconds", "cycles", "kill_after", "pin", "min_objects",
                             "partitions", "write_seconds"}
    names = [g.get("name") for g in groups]
    for group, name in zip(groups, names):
        if not isinstance(name, str) or not GROUP_NAME.fullmatch(name):
            raise ValueError(f"every group needs a name of up to 12 lowercase letters and digits, got {name!r}")
        if set(group) - known:
            raise ValueError(f"group {name!r} has unknown keys {sorted(set(group) - known)}")
    if len(set(names)) != len(names):
        raise ValueError(f"group names repeat: {names}")
    return [defaults | group for group in groups]


def build_plan(concurrency: str, rows: str, batch_rows: str, rounds: str, smoke: bool,
               configs: str = "R,D,E", extra_env: str = "", restart: bool = False, groups: str = "") -> dict:
    defaults = {"concurrency": concurrency, "rows": rows, "rounds": rounds, "configs": configs,
                "extra_env": extra_env, "restart": restart}
    resolved = parse_groups(groups, defaults)
    cells = [cell for group in resolved for cell in group_cells(group, smoke)]
    if len({cell["id"] for cell in cells}) != len(cells):
        raise ValueError("two cells share an id")
    corpora = []
    for count in dict.fromkeys(cell["rows"] for cell in cells if cell["mode"] == "load"):
        profile = DATA_PROFILES.get(count)
        corpora.append({"rows": count, "dir": f"corpus-{count}", "data_profile": profile or "custom",
                        "generator_args": ["--data-profile", profile] if profile else ["--line-count", str(count)]})
    first = cells[0]
    return {
        "smoke": smoke, "rows": first.get("rows"), "rounds": max(cell["round"] for cell in cells),
        "batch_rows": positive("batch_rows", batch_rows, 100_000),
        "concurrency": sorted({cell["concurrency"] for cell in cells}),
        "configs": list(dict.fromkeys(cell["config"] for cell in cells)),
        "extra_env": parse_extra_env(extra_env), "groups": [g.get("name", "") for g in resolved],
        "restart": restart, "corpora": corpora, "cells": cells,
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
    for name in ("extra-env", "groups"):
        plan.add_argument(f"--{name}", default="")
    for name in ("timeout-minutes", "ttl-hours", "output"):
        plan.add_argument(f"--{name}")
    image = commands.add_parser("image")
    image.add_argument("--tag", required=True)
    image.add_argument("--image", default="")
    regions = commands.add_parser("regions")
    for name in ("bucket", "runner"):
        regions.add_argument(f"--{name}", required=True)
    table = commands.add_parser("table")
    table.add_argument("--summary", type=Path, required=True)
    table.add_argument("--partitions", type=int, required=True)
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
                            args.configs, args.extra_env, args.restart == "true", args.groups)
        if args.output:
            with open(args.output, "w") as output:
                json.dump(result, output, indent=2)
        print(f"{len(result['cells'])} cells; corpora {[c['rows'] for c in result['corpora']]}")
    elif args.command == "image":
        reference = resolve_image(args.image, args.tag)
        append_github("GITHUB_OUTPUT", "greptimedb_ref", reference)
        print(f"GreptimeDB image: {reference}")
    elif args.command == "regions":
        check_regions(args.bucket, args.runner)
        print(f"bucket and runner both in {args.runner}")
    elif args.command == "table":
        print(partitioned_table(json.loads(args.summary.read_text()), args.partitions))
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
