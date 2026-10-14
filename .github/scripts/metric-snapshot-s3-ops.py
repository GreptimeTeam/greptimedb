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

"""Budget preflight and strictly scoped S3 operations for the metric snapshot benchmark."""

import argparse
from decimal import Decimal
import json
import os
from pathlib import Path
import re
import time
import urllib.request

REGION = "ap-southeast-1"
HOURLY_USD = Decimal("0.9408")
OTHER_RESERVE_USD = Decimal("1.50")


def owned_prefix(prefix):
    if not re.fullmatch(r"metric-snapshot-bench/[0-9]+-[0-9]+/", prefix):
        raise ValueError("prefix must be exactly metric-snapshot-bench/<run_id>-<attempt>/")
    return prefix


def plan(env):
    image = env["GREPTIMEDB_IMAGE"]
    if not re.fullmatch(r"greptime/greptimedb(?:-nightly)?@sha256:[0-9a-f]{64}", image):
        raise ValueError("image must be an immutable greptime/greptimedb sha256 reference")
    if not re.fullmatch(r"[0-9a-f]{40}", env["EXPECTED_COMMIT"]):
        raise ValueError("expected commit must be a full lowercase source SHA")
    tables, minutes = int(env["LOGICAL_TABLES_PER_GROUP"]), int(env["BENCHMARK_TIMEOUT_MINUTES"])
    if not 50 <= tables <= 10_000 or not 30 <= minutes <= 240:
        raise ValueError("logical tables per group must be 50..10000; timeout must be 30..240 minutes")
    if env["S3_REGION"] != REGION or env["RUNNER_REGION"] != REGION:
        raise ValueError("bucket and runner must both use ap-southeast-1")
    if not env["S3_BUCKET"]:
        raise ValueError("S3 bucket is required")
    prior = Decimal(env["PRIOR_SPEND_USD"])
    # Reserve 45m provision, 10m queue allowance and 20m cleanup beyond the job timeout.
    reservation = HOURLY_USD * Decimal(minutes + 75) / 60 + OTHER_RESERVE_USD
    if not prior.is_finite() or prior < 0 or prior >= 15 or prior + reservation > 20:
        raise ValueError("campaign budget exhausted: prior spend must be below $15 and leave the full attempt reserve within $20")
    return {"image": image, "expected_commit": env["EXPECTED_COMMIT"],
            "logical_tables_per_group": tables, "smoke_logical_tables": 500,
            "timeout_minutes": minutes, "prefix": owned_prefix(env["RUN_PREFIX"]),
            "instance_type": "c6id.4xlarge", "region": REGION,
            "prior_estimated_usd": str(prior), "attempt_reserve_usd": str(reservation),
            "ec2_hourly_usd": str(HOURLY_USD), "other_reserve_usd": str(OTHER_RESERVE_USD),
            "cost_note": "Estimate, not an AWS billing total or a hard spending cap."}


def client(region):
    import boto3
    from botocore.config import Config

    if region != REGION:
        raise ValueError("S3 region must be ap-southeast-1")
    return boto3.client("s3", region_name=region, config=Config(
        connect_timeout=10, read_timeout=30, retries={"mode": "standard", "max_attempts": 3}))


def entries(s3, operation, bucket, prefix, fields):
    owned_prefix(prefix)
    for page in s3.get_paginator(operation).paginate(Bucket=bucket, Prefix=prefix):
        for field in fields:
            for item in page.get(field, []):
                if not item["Key"].startswith(prefix):
                    raise ValueError("S3 returned an object outside the owned prefix")
                yield item


def inventory(s3, bucket, prefix):
    objects = list(entries(s3, "list_objects_v2", bucket, prefix, ("Contents",)))
    return {"prefix": prefix, "objects": len(objects), "bytes": sum(item["Size"] for item in objects),
            "measurement": "S3 listing inventory; not request counts or transferred bytes."}


def probe(s3, bucket, prefix):
    if inventory(s3, bucket, prefix)["objects"]:
        raise ValueError("run prefix is already occupied; use a fresh workflow attempt")
    key = prefix + "_probe"
    s3.put_object(Bucket=bucket, Key=key, Body=b"metric snapshot benchmark")
    try:
        body = s3.get_object(Bucket=bucket, Key=key)["Body"]
        try:
            if body.read() != b"metric snapshot benchmark":
                raise ValueError("S3 probe returned unexpected content")
        finally:
            body.close()
    finally:
        s3.delete_object(Bucket=bucket, Key=key)
    return inventory(s3, bucket, prefix)


def cleanup(s3, bucket, prefix):
    owned_prefix(prefix)
    for item in entries(s3, "list_objects_v2", bucket, prefix, ("Contents",)):
        s3.delete_object(Bucket=bucket, Key=item["Key"])
    after = inventory(s3, bucket, prefix)
    if after["objects"]:
        raise RuntimeError("owned S3 prefix is not empty after cleanup")
    return after


def scrub(root, env):
    secrets = [env[name].encode() for name in ("AWS_ACCESS_KEY_ID", "AWS_SECRET_ACCESS_KEY", "AWS_SESSION_TOKEN")
               if env.get(name)]
    if not secrets:
        raise ValueError("credential scan requires the CI credential environment")
    overlap = max(map(len, secrets)) - 1
    for result in Path(root).rglob("*"):
        if result.is_file():
            with result.open("rb") as stream:
                previous = b""
                while chunk := stream.read(1024 * 1024):
                    data = previous + chunk
                    if any(secret in data for secret in secrets):
                        raise ValueError("credential detected in results; artifact upload is blocked")
                    previous = data[-overlap:] if overlap else b""


def wait_for_benchmark(env, started_at):
    """Bound runner allocation even when the benchmark job never leaves its queue."""
    run_id, attempt = owned_prefix(env["RUN_PREFIX"]).split("/")[1].split("-")
    repo = env["GITHUB_REPOSITORY"]
    if run_id != env["GITHUB_RUN_ID"] or not re.fullmatch(r"[\w.-]+/[\w.-]+", repo):
        raise ValueError("workflow identity does not match its owned resource prefix")
    minutes = int(env["BENCHMARK_TIMEOUT_MINUTES"])
    if not 30 <= minutes <= 240 or started_at <= 0:
        raise ValueError("invalid benchmark allocation deadline")
    # Use the original allocation timestamp; a delayed or retried watcher cannot extend it.
    deadline = started_at + (45 + minutes + 10) * 60
    request = urllib.request.Request(
        f"https://api.github.com/repos/{repo}/actions/runs/{run_id}/attempts/{attempt}/jobs?per_page=100",
        headers={"Authorization": f"Bearer {env['GH_TOKEN']}", "Accept": "application/vnd.github+json",
                 "X-GitHub-Api-Version": "2022-11-28"})
    while True:
        remaining = deadline - time.time()
        if remaining <= 0:
            raise TimeoutError("benchmark allocation deadline reached; proceeding to resource cleanup")
        # API errors deliberately fail this step: subsequent always() steps clean up immediately.
        with urllib.request.urlopen(request, timeout=min(15, remaining)) as response:
            jobs = json.load(response)["jobs"]
        benchmark = next((job for job in jobs if job["name"] == "benchmark"), None)
        if benchmark and benchmark["status"] == "completed":
            return {"benchmark_conclusion": benchmark["conclusion"], "owned_attempt": attempt}
        remaining = deadline - time.time()
        if remaining > 0:
            time.sleep(min(30, remaining))


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("command", choices=("plan", "probe", "inventory", "cleanup", "scrub", "cost", "wait-for-benchmark"))
    parser.add_argument("--output")
    parser.add_argument("--started-at", type=int)
    args = parser.parse_args()
    if args.command == "plan":
        result = plan(os.environ)
    elif args.command == "scrub":
        scrub(os.environ["RESULTS"], os.environ)
        return
    elif args.command == "wait-for-benchmark":
        if args.started_at is None:
            parser.error("wait-for-benchmark requires --started-at")
        result = wait_for_benchmark(os.environ, args.started_at)
    elif args.command == "cost":
        if args.started_at is None:
            parser.error("cost requires --started-at")
        seconds = max(0, int(time.time()) - args.started_at)
        estimate = HOURLY_USD * Decimal(seconds) / 3600
        result = {"allocation_window_seconds": seconds, "ec2_estimate_usd": str(round(estimate, 4)),
                  "campaign_estimate_with_other_reserve_usd": str(round(
                      Decimal(os.environ["PRIOR_SPEND_USD"]) + estimate + OTHER_RESERVE_USD, 4)),
                  "note": "Allocation wall-time estimate plus $1.50 reserve for S3, disk and IPv4; not an AWS bill."}
    else:
        prefix = owned_prefix(os.environ["RUN_PREFIX"])
        result = globals()[args.command](client(os.environ["S3_REGION"]), os.environ["S3_BUCKET"], prefix)
    rendered = json.dumps(result, indent=2) + "\n"
    if args.output:
        destination = Path(args.output)
        destination.parent.mkdir(parents=True, exist_ok=True)
        destination.write_text(rendered)
    print(rendered, end="")


if __name__ == "__main__":
    main()
