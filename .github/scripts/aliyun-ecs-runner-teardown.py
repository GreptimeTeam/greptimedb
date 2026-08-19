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

# PEP 723 inline metadata (see aliyun-ecs-runner-provision.py for the
# convention). The SDK import stays lazy so unit tests run on a plain
# stdlib interpreter.
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "alibabacloud_ecs20140526>=4.1.0,<6",
#   "alibabacloud_tea_openapi>=2,<3",
# ]
# ///

"""Tear down ephemeral Aliyun ECS query-regression runners.

Two modes:

- Targeted (per workflow run): delete one instance by id and deregister its
  runner by name. Both steps are idempotent and best-effort; a missing
  instance or runner is not an error.
- Sweep (scheduled janitor): delete every instance tagged as managed by
  query-regression CI whose creation time is older than the given TTL, and
  deregister the matching runners. This is the safety net for runs whose
  teardown job never executed.
"""

from __future__ import annotations

import argparse
import importlib.util
import os
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

# Reuse the GitHub API client, runner lookup, and tag constants from the
# provision script.
_PROVISION_SPEC = importlib.util.spec_from_file_location(
    "aliyun_ecs_runner_provision",
    Path(__file__).resolve().parent / "aliyun-ecs-runner-provision.py",
)
assert _PROVISION_SPEC is not None and _PROVISION_SPEC.loader is not None
provision = importlib.util.module_from_spec(_PROVISION_SPEC)
sys.modules[_PROVISION_SPEC.name] = provision
_PROVISION_SPEC.loader.exec_module(provision)

SWEEP_TTL = timedelta(hours=4)
# ECS DescribeInstances creation_time is ISO8601 UTC, e.g. "2026-08-17T02:13Z".
CREATION_TIME_FORMATS = ("%Y-%m-%dT%H:%M:%SZ", "%Y-%m-%dT%H:%MZ")


def parse_creation_time(value: str) -> datetime:
    for fmt in CREATION_TIME_FORMATS:
        try:
            return datetime.strptime(value, fmt).replace(tzinfo=timezone.utc)
        except ValueError:
            continue
    raise ValueError(f"Unparseable ECS creation time: {value}")


def expired_instance_names(
    instances: list[tuple[str, str, str]], now: datetime, ttl: timedelta
) -> list[tuple[str, str]]:
    """Pick (instance_id, instance_name) pairs whose creation is older than ttl.

    `instances` items are (instance_id, instance_name, creation_time).
    """
    expired = []
    for instance_id, instance_name, creation_time in instances:
        age = now - parse_creation_time(creation_time)
        if age >= ttl:
            expired.append((instance_id, instance_name))
    return expired


def make_ecs_client(region_id: str):
    from alibabacloud_ecs20140526.client import Client as EcsClient
    from alibabacloud_tea_openapi.models import Config as OpenApiConfig

    return EcsClient(
        OpenApiConfig(
            access_key_id=os.environ["ALIBABA_CLOUD_ACCESS_KEY_ID"],
            access_key_secret=os.environ["ALIBABA_CLOUD_ACCESS_KEY_SECRET"],
            region_id=region_id,
            endpoint=f"ecs.{region_id}.aliyuncs.com",
        )
    )


def delete_instance(client, instance_id: str) -> bool:
    from alibabacloud_ecs20140526 import models as ecs_models

    try:
        client.delete_instance(ecs_models.DeleteInstanceRequest(instance_id=instance_id, force=True))
        print(f"Deleted instance {instance_id}", flush=True)
        return True
    except Exception as error:  # noqa: BLE001
        if "InvalidInstanceId.NotFound" in str(error):
            print(f"Instance {instance_id} already gone", flush=True)
            return True
        print(f"Failed to delete instance {instance_id}: {error}", flush=True)
        return False


def deregister_runner(token: str, repo: str, runner_name: str) -> bool:
    runner = provision.find_runner_by_name(token, repo, runner_name)
    if runner is None:
        print(f"Runner {runner_name} is not registered", flush=True)
        return True
    try:
        provision.github_api(token, "DELETE", f"/repos/{repo}/actions/runners/{runner['id']}")
        print(f"Deregistered runner {runner_name} (id {runner['id']})", flush=True)
        return True
    except Exception as error:  # noqa: BLE001
        print(f"Failed to deregister runner {runner_name}: {error}", flush=True)
        return False


def list_managed_instances(client, region_id: str) -> list[tuple[str, str, str]]:
    from alibabacloud_ecs20140526 import models as ecs_models

    result: list[tuple[str, str, str]] = []
    next_token = None
    while True:
        request = ecs_models.DescribeInstancesRequest(
            region_id=region_id,
            tag=[
                ecs_models.DescribeInstancesRequestTag(
                    key=provision.MANAGED_BY_TAG_KEY, value=provision.MANAGED_BY_TAG_VALUE
                )
            ],
            max_results=100,
            next_token=next_token,
        )
        response = client.describe_instances(request)
        for instance in response.body.instances.instance:
            result.append((instance.instance_id, instance.instance_name, instance.creation_time))
        next_token = response.body.next_token
        if not next_token:
            return result


def sweep(client, region_id: str, repo: str, github_token: str, ttl: timedelta) -> int:
    instances = list_managed_instances(client, region_id)
    print(f"Found {len(instances)} managed instance(s) in {region_id}", flush=True)
    expired = expired_instance_names(instances, datetime.now(timezone.utc), ttl)
    ok = True
    for instance_id, instance_name in expired:
        print(f"Instance {instance_id} ({instance_name}) exceeds TTL {ttl}; deleting", flush=True)
        # Runner names mirror instance names by construction in the provision
        # script (both are qreg-ecs-<run_id>).
        ok &= delete_instance(client, instance_id)
        ok &= deregister_runner(github_token, repo, instance_name)
    return 0 if ok else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--region-id", default=os.environ.get("ALIYUN_ECS_REGION_ID"))
    parser.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY"))
    parser.add_argument("--github-token", default=os.environ.get("GH_PERSONAL_ACCESS_TOKEN"))
    parser.add_argument("--instance-id", default=os.environ.get("QUERY_REGRESSION_ECS_INSTANCE_ID"))
    parser.add_argument("--runner-name", default=os.environ.get("QUERY_REGRESSION_ECS_RUNNER_NAME"))
    parser.add_argument(
        "--sweep",
        action="store_true",
        help="Janitor mode: delete all managed instances older than --sweep-ttl-hours.",
    )
    parser.add_argument("--sweep-ttl-hours", type=float, default=SWEEP_TTL.total_seconds() / 3600)
    args = parser.parse_args()

    for name in ("region_id", "repo", "github_token"):
        if not getattr(args, name):
            raise SystemExit(f"Missing required configuration: --{name.replace('_', '-')}")

    client = make_ecs_client(args.region_id)

    if args.sweep:
        return sweep(
            client, args.region_id, args.repo, args.github_token, timedelta(hours=args.sweep_ttl_hours)
        )

    ok = True
    if args.instance_id:
        ok &= delete_instance(client, args.instance_id)
    else:
        print("No instance id given; skipping instance deletion", flush=True)
    if args.runner_name:
        ok &= deregister_runner(args.github_token, args.repo, args.runner_name)
    else:
        print("No runner name given; skipping runner deregistration", flush=True)
    return 0 if ok else 1


if __name__ == "__main__":
    raise SystemExit(main())
