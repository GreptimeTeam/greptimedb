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

"""Delete managed EC2 CI runners, or sweep expired instances by ownership tags."""

from __future__ import annotations

import argparse
import importlib.util
import os
import sys
import time
from pathlib import Path
from datetime import datetime, timedelta, timezone

from runner_utils import deregister_runner

_SPEC = importlib.util.spec_from_file_location("aws_ec2_runner_provision", Path(__file__).with_name("aws-ec2-runner-provision.py"))
assert _SPEC is not None and _SPEC.loader is not None
provision = importlib.util.module_from_spec(_SPEC)
sys.modules[_SPEC.name] = provision
_SPEC.loader.exec_module(provision)
make_client = provision.make_client
dump_console = provision.dump_console
MANAGED_BY = provision.MANAGED_BY
REPO_TAG = provision.REPO_TAG
RUN_TAG = provision.RUN_TAG
RUNNER_TAG = provision.RUNNER_TAG
TTL_TAG_KEY = provision.TTL_TAG_KEY
SWEEP_TTL_HOURS = 4


def managed_instances(client, repo: str, run_id: str | None = None,
                      instance_id: str | None = None) -> list[dict]:
    filters = [{"Name": "tag:managed-by", "Values": [MANAGED_BY]},
               {"Name": f"tag:{REPO_TAG}", "Values": [repo]}]
    if instance_id:
        # A retried teardown may have a newer workflow attempt than creation.
        filters.append({"Name": "instance-id", "Values": [instance_id]})
    elif run_id:
        filters.append({"Name": f"tag:{RUN_TAG}", "Values": [run_id]})
    return [instance
            for page in client.get_paginator("describe_instances").paginate(Filters=filters)
            for reservation in page["Reservations"] for instance in reservation["Instances"]]


def delete_instance(client, instance_id: str) -> bool:
    dump_console(client, instance_id)
    for attempt in range(12):
        try:
            client.terminate_instances(InstanceIds=[instance_id])
            client.get_waiter("instance_terminated").wait(
                InstanceIds=[instance_id], WaiterConfig={"Delay": 5, "MaxAttempts": 60})
            return True
        except Exception as error:
            code = getattr(error, "response", {}).get("Error", {}).get("Code")
            if code == "InvalidInstanceID.NotFound":
                if attempt < 11:
                    time.sleep(5)
                    continue
                # The ownership lookup observed this instance. Treat sustained
                # NotFound after bounded retries as an already-deleted VM.
                return True
            print(f"Failed to terminate {instance_id}: {error}")
            return False
    return False


def teardown(client, repo: str, token: str, run_id: str | None = None,
             instance_id: str | None = None, now: datetime | None = None,
             runner_name: str | None = None) -> int:
    # Lookup by ownership/run tags also recovers a create whose response or job
    # outputs were lost. Sweep never touches legacy build/JSONBench instances.
    instances = managed_instances(client, repo, run_id, instance_id)
    # Tags can lag RunInstances. Retry before concluding a targeted VM is gone.
    if run_id and not instances:
        for _ in range(11):
            time.sleep(5)
            instances = managed_instances(client, repo, run_id, instance_id)
            if instances:
                break
    ok = True
    found = False
    for instance in instances:
        identity = instance["InstanceId"]
        if instance_id and identity != instance_id:
            continue
        found = True
        tags = {t["Key"]: t["Value"] for t in instance.get("Tags", [])}
        name = tags.get(RUNNER_TAG, "")
        if not name.startswith("ci-ec2-"):
            print(f"Skipping {identity}: missing managed runner name")
            ok = False
            continue
        if not run_id:
            ttl = tags.get(TTL_TAG_KEY)
            try:
                hours = SWEEP_TTL_HOURS if ttl is None else int(ttl)
                if not 1 <= hours <= 168:
                    raise ValueError("TTL outside 1..168 hours")
            except (TypeError, ValueError):
                print(f"Skipping {identity}: invalid TTL {ttl!r}")
                continue
            if (now or datetime.now(timezone.utc)) - instance["LaunchTime"] < timedelta(hours=hours):
                continue
        # A GitHub outage must not prevent EC2 deletion, or vice versa.
        ok &= delete_instance(client, identity)
        try:
            ok &= deregister_runner(token, repo, name)
        except (Exception, SystemExit) as error:
            print(f"Failed to unregister {name}: {error}")
            ok = False
    if run_id and not found:
        # EC2 may already be gone while its GitHub registration remains.
        name = runner_name or f"ci-ec2-{run_id}"
        if not name.startswith("ci-ec2-"):
            raise ValueError("Invalid CI runner name")
        ok &= deregister_runner(token, repo, name)
    return 0 if ok else 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for name, env in (("region", "AWS_REGION"), ("repo", "GITHUB_REPOSITORY"),
                      ("github-token", "GH_PERSONAL_ACCESS_TOKEN"),
                      ("run-id", "AWS_EC2_RUN_ID"), ("instance-id", "AWS_EC2_INSTANCE_ID"),
                      ("runner-name", "AWS_EC2_RUNNER_NAME")):
        parser.add_argument(f"--{name}", default=os.environ.get(env))
    parser.add_argument("--sweep", action="store_true")
    args = parser.parse_args()
    for name in ("region", "repo", "github_token"):
        if not getattr(args, name):
            parser.error(f"Missing {name.replace('_', '-')}")
    if not args.sweep and not args.run_id:
        parser.error("Missing run-id for targeted teardown")
    return teardown(make_client(args.region), args.repo, args.github_token,
                    None if args.sweep else args.run_id,
                    None if args.sweep else args.instance_id,
                    runner_name=None if args.sweep else args.runner_name)


if __name__ == "__main__":
    raise SystemExit(main())
