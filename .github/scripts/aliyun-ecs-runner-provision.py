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

# This is the first `.github/scripts/` user of PEP 723 inline script metadata:
# `uv run` installs the pinned Aliyun SDK before executing. The SDK import stays
# lazy (inside `make_ecs_client`) so unit tests can import this module with a
# plain stdlib interpreter.
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   # Pin exactly after the first live run against the Aliyun API.
#   "alibabacloud_ecs20140526>=4.1.0,<6",
#   "alibabacloud_tea_openapi>=0.3.12,<1",
# ]
# ///

"""Provision an ephemeral Aliyun ECS query-regression runner.

Creates one pay-as-you-go ECS instance from the query-regression custom image
and waits until the instance registers itself as an ephemeral GitHub Actions
runner with a per-run label. Build caches live on the instance system disk
and disappear with the VM. The instance id is written to job outputs as soon
as the VM exists so the teardown job is the single DeleteInstance caller (a
timeout here used to delete and then teardown deleted again). On runner
online timeout the console is dumped and the job fails; teardown releases
the instance.

The Aliyun credentials are used only in this control-plane job (ubuntu-latest)
and are never passed to the ECS instance; the instance receives only a
short-lived GitHub runner registration token via user data.
"""

from __future__ import annotations

import argparse
import base64
import json
import os
import time
import urllib.error
import urllib.request
from dataclasses import dataclass

RUNNER_NAME_PREFIX = "qreg-ecs"
RUNNER_LABEL_PREFIX = "query-regression-ecs"
MANAGED_BY_TAG_KEY = "managed-by"
MANAGED_BY_TAG_VALUE = "query-regression-ci"
RUN_TAG_KEY = "query-regression-run-id"

# Runner cache paths on the instance system disk. They are created empty
# every provision and deleted with the VM.
CACHE_PATHS = (
    "/home/runner/.cargo/registry",
    "/home/runner/.cargo/git",
    "/home/runner/query-regression-target",
    "/home/runner/query-regression-cache-meta",
    "/home/runner/.cache/sccache",
)

RUNNER_ONLINE_TIMEOUT_SECONDS = 10 * 60
POLL_INTERVAL_SECONDS = 5

# Nightly thin-LTO of greptime peaks above ecs.c9i.2xlarge's 16 GiB. A swap
# file plus masking systemd-oomd lets the kernel reclaim rustc pages instead
# of SIGTERM-ing the runner service cgroup (GitHub then reports the step as
# "The operation was canceled"). Sized to match that 16 GiB instance; a
# 32 GiB type still benefits as a safety net. Lives on the system disk next
# to the cold build caches.
SWAP_FILE = "/swapfile"
SWAP_SIZE_GIB = 16


@dataclass(frozen=True)
class ProvisionConfig:
    region_id: str
    vswitch_id: str
    security_group_id: str
    image_id: str
    instance_type: str
    repo: str
    run_id: str
    github_token: str
    # Optional resource group; required when the RAM grant is scoped to one.
    resource_group_id: str | None = None
    # Runner identity inside the image; the workflow asserts the same values.
    runner_uid: str = "1001"
    runner_gid: str = "1001"


def runner_name_for_run(run_id: str) -> str:
    return f"{RUNNER_NAME_PREFIX}-{run_id}"


def runner_label_for_run(run_id: str) -> str:
    return f"{RUNNER_LABEL_PREFIX}-{run_id}"


def render_user_data(
    runner_name: str,
    runner_label: str,
    runner_token: str,
    repo: str,
    runner_uid: str = "1001",
    runner_gid: str = "1001",
) -> str:
    """Render the cloud-init shell script for the runner instance."""
    destinations = " ".join(f'"{dst}"' for dst in CACHE_PATHS)
    cache_setup = f"""# Caches live on the system disk, are deleted with the instance, and every
# run compiles cold. Within-run reuse (base warming candidate via the shared
# target dir and sccache) still applies.
mkdir -p {destinations}
chown -R {runner_uid}:{runner_gid} /home/runner"""
    swap_setup = f"""# Mask systemd-oomd before enabling swap: Ubuntu 24.04 kills the whole
# service cgroup on PSI pressure, and swap thrashing looks like pressure.
systemctl disable --now systemd-oomd.socket systemd-oomd.service || true
systemctl mask systemd-oomd.socket systemd-oomd.service || true
# apt-daily-upgrade / unattended-upgrades can `systemctl restart` services
# whose libraries were updated. That SIGTERM-s the runner mid-job; GitHub
# records UserCancelled (job still valid, child exit 143). These VMs live
# ~1h; security updates belong in the image, not at job time.
systemctl disable --now unattended-upgrades.service apt-daily.timer apt-daily-upgrade.timer apt-daily.service apt-daily-upgrade.service || true
systemctl mask unattended-upgrades.service apt-daily.timer apt-daily-upgrade.timer apt-daily.service apt-daily-upgrade.service || true
systemctl stop unattended-upgrades.service || true
killall -9 unattended-upgr || true
cat > /etc/apt/apt.conf.d/99disable-auto-updates <<'APTEOF'
APT::Periodic::Update-Package-Lists "0";
APT::Periodic::Unattended-Upgrade "0";
APT::Periodic::Download-Upgradeable-Packages "0";
APTEOF
if [[ ! -f "{SWAP_FILE}" ]]; then
  fallocate --length {SWAP_SIZE_GIB}G "{SWAP_FILE}"
  chmod 600 "{SWAP_FILE}"
  mkswap "{SWAP_FILE}"
fi
swapon "{SWAP_FILE}"
sysctl --write vm.swappiness=10
swapon --show
free --human"""
    return f"""#!/bin/bash
set -euo pipefail

{cache_setup}

{swap_setup}

cat > /etc/ephemeral-github-runner.env <<'ENVEOF'
RUNNER_NAME={runner_name}
RUNNER_LABELS={runner_label}
RUNNER_TOKEN={runner_token}
REPO_URL=https://github.com/{repo}
# The container image baked /opt/cargo/bin into PATH via Dockerfile ENV; on
# the host nothing inherits that, so publish it through the unit's
# EnvironmentFile: runner job processes inherit the runner's environment.
PATH=/opt/cargo/bin:/usr/local/sbin:/usr/local/bin:/usr/sbin:/usr/bin:/sbin:/bin
ENVEOF

# Stream the runner service's own logs (config output, job lifecycle, crash
# messages) to the serial console, next to the kernel's OOM-killer records.
# Teardown dumps the console tail before deleting the instance, so the
# witness of what killed the runner survives the machine.
mkdir -p /etc/systemd/system/ephemeral-github-runner.service.d
cat > /etc/systemd/system/ephemeral-github-runner.service.d/console.conf <<'CONFEOF'
[Service]
StandardOutput=journal+console
StandardError=journal+console
CONFEOF
# systemd's DefaultOOMPolicy=stop SIGTERM-s the whole unit when *any*
# process in the cgroup is OOM-killed (typically rustc during thin LTO).
# That is the "Session terminated, killing shell..." / job Canceled path:
# runuser dies, GitHub deletes the ephemeral registration, always() steps
# never run. continue keeps the runner up so cargo can report signal 9.
cat > /etc/systemd/system/ephemeral-github-runner.service.d/oom.conf <<'CONFEOF'
[Service]
OOMPolicy=continue
CONFEOF
systemctl daemon-reload
# restart (not start): the image enables this unit, so it may already
# have come up without the drop-ins if it raced cloud-init. restart
# applies OOMPolicy=continue to the running cgroup. --no-block matters:
# newer image units carry After=cloud-final.service, and this script IS
# cloud-final — a blocking restart would deadlock until TimeoutStartSec.
systemctl restart --no-block ephemeral-github-runner.service
"""


def encode_user_data(script: str) -> str:
    return base64.b64encode(script.encode("utf-8")).decode("ascii")


def github_api(token: str, method: str, path: str, body: dict | None = None) -> dict:
    data = json.dumps(body).encode("utf-8") if body is not None else None
    request = urllib.request.Request(
        f"https://api.github.com{path}",
        data=data,
        method=method,
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        # GitHub's error body says exactly why (e.g. "Must have admin rights to
        # Repository" for a PAT without the required scope); surface it instead
        # of a bare "HTTP Error 403".
        body = error.read().decode("utf-8", "replace")
        raise SystemExit(
            f"GitHub API {method} {path} failed: HTTP {error.code}: {body}\n"
            "The token comes from the GH_PERSONAL_ACCESS_TOKEN secret; it needs "
            "'repo' scope (classic PAT) or 'Administration: write' on the "
            "repository (fine-grained PAT)."
        ) from error


def create_registration_token(github_token: str, repo: str) -> str:
    response = github_api(
        github_token,
        "POST",
        f"/repos/{repo}/actions/runners/registration-token",
        body={},
    )
    return response["token"]


def find_runner_by_name(github_token: str, repo: str, name: str) -> dict | None:
    page = 1
    while True:
        response = github_api(
            github_token,
            "GET",
            f"/repos/{repo}/actions/runners?per_page=100&page={page}",
        )
        runners = response.get("runners", [])
        for runner in runners:
            if runner.get("name") == name:
                return runner
        if len(runners) < 100:
            return None
        page += 1


def make_ecs_client(config: ProvisionConfig):
    from alibabacloud_ecs20140526.client import Client as EcsClient
    from alibabacloud_tea_openapi.models import Config as OpenApiConfig

    access_key_id = os.environ.get("ALIBABA_CLOUD_ACCESS_KEY_ID", "")
    access_key_secret = os.environ.get("ALIBABA_CLOUD_ACCESS_KEY_SECRET", "")
    if not access_key_id or not access_key_secret:
        raise SystemExit(
            "ALIBABA_CLOUD_ACCESS_KEY_ID/SECRET are empty; in CI they come from the "
            "ALICLOUD_ECS_ACCESS_KEY_ID/SECRET repository secrets (a missing or "
            "misnamed secret expands to an empty string)."
        )
    return EcsClient(
        OpenApiConfig(
            access_key_id=access_key_id,
            access_key_secret=access_key_secret,
            region_id=config.region_id,
            endpoint=f"ecs.{config.region_id}.aliyuncs.com",
        )
    )


def wait_for_instance_status(client, region_id: str, instance_id: str, wanted: str, deadline: float) -> None:
    from alibabacloud_ecs20140526 import models as ecs_models

    while time.monotonic() < deadline:
        response = client.describe_instances(
            ecs_models.DescribeInstancesRequest(
                region_id=region_id, instance_ids=json.dumps([instance_id])
            )
        )
        instances = response.body.instances.instance
        if instances and instances[0].status == wanted:
            return
        time.sleep(POLL_INTERVAL_SECONDS)
    raise TimeoutError(f"Instance {instance_id} did not reach status {wanted} in time")


def fetch_console_output(client, region_id: str, instance_id: str) -> str:
    from alibabacloud_ecs20140526 import models as ecs_models

    response = client.get_instance_console_output(
        ecs_models.GetInstanceConsoleOutputRequest(region_id=region_id, instance_id=instance_id)
    )
    return base64.b64decode(response.body.console_output or "").decode("utf-8", "replace")


class ConsoleTailer:
    """Incrementally prints new serial-console lines so cloud-init progress is
    visible in the CI log while waiting, not only after a failure dump."""

    def __init__(self, client, region_id: str, instance_id: str) -> None:
        self.client = client
        self.region_id = region_id
        self.instance_id = instance_id
        self.printed_lines = 0

    def poll(self) -> None:
        try:
            lines = fetch_console_output(self.client, self.region_id, self.instance_id).splitlines()
        except Exception as error:  # noqa: BLE001
            print(f"[console] unable to fetch console output: {error}", flush=True)
            return
        # The API returns a bounded tail; if it ever shrinks, restart from the
        # beginning of the new buffer rather than skipping lines.
        if len(lines) < self.printed_lines:
            self.printed_lines = 0
        for line in lines[self.printed_lines :]:
            print(f"[console] {line}", flush=True)
        self.printed_lines = len(lines)


def dump_console_output(client, region_id: str, instance_id: str) -> None:
    try:
        output = fetch_console_output(client, region_id, instance_id)
    except Exception as error:  # noqa: BLE001
        print(f"Unable to fetch console output for {instance_id}: {error}", flush=True)
        return
    tail = "\n".join(output.splitlines()[-80:])
    print(f"::group::Console output tail for {instance_id}\n{tail}\n::endgroup::", flush=True)
    summary_path = os.environ.get("GITHUB_STEP_SUMMARY")
    if summary_path:
        with open(summary_path, "a", encoding="utf-8") as summary:
            summary.write(f"<details><summary>ECS console output ({instance_id})</summary>\n\n```\n")
            summary.write(tail)
            summary.write("\n```\n</details>\n")


def append_github_output(name: str, value: str) -> None:
    output_path = os.environ.get("GITHUB_OUTPUT")
    if output_path:
        with open(output_path, "a", encoding="utf-8") as output:
            output.write(f"{name}={value}\n")


def run_instance(client, config: ProvisionConfig, user_data: str) -> str:
    from alibabacloud_ecs20140526 import models as ecs_models

    request = ecs_models.RunInstancesRequest(
        region_id=config.region_id,
        image_id=config.image_id,
        resource_group_id=config.resource_group_id,
        instance_type=config.instance_type,
        v_switch_id=config.vswitch_id,
        security_group_id=config.security_group_id,
        instance_name=runner_name_for_run(config.run_id),
        host_name=runner_name_for_run(config.run_id),
        description=f"Ephemeral query-regression runner for run {config.run_id}",
        amount=1,
        instance_charge_type="PostPaid",
        spot_strategy="NoSpot",
        internet_charge_type="PayByTraffic",
        internet_max_bandwidth_out=100,
        # Holds the image (~12G), the 16G swapfile, the source checkout, the
        # base+candidate cluster data homes, and cold build caches. Deleted
        # with the instance.
        system_disk=ecs_models.RunInstancesRequestSystemDisk(category="cloud_essd", size="40"),
        user_data=user_data,
        tag=[
            ecs_models.RunInstancesRequestTag(key=MANAGED_BY_TAG_KEY, value=MANAGED_BY_TAG_VALUE),
            ecs_models.RunInstancesRequestTag(key=RUN_TAG_KEY, value=config.run_id),
        ],
    )
    response = client.run_instances(request)
    instance_id = response.body.instance_id_sets.instance_id_set[0]
    return instance_id


def provision(config: ProvisionConfig) -> int:
    client = make_ecs_client(config)
    runner_name = runner_name_for_run(config.run_id)
    runner_label = runner_label_for_run(config.run_id)

    registration_token = create_registration_token(config.github_token, config.repo)
    user_data = encode_user_data(
        render_user_data(
            runner_name,
            runner_label,
            registration_token,
            config.repo,
            config.runner_uid,
            config.runner_gid,
        )
    )

    print(f"::group::Run instance {runner_name}", flush=True)
    instance_id = run_instance(client, config, user_data)
    print(f"Instance id: {instance_id}", flush=True)
    # Teardown is the only DeleteInstance caller. Write outputs immediately
    # so a later status/online failure still releases this VM.
    append_github_output("label", runner_label)
    append_github_output("instance_id", instance_id)
    append_github_output("runner_name", runner_name)
    wait_for_instance_status(client, config.region_id, instance_id, "Running", time.monotonic() + 5 * 60)
    print("::endgroup::", flush=True)

    print("::group::Wait for runner online", flush=True)
    deadline = time.monotonic() + RUNNER_ONLINE_TIMEOUT_SECONDS
    tailer = ConsoleTailer(client, config.region_id, instance_id)
    last_console_poll = 0.0
    while time.monotonic() < deadline:
        runner = find_runner_by_name(config.github_token, config.repo, runner_name)
        if runner is not None:
            status = runner.get("status")
            print(f"Runner {runner_name} status: {status}", flush=True)
            if status == "online":
                print("::endgroup::", flush=True)
                print(f"Runner {runner_name} is online with label {runner_label}", flush=True)
                return 0
        # The serial console lags the guest by a minute or so; 30s polling is
        # enough to follow cloud-init without tripping API throttling.
        if time.monotonic() - last_console_poll >= 30:
            tailer.poll()
            last_console_poll = time.monotonic()
        time.sleep(POLL_INTERVAL_SECONDS)

    print("::endgroup::", flush=True)
    print(
        f"Runner {runner_name} did not come online in time; "
        "leaving instance for the teardown job to delete",
        flush=True,
    )
    dump_console_output(client, config.region_id, instance_id)
    return 1


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--region-id", default=os.environ.get("ALIYUN_ECS_REGION_ID"))
    parser.add_argument("--vswitch-id", default=os.environ.get("ALIYUN_ECS_VSWITCH_ID"))
    parser.add_argument("--security-group-id", default=os.environ.get("ALIYUN_ECS_SECURITY_GROUP_ID"))
    parser.add_argument("--image-id", default=os.environ.get("QUERY_REGRESSION_ECS_IMAGE_ID"))
    parser.add_argument("--instance-type", default=os.environ.get("ALIYUN_ECS_INSTANCE_TYPE"))
    parser.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY"))
    parser.add_argument("--run-id", default=os.environ.get("GITHUB_RUN_ID"))
    parser.add_argument("--github-token", default=os.environ.get("GH_PERSONAL_ACCESS_TOKEN"))
    parser.add_argument("--runner-uid", default=os.environ.get("QUERY_REGRESSION_RUNNER_UID", "1001"))
    parser.add_argument("--runner-gid", default=os.environ.get("QUERY_REGRESSION_RUNNER_GID", "1001"))
    parser.add_argument("--resource-group-id", default=os.environ.get("ALIYUN_ECS_RESOURCE_GROUP_ID"))
    args = parser.parse_args()

    missing = [
        name
        for name, value in vars(args).items()
        if name not in ("resource_group_id",)
        and (value is None or (isinstance(value, str) and not value))
    ]
    if missing:
        flags = ", ".join(f"--{name.replace('_', '-')}" for name in missing)
        raise SystemExit(f"Missing required configuration: {flags} (or their env defaults)")

    config = ProvisionConfig(
        region_id=args.region_id,
        vswitch_id=args.vswitch_id,
        security_group_id=args.security_group_id,
        image_id=args.image_id,
        instance_type=args.instance_type,
        repo=args.repo,
        run_id=args.run_id,
        github_token=args.github_token,
        resource_group_id=args.resource_group_id or None,
        runner_uid=args.runner_uid,
        runner_gid=args.runner_gid,
    )
    return provision(config)


if __name__ == "__main__":
    raise SystemExit(main())
