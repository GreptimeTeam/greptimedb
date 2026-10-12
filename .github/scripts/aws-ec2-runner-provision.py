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

"""Provision an ephemeral EC2 CI runner from an Ubuntu x86_64 AMI."""

from __future__ import annotations

import argparse
import os
import re
import shlex
import time
from dataclasses import dataclass

from runner_utils import append_github_output, create_registration_token, find_runner_by_name, github_api

MANAGED_BY = "query-regression-ci"
REPO_TAG = "github-repository"
RUN_TAG = "query-regression-run-id"
RUNNER_TAG = "github-runner-name"
TTL_TAG_KEY = "runner-ttl-hours"


def make_client(region: str):
    import boto3
    from botocore.config import Config

    return boto3.client("ec2", region_name=region, config=Config(
        connect_timeout=10, read_timeout=30,
        retries={"mode": "standard", "max_attempts": 3},
    ))


@dataclass(frozen=True)
class ProvisionConfig:
    region: str
    image_id: str
    instance_type: str
    subnet_id: str
    security_group_id: str
    repo: str
    run_id: str
    github_token: str
    system_disk_gib: int = 500
    ttl_hours: int = 8

    def __post_init__(self):
        if not re.fullmatch(r"[0-9]+-[0-9]+", self.run_id):
            raise ValueError("run-id must be GitHub run_id-run_attempt")
        if not 20 <= self.system_disk_gib <= 2048:
            raise ValueError("system-disk-gib must be between 20 and 2048")
        if not 1 <= self.ttl_hours <= 168:
            raise ValueError("ttl-hours must be between 1 and 168")

    @property
    def runner_name(self) -> str:
        return f"ci-ec2-{self.run_id}"


def resolve_image(client, image_id: str) -> str:
    if image_id != "auto":
        return image_id
    images = client.describe_images(
        Owners=["099720109477"],
        Filters=[
            {"Name": "name", "Values": ["ubuntu/images/hvm-ssd-gp3/ubuntu-noble-24.04-amd64-server-*"]},
            {"Name": "state", "Values": ["available"]},
            {"Name": "architecture", "Values": ["x86_64"]},
        ],
    )["Images"]
    if not images:
        raise ValueError("No Canonical Ubuntu 24.04 amd64 AMI found in this region")
    image = max(images, key=lambda image: image["CreationDate"])
    print(f"Using AMI {image['ImageId']}: {image['Name']}")
    return image["ImageId"]


def root_disk(client, image_id: str, size: int) -> dict:
    images = client.describe_images(ImageIds=[image_id])["Images"]
    if len(images) != 1:
        raise ValueError(f"AMI not found: {image_id}")
    image = images[0]
    if image["State"] != "available" or image["Architecture"] != "x86_64":
        raise ValueError("Runner AMI must be available and x86_64")
    root = next((d for d in image["BlockDeviceMappings"]
                 if d["DeviceName"] == image["RootDeviceName"] and "Ebs" in d), None)
    if root is None:
        raise ValueError("Runner AMI must have an EBS root disk")
    minimum = root["Ebs"]["VolumeSize"]
    if size < minimum:
        raise ValueError(f"Requested root disk {size} GiB is smaller than AMI minimum {minimum} GiB")
    return {"DeviceName": image["RootDeviceName"], "Ebs": {
        "VolumeSize": size, "VolumeType": "gp3", "Iops": 3000,
        "Throughput": 125, "DeleteOnTermination": True,
    }}


def runner_download(token: str, repo: str) -> str:
    runners = github_api(token, "GET", f"/repos/{repo}/actions/runners/downloads")
    return next(r["download_url"] for r in runners if r["os"] == "linux" and r["architecture"] == "x64")


def render_user_data(name: str, token: str, repo: str, download_url: str) -> str:
    # Quote every external value before embedding it in the root shell script.
    name, token, repo, download_url = map(shlex.quote, (name, token, repo, download_url))
    return f"""#!/bin/bash
set -euo pipefail
for tool in git curl tar sudo; do command -v "$tool"; done
# Mask systemd-oomd before enabling swap: Ubuntu 24.04 kills the whole
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
if [[ ! -f "/swapfile" ]]; then
  fallocate --length 16G "/swapfile"
  chmod 600 "/swapfile"
  mkswap "/swapfile"
fi
swapon "/swapfile"
sysctl --write vm.swappiness=10
swapon --show
free --human

id runner >/dev/null 2>&1 || useradd --create-home --shell /bin/bash runner
# Establish membership before the runner service starts; setup installs Docker later.
getent group docker >/dev/null || groupadd --system docker
usermod -aG docker runner
printf 'runner ALL=(ALL) NOPASSWD: ALL\\n' > /etc/sudoers.d/ci-runner
chmod 0440 /etc/sudoers.d/ci-runner
visudo -cf /etc/sudoers.d/ci-runner
install -d -o runner -g "$(id -gn runner)" /home/runner/benchmark-data /opt/ci-github-runner
cd /opt/ci-github-runner
curl --fail --location --retry 3 {download_url} | tar xz
./bin/installdependencies.sh
chown -R runner:"$(id -gn runner)" /opt/ci-github-runner
runuser -u runner -- ./config.sh --unattended --ephemeral --disableupdate --url https://github.com/{repo} --token {token} --name {name} --labels {name}
./svc.sh install runner
# Match the ECS runner's behavior when a child process is OOM-killed.
service_name=$(cat .service)
mkdir -p "/etc/systemd/system/$service_name.d"
cat > "/etc/systemd/system/$service_name.d/oom.conf" <<'EOF'
[Service]
OOMPolicy=continue
EOF
systemctl daemon-reload
# Package installation in a job must not restart its runner service.
install -d /etc/needrestart/conf.d
cat > /etc/needrestart/conf.d/actions_runner_services.conf <<'EOF'
$nrconf{{override_rc}}{{qr(^actions\\.runner\\..+\\.service$)}} = 0;
EOF
./svc.sh start
"""


def run_instance(client, config: ProvisionConfig) -> str:
    image_id = resolve_image(client, config.image_id)
    disk = root_disk(client, image_id, config.system_disk_gib)
    instance = client.describe_instance_types(InstanceTypes=[config.instance_type])["InstanceTypes"][0]
    if "x86_64" not in instance["ProcessorInfo"]["SupportedArchitectures"]:
        raise ValueError("Runner instance type must support x86_64")
    token = create_registration_token(config.github_token, config.repo)
    print(f"::add-mask::{token}")
    script = render_user_data(config.runner_name, token, config.repo, runner_download(config.github_token, config.repo))
    tags = [
        {"Key": "Name", "Value": config.runner_name},
        {"Key": "managed-by", "Value": MANAGED_BY},
        {"Key": REPO_TAG, "Value": config.repo},
        {"Key": RUN_TAG, "Value": config.run_id},
        {"Key": RUNNER_TAG, "Value": config.runner_name},
        {"Key": TTL_TAG_KEY, "Value": str(config.ttl_hours)},
    ]
    response = client.run_instances(
        ImageId=image_id, InstanceType=config.instance_type,
        MinCount=1, MaxCount=1, SubnetId=config.subnet_id,
        SecurityGroupIds=[config.security_group_id],
        BlockDeviceMappings=[disk], UserData=script,
        ClientToken=config.runner_name,
        MetadataOptions={"HttpTokens": "required", "HttpEndpoint": "enabled"},
        TagSpecifications=[{"ResourceType": kind, "Tags": tags} for kind in ("instance", "volume")],
    )
    return response["Instances"][0]["InstanceId"]


def dump_console(client, instance_id: str) -> None:
    try:
        # Boto3 decodes the API's base64 console output.
        output = client.get_console_output(InstanceId=instance_id, Latest=True).get("Output", "")
        print(f"::group::EC2 console {instance_id}")
        print("\n".join(output.splitlines()[-80:]))
        print("::endgroup::")
    except Exception as error:
        print(f"Unable to read console for {instance_id}: {error}")


def provision(client, config: ProvisionConfig) -> int:
    instance_id = run_instance(client, config)
    # Publish before waiting: failed registration must still reach teardown.
    for key, value in (("instance_id", instance_id),
                       ("label", config.runner_name), ("runner_name", config.runner_name)):
        append_github_output(key, value)
    try:
        client.get_waiter("instance_running").wait(
            InstanceIds=[instance_id], WaiterConfig={"Delay": 5, "MaxAttempts": 60})
        deadline = time.monotonic() + 10 * 60
        while time.monotonic() < deadline:
            runner = find_runner_by_name(config.github_token, config.repo, config.runner_name)
            if runner and runner.get("status") == "online":
                print(f"Runner {config.runner_name} online: {instance_id}")
                return 0
            time.sleep(5)
        raise TimeoutError(f"Runner {config.runner_name} did not come online")
    finally:
        dump_console(client, instance_id)


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    for name, env in (("region", "AWS_REGION"), ("repo", "GITHUB_REPOSITORY"),
                      ("github-token", "GH_PERSONAL_ACCESS_TOKEN"),
                      ("run-id", "AWS_EC2_RUN_ID"), ("image-id", "AWS_EC2_IMAGE_ID"),
                      ("instance-type", "AWS_EC2_INSTANCE_TYPE"),
                      ("subnet-id", "AWS_EC2_SUBNET_ID"),
                      ("security-group-id", "AWS_EC2_SECURITY_GROUP_ID")):
        parser.add_argument(f"--{name}", default=os.environ.get(env) or ("auto" if name == "image-id" else None))
    parser.add_argument("--system-disk-gib", type=int, default=os.environ.get("AWS_EC2_SYSTEM_DISK_GIB", "500"))
    parser.add_argument("--ttl-hours", type=int, default=os.environ.get("AWS_EC2_TTL_HOURS", "8"))
    args = parser.parse_args()
    for name, value in vars(args).items():
        if value is None or value == "":
            parser.error(f"Missing {name.replace('_', '-')}")
    config = ProvisionConfig(**vars(args))
    return provision(make_client(args.region), config)


if __name__ == "__main__":
    raise SystemExit(main())
