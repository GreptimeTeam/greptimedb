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

# PEP 723 inline metadata (see .github/scripts/aliyun-ecs-runner-provision.py
# for the convention).
# /// script
# requires-python = ">=3.10"
# dependencies = [
#   "alibabacloud_ecs20140526>=4.1.0,<6",
#   "alibabacloud_tea_openapi>=0.3.12,<1",
# ]
# ///

"""Build the query-regression ECS custom image (manual ops tool).

Boots a temporary pay-as-you-go ECS instance from a public Ubuntu 24.04 image,
builds the existing runner container image (the Dockerfile in the parent
directory remains the single source of the tool contract), materializes the
tool directories onto the host filesystem so the workflow's "Verify runner
image tools" step holds unchanged, installs the ephemeral-runner systemd unit,
and snapshots the result as a custom image. The temporary instance is deleted
afterwards.

Sentinel polling uses Cloud Assistant (RunCommand/DescribeInvocations), which
is preinstalled on Aliyun public Ubuntu images.

Usage:
  uv run .github/runner-scale-sets/query-regression/ecs-image/build-ecs-image.py \
    --region-id cn-hangzhou --vswitch-id vsw-... --security-group-id sg-... \
    --base-image-id ubuntu_24_04_x64_20G_alibase_*.vhd
"""

from __future__ import annotations

import argparse
import base64
import os
import time
from pathlib import Path

ASSETS_DIR = Path(__file__).resolve().parent
DONE_SENTINEL = "/var/lib/qreg-image-done"
POLL_INTERVAL_SECONDS = 15
BUILD_TIMEOUT_SECONDS = 60 * 60

# Same apt package contract as the runner Dockerfile; the base
# actions-runner image is Ubuntu 24.04, so an Ubuntu 24.04 host resolves the
# same tool versions (protoc 3.21.12, mold 2.30.0, Python 3.12.3).
# Docker itself comes from Docker's official repository (docker-ce), not the
# distribution-packaged docker.io.
APT_PACKAGES = [
    "build-essential",
    "ca-certificates",
    "clang",
    "cmake",
    "curl",
    "git",
    "gpg",
    "gzip",
    "jq",
    "libprotobuf-dev",
    "libssl-dev",
    "mold",
    "openssh-client",
    "pkg-config",
    "protobuf-compiler",
    "python3",
    "sudo",
    "tar",
    "unzip",
    "wget",
    "xz-utils",
    "zip",
    "zstd",
]

DOCKER_CE_PACKAGES = "docker-ce docker-ce-cli containerd.io docker-buildx-plugin"


def render_user_data(dockerfile: str, start_runner: str, unit: str) -> str:
    dockerfile_b64 = base64.b64encode(dockerfile.encode()).decode()
    start_runner_b64 = base64.b64encode(start_runner.encode()).decode()
    unit_b64 = base64.b64encode(unit.encode()).decode()
    packages = " ".join(APT_PACKAGES)
    return f"""#!/bin/bash
set -euo pipefail

apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends {packages}

# Docker from the official repository, not the distribution-packaged docker.io.
install -m 0755 -d /etc/apt/keyrings
curl -fsSL https://download.docker.com/linux/ubuntu/gpg | gpg --dearmor -o /etc/apt/keyrings/docker.gpg
chmod a+r /etc/apt/keyrings/docker.gpg
. /etc/os-release
echo "deb [arch=$(dpkg --print-architecture) signed-by=/etc/apt/keyrings/docker.gpg] https://download.docker.com/linux/ubuntu ${{VERSION_CODENAME}} stable" \
  > /etc/apt/sources.list.d/docker.list
apt-get update
DEBIAN_FRONTEND=noninteractive apt-get install -y --no-install-recommends {DOCKER_CE_PACKAGES}

base64 -d > /tmp/Dockerfile <<'EOF'
{dockerfile_b64}
EOF
mkdir -p /tmp/image-context
docker build --platform linux/amd64 -f /tmp/Dockerfile -t qreg-runner:local /tmp/image-context

# Materialize the tool contract onto the host filesystem.
container="$(docker create qreg-runner:local)"
trap 'docker rm -f "${{container}}" >/dev/null 2>&1 || true' EXIT
docker cp "${{container}}:/opt/rustup" /opt/rustup
docker cp "${{container}}:/opt/cargo" /opt/cargo
for tool in uv uvx otelgen sccache; do
  docker cp "${{container}}:/usr/local/bin/${{tool}}" "/usr/local/bin/${{tool}}"
done
docker cp "${{container}}:/home/runner" /home/runner

# Runner identity mirrors the container image (UID/GID 1001).
groupadd --gid 1001 runner 2>/dev/null || true
useradd --uid 1001 --gid 1001 --home-dir /home/runner --shell /bin/bash runner 2>/dev/null || true
chown -R 1001:1001 /home/runner

mkdir -p /opt/ephemeral-github-runner
base64 -d > /opt/ephemeral-github-runner/start-runner.sh <<'EOF'
{start_runner_b64}
EOF
chmod 0755 /opt/ephemeral-github-runner/start-runner.sh
base64 -d > /etc/systemd/system/ephemeral-github-runner.service <<'EOF'
{unit_b64}
EOF
systemctl daemon-reload
systemctl enable ephemeral-github-runner.service

# Keep the image free of the build-time docker state.
docker rm -f "${{container}}" >/dev/null
docker image rm qreg-runner:local >/dev/null
trap - EXIT

touch {DONE_SENTINEL}
echo "query-regression image build completed"
"""


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


def wait_for_instance_status(client, region_id: str, instance_id: str, wanted: str, deadline: float) -> None:
    from alibabacloud_ecs20140526 import models as ecs_models

    import json

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


def run_check_command(client, region_id: str, instance_id: str, command: str) -> tuple[str, str]:
    """Run a shell command via Cloud Assistant; return (status, decoded output)."""
    from alibabacloud_ecs20140526 import models as ecs_models

    invoke = client.run_command(
        ecs_models.RunCommandRequest(
            region_id=region_id,
            type="RunShellScript",
            command_content=base64.b64encode(command.encode()).decode(),
            instance_id=[instance_id],
            timeout=60,
        )
    )
    invoke_id = invoke.body.invoke_id
    deadline = time.monotonic() + 120
    while time.monotonic() < deadline:
        description = client.describe_invocations(
            ecs_models.DescribeInvocationsRequest(region_id=region_id, invoke_id=invoke_id)
        )
        invocations = description.body.invocations.invocation
        if invocations:
            invocation = invocations[0]
            status = invocation.invocation_status
            if status in ("Finished", "Failed", "Stopped", "PartialFailed"):
                results = invocation.invoke_instances.invoke_instance
                output = ""
                if results and results[0].invocation_result.output:
                    output = base64.b64decode(results[0].invocation_result.output).decode(
                        "utf-8", "replace"
                    )
                return status, output
        time.sleep(POLL_INTERVAL_SECONDS)
    return "Timeout", ""


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--region-id", default=os.environ.get("ALIYUN_ECS_REGION_ID"))
    parser.add_argument("--vswitch-id", default=os.environ.get("ALIYUN_ECS_VSWITCH_ID"))
    parser.add_argument("--security-group-id", default=os.environ.get("ALIYUN_ECS_SECURITY_GROUP_ID"))
    parser.add_argument("--base-image-id", default=os.environ.get("ALIYUN_ECS_BASE_IMAGE_ID"))
    parser.add_argument("--resource-group-id", default=os.environ.get("ALIYUN_ECS_RESOURCE_GROUP_ID"))
    parser.add_argument("--instance-type", default="ecs.g7.xlarge")
    parser.add_argument("--image-name", default=None, help="Defaults to a timestamped name.")
    args = parser.parse_args()

    for name in ("region_id", "vswitch_id", "security_group_id", "base_image_id"):
        if not getattr(args, name):
            raise SystemExit(f"Missing required configuration: --{name.replace('_', '-')}")

    from alibabacloud_ecs20140526 import models as ecs_models

    client = make_ecs_client(args.region_id)
    image_name = args.image_name or time.strftime(
        "greptimedb-query-regression-runner-%Y%m%d%H%M%S", time.gmtime()
    )

    user_data = base64.b64encode(
        render_user_data(
            (ASSETS_DIR.parent / "Dockerfile").read_text(),
            (ASSETS_DIR / "start-runner.sh").read_text(),
            (ASSETS_DIR / "ephemeral-github-runner.service").read_text(),
        ).encode()
    ).decode()

    instance_id = None
    try:
        response = client.run_instances(
            ecs_models.RunInstancesRequest(
                region_id=args.region_id,
                image_id=args.base_image_id,
                resource_group_id=args.resource_group_id,
                instance_type=args.instance_type,
                v_switch_id=args.vswitch_id,
                security_group_id=args.security_group_id,
                instance_name=f"build-{image_name}",
                description="Temporary builder for the query-regression ECS image",
                amount=1,
                instance_charge_type="PostPaid",
                spot_strategy="NoSpot",
                internet_charge_type="PayByTraffic",
                internet_max_bandwidth_out=100,
                system_disk=ecs_models.RunInstancesRequestSystemDisk(
                    category="cloud_essd", size="100"
                ),
                user_data=user_data,
                tag=[
                    ecs_models.RunInstancesRequestTag(key="managed-by", value="query-regression-ci"),
                    ecs_models.RunInstancesRequestTag(key="role", value="image-builder"),
                ],
            )
        )
        instance_id = response.body.instance_id_sets.instance_id_set[0]
        print(f"Builder instance: {instance_id}", flush=True)
        wait_for_instance_status(client, args.region_id, instance_id, "Running", time.monotonic() + 10 * 60)

        deadline = time.monotonic() + BUILD_TIMEOUT_SECONDS
        while time.monotonic() < deadline:
            status, output = run_check_command(
                client, args.region_id, instance_id, f"test -f {DONE_SENTINEL} && echo done || echo pending"
            )
            print(f"Build sentinel check: {status} {output.strip()}", flush=True)
            if "done" in output:
                break
            time.sleep(POLL_INTERVAL_SECONDS)
        else:
            raise TimeoutError("Image build did not finish in time")

        print("Stopping builder before image creation", flush=True)
        client.stop_instance(ecs_models.StopInstanceRequest(instance_id=instance_id))
        wait_for_instance_status(client, args.region_id, instance_id, "Stopped", time.monotonic() + 10 * 60)

        image = client.create_image(
            ecs_models.CreateImageRequest(
                region_id=args.region_id, instance_id=instance_id, image_name=image_name
            )
        )
        image_id = image.body.image_id
        deadline = time.monotonic() + 30 * 60
        while time.monotonic() < deadline:
            description = client.describe_images(
                ecs_models.DescribeImagesRequest(region_id=args.region_id, image_id=image_id)
            )
            images = description.body.images.image
            if images and images[0].status == "Available":
                break
            time.sleep(POLL_INTERVAL_SECONDS)
        else:
            raise TimeoutError(f"Image {image_id} did not become Available in time")

        print(f"Custom image ready: {image_id} ({image_name})", flush=True)
        print(f"Set the repo variable QUERY_REGRESSION_ECS_IMAGE_ID={image_id}", flush=True)
        return 0
    finally:
        if instance_id:
            try:
                client.delete_instance(
                    ecs_models.DeleteInstanceRequest(instance_id=instance_id, force=True)
                )
                print(f"Deleted builder instance {instance_id}", flush=True)
            except Exception as error:  # noqa: BLE001
                print(f"Failed to delete builder instance {instance_id}: {error}", flush=True)


if __name__ == "__main__":
    raise SystemExit(main())
