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

Sentinel polling reads the instance's serial console output
(GetInstanceConsoleOutput) and looks for marker lines the user data writes to
/dev/console. This has no in-guest agent dependency.

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
DONE_MARKER = "QREG_IMAGE_BUILD_DONE"
FAILED_MARKER = "QREG_IMAGE_BUILD_FAILED"
POLL_INTERVAL_SECONDS = 15
CONSOLE_POLL_INTERVAL_SECONDS = 30
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
trap 'echo "{FAILED_MARKER} at line $LINENO" > /dev/console' ERR
# Stream the full build log to the serial console so the poller (and anyone
# watching GetInstanceConsoleOutput) sees real progress, not a silent login
# prompt for the whole build.
exec > >(tee -a /dev/console) 2>&1

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

# Keep the image free of the build-time docker state (also shrinks the
# snapshot: buildkit cache is several GB).
docker rm -f "${{container}}" >/dev/null
docker system prune -af >/dev/null
trap - EXIT

echo "{DONE_MARKER}" > /dev/console
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
        response = call_api_with_retry(
            lambda: client.describe_instances(
                ecs_models.DescribeInstancesRequest(
                    region_id=region_id, instance_ids=json.dumps([instance_id])
                )
            ),
            "DescribeInstances",
        )
        instances = response.body.instances.instance
        if instances and instances[0].status == wanted:
            return
        time.sleep(POLL_INTERVAL_SECONDS)
    raise TimeoutError(f"Instance {instance_id} did not reach status {wanted} in time")


# Aliyun error codes worth retrying: throttling and server-side faults. Client
# errors (4xx: permissions, bad parameters) are configuration problems and must
# fail fast instead of being retried.
TRANSIENT_ERROR_CODES = {"Throttling", "Throttling.User", "InternalError", "ServiceUnavailable"}


def is_transient(error: Exception) -> bool:
    # UnretryableException comes from the darabonba network layer: connection
    # reset, read timeout, DNS blip.
    if type(error).__name__ == "UnretryableException":
        return True
    code = getattr(error, "code", "") or ""
    status = getattr(error, "statusCode", None)
    return code in TRANSIENT_ERROR_CODES or (isinstance(status, int) and status >= 500)


def call_api_with_retry(fn, description: str, attempts: int = 5):
    """Retry transient API/network failures; the build is too long to die on a blip.

    Only call this with idempotent or safely-repeatable requests (reads,
    StopInstance, CreateImage). Never with RunInstances: if the request
    succeeded but the response was lost, a retry double-creates instances.
    """
    for attempt in range(1, attempts + 1):
        try:
            return fn()
        except Exception as error:  # noqa: BLE001
            if attempt == attempts or not is_transient(error):
                raise
            print(f"{description} failed (attempt {attempt}/{attempts}): {error}", flush=True)
            time.sleep(POLL_INTERVAL_SECONDS)
    raise RuntimeError("unreachable: retry loop exited without returning")


def read_console_output(client, region_id: str, instance_id: str) -> str:
    """Fetch the instance's serial console output; no in-guest agent needed."""
    from alibabacloud_ecs20140526 import models as ecs_models

    response = call_api_with_retry(
        lambda: client.get_instance_console_output(
            ecs_models.GetInstanceConsoleOutputRequest(region_id=region_id, instance_id=instance_id)
        ),
        "GetInstanceConsoleOutput",
    )
    return base64.b64decode(response.body.console_output or "").decode("utf-8", "replace")


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
                # Peak usage is ~18G (OS+apt, docker image, and the materialized
                # toolchain coexist briefly): deliberately tight, and a smaller
                # disk makes the image snapshot faster. Instances created from
                # the image get a larger system disk from the provision side.
                system_disk=ecs_models.RunInstancesRequestSystemDisk(
                    category="cloud_essd", size="20"
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
            console = read_console_output(client, args.region_id, instance_id)
            if DONE_MARKER in console:
                break
            if FAILED_MARKER in console:
                tail = "\n".join(console.splitlines()[-20:])
                raise RuntimeError(f"Image build failed on the builder; console tail:\n{tail}")
            lines = console.splitlines()
            print(f"Build in progress; last console line: {lines[-1] if lines else '(none yet)'}", flush=True)
            time.sleep(CONSOLE_POLL_INTERVAL_SECONDS)
        else:
            raise TimeoutError("Image build did not finish in time")

        print("Stopping builder before image creation", flush=True)
        try:
            call_api_with_retry(
                lambda: client.stop_instance(
                    ecs_models.StopInstanceRequest(
                        instance_id=instance_id,
                        # The instance is deleted right after the snapshot, but
                        # there is no reason to keep billing vCPU during the stop.
                        stopped_mode="StopCharging",
                    )
                ),
                "StopInstance",
            )
        except Exception as error:  # noqa: BLE001
            # Instance families with local disks do not support StopCharging.
            print(f"StopCharging unavailable ({error}); stopping with default mode", flush=True)
            call_api_with_retry(
                lambda: client.stop_instance(ecs_models.StopInstanceRequest(instance_id=instance_id)),
                "StopInstance",
            )
        wait_for_instance_status(client, args.region_id, instance_id, "Stopped", time.monotonic() + 10 * 60)

        image = call_api_with_retry(
            lambda: client.create_image(
                ecs_models.CreateImageRequest(
                    region_id=args.region_id, instance_id=instance_id, image_name=image_name
                )
            ),
            "CreateImage",
        )
        image_id = image.body.image_id
        deadline = time.monotonic() + 60 * 60
        while time.monotonic() < deadline:
            description = call_api_with_retry(
                lambda: client.describe_images(
                    ecs_models.DescribeImagesRequest(region_id=args.region_id, image_id=image_id)
                ),
                "DescribeImages",
            )
            images = description.body.images.image
            if images:
                status = images[0].status
                print(f"Image {image_id} status: {status} (progress {images[0].progress})", flush=True)
                if status == "Available":
                    break
            time.sleep(POLL_INTERVAL_SECONDS)
        else:
            raise TimeoutError(
                f"Image {image_id} did not become Available in time. The snapshot "
                f"continues server-side: check its status in the console and reuse it "
                f"once Available instead of rebuilding."
            )

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
