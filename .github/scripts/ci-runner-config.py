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

"""Resolve dispatch provider and instance type before allocating a runner."""

import argparse
import re

from runner_utils import append_github_output

DEFAULT_INSTANCE_TYPES = {"Aliyun": "ecs.c9i.2xlarge", "AWS": "c7i.2xlarge"}


def resolve_instance_type(provider: str, requested: str) -> str:
    if provider not in DEFAULT_INSTANCE_TYPES:
        raise ValueError("provider must be Aliyun or AWS")
    instance_type = DEFAULT_INSTANCE_TYPES[provider] if requested == "auto" else requested
    pattern = r"ecs\.[a-z0-9-]+\.[a-z0-9-]+" if provider == "Aliyun" else r"[a-z0-9-]+\.[a-z0-9-]+"
    if not re.fullmatch(pattern, instance_type):
        raise ValueError(f"Invalid {provider} instance type: {instance_type!r}")
    return instance_type


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--provider", required=True)
    parser.add_argument("--instance-type", default="auto")
    args = parser.parse_args()
    instance_type = resolve_instance_type(args.provider, args.instance_type)
    append_github_output("instance_type", instance_type)
    print(f"CI runner: {args.provider} / {instance_type}")


if __name__ == "__main__":
    main()
