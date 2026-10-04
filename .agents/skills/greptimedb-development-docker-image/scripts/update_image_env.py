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

"""Create or update non-secret GreptimeDB image settings in a .env file."""

import argparse
import sys
from pathlib import Path

from image_config import image_reference, normalize_values, update_env, validate_values

def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", type=Path)
    parser.add_argument("--registry", required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--tag", required=True)
    parser.add_argument("--validate-only", action="store_true")
    args = parser.parse_args()

    try:
        values = normalize_values({"IMAGE_REGISTRY": args.registry, "IMAGE_REPOSITORY": args.repository, "IMAGE_TAG": args.tag})
        validate_values(values)
    except ValueError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2

    if args.validate_only:
        print(image_reference(values))
        return 0
    if args.env is None:
        parser.error("--env is required unless --validate-only is set")
    if update_env(args.env, values):
        print(f"updated {args.env}")
    else:
        print(f"unchanged {args.env}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
