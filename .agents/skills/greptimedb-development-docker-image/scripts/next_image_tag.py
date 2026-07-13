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

"""Print a tag with its final numeric component incremented."""

import argparse
import re
import sys


def increment_tag(tag: str) -> str:
    match = re.search(r"(\d+)$", tag)
    if match is None:
        raise ValueError("tag must end with a numeric version component")

    number = match.group(1)
    return f"{tag[:match.start()]}{int(number) + 1:0{len(number)}d}"


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Increment the final numeric component of a Docker image tag."
    )
    parser.add_argument("--tag", required=True, help="existing Docker image tag")
    args = parser.parse_args()

    try:
        print(increment_tag(args.tag))
    except ValueError as error:
        print(f"error: {error}: {args.tag!r}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    sys.exit(main())
