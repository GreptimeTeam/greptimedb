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

"""Pick the query-regression baseline tag for a release.

Compatibility tests do not use this tag. They read ``tests/compatibility/ci.toml``.

Release branches diverge, so a tag on ``release/v1.3`` is not a descendant of
``v1.2.1``. Ancestry (``git tag --merged``) therefore skips the real previous
release. Compare version numbers instead: the baseline is the greatest
``vX.Y.Z`` tag whose version is strictly older than the release being built.
Prerelease and nightly tags are not baselines.
"""

from __future__ import annotations

import argparse
import re
import sys

STABLE_TAG = re.compile(r"^v(\d+)\.(\d+)\.(\d+)$")
RELEASE_VERSION = re.compile(r"^v(\d+)\.(\d+)\.(\d+)(?:-.+)?$")


def _triple(match: re.Match[str]) -> tuple[int, int, int]:
    return int(match.group(1)), int(match.group(2)), int(match.group(3))


def release_version(version: str) -> tuple[int, int, int]:
    match = RELEASE_VERSION.fullmatch(version.strip())
    if match is None:
        raise ValueError(f"not a release version: {version}")
    return _triple(match)


def previous_stable_tag(version: str, tags: list[str]) -> str | None:
    """Return the newest stable tag older than ``version``, or None."""
    current = release_version(version)
    best: tuple[int, int, int] | None = None
    best_tag: str | None = None
    for raw in tags:
        tag = raw.strip()
        if tag.endswith("^{}"):
            tag = tag[: -len("^{}")]
        match = STABLE_TAG.fullmatch(tag)
        if match is None:
            continue
        triple = _triple(match)
        if triple < current and (best is None or triple > best):
            best = triple
            best_tag = tag
    return best_tag


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--version", required=True, help="Release version, e.g. v1.3.0-beta.2")
    parser.add_argument(
        "tags",
        nargs="*",
        help="Candidate tags. Read stdin when omitted.",
    )
    args = parser.parse_args(argv)
    tags = list(args.tags)
    if not tags and not sys.stdin.isatty():
        tags = sys.stdin.read().splitlines()
    try:
        tag = previous_stable_tag(args.version, tags)
    except ValueError as exc:
        print(exc, file=sys.stderr)
        return 1
    if tag is None:
        return 0
    print(tag)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
