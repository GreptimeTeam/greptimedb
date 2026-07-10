#!/usr/bin/env python3
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
