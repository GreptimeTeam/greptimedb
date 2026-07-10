#!/usr/bin/env python3
"""Create or update non-secret GreptimeDB image settings in a .env file."""

import argparse
import re
import sys
from pathlib import Path


MANAGED_KEYS = ("IMAGE_REGISTRY", "IMAGE_REPOSITORY", "IMAGE_TAG")
REGISTRY_PATTERN = re.compile(r"[a-z0-9][a-z0-9.-]*(?::[0-9]+)?(?:/[a-z0-9][a-z0-9._-]*)*")
REPOSITORY_PATTERN = re.compile(r"[a-z0-9][a-z0-9._-]*(?:/[a-z0-9][a-z0-9._-]*)*")
TAG_PATTERN = re.compile(r"[A-Za-z0-9_][A-Za-z0-9_.-]{0,127}")


def validate_value(name: str, value: str, pattern: re.Pattern[str], optional: bool = False) -> None:
    if optional and not value:
        return
    if not pattern.fullmatch(value) or any(character.isspace() for character in value):
        raise ValueError(f"invalid {name}; credentials, URL schemes, whitespace, and image-reference delimiters are not allowed")


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--env", required=True, type=Path)
    parser.add_argument("--registry", required=True)
    parser.add_argument("--repository", required=True)
    parser.add_argument("--tag", required=True)
    args = parser.parse_args()

    try:
        validate_value("registry", args.registry, REGISTRY_PATTERN, optional=True)
        validate_value("repository", args.repository, REPOSITORY_PATTERN)
        validate_value("tag", args.tag, TAG_PATTERN)
    except ValueError as error:
        print(f"error: {error}", file=sys.stderr)
        return 2

    values = {
        "IMAGE_REGISTRY": args.registry,
        "IMAGE_REPOSITORY": args.repository,
        "IMAGE_TAG": args.tag,
    }
    existing_content = args.env.read_text() if args.env.exists() else ""
    lines = existing_content.splitlines()
    replaced = set()
    output = []
    for line in lines:
        key, separator, _ = line.partition("=")
        if separator and key in values:
            output.append(f"{key}={values[key]}")
            replaced.add(key)
        else:
            output.append(line)

    output.extend(f"{key}={values[key]}" for key in MANAGED_KEYS if key not in replaced)
    new_content = "\n".join(output) + "\n"
    if existing_content == new_content:
        print(f"unchanged {args.env}")
        return 0

    args.env.parent.mkdir(parents=True, exist_ok=True)
    args.env.write_text(new_content)
    print(f"updated {args.env}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
