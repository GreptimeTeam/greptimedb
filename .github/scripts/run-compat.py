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

"""Run sqlness compatibility checks for a small release window.

The workflow intentionally keeps YAML small and delegates the maintainable parts
here:

- read `tests/compatibility/ci.toml`
- validate the checked-in recent-release window
- preview selected cases with `compat --dry-run`
- run the real compat check for each sampled `from` version
"""

from __future__ import annotations

import argparse
import ast
import os
import re
import shlex
import subprocess
import sys
from pathlib import Path


VERSION_RE = re.compile(r"v[0-9]+\.[0-9]+\.[0-9]+")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--config",
        default="tests/compatibility/ci.toml",
        help="Path to the compatibility CI window config.",
    )
    parser.add_argument(
        "--runner",
        default="./bins/sqlness-runner",
        help="Path to the sqlness-runner binary built by CI.",
    )
    parser.add_argument(
        "--to-bins-dir",
        default="./bins",
        help="Directory containing the PR-built greptime binary.",
    )
    parser.add_argument(
        "--preserve-state",
        action="store_true",
        help="Pass --preserve-state to the real compat run for artifact upload.",
    )
    return parser.parse_args()


def load_from_versions(config_path: Path) -> list[str]:
    if not config_path.is_file():
        raise SystemExit(f"Compatibility CI config not found: {config_path}")

    # Parse the simple TOML string array without depending on Python 3.11+
    # tomllib. This intentionally supports only the checked-in shape used by
    # the CI job: from_versions = ["vX.Y.Z", ...].
    content = config_path.read_text(encoding="utf-8")
    content_without_comments = "\n".join(
        line.split("#", 1)[0] for line in content.splitlines()
    )
    match = re.search(
        r"(?ms)^\s*from_versions\s*=\s*(\[[^\]]*\])",
        content_without_comments,
    )
    if match is None:
        raise SystemExit(f"{config_path} must define from_versions")

    try:
        versions = ast.literal_eval(match.group(1))
    except (SyntaxError, ValueError) as err:
        raise SystemExit(f"Invalid from_versions in {config_path}: {err}") from err

    if not isinstance(versions, list) or not versions:
        raise SystemExit(f"{config_path} must define a non-empty from_versions list")

    seen: set[str] = set()
    validated: list[str] = []
    for version in versions:
        if not isinstance(version, str) or VERSION_RE.fullmatch(version) is None:
            raise SystemExit(f"Invalid compat from version: {version!r}")
        if version in seen:
            raise SystemExit(f"Duplicate compat from version: {version}")
        seen.add(version)
        validated.append(version)

    return validated


def check_inputs(runner: Path, to_bins_dir: Path) -> None:
    if not runner.is_file():
        raise SystemExit(f"sqlness-runner binary not found: {runner}")
    if not to_bins_dir.is_dir():
        raise SystemExit(f"to-bins directory not found: {to_bins_dir}")
    if not to_bins_dir.joinpath("greptime").is_file():
        raise SystemExit(f"greptime binary not found in to-bins directory: {to_bins_dir}")


def github_group(title: str):
    class Group:
        def __enter__(self) -> None:
            print(f"::group::{title}", flush=True)

        def __exit__(self, exc_type, exc, traceback) -> None:
            print("::endgroup::", flush=True)

    return Group()


def run_command(command: list[str], *, env: dict[str, str] | None = None) -> None:
    print(f"$ {shlex.join(command)}", flush=True)
    subprocess.run(command, check=True, env=env)


def run_for_version(
    *,
    runner: Path,
    to_bins_dir: Path,
    from_version: str,
    preserve_state: bool,
) -> None:
    base_command = [
        str(runner),
        "compat",
        "--from-version",
        from_version,
        "--to-bins-dir",
        str(to_bins_dir),
    ]

    with github_group(f"Preview {from_version} -> current"):
        run_command([*base_command, "--dry-run"])

    real_command = [*base_command]
    if preserve_state:
        real_command.append("--preserve-state")

    env = os.environ.copy()
    env.setdefault("RUST_BACKTRACE", "1")
    with github_group(f"Compatibility {from_version} -> current"):
        run_command(real_command, env=env)


def main() -> int:
    args = parse_args()
    config_path = Path(args.config)
    runner = Path(args.runner)
    to_bins_dir = Path(args.to_bins_dir)

    check_inputs(runner, to_bins_dir)
    from_versions = load_from_versions(config_path)

    print("Compatibility from-version window:", flush=True)
    for version in from_versions:
        print(f"  - {version}", flush=True)

    for from_version in from_versions:
        run_for_version(
            runner=runner,
            to_bins_dir=to_bins_dir,
            from_version=from_version,
            preserve_state=args.preserve_state,
        )

    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except subprocess.CalledProcessError as err:
        raise SystemExit(err.returncode) from err
