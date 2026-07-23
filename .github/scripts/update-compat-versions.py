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

"""Update the compatibility CI version window from local release tags and cases."""

from __future__ import annotations

import argparse
import ast
import difflib
import re
import subprocess
import sys
from pathlib import Path
from typing import Iterable


VERSION_RE = re.compile(r"^v(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)\.(0|[1-9][0-9]*)$")
FROM_VERSIONS_RE = re.compile(
    r"(?ms)^(?P<indent>[ \t]*)from_versions[ \t]*=[ \t]*(?P<array>\[(?:[^\]]|\n)*?\])"
)
FROM_VERSIONS_ASSIGNMENT_RE = re.compile(r"(?m)^[ \t]*from_versions[ \t]*=")
FROM_RANGE_RE = re.compile(
    r"(?ms)^[ \t]*from_range[ \t]*=[ \t]*(?P<array>\[(?:[^\]]|\n)*?\])"
)
FROM_RANGE_ASSIGNMENT_RE = re.compile(r"(?m)^[ \t]*from_range[ \t]*=")


class CompatVersionError(ValueError):
    """A compatibility version source does not satisfy the updater policy."""


def version_key(version: str) -> tuple[int, int, int]:
    """Return a sortable semantic-version key for a stable version tag."""
    match = VERSION_RE.fullmatch(version)
    if match is None:
        raise CompatVersionError(f"Not a stable release tag: {version!r}")
    major, minor, patch = (int(part) for part in match.groups())
    return major, minor, patch


def stable_tags(tags: Iterable[str]) -> list[str]:
    """Keep only exact stable release tags, excluding prereleases and nightlies."""
    return sorted({tag for tag in tags if VERSION_RE.fullmatch(tag)}, key=version_key)


def sliding_versions(tags: Iterable[str]) -> list[str]:
    """Return the latest patch in each of the two newest stable minor lines."""
    stable = stable_tags(tags)
    if not stable:
        raise CompatVersionError("No stable release tags found (expected exact v<major>.<minor>.<patch> tags)")

    latest_by_minor: dict[tuple[int, int], str] = {}
    for tag in stable:
        major, minor, _ = version_key(tag)
        minor_line = (major, minor)
        if minor_line not in latest_by_minor or version_key(tag) > version_key(
            latest_by_minor[minor_line]
        ):
            latest_by_minor[minor_line] = tag

    newest_minor_lines = sorted(latest_by_minor, reverse=True)[:2]
    return sorted((latest_by_minor[line] for line in newest_minor_lines), key=version_key)


def _parse_string_array(array: str, context: str) -> list[str]:
    without_comments = "\n".join(line.split("#", 1)[0] for line in array.splitlines())
    try:
        values = ast.literal_eval(without_comments)
    except (SyntaxError, ValueError) as err:
        raise CompatVersionError(f"Malformed string array in {context}: {err}") from err

    if not isinstance(values, list) or any(not isinstance(value, str) for value in values):
        raise CompatVersionError(f"{context} must be a TOML string array")
    return values


def _from_versions_match(content: str, config_path: Path) -> re.Match[str]:
    matches = list(FROM_VERSIONS_RE.finditer(content))
    assignments = list(FROM_VERSIONS_ASSIGNMENT_RE.finditer(content))
    if len(assignments) != 1 or len(matches) != 1:
        raise CompatVersionError(f"{config_path} must define exactly one from_versions array")
    match = matches[0]
    remainder = content[match.end() :].split("\n", 1)[0].strip()
    if remainder and not remainder.startswith("#"):
        raise CompatVersionError(f"Malformed from_versions assignment in {config_path}")
    return match


def load_from_versions(config_path: Path) -> list[str]:
    """Validate and load the current checked-in compatibility window."""
    if not config_path.is_file():
        raise CompatVersionError(f"Compatibility CI config not found: {config_path}")

    match = _from_versions_match(config_path.read_text(encoding="utf-8"), config_path)
    versions = _parse_string_array(match.group("array"), str(config_path))
    if not versions:
        raise CompatVersionError(f"{config_path} must define a non-empty from_versions list")

    duplicates = {version for version in versions if versions.count(version) > 1}
    if duplicates:
        raise CompatVersionError(
            f"Duplicate from_versions in {config_path}: {', '.join(sorted(duplicates))}"
        )
    for version in versions:
        version_key(version)
    return versions


def extract_pinned_anchors(cases_dir: Path) -> set[str]:
    """Extract exact from_range constraints that must remain in the CI window."""
    if not cases_dir.is_dir():
        raise CompatVersionError(f"Compatibility cases directory not found: {cases_dir}")
    anchors: set[str] = set()
    for case_path in sorted(cases_dir.glob("**/case.toml")):
        content = case_path.read_text(encoding="utf-8")
        assignments = list(FROM_RANGE_ASSIGNMENT_RE.finditer(content))
        matches = list(FROM_RANGE_RE.finditer(content))
        if len(assignments) != 1 or len(matches) != 1:
            raise CompatVersionError(f"{case_path} must define exactly one from_range array")

        for constraint in _parse_string_array(matches[0].group("array"), str(case_path)):
            anchor = exact_anchor(constraint, case_path)
            if anchor is not None:
                anchors.add(anchor)
    return anchors


def _runner_version(raw: str, case_path: Path, constraint: str) -> str:
    """Parse and normalize a version exactly like compat_case.rs Version::parse."""
    stripped = raw.strip()
    if stripped.startswith("v"):
        stripped = stripped[1:]
    core = stripped.split("-", 1)[0].split("+", 1)[0]
    parts = core.split(".")
    if len(parts) != 3:
        raise CompatVersionError(
            f"Malformed version in from_range constraint in {case_path}: {constraint!r}"
        )

    numbers: list[int] = []
    for part in parts:
        if not part.isascii() or not part.isdigit():
            raise CompatVersionError(
                f"Malformed version in from_range constraint in {case_path}: {constraint!r}"
            )
        value = int(part)
        if value > 2**64 - 1:
            raise CompatVersionError(
                f"Version component overflows u64 in {case_path}: {constraint!r}"
            )
        numbers.append(value)
    return f"v{numbers[0]}.{numbers[1]}.{numbers[2]}"


def exact_anchor(constraint: str, case_path: Path) -> str | None:
    """Validate a runner constraint and return its normalized exact anchor."""
    normalized = constraint.strip()
    if not normalized:
        raise CompatVersionError(f"Empty from_range constraint in {case_path}")
    if normalized == "*":
        return None

    for operator in (">=", "<=", "==", "=", ">", "<"):
        if normalized.startswith(operator):
            version = _runner_version(normalized[len(operator) :], case_path, constraint)
            return version if operator in ("==", "=") else None

    return _runner_version(normalized, case_path, constraint)


def effective_versions(tags: Iterable[str], cases_dir: Path) -> list[str]:
    """Combine historical exact anchors with the two-line sliding release window."""
    stable = stable_tags(tags)
    sliding = sliding_versions(stable)
    anchors = extract_pinned_anchors(cases_dir)
    unavailable = sorted(anchors - set(stable), key=version_key)
    if unavailable:
        raise CompatVersionError(
            "Pinned compatibility anchor(s) are unavailable as stable git tags: "
            + ", ".join(unavailable)
        )
    return sorted(anchors | set(sliding), key=version_key)


def updated_config(content: str, config_path: Path, versions: list[str]) -> str:
    """Return config content with only the from_versions assignment replaced."""
    match = _from_versions_match(content, config_path)
    # Validate the existing setting before replacing it, so malformed or duplicate
    # checked-in configs never get silently repaired by automation.
    load_from_versions(config_path)
    rendered = (
        f'{match.group("indent")}from_versions = ['
        + ", ".join(f'"{version}"' for version in versions)
        + "]"
    )
    return content[: match.start()] + rendered + content[match.end() :]


def git_tags(repo_root: Path) -> list[str]:
    try:
        result = subprocess.run(
            ["git", "tag", "--list"],
            cwd=repo_root,
            check=True,
            text=True,
            capture_output=True,
        )
    except (OSError, subprocess.CalledProcessError) as err:
        raise CompatVersionError(f"Unable to read local git tags in {repo_root}: {err}") from err
    return result.stdout.splitlines()


def _path_from_repo(repo_root: Path, value: str | None, default: str) -> Path:
    path = Path(value) if value is not None else Path(default)
    return path if path.is_absolute() else repo_root / path


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group(required=True)
    modes.add_argument("--check", action="store_true", help="Fail if ci.toml is stale.")
    modes.add_argument("--update", action="store_true", help="Rewrite only ci.toml from_versions.")
    parser.add_argument(
        "--repo-root",
        default=str(Path(__file__).resolve().parents[2]),
        help="Repository root used for default paths and git tags.",
    )
    parser.add_argument("--config", help="Compatibility CI TOML path, relative to --repo-root.")
    parser.add_argument("--cases-dir", help="Compatibility cases path, relative to --repo-root.")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    repo_root = Path(args.repo_root).resolve()
    config_path = _path_from_repo(repo_root, args.config, "tests/compatibility/ci.toml")
    cases_dir = _path_from_repo(repo_root, args.cases_dir, "tests/compatibility/cases")

    try:
        content = config_path.read_text(encoding="utf-8")
        versions = effective_versions(git_tags(repo_root), cases_dir)
        updated = updated_config(content, config_path, versions)
    except (CompatVersionError, OSError) as err:
        print(f"error: {err}", file=sys.stderr)
        return 2

    if args.check:
        if content == updated:
            return 0
        try:
            display_path = config_path.relative_to(repo_root)
        except ValueError:
            display_path = config_path
        sys.stderr.writelines(
            difflib.unified_diff(
                content.splitlines(keepends=True),
                updated.splitlines(keepends=True),
                fromfile=f"a/{display_path}",
                tofile=f"b/{display_path}",
            )
        )
        return 1

    if content != updated:
        config_path.write_text(updated, encoding="utf-8")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
