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

"""Update the compatibility CI version window from local release tags and cases.

The PR window keeps only the sliding window over stable release tags whose
GitHub releases carry the sqlness compat artifacts (greptime-linux-amd64
tar.gz and sha256sum). Exact =vX.Y.Z `from_range` anchors in case.toml are not
retained in the PR window: they are validated with --check-anchors and
exercised by nightly runs via --nightly-window.
"""

from __future__ import annotations

import argparse
import ast
import difflib
import json
import os
import re
import subprocess
import sys
import urllib.error
import urllib.request
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


def required_asset_names(tag: str) -> set[str]:
    """Return the release assets the sqlness compat runner downloads for a tag."""
    return {
        f"greptime-linux-amd64-{tag}.tar.gz",
        f"greptime-linux-amd64-{tag}.sha256sum",
    }


def published_release_tags(owner: str, repo: str, token: str | None) -> set[str]:
    """Return stable tags whose non-draft GitHub releases carry the required assets.

    Only draft releases are skipped: prerelease-flagged releases with the
    required assets are included, and VERSION_RE already excludes prerelease
    version tags (e.g. v1.2.0-beta.1).
    """
    published: set[str] = set()
    page = 1
    while True:
        url = (
            f"https://api.github.com/repos/{owner}/{repo}/releases"
            f"?per_page=100&page={page}"
        )
        headers = {
            "Accept": "application/vnd.github+json",
            "User-Agent": "update-compat-versions",
        }
        if token:
            headers["Authorization"] = f"Bearer {token}"
        request = urllib.request.Request(url, headers=headers)
        try:
            with urllib.request.urlopen(request) as response:
                payload = json.loads(response.read().decode("utf-8"))
        except (urllib.error.HTTPError, urllib.error.URLError, OSError, ValueError) as err:
            raise CompatVersionError(
                f"Unable to fetch published releases from {url}: {err}"
            ) from err
        if not isinstance(payload, list):
            raise CompatVersionError(
                f"Unexpected response from {url}: expected a JSON release list"
            )

        for release in payload:
            if not isinstance(release, dict):
                continue
            if release.get("draft"):
                continue
            tag = release.get("tag_name")
            if not isinstance(tag, str) or VERSION_RE.fullmatch(tag) is None:
                continue
            assets = release.get("assets")
            asset_names: set[str] = set()
            if isinstance(assets, list):
                asset_names = {
                    asset.get("name")
                    for asset in assets
                    if isinstance(asset, dict) and isinstance(asset.get("name"), str)
                }
            if required_asset_names(tag) <= asset_names:
                published.add(tag)

        if len(payload) < 100:
            break
        page += 1
    return published


def effective_versions(tags: Iterable[str], published: set[str] | None = None) -> list[str]:
    """Return the PR window: the sliding window over stable release tags.

    Exact =vX.Y.Z case anchors are intentionally excluded; they are validated
    with --check-anchors and exercised by nightly runs (--nightly-window).
    When ``published`` is given, only stable tags with published release
    assets are considered.
    """
    stable = stable_tags(tags)
    if published is not None:
        stable = sorted(set(stable) & set(published), key=version_key)
        if not stable:
            raise CompatVersionError(
                "No stable tags have published release assets "
                "(greptime-linux-amd64 tar.gz and sha256sum); cannot compute the sliding window"
            )
    return sliding_versions(stable)


def nightly_versions(
    tags: Iterable[str], cases_dir: Path, published: set[str] | None = None
) -> list[str]:
    """Return the nightly window: exact anchors plus the sliding release window."""
    anchors = extract_pinned_anchors(cases_dir)
    window = effective_versions(tags, published)
    available = set(stable_tags(tags))
    if published is not None:
        available &= set(published)
    unavailable = sorted(anchors - available, key=version_key)
    if unavailable:
        raise CompatVersionError(
            "Pinned compatibility anchor(s) are unavailable as stable git tags: "
            + ", ".join(unavailable)
        )
    return sorted(anchors | set(window), key=version_key)


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


def _owner_repo() -> tuple[str, str]:
    """Return the (owner, repo) pair from GITHUB_REPOSITORY or the default repo."""
    repository = os.environ.get("GITHUB_REPOSITORY", "GreptimeTeam/greptimedb")
    owner, _, repo = repository.partition("/")
    if not owner or not repo:
        return "GreptimeTeam", "greptimedb"
    return owner, repo


def _github_token() -> str | None:
    """Return the GitHub token from the environment, if any."""
    return os.environ.get("GH_TOKEN") or os.environ.get("GITHUB_TOKEN")


def check_anchors(repo_root: Path, cases_dir: Path) -> int:
    """Validate every exact anchor is a stable git tag with a published release.

    Returns 0 when all anchors are runnable; prints problems to stderr and
    returns 2 when any anchor fails.
    """
    tags = set(git_tags(repo_root))
    anchors = extract_pinned_anchors(cases_dir)
    if not anchors:
        return 0
    published = published_release_tags(*_owner_repo(), _github_token())
    problems: list[str] = []
    for anchor in sorted(anchors, key=version_key):
        if anchor not in tags:
            problems.append(f"{anchor}: not a stable git tag")
        elif anchor not in published:
            problems.append(f"{anchor}: missing published release with required assets")
    if problems:
        print(
            "error: exact from_range anchor(s) are not runnable in compatibility CI:",
            file=sys.stderr,
        )
        for problem in problems:
            print(f"  - {problem}", file=sys.stderr)
        return 2
    return 0


def _path_from_repo(repo_root: Path, value: str | None, default: str) -> Path:
    path = Path(value) if value is not None else Path(default)
    return path if path.is_absolute() else repo_root / path


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    modes = parser.add_mutually_exclusive_group()
    modes.add_argument("--check", action="store_true", help="Fail if ci.toml is stale.")
    modes.add_argument("--update", action="store_true", help="Rewrite only ci.toml from_versions.")
    modes.add_argument(
        "--nightly-window",
        action="store_true",
        help="Print the comma-separated nightly window (anchors plus sliding) to stdout.",
    )
    parser.add_argument(
        "--check-anchors",
        action="store_true",
        help="Validate exact case anchors are stable git tags with published release assets.",
    )
    parser.add_argument(
        "--published-only",
        action="store_true",
        help=(
            "With --check/--update, restrict the window to stable tags with "
            "published release assets (requires network)."
        ),
    )
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

    if not (args.check or args.update or args.nightly_window or args.check_anchors):
        print(
            "error: one of --check, --update, --nightly-window, or --check-anchors is required",
            file=sys.stderr,
        )
        return 2

    try:
        if args.nightly_window:
            versions = nightly_versions(git_tags(repo_root), cases_dir)
            print(",".join(versions))
            return 0

        if args.check or args.update:
            content = config_path.read_text(encoding="utf-8")
            published = (
                published_release_tags(*_owner_repo(), _github_token())
                if args.published_only
                else None
            )
            versions = effective_versions(git_tags(repo_root), published)
            updated = updated_config(content, config_path, versions)
        else:
            content = updated = ""

        if args.check:
            if content == updated:
                if args.check_anchors:
                    return check_anchors(repo_root, cases_dir)
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

        if args.check_anchors:
            return check_anchors(repo_root, cases_dir)
    except (CompatVersionError, OSError) as err:
        print(f"error: {err}", file=sys.stderr)
        return 2
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
