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

"""Select executions within fuzz groups, never whether a CI job may start.

The plan command runs from the PR base checkout, using its manifest and rules.
Any uncertainty selects all targets. The group command retains the first target
as a smoke test and leaves the complete corpus group unchanged.
"""

import argparse
from fnmatch import fnmatchcase
import json
import os
from pathlib import Path, PurePosixPath
import re
import sys
import tomllib
import urllib.request


SCRIPT_DIR = Path(__file__).resolve().parent
NAME = re.compile(r"[A-Za-z0-9_]+")


def target_paths(manifest):
    """Read the declared fuzz binaries, excluding service helper executables."""
    with Path(manifest).open("rb") as source:
        bins = tomllib.load(source)["bin"]
    targets = {}
    for binary in bins:
        name = binary["name"]
        path = binary["path"]
        if name.startswith(("fuzz_", "unstable_fuzz_")):
            if not NAME.fullmatch(name) or not safe_path(path):
                raise ValueError("invalid fuzz target declaration")
            key = f"tests-fuzz/{path}"
            if key in targets or name in targets.values():
                raise ValueError("duplicate fuzz target declaration")
            targets[key] = name
    if not targets:
        raise ValueError("no declared fuzz targets")
    return targets


def safe_path(path):
    return (isinstance(path, str) and bool(path) and "\\" not in path
            and not any(ord(char) < 32 for char in path)
            and not PurePosixPath(path).is_absolute()
            and ".." not in PurePosixPath(path).parts)


def select_paths(paths, targets, policy):
    """Union known target/helper paths; an unknown path requires a full run."""
    if (not isinstance(policy, dict) or policy.get("version") != 1
            or not isinstance(policy.get("rules"), list)):
        raise ValueError("unsupported fuzz selection policy")
    known = set(targets.values())
    for rule in policy["rules"]:
        if (not isinstance(rule, dict) or not isinstance(rule.get("paths"), list)
                or not isinstance(rule.get("targets"), list)
                or not rule["paths"] or not rule["targets"]
                or any(not isinstance(target, str) or target not in known for target in rule["targets"])):
            raise ValueError("unknown or empty rule target set")
        for path in rule["paths"]:
            if not safe_path(path):
                raise ValueError("invalid rule path")
    if not paths:
        return {"targets": None, "reason": "empty diff"}
    selected = set()
    for path in sorted(paths):
        matched = {targets[path]} if path in targets else set()
        for rule in policy["rules"]:
            if any(fnmatchcase(path, pattern) for pattern in rule["paths"]):
                matched.update(rule["targets"])
        if not safe_path(path) or not matched:
            return {"targets": None, "reason": "unmapped path; full run", "path": path}
        selected.update(matched)
    return {"targets": sorted(selected), "reason": "mapped paths", "paths": sorted(paths)}


def api(path):
    request = urllib.request.Request(
        os.environ["GITHUB_API_URL"] + path,
        headers={"Authorization": "Bearer " + os.environ["GITHUB_TOKEN"],
                 "Accept": "application/vnd.github+json"},
    )
    with urllib.request.urlopen(request, timeout=20) as response:
        return json.load(response)


def changed_paths(event):
    """Reject stale or incomplete API diffs, and include both rename paths."""
    number = event["number"]
    if not isinstance(number, int) or number <= 0:
        raise ValueError("invalid PR number")
    endpoint = f"/repos/{os.environ['GITHUB_REPOSITORY']}/pulls/{number}"
    expected = event["pull_request"]
    before = api(endpoint)

    def check_revision(pr):
        for side in ("base", "head"):
            if pr[side]["sha"] != expected[side]["sha"]:
                raise ValueError("PR revision changed since this run")

    check_revision(before)
    if any(label["name"] == "ci:full" for label in before.get("labels", [])):
        raise ValueError("ci:full requested")
    count = before["changed_files"]
    if not isinstance(count, int) or not 0 < count <= 300:
        raise ValueError("empty or large diff")
    files = []
    for page in range(1, (count + 99) // 100 + 1):
        batch = api(f"{endpoint}/files?per_page=100&page={page}")
        if not isinstance(batch, list):
            raise ValueError("invalid diff response")
        files.extend(batch)
    after = api(endpoint)
    check_revision(after)
    if (len(files) != count or len({item["filename"] for item in files}) != count
            or after["changed_files"] != count
            or any(label["name"] == "ci:full" for label in after.get("labels", []))):
        raise ValueError("incomplete or changed diff")
    paths = set()
    for item in files:
        paths.add(item["filename"])
        if item["status"] == "renamed":
            paths.add(item["previous_filename"])
    return paths


def plan():
    if (os.environ.get("GITHUB_EVENT_NAME") != "pull_request"
            or os.environ.get("FUZZ_FORCE_ALL", "").lower() not in ("", "false", "0")):
        return {"targets": None, "reason": "full-run event or override"}
    try:
        root = SCRIPT_DIR.parents[1]
        targets = target_paths(root / "tests-fuzz/Cargo.toml")
        candidate = target_paths(os.environ["FUZZ_CANDIDATE_MANIFEST"])
        if targets != candidate:
            raise ValueError("fuzz target declarations changed")
        with (SCRIPT_DIR / "fuzz-target-rules.toml").open("rb") as source:
            policy = tomllib.load(source)
        with open(os.environ["GITHUB_EVENT_PATH"], encoding="utf-8") as source:
            event = json.load(source)
        return select_paths(changed_paths(event), targets, policy)
    except (OSError, ValueError, KeyError, TypeError) as error:
        # Do not print exception details: API errors may contain request data.
        print(f"::warning::Fuzz selection unavailable ({type(error).__name__}); running all targets")
        return {"targets": None, "reason": "selection unavailable; full run"}


def select_group(targets, selection, known_targets):
    """Keep group order, a smoke target, and all targets on invalid decisions."""
    group = list(dict.fromkeys(target.strip() for target in targets.splitlines() if target.strip()))
    if not group or any(not NAME.fullmatch(target) for target in group):
        raise ValueError("invalid fuzz group")
    try:
        selected = json.loads(selection)["targets"]
        if (not isinstance(selected, list) or not selected
                or any(not isinstance(target, str) or target not in known_targets for target in selected)):
            return group
        return [target for target in group if target == group[0] or target in selected]
    except (ValueError, KeyError, TypeError):
        return group


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("mode", choices=("plan", "group"))
    args = parser.parse_args()
    if args.mode == "plan":
        decision = plan()
        print("Fuzz target selection: " + json.dumps(decision, sort_keys=True))
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
            output.write("affected-targets=" + json.dumps(decision, separators=(",", ":")) + "\n")
    else:
        manifest = SCRIPT_DIR.parents[1] / "tests-fuzz/Cargo.toml"
        known = set(target_paths(os.environ.get("FUZZ_CANDIDATE_MANIFEST", manifest)).values())
        selection = os.environ.get("FUZZ_AFFECTED_TARGETS", "")
        targets = select_group(os.environ["FUZZ_TARGETS"], selection, known)
        print("Fuzz group decision (empty means full): " + json.dumps(selection))
        print("Fuzz group executes: " + ", ".join(targets))
        print("Smoke target: " + targets[0])
        with open(os.environ["GITHUB_OUTPUT"], "a", encoding="utf-8") as output:
            output.write("targets<<FUZZ_SELECTED_TARGETS\n" + "\n".join(targets) + "\nFUZZ_SELECTED_TARGETS\n")
    return 0


if __name__ == "__main__":
    sys.exit(main())
