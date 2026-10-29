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

"""Policy for the integration CI admission gate.

The rules are data and live in `.github/ci/scope-rules.toml` so they can be
reviewed and changed without touching the classifier. This module parses that
file once at import, validates it, and exposes the names `ci_scope.py` uses.

A malformed file raises at import: the classifier job's steps are
`continue-on-error`, so a broken rules file leaves the gate outputs empty and
every gated job runs.
"""

import argparse
import pathlib
import sys
import tomllib

RULES_PATH = pathlib.Path(__file__).resolve().with_name("scope-rules.toml")
# `.github/ci/scope-rules.toml`: parents[0] is `.github/ci`, parents[2] the repo.
REPO_ROOT = RULES_PATH.parents[2]



class RulesError(ValueError):
    """The rules file is missing, malformed or internally inconsistent."""


def _require(table, key, where):
    if key not in table:
        raise RulesError(f"{where} is missing `{key}`")
    return table[key]


def _strings(value, where):
    if not isinstance(value, list) or not all(isinstance(item, str) for item in value):
        raise RulesError(f"{where} must be an array of strings")
    return list(value)


def _trees(entries, where):
    """Normalise `[[matching.*_tree]]` tables into bare path prefixes.

    Each entry carries a `reason`: the exclusions decide what CI never runs, so
    the justification is part of the contract, not a comment.
    """
    if not isinstance(entries, list):
        raise RulesError(f"{where} must be an array of tables")
    paths = []
    for index, entry in enumerate(entries):
        if not isinstance(entry, dict):
            raise RulesError(f"{where}[{index}] is not a table")
        path = _require(entry, "path", f"{where}[{index}]")
        if not isinstance(path, str) or not path:
            raise RulesError(f"{where}[{index}].path must be a non-empty string")
        reason = _require(entry, "reason", f"{where}[{index}]")
        if not isinstance(reason, str) or not reason.strip():
            raise RulesError(f"{where}[{index}].reason must explain the exclusion")
        paths.append(path)
    return paths


def validate(data):
    """Check the shape of a parsed rules document and return it normalised."""
    if not isinstance(data, dict):
        raise RulesError("rules document is not a table")
    matching = _require(data, "matching", "the rules file")
    gates_in = _require(data, "gates", "the rules file")
    groups_in = _require(data, "groups", "the rules file")
    suites_in = _require(data, "suites", "the rules file")

    groups = {}
    for name, paths in groups_in.items():
        groups[name] = _strings(paths, f"groups.{name}")

    def expand(entries, stack=()):
        out = []
        for entry in entries:
            if not entry.startswith("@"):
                out.append(entry)
                continue
            name = entry[1:]
            if name not in groups:
                raise RulesError(f"unknown path group `{entry}`")
            if name in stack:
                raise RulesError(f"path group cycle: {' -> '.join(stack + (name,))}")
            out.extend(expand(groups[name], stack + (name,)))
        return out

    for name in groups:
        expand([f"@{name}"])  # every group must resolve on its own

    suites = {}
    for suite_id, suite in suites_in.items():
        if not isinstance(suite, dict):
            raise RulesError(f"suites.{suite_id} is not a table")
        gate = _require(suite, "gate", f"suites.{suite_id}")
        paths = _strings(
            _require(suite, "paths", f"suites.{suite_id}"), f"suites.{suite_id}.paths"
        )
        description = _require(suite, "description", f"suites.{suite_id}")
        if not isinstance(description, str) or not description.strip():
            raise RulesError(f"suites.{suite_id}.description is empty")
        # `gate/leg` for a matrix job, or a bare gate for a single-leg job.
        prefix = suite_id.split("/", 1)[0].replace("-", "_")
        if prefix != gate:
            raise RulesError(
                f"suites.{suite_id}.gate is `{gate}`, expected `{prefix}`"
            )
        expand(paths)
        suites[suite_id] = {
            "gate": gate,
            "paths": paths,
            "description": description,
        }

    gate_jobs = {}
    for gate, job in gates_in.items():
        if not isinstance(job, str) or not job:
            raise RulesError(f"gates.{gate} must name a job id")
        gate_jobs[gate] = job
    used = {suite["gate"] for suite in suites.values()}
    unknown = sorted(used - set(gate_jobs))
    if unknown:
        raise RulesError(f"suites use gates missing from [gates]: {', '.join(unknown)}")
    dead = sorted(set(gate_jobs) - used)
    if dead:
        raise RulesError(f"[gates] names gates no suite uses: {', '.join(dead)}")

    return {
        "always_run": _strings(
            _require(matching, "always_run", "matching"), "matching.always_run"
        ),
        "src_tree": _require(matching, "src_tree", "matching"),
        "exempt_trees": _trees(
            _require(matching, "exempt", "matching"), "matching.exempt"
        ),
        "gates": gate_jobs,
        "groups": groups,
        "suites": suites,
    }


def load(path=None):
    """Parse and validate the rules file."""
    path = RULES_PATH if path is None else path
    try:
        raw = pathlib.Path(path).read_bytes()
    except OSError as err:
        raise RulesError(f"cannot read {path}: {err}") from err
    try:
        data = tomllib.loads(raw.decode("utf-8"))
    except (tomllib.TOMLDecodeError, UnicodeDecodeError) as err:
        raise RulesError(f"{path} is not valid TOML: {err}") from err
    return validate(data)


_RULES = load()

ALWAYS_RUN = _RULES["always_run"]
SRC_TREE = _RULES["src_tree"]
EXEMPT_TREES = tuple(_RULES["exempt_trees"])
GATE_JOBS = _RULES["gates"]
PATH_GROUPS = _RULES["groups"]
SUITES = _RULES["suites"]


def main(argv=None):
    """Validate the rules file and print what it holds; non-zero on failure."""
    parser = argparse.ArgumentParser(description="Validate the CI scope rules.")
    parser.add_argument(
        "--check-paths",
        action="store_true",
        help="also fail when a pattern's literal prefix does not exist",
    )
    args = parser.parse_args(argv)
    try:
        rules = load()
    except RulesError as err:
        print(f"scope rules invalid: {err}", file=sys.stderr)
        return 1
    if args.check_paths:
        missing = missing_paths(rules)
        for pattern, prefix in missing:
            print(f"missing path: {pattern} (nothing under {prefix})", file=sys.stderr)
        return 1 if missing else 0
    print(
        f"scope rules ok: {len(rules['suites'])} suites, "
        f"{len(rules['groups'])} path groups, "
        f"{len(rules['always_run'])} always-run entries"
    )
    return 0


def suite_group(suite_id):
    """Return the matrix group id encoded in a suite id, or None."""
    _, _, group = suite_id.partition("/")
    return group or None


def expand_paths(entries, groups=None):
    """Expand `@group` references, including groups nested in other groups."""
    groups = PATH_GROUPS if groups is None else groups
    patterns = []
    for entry in entries:
        if entry.startswith("@"):
            patterns.extend(expand_paths(groups[entry[1:]], groups))
        else:
            patterns.append(entry)
    return patterns


def suite_paths(suite):
    """Expand `@group` references in one suite definition into globs."""
    return expand_paths(suite["paths"])


def _literal_prefix(pattern):
    """Return the longest leading path that the glob cannot vary.

    Everything from the first `*`, `?` or `[` onwards is variable. When that
    wildcard sits inside a segment the incomplete segment is dropped, so
    `src/a/foo*.rs` yields `src/a/`; a wildcard in the very first segment (for
    example `*.md`) yields `""` because the whole path is variable.
    """
    cuts = [pattern.find(ch) for ch in "*?[" if ch in pattern]
    if not cuts:
        return pattern
    prefix = pattern[: min(cuts)]
    if "/" not in prefix:
        return ""
    return prefix.rsplit("/", 1)[0] + "/"


def _all_patterns(rules):
    """Every glob the rules mention, with `@group` references expanded."""
    patterns = []
    for key in ("always_run", "exempt_trees"):
        patterns.extend(expand_paths(rules[key], rules["groups"]))
    for suite in rules["suites"].values():
        patterns.extend(expand_paths(suite["paths"], rules["groups"]))
    return patterns


def missing_paths(rules=None, root=None):
    """Return `(pattern, prefix)` for patterns whose literal prefix is absent.

    A rule that names a directory nobody renamed would otherwise match nothing
    forever, silently narrowing the gate. A pattern with no fixed prefix is
    skipped: there is nothing on disk to check.
    """
    rules = _RULES if rules is None else rules
    root = pathlib.Path(REPO_ROOT if root is None else root)
    missing = set()
    for pattern in _all_patterns(rules):
        prefix = _literal_prefix(pattern)
        if prefix and not (root / prefix).exists():
            missing.add((pattern, prefix))
    return sorted(missing)



if __name__ == "__main__":
    sys.exit(main())
