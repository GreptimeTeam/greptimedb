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

"""Decide which integration CI suites must run for one pull request.

Pipeline, in priority order:

1. forced mode -- `merge_group`, `schedule`, `workflow_dispatch`,
   `CI_SCOPE_FORCE_ALL`, or the `ci:full` label. Every suite runs.
2. always-run floor -- a changed file matches `ALWAYS_RUN`. Every suite runs.
3. uncovered build change -- a path under `SRC_TREE` that no suite claims, so a
   crate added or renamed later still runs everything.
4. static path matches -- glob match of the changed files against the suite
   catalog in `scope-rules.toml`. Paths under `EXEMPT_TREES` are outside the
   gate's remit and are dropped first: `@server` claims `src/query/**`, which
   would otherwise put a PromQL-only change through the whole fleet.

The verdict is deterministic: the same diff always produces the same plan, and
a change that no rule explains falls through to "run everything".

The plan maps to `$GITHUB_OUTPUT` keys that the `integration.yml` gate jobs
consume; `--dry-run` prints them as JSON.
"""

import argparse
import json
import os
import re
import sys
import urllib.error
import urllib.request

from ci_scope_rules import (
    ALWAYS_RUN,
    EXEMPT_TREES,
    SRC_TREE,
    SUITES,
    suite_group,
    suite_paths,
)

MAX_FILES = 300
HTTP_TIMEOUT = 45
FORCED_EVENTS = {"merge_group", "schedule", "workflow_dispatch"}
# A diff with no code, config or workflow file never needs a suite.
CODE_FILE = re.compile(
    r"\.(rs|toml|lock|sh|ya?ml)$|(^|/)Makefile$|(^|/)Cargo\.toml$|\.sql$"
)
FUZZ_GATES = ("fuzz_standalone", "fuzz_distributed", "fuzz_chaos")


class ScopeError(Exception):
    """A handled failure: the caller falls back instead of crashing."""


# ---------------------------------------------------------------------------
# HTTP seam (tests replace `_request`)
# ---------------------------------------------------------------------------


def _request(method, url, headers, body=None, timeout=HTTP_TIMEOUT):
    request = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(request, timeout=timeout) as response:
            return response.status, response.read()
    except urllib.error.HTTPError as err:
        return err.code, err.read()
    except (urllib.error.URLError, TimeoutError, OSError) as err:
        raise ScopeError(f"{method} {url}: {err}") from err


def _get(url, headers, timeout=HTTP_TIMEOUT):
    return _request("GET", url, headers, None, timeout)


def _post(url, headers, body, timeout=HTTP_TIMEOUT):
    return _request("POST", url, headers, body, timeout)


def get_json(url, headers, timeout=HTTP_TIMEOUT):
    """GET returning (status, parsed json); transport failures raise `ScopeError`."""
    status, raw = _get(url, headers, timeout)
    try:
        payload = json.loads(raw.decode("utf-8", "replace")) if raw else None
    except json.JSONDecodeError:
        payload = None
    return status, payload


# ---------------------------------------------------------------------------
# Path matching
# ---------------------------------------------------------------------------


def glob_to_regex(pattern):
    """Translate a repo glob into an anchored regexp.

    `**/` also matches zero directories, `**` spans `/`, `*` and `?` do not.
    """
    out = ["^"]
    index = 0
    while index < len(pattern):
        char = pattern[index]
        if char == "*":
            end = index
            while end < len(pattern) and pattern[end] == "*":
                end += 1
            if end - index >= 2:
                if end < len(pattern) and pattern[end] == "/":
                    out.append("(?:.*/)?")
                    index = end + 1
                    continue
                out.append(".*")
            else:
                out.append("[^/]*")
            index = end
        elif char == "?":
            out.append("[^/]")
            index += 1
        else:
            out.append(re.escape(char))
            index += 1
    out.append("$")
    return re.compile("".join(out))


def compile_patterns(patterns):
    return [glob_to_regex(pattern) for pattern in patterns]


def matches_any(filename, compiled):
    return any(regex.match(filename) for regex in compiled)


# ---------------------------------------------------------------------------
# GitHub API
# ---------------------------------------------------------------------------


def github_api(method, path, token, payload=None):
    base = os.environ.get("GITHUB_API_URL", "https://api.github.com").rstrip("/")
    url = base + path
    headers = {
        "Accept": "application/vnd.github+json",
        "User-Agent": "greptimedb-ci-scope",
    }
    if token:
        headers["Authorization"] = "Bearer " + token
    body = None
    if payload is not None:
        body = json.dumps(payload).encode()
        headers["Content-Type"] = "application/json"
    if method == "GET":
        return get_json(url, headers)
    status, raw = _request(method, url, headers, body, HTTP_TIMEOUT)
    try:
        parsed = json.loads(raw.decode("utf-8", "replace")) if raw else None
    except json.JSONDecodeError:
        parsed = None
    return status, parsed


def fetch_changed_files(repo, number, token):
    """Return (files, truncated). `truncated` means the PR has more than MAX_FILES."""
    files = []
    page = 1
    while True:
        status, payload = github_api(
            "GET", f"/repos/{repo}/pulls/{number}/files?per_page=100&page={page}", token
        )
        if status != 200 or not isinstance(payload, list):
            raise ScopeError(f"cannot list pull request files (HTTP {status})")
        for item in payload:
            if not isinstance(item, dict):
                continue
            patch = item.get("patch")
            files.append(
                {
                    "filename": item.get("filename", ""),
                    "previous_filename": item.get("previous_filename") or "",
                    "status": item.get("status", "modified"),
                    "additions": item.get("additions", 0),
                    "deletions": item.get("deletions", 0),
                    "patch": patch if isinstance(patch, str) else "",
                    "patch_included": isinstance(patch, str) and bool(patch),
                }
            )
        if len(payload) < 100:
            return files, False
        if len(files) >= MAX_FILES:
            # Keep `per_page=100`: with `per_page=1`, page N is item N, so the
            # probe would answer "is there a file 4?" instead of "file 301?".
            status, extra = github_api(
                "GET",
                f"/repos/{repo}/pulls/{number}/files?per_page=100&page={page + 1}",
                token,
            )
            if status != 200 or not isinstance(extra, list):
                raise ScopeError(
                    "cannot confirm the changed file list is complete "
                    f"(HTTP {status})"
                )
            del files[MAX_FILES:]
            return files, bool(extra)
        page += 1


def fetch_pull(repo, number, token):
    """Return (labels, head sha, base sha) for one pull request."""
    status, payload = github_api("GET", f"/repos/{repo}/pulls/{number}", token)
    if status != 200 or not isinstance(payload, dict):
        raise ScopeError(f"cannot read pull request (HTTP {status})")
    labels = payload.get("labels") or []
    head = ((payload.get("head") or {}).get("sha") or "").strip()
    base = ((payload.get("base") or {}).get("sha") or "").strip()
    names = {label.get("name", "") for label in labels if isinstance(label, dict)}
    return names, head, base


def revision_drift(args, head, base):
    """Return a note when the tested revisions are no longer the live ones.

    A re-run keeps the run's original SHAs while the file list served by GitHub
    is the pull request's current one. After a force-push the classifier would
    judge the new diff for the old tree, so a mismatch runs every suite instead.
    """
    for supplied, live, name in (
        (args.head_sha, head, "head"),
        (args.base_sha, base, "base"),
    ):
        if supplied and live and supplied.lower() != live.lower():
            return (
                f"pull request {name} moved from {supplied[:12]} to "
                f"{live[:12]} after this run started"
            )
    return ""


# ---------------------------------------------------------------------------
# Decisions
# ---------------------------------------------------------------------------


def file_paths(item):
    """Every path the change touched; a rename keeps the deleted side too."""
    names = [item["filename"]]
    previous = item.get("previous_filename")
    if previous and previous != item["filename"]:
        names.append(previous)
    return names


def gated_paths(item):
    """The paths of one changed file that the gate judges.

    A path under `EXEMPT_TREES` is outside the remit: it selects no suite, and
    it is not a change that "no rule covers", so it cannot widen the gate.
    """
    return [name for name in file_paths(item) if not name.startswith(EXEMPT_TREES)]


def path_matches(files):
    """Return (always_run_hits, {suite_id: [matching file paths]})."""
    always_compiled = compile_patterns(ALWAYS_RUN)
    suite_matchers = {
        suite_id: compile_patterns(suite_paths(suite))
        for suite_id, suite in SUITES.items()
    }
    always_hits = set()
    matches = {suite_id: set() for suite_id in SUITES}
    for item in files:
        for name in file_paths(item):
            if matches_any(name, always_compiled):
                always_hits.add(name)
        for name in gated_paths(item):
            for suite_id, compiled in suite_matchers.items():
                if matches_any(name, compiled):
                    matches[suite_id].add(name)
    return sorted(always_hits), {
        suite_id: sorted(matched) for suite_id, matched in matches.items()
    }


def uncovered_changes(files, matches):
    """True when a change can reach code that no suite path claims.

    This is the net for a crate that was added, split or renamed after the rules
    were written: without it such a change matches no suite and would skip every
    gated job. A single unmatched path is enough, even when other paths in the
    same diff match a suite.
    """
    return first_uncovered(files, matches) is not None


def first_uncovered(files, matches):
    """The first changed path that no suite claims, or None.

    Naming it is what makes the widening explainable in the log.
    """
    matched = {name for names in matches.values() for name in names}
    return next(
        (
            name
            for item in files
            for name in gated_paths(item)
            if name.startswith(SRC_TREE) and name not in matched
        ),
        None,
    )


def has_meaningful_changes(files):
    return any(CODE_FILE.search(item["filename"]) for item in files)


def finalize(plan):
    """`full` means every gated suite runs, whatever produced the decision."""
    plan["full"] = all(entry["run"] for entry in plan["suites"].values())
    return plan


def all_suites(run, reason, source):
    return {
        suite_id: {"run": run, "source": source, "reason": reason}
        for suite_id in SUITES
    }


def full_plan(plan, reason, source, mode="static"):
    """Every suite runs; `finalize` derives `full` from the verdicts."""
    plan.update(mode=mode, suites=all_suites(True, reason, source))
    return finalize(plan)


def forced_reason(args, labels):
    """Return why every suite must run, or an empty string."""
    event = args.event or os.environ.get("GITHUB_EVENT_NAME") or "pull_request"
    if event in FORCED_EVENTS:
        return f"event={event}"
    if os.environ.get("CI_SCOPE_FORCE_ALL", "").strip().lower() in {
        "1",
        "true",
        "yes",
        "on",
    }:
        return "CI_SCOPE_FORCE_ALL"
    if "ci:full" in labels:
        return "label ci:full"
    return ""


def build_plan(args, token):
    notes = []
    plan = {
        "mode": "static",
        "head_sha": args.head_sha or "",
        "base_sha": args.base_sha or "",
        "full": False,
        "suites": {},
        "matching": {},
        "notes": notes,
    }

    # Events and variables that force a run need no GitHub call at all; the
    # `ci:full` label is the only reason to read the pull request early.
    forced = forced_reason(args, set())
    labels, head_live, base_live = set(), "", ""
    if args.pr and not forced:
        try:
            labels, head_live, base_live = fetch_pull(
                args.repo, args.pr, token
            )
        except ScopeError as err:
            # A failed lookup must widen the gate, never narrow it.
            notes.append(f"pull request lookup failed: {err}")
            return full_plan(plan, "pull request lookup failed", "always-run")
        forced = forced_reason(args, labels)

    if forced:
        notes.append(f"forced: {forced}")
        return full_plan(plan, f"forced by {forced}", "forced", mode="forced")

    if not args.pr:
        raise ScopeError("--pr is required unless the event forces every suite")

    drift = revision_drift(args, head_live, base_live)
    if drift:
        notes.append(drift)
        return full_plan(plan, drift, "always-run")

    files, truncated = fetch_changed_files(args.repo, args.pr, token)
    if truncated:
        notes.append(f"changed file list truncated at {MAX_FILES}")
        return full_plan(
            plan,
            f"pull request has more than {MAX_FILES} changed files",
            "always-run",
        )

    always_hits, matches = path_matches(files)
    # Keep the evidence: the step summary explains a verdict by naming the files
    # that matched, which is what triage asks for first.
    plan["matching"] = {suite_id: sorted(names) for suite_id, names in matches.items()}
    if always_hits:
        notes.append(f"always-run: {', '.join(sorted(always_hits)[:5])}")
        return full_plan(
            plan,
            "always-run path: " + ", ".join(sorted(always_hits)[:3]),
            "always-run",
        )

    first = first_uncovered(files, matches)
    if first is not None:
        notes.append(f"uncovered build change: {first}")
        return full_plan(plan, f"uncovered build change: {first}", "uncovered")

    static_run = {suite_id: bool(matched) for suite_id, matched in matches.items()}

    if not has_meaningful_changes(files) and not any(static_run.values()):
        plan.update(
            mode="static",
            full=False,
            suites={
                suite_id: {
                    "run": False,
                    "source": "static",
                    "reason": "no code, config or workflow file changed",
                }
                for suite_id in SUITES
            },
        )
        notes.append("no meaningful changes: skip every suite")
        return finalize(plan)

    plan.update(
        mode="static",
        suites={
            suite_id: {
                "run": static_run[suite_id],
                "source": "static",
                "reason": ("path rule: " + ", ".join(matches[suite_id][:3]))
                if matches[suite_id]
                else "no path rule matched",
            }
            for suite_id in SUITES
        },
    )
    return finalize(plan)


def derive_outputs(plan):
    """Flatten the plan into the `$GITHUB_OUTPUT` keys the workflow gates on.


    Every gate in `SUITES` must route to a key here, otherwise its suites are
    classified but never switch a job; `test_every_gate_routes_to_an_output`
    asks this function directly, so a new gate fails there until it is routed.


    Job-level `if:` cannot read the `matrix` context, so the workflow gates a
    whole fuzz gate instead of individual matrix legs; the per-suite verdicts
    stay visible in the step log.
    """

    def suites_of(gate):
        return [
            suite_id
            for suite_id, entry in plan["suites"].items()
            if entry["run"] and SUITES[suite_id]["gate"] == gate
        ]

    outputs = {}
    legs = {}
    for gate in FUZZ_GATES:
        running = suites_of(gate)
        legs[gate] = [suite_group(suite_id) for suite_id in running]
        legs[gate] = [leg for leg in legs[gate] if leg]
        outputs[gate] = "true" if running else "false"
    unstable = bool(suites_of("fuzz_unstable"))
    # Every gate outside the fuzz group switches exactly one job of its own.
    for gate in sorted({suite["gate"] for suite in SUITES.values()}):
        if gate not in FUZZ_GATES:
            outputs[gate] = "true" if suites_of(gate) else "false"
    outputs["any_fuzz"] = (
        "true"
        if unstable or any(outputs[gate] == "true" for gate in FUZZ_GATES)
        else "false"
    )
    distributed = outputs["fuzz_distributed"] == "true" or outputs["fuzz_chaos"] == "true"
    outputs["build_ci_needed"] = "true" if unstable or distributed else "false"
    return outputs, legs


def render_log(plan):
    """One line per suite plus the emitted gates, for the step log.

    Triage starts in the run log, so the verdict has to be readable there
    without any other artifact.
    """
    head = f"CI scope: mode={plan['mode']} full={'true' if plan['full'] else 'false'}"
    lines = [head]
    width = max((len(suite_id) for suite_id in plan["suites"]), default=0)
    for suite_id, entry in plan["suites"].items():
        lines.append(
            f"  {'run ' if entry['run'] else 'skip'} {suite_id:<{width}}  "
            f"{entry['source']:<12} {entry['reason']}"
        )
    outputs, _ = derive_outputs(plan)
    lines.append(
        "  gates: " + " ".join(f"{key}={value}" for key, value in outputs.items())
    )
    for note in plan["notes"]:
        lines.append(f"  note: {note}")
    return "\n".join(lines)


SUMMARY_MAX_FILES = 3


def render_summary(plan):
    """Markdown for the job summary: the matching evidence behind each verdict.

    The step log answers "what ran"; this answers "why", which is the question
    when a pull request unexpectedly skips or starts a suite.
    """
    lines = [
        f"### CI scope: mode={plan['mode']}, full={'true' if plan['full'] else 'false'}",
        "",
        "| suite | verdict | rule | matched files |",
        "| --- | --- | --- | --- |",
    ]
    matching = plan.get("matching") or {}
    for suite_id, entry in plan["suites"].items():
        matched = matching.get(suite_id) or []
        shown = ", ".join(f"`{name}`" for name in matched[:SUMMARY_MAX_FILES])
        if len(matched) > SUMMARY_MAX_FILES:
            shown += f" +{len(matched) - SUMMARY_MAX_FILES} more"
        lines.append(
            f"| `{suite_id}` | {'run' if entry['run'] else 'skip'} | "
            f"{entry['source']} | {shown or '—'} |"
        )
    if plan["notes"]:
        lines += ["", "<details><summary>notes</summary>", ""]
        lines += [f"- {note}" for note in plan["notes"]]
        lines += ["", "</details>"]
    return "\n".join(lines)


def log_annotations(plan):
    """Workflow-run annotations for every degraded decision.

    A lookup failure widens the gate; it must be visible in the run list without
    opening the job log, otherwise it looks like a confident verdict.
    """
    annotations = []
    for note in plan["notes"]:
        if note.startswith("pull request lookup failed"):
            suffix = "every suite runs"
            level = "warning"
        else:
            continue
        message = f"CI scope: {note}; {suffix}"[:300]
        annotations.append(f"::{level}::{message}")
    return annotations


def emit(plan, args):
    outputs, _ = derive_outputs(plan)
    if args.dry_run:
        print(json.dumps(outputs, indent=2))
    elif args.github_output:
        with open(args.github_output, "a", encoding="utf-8") as handle:
            for key, value in outputs.items():
                handle.write(f"{key}={value}\n")
    if args.summary:
        with open(args.summary, "a", encoding="utf-8") as handle:
            handle.write(render_summary(plan) + "\n")


def parse_args(argv):
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    parser.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY", ""))
    parser.add_argument("--pr", type=int)
    parser.add_argument("--head-sha", default="")
    parser.add_argument("--base-sha", default="")
    parser.add_argument(
        "--event",
        choices=["pull_request", "merge_group", "schedule", "workflow_dispatch"],
    )
    parser.add_argument("--github-output")
    parser.add_argument(
        "--summary",
        help="append the markdown matching table to this file ($GITHUB_STEP_SUMMARY)",
    )
    parser.add_argument("--dry-run", action="store_true")
    return parser.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    token = os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN") or ""
    plan = build_plan(args, token)
    for annotation in log_annotations(plan):
        print(annotation)
    print(render_log(plan))
    emit(plan, args)
    return 0


if __name__ == "__main__":
    sys.exit(main())
