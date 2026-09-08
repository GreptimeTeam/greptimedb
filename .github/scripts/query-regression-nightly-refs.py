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

"""Resolve consecutive successful Nightly Build SHAs for query-regression.

Query regression builds both binaries from git; this script only picks the
head SHAs of two Nightly Build workflow runs (today vs the previous success).
"""

from __future__ import annotations

import argparse
import json
import os
import sys
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any


NIGHTLY_WORKFLOW = "nightly-build.yml"
NIGHTLY_EVENTS = frozenset({"schedule", "workflow_dispatch"})


@dataclass(frozen=True)
class WorkflowRun:
    id: int
    head_sha: str
    head_branch: str
    html_url: str
    created_at: str
    conclusion: str
    event: str


@dataclass(frozen=True)
class RefPair:
    base: WorkflowRun | None
    candidate: WorkflowRun | None
    skip: bool
    reason: str


def parse_run(payload: dict[str, Any]) -> WorkflowRun:
    return WorkflowRun(
        id=int(payload["id"]),
        head_sha=str(payload["head_sha"]),
        head_branch=str(payload.get("head_branch") or ""),
        html_url=str(payload.get("html_url") or ""),
        created_at=str(payload.get("created_at") or ""),
        conclusion=str(payload.get("conclusion") or ""),
        event=str(payload.get("event") or ""),
    )


def github_get(token: str, path: str, query: dict[str, str] | None = None) -> dict[str, Any]:
    encoded = f"?{urllib.parse.urlencode(query)}" if query else ""
    request = urllib.request.Request(
        f"https://api.github.com{path}{encoded}",
        method="GET",
        headers={
            "Accept": "application/vnd.github+json",
            "Authorization": f"Bearer {token}",
            "X-GitHub-Api-Version": "2022-11-28",
        },
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            return json.loads(response.read().decode("utf-8"))
    except urllib.error.HTTPError as error:
        body = error.read().decode("utf-8", "replace")
        raise SystemExit(
            f"GitHub API GET {path} failed: HTTP {error.code}: {body}"
        ) from error


def list_successful_nightly_runs(
    token: str,
    repo: str,
    *,
    branch: str | None = None,
    per_page: int = 30,
) -> list[WorkflowRun]:
    query = {
        "status": "success",
        "per_page": str(per_page),
    }
    if branch:
        query["branch"] = branch
    payload = github_get(
        token,
        f"/repos/{repo}/actions/workflows/{NIGHTLY_WORKFLOW}/runs",
        query,
    )
    runs = [parse_run(item) for item in payload.get("workflow_runs") or []]
    return [
        run
        for run in runs
        if run.conclusion == "success" and run.event in NIGHTLY_EVENTS and run.head_sha
    ]


def fetch_run(token: str, repo: str, run_id: int) -> WorkflowRun:
    payload = github_get(token, f"/repos/{repo}/actions/runs/{run_id}")
    return parse_run(payload)


def select_base_and_candidate(
    runs: list[WorkflowRun],
    *,
    candidate_run_id: int | None = None,
    candidate: WorkflowRun | None = None,
) -> RefPair:
    """Pick candidate (newest / requested) and the previous successful run.

    `runs` must be newest-first. Same-SHA consecutive nightlies are skipped
    (no commits that day).
    """
    if candidate is None:
        if candidate_run_id is not None:
            candidate = next((run for run in runs if run.id == candidate_run_id), None)
            if candidate is None:
                return RefPair(
                    None,
                    None,
                    True,
                    f"candidate nightly run {candidate_run_id} was not a successful "
                    f"{NIGHTLY_WORKFLOW} run",
                )
        elif runs:
            candidate = runs[0]
        else:
            return RefPair(None, None, True, "no successful Nightly Build runs")

    branch = candidate.head_branch
    older = [
        run
        for run in runs
        if run.id != candidate.id
        and (not branch or not run.head_branch or run.head_branch == branch)
        and (not candidate.created_at or run.created_at <= candidate.created_at)
    ]
    if not older:
        return RefPair(
            None,
            candidate,
            True,
            "no previous successful Nightly Build to compare against",
        )
    base = older[0]
    if base.head_sha.lower() == candidate.head_sha.lower():
        return RefPair(
            base,
            candidate,
            True,
            f"previous nightly SHA {base.head_sha} matches candidate; nothing to compare",
        )
    return RefPair(base, candidate, False, "")


def _override_run(sha: str) -> WorkflowRun:
    return WorkflowRun(
        id=0,
        head_sha=sha,
        head_branch="",
        html_url="",
        created_at="",
        conclusion="success",
        event="workflow_dispatch",
    )


def override_pair(base_ref: str, candidate_ref: str) -> RefPair:
    return RefPair(_override_run(base_ref), _override_run(candidate_ref), False, "explicit refs")


def write_outputs(pair: RefPair) -> None:
    values = {
        "skip": "true" if pair.skip else "false",
        "reason": pair.reason,
        "base_sha": pair.base.head_sha if pair.base else "",
        "candidate_sha": pair.candidate.head_sha if pair.candidate else "",
        "base_run_url": pair.base.html_url if pair.base else "",
        "candidate_run_url": pair.candidate.html_url if pair.candidate else "",
        "base_run_id": str(pair.base.id) if pair.base and pair.base.id else "",
        "candidate_run_id": str(pair.candidate.id) if pair.candidate and pair.candidate.id else "",
    }
    output_path = os.environ.get("GITHUB_OUTPUT")
    if output_path:
        with open(output_path, "a", encoding="utf-8") as handle:
            for key, value in values.items():
                handle.write(f"{key}={value}\n")
    for key, value in values.items():
        print(f"{key}={value}")


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY"))
    parser.add_argument(
        "--token",
        default=os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN"),
    )
    parser.add_argument("--branch", default=os.environ.get("NIGHTLY_BRANCH") or "")
    parser.add_argument(
        "--candidate-run-id",
        default=os.environ.get("CANDIDATE_RUN_ID") or "",
    )
    parser.add_argument("--base-ref", default=os.environ.get("BASE_REF") or "")
    parser.add_argument("--candidate-ref", default=os.environ.get("CANDIDATE_REF") or "")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    base_ref = args.base_ref.strip()
    candidate_ref = args.candidate_ref.strip()
    if base_ref and candidate_ref:
        write_outputs(override_pair(base_ref, candidate_ref))
        return 0
    if bool(base_ref) != bool(candidate_ref):
        print("Provide both --base-ref and --candidate-ref, or neither.", file=sys.stderr)
        return 2
    if not args.repo or not args.token:
        print("--repo and --token (or GITHUB_REPOSITORY / GITHUB_TOKEN) are required.", file=sys.stderr)
        return 2

    candidate_run_id = int(args.candidate_run_id) if str(args.candidate_run_id).strip() else None
    candidate: WorkflowRun | None = None
    branch = args.branch.strip() or None
    if candidate_run_id is not None:
        candidate = fetch_run(args.token, args.repo, candidate_run_id)
        if candidate.conclusion != "success":
            write_outputs(
                RefPair(
                    None,
                    candidate,
                    True,
                    f"nightly run {candidate_run_id} conclusion is {candidate.conclusion}",
                )
            )
            return 0
        branch = branch or candidate.head_branch or None

    runs = list_successful_nightly_runs(args.token, args.repo, branch=branch)
    pair = select_base_and_candidate(
        runs,
        candidate_run_id=candidate_run_id if candidate is None else None,
        candidate=candidate,
    )
    write_outputs(pair)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
