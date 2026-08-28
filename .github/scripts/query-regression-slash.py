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

"""Admit a dispatched `/query-regression` command as a query-regression run.

slash-command-dispatch owns comment parsing and admin permission. This script
re-fetches the triggering comment by id (so a forged repository_dispatch
payload cannot spoof the actor or PR), then validates case args, the
QUERY_REGRESSION_COMMENT_ALLOWLIST subset, and the PR's current merge commit.
"""

from __future__ import annotations

import argparse
import json
import os
import re
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from dataclasses import dataclass
from typing import Any


COMMAND = "/query-regression"
ALLOWED_PERMISSIONS = frozenset({"admin"})
FULL_SHA = re.compile(r"^[0-9a-fA-F]{40}$")
CASE_TOKEN = re.compile(r"^(?:all|heavy|tests/perf/query_cases/[A-Za-z0-9_][A-Za-z0-9_./-]*)$")
COMMAND_LINE = re.compile(rf"^{re.escape(COMMAND)}(?:\s+(.*))?$")
ISSUE_URL = re.compile(r"/repos/([^/]+/[^/]+)/issues/(\d+)$")


@dataclass(frozen=True)
class CommandParse:
    matched: bool
    case: str = ""
    error: str = ""


@dataclass(frozen=True)
class Decision:
    skip: bool
    reason: str
    case: str = ""
    pr_number: str = ""
    base_sha: str = ""
    candidate_sha: str = ""
    head_sha: str = ""
    head_repo: str = ""
    base_repo: str = ""
    reply: str = ""


@dataclass(frozen=True)
class CommentIdentity:
    actor: str = ""
    pr_number: int = 0
    command: CommandParse = CommandParse(matched=False)
    error: str = ""


def parse_case_args(raw_args: str) -> CommandParse:
    tokens = [part for part in re.split(r"[\s,]+", (raw_args or "").strip()) if part]
    if not tokens:
        return CommandParse(matched=True, case="all")
    if "all" in tokens and len(tokens) > 1:
        return CommandParse(matched=True, error="'all' cannot be mixed with other case selectors")
    for token in tokens:
        if ".." in token or not CASE_TOKEN.fullmatch(token):
            return CommandParse(
                matched=True,
                error=(
                    "case selector must be 'all', 'heavy', or tests/perf/query_cases/... "
                    f"paths without '..'; got {token!r}"
                ),
            )
    return CommandParse(matched=True, case=",".join(tokens))


def parse_command(body: str) -> CommandParse:
    first = (body or "").replace("\r\n", "\n").replace("\r", "\n").split("\n", 1)[0].strip()
    match = COMMAND_LINE.fullmatch(first)
    if match is None:
        return CommandParse(matched=False)
    return parse_case_args(match.group(1) or "")


def parse_allowlist(raw: str) -> frozenset[str]:
    names: set[str] = set()
    for part in re.split(r"[\s,]+", raw or ""):
        login = part.strip().lstrip("@")
        if login:
            names.add(login.lower())
    return frozenset(names)


def is_full_sha(value: str) -> bool:
    return bool(FULL_SHA.fullmatch(value or ""))


def parse_github_id(value: str) -> int | None:
    stripped = (value or "").strip()
    if not stripped.isdigit():
        return None
    number = int(stripped)
    return number if number > 0 else None


def github_get(token: str, api_url: str, path: str) -> dict[str, Any]:
    request = urllib.request.Request(
        f"{api_url.rstrip('/')}{path}",
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


def fetch_comment(token: str, api_url: str, repo: str, comment_id: int) -> dict[str, Any]:
    return github_get(token, api_url, f"/repos/{repo}/issues/comments/{comment_id}")


def identity_from_comment(comment: dict[str, Any], expected_repo: str) -> CommentIdentity:
    login = str((comment.get("user") or {}).get("login") or "")
    issue_url = str(comment.get("issue_url") or "")
    match = ISSUE_URL.search(issue_url)
    if match is None:
        return CommentIdentity(error="comment issue_url is missing or invalid")
    repo, pr_number = match.group(1), int(match.group(2))
    if repo.lower() != expected_repo.lower():
        return CommentIdentity(
            actor=login,
            pr_number=pr_number,
            error=f"comment targets {repo}, not {expected_repo}",
        )
    command = parse_command(str(comment.get("body") or ""))
    if not command.matched:
        return CommentIdentity(
            actor=login,
            pr_number=pr_number,
            command=command,
            error="comment is not a query-regression command",
        )
    if command.error:
        return CommentIdentity(
            actor=login,
            pr_number=pr_number,
            command=command,
            error=command.error,
        )
    return CommentIdentity(actor=login, pr_number=pr_number, command=command)


def payload_matches_comment(
    identity: CommentIdentity,
    *,
    actor: str,
    pr_number: str,
    command_args: str,
) -> str:
    if actor and actor.strip().lstrip("@").lower() != identity.actor.strip().lstrip("@").lower():
        return "payload actor does not match comment author"
    if pr_number.strip():
        parsed = parse_github_id(pr_number)
        if parsed is None:
            return "payload PR number is not a valid integer"
        if parsed != identity.pr_number:
            return "payload PR number does not match comment"
    payload_command = parse_case_args(command_args)
    if payload_command.error:
        return payload_command.error
    if payload_command.case != identity.command.case:
        return "payload command args do not match comment"
    return ""


def fetch_pull(
    token: str,
    api_url: str,
    repo: str,
    pr_number: int,
    *,
    attempts: int = 5,
) -> dict[str, Any]:
    path = f"/repos/{repo}/pulls/{pr_number}"
    payload: dict[str, Any] = {}
    for attempt in range(1, attempts + 1):
        payload = github_get(token, api_url, path)
        if payload.get("mergeable") is not None or payload.get("draft"):
            return payload
        if attempt < attempts:
            time.sleep(2 ** (attempt - 1))
    return payload


def fetch_permission(token: str, api_url: str, repo: str, username: str) -> str:
    encoded = urllib.parse.quote(username)
    try:
        payload = github_get(
            token,
            api_url,
            f"/repos/{repo}/collaborators/{encoded}/permission",
        )
    except SystemExit as error:
        message = str(error)
        if "HTTP 404" in message:
            return ""
        raise
    permission = str(payload.get("permission") or "")
    return permission if permission in ALLOWED_PERMISSIONS else "denied"


def deny(reason: str, *, reply: str = "", pr_number: str = "") -> Decision:
    return Decision(skip=True, reason=reason, reply=reply, pr_number=pr_number)


def admit_pull(
    pull: dict[str, Any],
    *,
    actor: str,
    allowlist: frozenset[str],
    permission: str,
    command: CommandParse,
    expected_repo: str,
    pr_number: str = "",
) -> Decision:
    def reject(reason: str, *, reply: str = "") -> Decision:
        return deny(reason, reply=reply, pr_number=pr_number)

    if not command.matched:
        return reject("comment is not a query-regression command")
    if command.error:
        return reject(
            command.error,
            reply=f"Query regression command ignored: {command.error}.",
        )

    actor_key = actor.strip().lstrip("@").lower()
    if not allowlist:
        return reject(
            "QUERY_REGRESSION_COMMENT_ALLOWLIST is unset",
            reply=(
                "Query regression command ignored: repository variable "
                "`QUERY_REGRESSION_COMMENT_ALLOWLIST` is empty."
            ),
        )
    if actor_key not in allowlist:
        return reject(
            "commenter is not on QUERY_REGRESSION_COMMENT_ALLOWLIST",
            reply="Query regression command ignored: you are not on the allowlist.",
        )
    if permission not in ALLOWED_PERMISSIONS:
        return reject(
            "commenter is not a repository admin",
            reply="Query regression command ignored: repository admin permission is required.",
        )

    if pull.get("draft"):
        return reject(
            "PR is a draft",
            reply="Query regression command ignored: draft PRs are not admitted.",
        )
    if pull.get("state") != "open":
        return reject(
            f"PR is {pull.get('state')}",
            reply="Query regression command ignored: the pull request is not open.",
        )
    if pull.get("merged"):
        return reject(
            "PR is already merged",
            reply="Query regression command ignored: the pull request is already merged.",
        )
    if pull.get("mergeable") is False:
        return reject(
            "PR has merge conflicts",
            reply=(
                "Query regression command ignored: the pull request is not mergeable. "
                "Resolve conflicts and comment `/query-regression` again."
            ),
        )
    if pull.get("mergeable") is None:
        return reject(
            "PR mergeability is not yet computed",
            reply=(
                "Query regression command ignored: GitHub has not computed mergeability yet. "
                "Retry in a few seconds."
            ),
        )

    base_repo = str(pull.get("base", {}).get("repo", {}).get("full_name") or "")
    head_repo = str(pull.get("head", {}).get("repo", {}).get("full_name") or "")
    base_sha = str(pull.get("base", {}).get("sha") or "")
    head_sha = str(pull.get("head", {}).get("sha") or "")
    merge_sha = str(pull.get("merge_commit_sha") or "")
    pr_number = str(pull.get("number") or pr_number)

    if base_repo != expected_repo:
        return reject(
            f"PR targets {base_repo}, not {expected_repo}",
            reply="Query regression command ignored: pull request is not against this repository.",
        )
    if not is_full_sha(base_sha) or not is_full_sha(head_sha) or not is_full_sha(merge_sha):
        return reject(
            "PR is missing an immutable merge, head, or base SHA",
            reply=(
                "Query regression command ignored: GitHub did not provide a full merge SHA. "
                "Retry once the PR is mergeable."
            ),
        )

    return Decision(
        skip=False,
        reason="",
        case=command.case,
        pr_number=pr_number,
        base_sha=base_sha.lower(),
        candidate_sha=merge_sha.lower(),
        head_sha=head_sha.lower(),
        head_repo=head_repo,
        base_repo=base_repo,
    )


def write_outputs(decision: Decision) -> None:
    values = {
        "skip": "true" if decision.skip else "false",
        "reason": decision.reason,
        "case": decision.case,
        "pr_number": decision.pr_number,
        "base_sha": decision.base_sha,
        "candidate_sha": decision.candidate_sha,
        "head_sha": decision.head_sha,
        "head_repo": decision.head_repo,
        "base_repo": decision.base_repo,
        "reply": decision.reply,
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
    parser.add_argument("--repo", default=os.environ.get("GITHUB_REPOSITORY") or "")
    parser.add_argument(
        "--token",
        default=os.environ.get("GITHUB_TOKEN") or os.environ.get("GH_TOKEN") or "",
    )
    parser.add_argument("--api-url", default=os.environ.get("GITHUB_API_URL") or "https://api.github.com")
    parser.add_argument("--actor", default=os.environ.get("COMMENT_ACTOR") or "")
    parser.add_argument("--args", default=os.environ.get("COMMAND_ARGS") or "")
    parser.add_argument("--pr-number", default=os.environ.get("PR_NUMBER") or "")
    parser.add_argument("--comment-id", default=os.environ.get("COMMENT_ID") or "")
    parser.add_argument(
        "--allowlist",
        default=os.environ.get("QUERY_REGRESSION_COMMENT_ALLOWLIST") or "",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    comment_id = parse_github_id(str(args.comment_id))
    if not args.repo or not args.token or comment_id is None:
        print("--repo, --token, and a numeric --comment-id are required.", file=sys.stderr)
        return 2

    comment = fetch_comment(args.token, args.api_url, args.repo, comment_id)
    identity = identity_from_comment(comment, args.repo)
    pr_number = str(identity.pr_number or args.pr_number or "")
    if identity.error:
        write_outputs(
            deny(
                identity.error,
                reply=f"Query regression command ignored: {identity.error}.",
                pr_number=pr_number,
            )
        )
        return 0
    mismatch = payload_matches_comment(
        identity,
        actor=args.actor,
        pr_number=args.pr_number,
        command_args=args.args,
    )
    if mismatch:
        write_outputs(
            deny(
                mismatch,
                reply=f"Query regression command ignored: {mismatch}.",
                pr_number=str(identity.pr_number),
            )
        )
        return 0

    allowlist = parse_allowlist(args.allowlist)
    permission = fetch_permission(args.token, args.api_url, args.repo, identity.actor)
    pull = fetch_pull(args.token, args.api_url, args.repo, identity.pr_number)
    decision = admit_pull(
        pull,
        actor=identity.actor,
        allowlist=allowlist,
        permission=permission,
        command=identity.command,
        expected_repo=args.repo,
        pr_number=str(identity.pr_number),
    )
    write_outputs(decision)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
