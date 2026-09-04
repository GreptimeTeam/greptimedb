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
requires the dispatch sender to be github-actions[bot], re-fetches the
triggering comment by id (so a forged repository_dispatch payload cannot
spoof the actor or PR), requires the current PR head to match the
dispatcher-snapshotted head SHA, then validates case args, the
QUERY_REGRESSION_COMMENT_ALLOWLIST subset, and the PR's current merge commit.
On admit it posts a hidden HMAC-signed marker comment
(QUERY_REGRESSION_ADMISSION_HMAC) that the sticky-comment workflow verifies;
the ECS job cannot forge that marker.
"""

from __future__ import annotations

import argparse
import hashlib
import hmac
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
ALLOWED_DISPATCH_SENDERS = frozenset({"github-actions[bot]"})
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


def nested_str(payload: Any, *keys: str) -> str:
    current: Any = payload
    for key in keys:
        if not isinstance(current, dict):
            return ""
        current = current.get(key)
    if current is None:
        return ""
    return str(current)


MARKER_PREFIX = "<!-- query-regression-admission v1"
MARKER_SUFFIX = "-->"


def admission_mac_message(identity: dict[str, Any]) -> str:
    return "|".join(
        [
            str(identity["run_id"]),
            str(identity["pr_number"]),
            str(identity["head_sha"]).lower(),
            str(identity["head_repo"]),
            str(identity["base_repo"]),
            str(identity["candidate_sha"]).lower(),
            str(identity["base_sha"]).lower(),
        ]
    )


def sign_admission(secret: str, identity: dict[str, Any]) -> str:
    return hmac.new(
        secret.encode("utf-8"),
        admission_mac_message(identity).encode("utf-8"),
        hashlib.sha256,
    ).hexdigest()


def verify_admission_mac(secret: str, identity: dict[str, Any], mac: str) -> bool:
    if not secret or not mac:
        return False
    expected = sign_admission(secret, identity)
    try:
        return hmac.compare_digest(expected, mac)
    except (TypeError, ValueError):
        return False


def format_admission_marker(identity: dict[str, Any]) -> str:
    body = json.dumps(identity, sort_keys=True)
    return f"{MARKER_PREFIX}\n{body}\n{MARKER_SUFFIX}\n"


def parse_admission_marker(body: str) -> dict[str, Any] | None:
    start = (body or "").find(MARKER_PREFIX)
    if start < 0:
        return None
    rest = body[start + len(MARKER_PREFIX) :]
    end = rest.find(MARKER_SUFFIX)
    if end < 0:
        return None
    try:
        payload = json.loads(rest[:end].strip())
    except json.JSONDecodeError:
        return None
    return payload if isinstance(payload, dict) else None


def parse_github_id(value: str) -> int | None:
    stripped = (value or "").strip()
    if not stripped.isdigit():
        return None
    number = int(stripped)
    return number if number > 0 else None


def github_request(
    token: str,
    api_url: str,
    path: str,
    *,
    method: str = "GET",
    payload: dict[str, Any] | None = None,
) -> dict[str, Any]:
    data = None if payload is None else json.dumps(payload).encode("utf-8")
    headers = {
        "Accept": "application/vnd.github+json",
        "Authorization": f"Bearer {token}",
        "X-GitHub-Api-Version": "2022-11-28",
    }
    if data is not None:
        headers["Content-Type"] = "application/json"
    request = urllib.request.Request(
        f"{api_url.rstrip('/')}{path}",
        method=method,
        data=data,
        headers=headers,
    )
    try:
        with urllib.request.urlopen(request, timeout=30) as response:
            raw = response.read().decode("utf-8")
            return json.loads(raw) if raw else {}
    except urllib.error.HTTPError as error:
        body = error.read().decode("utf-8", "replace")
        raise SystemExit(
            f"GitHub API {method} {path} failed: HTTP {error.code}: {body}"
        ) from error


def github_get(token: str, api_url: str, path: str) -> dict[str, Any]:
    return github_request(token, api_url, path)


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


def dispatch_sender_ok(sender: str) -> str:
    """Empty if repository_dispatch came from Actions; otherwise a deny reason."""
    login = (sender or "").strip().lstrip("@").lower()
    if login not in ALLOWED_DISPATCH_SENDERS:
        return "repository_dispatch sender is not github-actions[bot]"
    return ""


def dispatch_head_matches(pull: dict[str, Any], snapshot_sha: str) -> str:
    """Empty if the current PR head is the dispatcher snapshot; otherwise a deny reason."""
    snapshot = (snapshot_sha or "").strip().lower()
    if not is_full_sha(snapshot):
        return "dispatch payload is missing an immutable PR head SHA"
    current = nested_str(pull, "head", "sha").lower()
    if current != snapshot:
        return "PR head changed since the slash command was dispatched"
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

    base_repo = nested_str(pull, "base", "repo", "full_name")
    head_repo = nested_str(pull, "head", "repo", "full_name")
    base_sha = nested_str(pull, "base", "sha")
    head_sha = nested_str(pull, "head", "sha")
    merge_sha = nested_str(pull, "merge_commit_sha")
    pr_number = str(pull.get("number") or pr_number)

    if not head_repo:
        return reject(
            "PR head repository is missing",
            reply=(
                "Query regression command ignored: the pull request head repository "
                "is unavailable (the fork may have been deleted)."
            ),
        )
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


def build_admission_identity(decision: Decision) -> dict[str, Any] | None:
    """PR identity from the trusted admission job. MAC is added at persist time."""
    if decision.skip:
        return None
    run_id = parse_github_id(os.environ.get("GITHUB_RUN_ID") or "")
    pr_number = parse_github_id(decision.pr_number)
    if run_id is None or pr_number is None:
        return None
    if not all(
        [
            decision.head_sha,
            decision.head_repo,
            decision.base_repo,
            decision.candidate_sha,
            decision.base_sha,
        ]
    ):
        return None
    return {
        "pr_number": pr_number,
        "head_sha": decision.head_sha,
        "head_repo": decision.head_repo,
        "base_repo": decision.base_repo,
        "candidate_sha": decision.candidate_sha,
        "base_sha": decision.base_sha,
        "run_id": run_id,
        "run_attempt": parse_github_id(os.environ.get("GITHUB_RUN_ATTEMPT") or "1") or 1,
    }


def post_admission_marker(
    token: str,
    api_url: str,
    repo: str,
    identity: dict[str, Any],
) -> None:
    """Hide a signed identity on the admitted PR. ECS cannot forge this."""
    github_request(
        token,
        api_url,
        f"/repos/{repo}/issues/{identity['pr_number']}/comments",
        method="POST",
        payload={"body": format_admission_marker(identity)},
    )


def persist_admission_identity(
    decision: Decision,
    *,
    token: str,
    api_url: str,
    repo: str,
    secret: str,
) -> str:
    """Write the lookup artifact and post the HMAC marker. Empty string on success."""
    if decision.skip:
        return ""
    if not secret.strip():
        return "QUERY_REGRESSION_ADMISSION_HMAC is unset"
    identity = build_admission_identity(decision)
    if identity is None:
        return "could not persist admission identity"
    payload = {**identity, "mac": sign_admission(secret, identity)}
    with open("query-regression-admission.json", "w", encoding="utf-8") as handle:
        json.dump(payload, handle, sort_keys=True)
        handle.write("\n")
    post_admission_marker(token, api_url, repo, payload)
    return ""


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
    parser.add_argument(
        "--dispatch-sender",
        default=os.environ.get("DISPATCH_SENDER") or "",
    )
    parser.add_argument(
        "--dispatch-head-sha",
        default=os.environ.get("DISPATCH_HEAD_SHA") or "",
    )
    return parser.parse_args(argv)


def admit_dispatched_command(args: argparse.Namespace, comment_id: int) -> int:
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
    head_mismatch = dispatch_head_matches(pull, args.dispatch_head_sha)
    if head_mismatch:
        write_outputs(
            deny(
                head_mismatch,
                reply=(
                    "Query regression command ignored: the PR head changed since the "
                    "command was dispatched. Review the current revision and comment "
                    "`/query-regression` again."
                ),
                pr_number=str(identity.pr_number),
            )
        )
        return 0
    decision = admit_pull(
        pull,
        actor=identity.actor,
        allowlist=allowlist,
        permission=permission,
        command=identity.command,
        expected_repo=args.repo,
        pr_number=str(identity.pr_number),
    )
    if not decision.skip:
        persist_error = persist_admission_identity(
            decision,
            token=args.token,
            api_url=args.api_url,
            repo=args.repo,
            secret=os.environ.get("QUERY_REGRESSION_ADMISSION_HMAC") or "",
        )
        if persist_error:
            write_outputs(
                deny(
                    persist_error,
                    reply=f"Query regression command ignored: {persist_error}.",
                    pr_number=decision.pr_number,
                )
            )
            print(persist_error, file=sys.stderr)
            return 1
    write_outputs(decision)
    return 0


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    comment_id = parse_github_id(str(args.comment_id))
    if not args.repo or not args.token or comment_id is None:
        print("--repo, --token, and a numeric --comment-id are required.", file=sys.stderr)
        return 2

    sender_error = dispatch_sender_ok(args.dispatch_sender)
    if sender_error:
        # Do not reply: the payload PR number is untrusted when the sender is not Actions.
        write_outputs(deny(sender_error))
        return 0

    try:
        return admit_dispatched_command(args, comment_id)
    except SystemExit as error:
        write_outputs(
            deny(
                f"admission failed: {error}",
                reply=(
                    "Query regression command ignored: GitHub API error while "
                    "admitting; please retry."
                ),
                pr_number=str(args.pr_number or ""),
            )
        )
        print(error, file=sys.stderr)
        return 1


if __name__ == "__main__":
    raise SystemExit(main())
