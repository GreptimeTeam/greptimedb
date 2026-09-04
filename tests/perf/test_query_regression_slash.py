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

"""Coverage for PR comment admission of query-regression."""

import importlib.util
import os
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


SCRIPTS_DIR = Path(__file__).parents[2] / ".github/scripts"


def load_module():
    spec = importlib.util.spec_from_file_location(
        "query_regression_slash_under_test",
        SCRIPTS_DIR / "query-regression-slash.py",
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


slash = load_module()

HEAD = "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
BASE = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"
MERGE = "cccccccccccccccccccccccccccccccccccccccc"


def pull_payload(**overrides: object) -> dict:
    payload = {
        "number": 42,
        "state": "open",
        "draft": False,
        "merged": False,
        "mergeable": True,
        "merge_commit_sha": MERGE,
        "base": {
            "sha": BASE,
            "repo": {"full_name": "GreptimeTeam/greptimedb"},
        },
        "head": {
            "sha": HEAD,
            "repo": {"full_name": "alice/greptimedb"},
        },
    }
    payload.update(overrides)
    return payload


def admit(*, body: str = "/query-regression", actor: str = "maintainer", **pull_overrides: object):
    payload = pull_payload(**pull_overrides)
    return slash.admit_pull(
        payload,
        actor=actor,
        allowlist=slash.parse_allowlist("maintainer, other-admin"),
        permission="admin",
        command=slash.parse_command(body),
        expected_repo="GreptimeTeam/greptimedb",
        pr_number=str(payload.get("number") or ""),
    )


class ParseCaseArgsTest(unittest.TestCase):
    def test_empty_args_select_all(self) -> None:
        parsed = slash.parse_case_args("")
        self.assertTrue(parsed.matched)
        self.assertEqual(parsed.case, "all")
        self.assertEqual(parsed.error, "")

    def test_heavy_and_explicit_paths(self) -> None:
        self.assertEqual(slash.parse_case_args("heavy").case, "heavy")
        self.assertEqual(
            slash.parse_case_args("tests/perf/query_cases/sql_topk_order_by/case.toml").case,
            "tests/perf/query_cases/sql_topk_order_by/case.toml",
        )

    def test_rejects_path_escape_and_all_mixed(self) -> None:
        traversal = slash.parse_case_args("tests/perf/query_cases/../secret.toml")
        self.assertIn("..", traversal.error)
        mixed = slash.parse_case_args("all heavy")
        self.assertIn("cannot be mixed", mixed.error)


class ParseCommandTest(unittest.TestCase):
    def test_bare_command_selects_all(self) -> None:
        parsed = slash.parse_command("/query-regression")
        self.assertTrue(parsed.matched)
        self.assertEqual(parsed.case, "all")
        self.assertEqual(parsed.error, "")

    def test_heavy_and_explicit_paths(self) -> None:
        heavy = slash.parse_command("/query-regression heavy")
        self.assertEqual(heavy.case, "heavy")
        paths = slash.parse_command(
            "/query-regression tests/perf/query_cases/sql_topk_order_by/case.toml"
        )
        self.assertEqual(paths.case, "tests/perf/query_cases/sql_topk_order_by/case.toml")

    def test_ignores_body_after_first_line(self) -> None:
        parsed = slash.parse_command("/query-regression heavy\nplease run this")
        self.assertEqual(parsed.case, "heavy")

    def test_quoted_or_unrelated_comments_do_not_match(self) -> None:
        self.assertFalse(slash.parse_command("> /query-regression").matched)
        self.assertFalse(slash.parse_command("please /query-regression").matched)
        self.assertFalse(slash.parse_command("/query-regression-extra").matched)


class CommentIdentityTest(unittest.TestCase):
    def comment(self, **overrides: object) -> dict:
        payload = {
            "user": {"login": "maintainer"},
            "body": "/query-regression heavy",
            "issue_url": "https://api.github.com/repos/GreptimeTeam/greptimedb/issues/42",
        }
        payload.update(overrides)
        return payload

    def test_reads_actor_pr_and_case_from_comment(self) -> None:
        identity = slash.identity_from_comment(
            self.comment(),
            "GreptimeTeam/greptimedb",
        )
        self.assertEqual(identity.error, "")
        self.assertEqual(identity.actor, "maintainer")
        self.assertEqual(identity.pr_number, 42)
        self.assertEqual(identity.command.case, "heavy")

    def test_rejects_wrong_repo_and_non_command_body(self) -> None:
        other_repo = slash.identity_from_comment(
            self.comment(),
            "other/repo",
        )
        self.assertIn("not other/repo", other_repo.error)
        not_command = slash.identity_from_comment(
            self.comment(body="please run this"),
            "GreptimeTeam/greptimedb",
        )
        self.assertIn("not a query-regression command", not_command.error)

    def test_payload_mismatch_is_rejected(self) -> None:
        identity = slash.identity_from_comment(
            self.comment(),
            "GreptimeTeam/greptimedb",
        )
        self.assertEqual(
            slash.payload_matches_comment(
                identity,
                actor="stranger",
                pr_number="42",
                command_args="heavy",
            ),
            "payload actor does not match comment author",
        )
        self.assertEqual(
            slash.payload_matches_comment(
                identity,
                actor="maintainer",
                pr_number="99",
                command_args="heavy",
            ),
            "payload PR number does not match comment",
        )
        self.assertEqual(
            slash.payload_matches_comment(
                identity,
                actor="maintainer",
                pr_number="42",
                command_args="all",
            ),
            "payload command args do not match comment",
        )
        self.assertEqual(
            slash.payload_matches_comment(
                identity,
                actor="maintainer",
                pr_number="42",
                command_args="heavy",
            ),
            "",
        )
        self.assertEqual(
            slash.payload_matches_comment(
                identity,
                actor="maintainer",
                pr_number="abc",
                command_args="heavy",
            ),
            "payload PR number is not a valid integer",
        )


class DispatchTrustTest(unittest.TestCase):
    def test_accepts_github_actions_bot_sender(self) -> None:
        self.assertEqual(slash.dispatch_sender_ok("github-actions[bot]"), "")
        self.assertEqual(slash.dispatch_sender_ok("GitHub-Actions[bot]"), "")

    def test_rejects_non_actions_sender(self) -> None:
        self.assertEqual(
            slash.dispatch_sender_ok("alice"),
            "repository_dispatch sender is not github-actions[bot]",
        )
        self.assertEqual(
            slash.dispatch_sender_ok(""),
            "repository_dispatch sender is not github-actions[bot]",
        )

    def test_main_rejects_non_actions_sender_without_fetching(self) -> None:
        def boom(*_args: object, **_kwargs: object) -> dict:
            raise AssertionError("should not fetch when sender is untrusted")

        with patch.object(slash, "fetch_comment", boom):
            self.assertEqual(
                slash.main(
                    [
                        "--repo",
                        "o/r",
                        "--token",
                        "t",
                        "--comment-id",
                        "1",
                        "--dispatch-sender",
                        "alice",
                    ]
                ),
                0,
            )

    def test_head_must_match_dispatcher_snapshot(self) -> None:
        pull = pull_payload()
        self.assertEqual(slash.dispatch_head_matches(pull, HEAD), "")
        self.assertEqual(slash.dispatch_head_matches(pull, HEAD.upper()), "")
        self.assertIn("changed", slash.dispatch_head_matches(pull, BASE))
        self.assertIn("missing", slash.dispatch_head_matches(pull, "not-a-sha"))
        self.assertIn("missing", slash.dispatch_head_matches(pull, ""))

    def test_api_failure_writes_retry_reply_and_stays_red(self) -> None:
        def boom(*_args: object, **_kwargs: object) -> dict:
            raise SystemExit(
                "GitHub API GET /repos/o/r/issues/comments/1 failed: HTTP 502: no"
            )

        with patch.object(slash, "fetch_comment", boom):
            code = slash.main(
                [
                    "--repo",
                    "o/r",
                    "--token",
                    "t",
                    "--comment-id",
                    "1",
                    "--dispatch-sender",
                    "github-actions[bot]",
                    "--pr-number",
                    "42",
                ]
            )
        self.assertEqual(code, 1)


class ParseAllowlistTest(unittest.TestCase):
    def test_splits_commas_whitespace_and_strips_at(self) -> None:
        self.assertEqual(
            slash.parse_allowlist("@Ada, bob\nCarol"),
            frozenset({"ada", "bob", "carol"}),
        )


class ParseGithubIdTest(unittest.TestCase):
    def test_accepts_positive_integers(self) -> None:
        self.assertEqual(slash.parse_github_id("42"), 42)
        self.assertEqual(slash.parse_github_id(" 7 "), 7)

    def test_rejects_non_numeric_and_non_positive(self) -> None:
        self.assertIsNone(slash.parse_github_id("abc"))
        self.assertIsNone(slash.parse_github_id(""))
        self.assertIsNone(slash.parse_github_id("0"))
        self.assertIsNone(slash.parse_github_id("-1"))

    def test_non_numeric_comment_id_fails_closed(self) -> None:
        self.assertEqual(
            slash.main(["--repo", "o/r", "--token", "t", "--comment-id", "abc"]),
            2,
        )


class AdmitPullTest(unittest.TestCase):
    def test_allowlisted_admin_admits_merge_sha(self) -> None:
        decision = admit()
        self.assertFalse(decision.skip)
        self.assertEqual(decision.case, "all")
        self.assertEqual(decision.candidate_sha, MERGE)
        self.assertEqual(decision.head_sha, HEAD)
        self.assertEqual(decision.base_sha, BASE)
        self.assertEqual(decision.pr_number, "42")
        self.assertEqual(decision.head_repo, "alice/greptimedb")
        self.assertEqual(decision.reply, "")

    def test_unknown_commenter_is_denied(self) -> None:
        decision = admit(actor="stranger")
        self.assertTrue(decision.skip)
        self.assertIn("ALLOWLIST", decision.reason)
        self.assertEqual(decision.pr_number, "42")

    def test_allowlisted_non_admin_is_denied(self) -> None:
        decision = slash.admit_pull(
            pull_payload(),
            actor="maintainer",
            allowlist=slash.parse_allowlist("maintainer"),
            permission="write",
            command=slash.parse_command("/query-regression"),
            expected_repo="GreptimeTeam/greptimedb",
            pr_number="42",
        )
        self.assertTrue(decision.skip)
        self.assertIn("admin", decision.reason)
        self.assertEqual(decision.pr_number, "42")

    def test_empty_allowlist_fails_closed(self) -> None:
        decision = slash.admit_pull(
            pull_payload(),
            actor="maintainer",
            allowlist=frozenset(),
            permission="admin",
            command=slash.parse_command("/query-regression"),
            expected_repo="GreptimeTeam/greptimedb",
            pr_number="42",
        )
        self.assertTrue(decision.skip)
        self.assertIn("ALLOWLIST", decision.reason)

    def test_draft_and_conflicted_prs_are_not_admitted(self) -> None:
        draft = admit(draft=True)
        self.assertTrue(draft.skip)
        conflicted = admit(mergeable=False)
        self.assertTrue(conflicted.skip)
        pending = admit(mergeable=None)
        self.assertTrue(pending.skip)

    def test_missing_merge_sha_fails_closed(self) -> None:
        decision = admit(merge_commit_sha="not-a-sha")
        self.assertTrue(decision.skip)

    def test_deleted_fork_head_repo_fails_closed(self) -> None:
        decision = admit(head={"sha": HEAD, "repo": None})
        self.assertTrue(decision.skip)
        self.assertIn("head repository", decision.reason)
        self.assertEqual(decision.pr_number, "42")


class AdmissionMarkerTest(unittest.TestCase):
    def identity(self) -> dict:
        return {
            "run_id": 123,
            "pr_number": 42,
            "head_sha": HEAD,
            "head_repo": "alice/greptimedb",
            "base_repo": "GreptimeTeam/greptimedb",
            "candidate_sha": MERGE,
            "base_sha": BASE,
        }

    def test_sign_and_verify_round_trip(self) -> None:
        identity = self.identity()
        mac = slash.sign_admission("secret", identity)
        self.assertTrue(slash.verify_admission_mac("secret", identity, mac))
        self.assertFalse(slash.verify_admission_mac("other", identity, mac))
        tampered = {**identity, "pr_number": 99}
        self.assertFalse(slash.verify_admission_mac("secret", tampered, mac))

    def test_parse_ignores_surrounding_text(self) -> None:
        identity = {**self.identity(), "mac": slash.sign_admission("secret", self.identity())}
        parsed = slash.parse_admission_marker(f"noise\n{slash.format_admission_marker(identity)}trailing")
        assert parsed is not None
        self.assertEqual(parsed["pr_number"], 42)
        self.assertTrue(slash.verify_admission_mac("secret", parsed, parsed["mac"]))

    def test_missing_hmac_secret_fails_closed(self) -> None:
        self.assertEqual(
            slash.persist_admission_identity(
                admit(),
                token="t",
                api_url="https://api.github.com",
                repo="GreptimeTeam/greptimedb",
                secret="",
            ),
            "QUERY_REGRESSION_ADMISSION_HMAC is unset",
        )

    def test_persist_posts_signed_marker(self) -> None:
        posted: dict[str, object] = {}

        def fake_request(
            token: str,
            api_url: str,
            path: str,
            *,
            method: str = "GET",
            payload: dict | None = None,
        ) -> dict:
            posted["method"] = method
            posted["path"] = path
            posted["payload"] = payload
            return {"id": 7}

        decision = admit()
        with tempfile.TemporaryDirectory() as tmp:
            cwd = os.getcwd()
            os.chdir(tmp)
            try:
                with patch.dict(os.environ, {"GITHUB_RUN_ID": "99", "GITHUB_RUN_ATTEMPT": "1"}):
                    with patch.object(slash, "github_request", fake_request):
                        error = slash.persist_admission_identity(
                            decision,
                            token="t",
                            api_url="https://api.github.com",
                            repo="GreptimeTeam/greptimedb",
                            secret="secret",
                        )
                self.assertEqual(error, "")
                self.assertEqual(posted["method"], "POST")
                self.assertEqual(
                    posted["path"],
                    "/repos/GreptimeTeam/greptimedb/issues/42/comments",
                )
                body = (posted["payload"] or {})["body"]  # type: ignore[index]
                parsed = slash.parse_admission_marker(str(body))
                assert parsed is not None
                self.assertEqual(parsed["run_id"], 99)
                self.assertTrue(slash.verify_admission_mac("secret", parsed, parsed["mac"]))
            finally:
                os.chdir(cwd)


if __name__ == "__main__":
    unittest.main()
