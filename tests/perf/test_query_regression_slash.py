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
import sys
import unittest
from pathlib import Path


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
    return slash.admit_pull(
        pull_payload(**pull_overrides),
        actor=actor,
        allowlist=slash.parse_allowlist("maintainer, other-admin"),
        permission="admin",
        command=slash.parse_command(body),
        expected_repo="GreptimeTeam/greptimedb",
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


class ParseAllowlistTest(unittest.TestCase):
    def test_splits_commas_whitespace_and_strips_at(self) -> None:
        self.assertEqual(
            slash.parse_allowlist("@Ada, bob\nCarol"),
            frozenset({"ada", "bob", "carol"}),
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

    def test_allowlisted_non_admin_is_denied(self) -> None:
        decision = slash.admit_pull(
            pull_payload(),
            actor="maintainer",
            allowlist=slash.parse_allowlist("maintainer"),
            permission="write",
            command=slash.parse_command("/query-regression"),
            expected_repo="GreptimeTeam/greptimedb",
        )
        self.assertTrue(decision.skip)
        self.assertIn("admin", decision.reason)

    def test_empty_allowlist_fails_closed(self) -> None:
        decision = slash.admit_pull(
            pull_payload(),
            actor="maintainer",
            allowlist=frozenset(),
            permission="admin",
            command=slash.parse_command("/query-regression"),
            expected_repo="GreptimeTeam/greptimedb",
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


if __name__ == "__main__":
    unittest.main()
