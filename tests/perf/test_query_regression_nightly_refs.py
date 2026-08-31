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

"""Unit tests for consecutive Nightly Build SHA selection."""

import importlib.util
import sys
import unittest
from pathlib import Path


SCRIPTS_DIR = Path(__file__).parents[2] / ".github/scripts"


def load_module():
    spec = importlib.util.spec_from_file_location(
        "query_regression_nightly_refs_under_test",
        SCRIPTS_DIR / "query-regression-nightly-refs.py",
    )
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    sys.modules[spec.name] = module
    spec.loader.exec_module(module)
    return module


refs = load_module()


def run(*, run_id: int, sha: str, branch: str = "main", created_at: str = "2026-08-24T00:00:00Z"):
    return refs.WorkflowRun(
        id=run_id,
        head_sha=sha,
        head_branch=branch,
        html_url=f"https://github.com/example/run/{run_id}",
        created_at=created_at,
        conclusion="success",
        event="schedule",
    )


class SelectNightlyRefsTest(unittest.TestCase):
    def test_picks_newest_and_previous_on_same_branch(self) -> None:
        pair = refs.select_base_and_candidate(
            [
                run(run_id=3, sha="ccc", created_at="2026-08-22T00:00:00Z"),
                run(run_id=2, sha="bbb", created_at="2026-08-21T00:00:00Z"),
                run(run_id=1, sha="aaa", created_at="2026-08-20T00:00:00Z"),
            ]
        )
        self.assertFalse(pair.skip)
        assert pair.candidate is not None and pair.base is not None
        self.assertEqual(pair.candidate.head_sha, "ccc")
        self.assertEqual(pair.base.head_sha, "bbb")

    def test_candidate_run_id_uses_that_run_and_the_next_older(self) -> None:
        pair = refs.select_base_and_candidate(
            [
                run(run_id=3, sha="ccc", created_at="2026-08-22T00:00:00Z"),
                run(run_id=2, sha="bbb", created_at="2026-08-21T00:00:00Z"),
                run(run_id=1, sha="aaa", created_at="2026-08-20T00:00:00Z"),
            ],
            candidate_run_id=2,
        )
        self.assertFalse(pair.skip)
        assert pair.candidate is not None and pair.base is not None
        self.assertEqual(pair.candidate.id, 2)
        self.assertEqual(pair.base.id, 1)

    def test_skips_when_consecutive_nightlies_share_a_sha(self) -> None:
        pair = refs.select_base_and_candidate(
            [
                run(run_id=2, sha="same", created_at="2026-08-22T00:00:00Z"),
                run(run_id=1, sha="same", created_at="2026-08-21T00:00:00Z"),
            ]
        )
        self.assertTrue(pair.skip)
        self.assertIn("matches candidate", pair.reason)

    def test_skips_other_branches_when_picking_previous(self) -> None:
        pair = refs.select_base_and_candidate(
            [
                run(run_id=3, sha="ccc", branch="main", created_at="2026-08-22T00:00:00Z"),
                run(run_id=2, sha="other", branch="feat", created_at="2026-08-21T12:00:00Z"),
                run(run_id=1, sha="aaa", branch="main", created_at="2026-08-21T00:00:00Z"),
            ]
        )
        self.assertFalse(pair.skip)
        assert pair.base is not None
        self.assertEqual(pair.base.head_sha, "aaa")

    def test_skips_when_only_one_nightly_exists(self) -> None:
        pair = refs.select_base_and_candidate(
            [run(run_id=1, sha="aaa")],
        )
        self.assertTrue(pair.skip)
        self.assertIn("no previous", pair.reason)

    def test_explicit_refs_bypass_github(self) -> None:
        pair = refs.override_pair("base-sha", "cand-sha")
        self.assertFalse(pair.skip)
        assert pair.base is not None and pair.candidate is not None
        self.assertEqual(pair.base.head_sha, "base-sha")
        self.assertEqual(pair.candidate.head_sha, "cand-sha")


if __name__ == "__main__":
    unittest.main()
