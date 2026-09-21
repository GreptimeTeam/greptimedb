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

import importlib.util
import os
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

SCRIPT = Path(__file__).parents[1] / "ci-slash.py"
spec = importlib.util.spec_from_file_location("ci_slash", SCRIPT)
assert spec and spec.loader
ci = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ci)


class CiSlashTest(unittest.TestCase):
    def setUp(self):
        self.output = tempfile.NamedTemporaryFile(delete=False)
        self.output.close()
        self.env = {
            "GITHUB_OUTPUT": self.output.name,
            "GITHUB_REPOSITORY": "GreptimeTeam/greptimedb",
            "GITHUB_API_URL": "https://api.github.test",
            "GITHUB_TOKEN": "token",
            "COMMENT_ID": "1",
            "DISPATCH_SENDER": "github-actions[bot]",
            "DISPATCH_HEAD_SHA": "a" * 40,
        }

    def tearDown(self):
        os.unlink(self.output.name)

    def run_main(self, responses):
        with patch.dict(os.environ, self.env, clear=False), patch.object(ci, "api", side_effect=responses):
            self.assertEqual(0, ci.main())
        return Path(self.output.name).read_text()

    def test_help_needs_no_pr_lookup(self):
        output = self.run_main([{"body": "/ci help", "issue_url": "https://api.github.test/repos/GreptimeTeam/greptimedb/issues/42"}])
        self.assertIn("skip=true", output)
        self.assertIn("Available draft-PR CI commands", output)

    def test_dispatches_full_suite_with_top_level_admin_permission(self):
        output = self.run_main([
            {"body": "/ci", "issue_url": "https://api.github.test/repos/GreptimeTeam/greptimedb/issues/42", "user": {"login": "admin"}},
            {"state": "open", "draft": True, "head": {"sha": "a" * 40, "repo": {"full_name": "GreptimeTeam/greptimedb"}}},
            {"permission": "admin", "user": {"login": "admin"}},
        ])
        self.assertIn("skip=false", output)
        self.assertIn("workflow=rust.yml,integration.yml,checks.yml,docs.yml", output)

    def test_rejects_non_draft_before_permission_lookup(self):
        output = self.run_main([
            {"body": "/ci fuzz chaos", "issue_url": "https://api.github.test/repos/GreptimeTeam/greptimedb/issues/42", "user": {"login": "admin"}},
            {"state": "open", "draft": False, "head": {"sha": "a" * 40, "repo": {"full_name": "GreptimeTeam/greptimedb"}}},
        ])
        self.assertIn("skip=true", output)
        self.assertIn("PR must be open and draft", output)


if __name__ == "__main__":
    unittest.main()
