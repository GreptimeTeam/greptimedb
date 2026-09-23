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
import json
import os
import tempfile
import subprocess
import textwrap
import unittest
from pathlib import Path
from unittest.mock import patch

SCRIPT = Path(__file__).parents[1] / "ci-slash.py"
spec = importlib.util.spec_from_file_location("ci_slash", SCRIPT)
assert spec and spec.loader
ci = importlib.util.module_from_spec(spec)
spec.loader.exec_module(ci)


OPTIONS_TO_TEST = ["/ci", "/ci rust", "/ci fuzz chaos", "/ci fuzz all"]


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
            {"state": "open", "draft": True, "head": {"sha": "a" * 40, "ref": "fix/draft-ci", "repo": {"full_name": "GreptimeTeam/greptimedb"}}},
            {"permission": "admin", "user": {"login": "admin"}},
        ])
        self.assertIn("skip=false", output)
        self.assertIn("head_ref=fix/draft-ci", output)
        self.assertIn("workflow=rust.yml,integration.yml,checks.yml,docs.yml", output)

    def test_rejects_non_draft_before_permission_lookup(self):
        output = self.run_main([
            {"body": "/ci fuzz chaos", "issue_url": "https://api.github.test/repos/GreptimeTeam/greptimedb/issues/42", "user": {"login": "admin"}},
            {"state": "open", "draft": False, "head": {"sha": "a" * 40, "ref": "fix/draft-ci", "repo": {"full_name": "GreptimeTeam/greptimedb"}}},
        ])
        self.assertIn("skip=true", output)
        self.assertIn("PR must be open and draft", output)

    def test_rejects_missing_head_ref_before_permission_lookup(self):
        output = self.run_main([
            {"body": "/ci", "issue_url": "https://api.github.test/repos/GreptimeTeam/greptimedb/issues/42", "user": {"login": "admin"}},
            {"state": "open", "draft": True, "head": {"sha": "a" * 40, "repo": {"full_name": "GreptimeTeam/greptimedb"}}},
        ])
        self.assertIn("skip=true", output)
        self.assertIn("PR head changed; comment again.", output)

    def test_permission_matrix(self):
        for command in OPTIONS_TO_TEST:
            for author in (True, False):
                for permission in ("admin", "maintain", "write", "triage", "read", "none", None):
                    with self.subTest(command=command, author=author, permission=permission):
                        Path(self.output.name).write_text("")
                        responses = [
                            {"body": command, "issue_url": "https://api.github.test/repos/GreptimeTeam/greptimedb/issues/42", "user": {"login": "actor"}},
                            {"state": "open", "draft": True, "user": {"login": "actor" if author else "other"}, "head": {"sha": "a" * 40, "ref": "fix/draft-ci", "repo": {"full_name": "GreptimeTeam/greptimedb"}}},
                        ]
                        if command == "/ci fuzz all" or not author:
                            responses.append({"permission": permission})
                        output = self.run_main(responses)
                        allowed = permission == "admin" if command == "/ci fuzz all" else author or permission in ("admin", "maintain", "write")
                        self.assertIn("skip=false" if allowed else "skip=true", output)

    def test_reply_uses_issue_comment_endpoint(self):
        workflow = SCRIPT.parents[1] / "workflows" / "ci-slash.yml"
        step = workflow.read_text().split("      - name: Reply with command result\n", 1)[1]
        command = textwrap.dedent(step.split("        run: |\n", 1)[1])
        reply = "Denied: `example`\nSecond line with $variables and 'quotes'"
        with tempfile.TemporaryDirectory() as directory:
            gh = Path(directory) / "gh"
            gh.write_text("#!/usr/bin/env python3\nimport json, sys\nprint(json.dumps(sys.argv[1:]))\n")
            gh.chmod(0o755)
            env = {**os.environ, "PATH": directory + os.pathsep + os.environ["PATH"], "GITHUB_REPOSITORY": "GreptimeTeam/greptimedb", "PR_NUMBER": "42", "REPLY": reply}
            result = subprocess.run(["bash", "-eu", "-c", command], env=env, check=True, capture_output=True, text=True)
        self.assertEqual(json.loads(result.stdout), ["api", "--method", "POST", "/repos/GreptimeTeam/greptimedb/issues/42/comments", "-f", "body=" + reply])

    def test_author_cannot_bypass_pr_guards(self):
        for change, reason in [
            ({"state": "closed"}, "PR must be open and draft"),
            ({"draft": False}, "PR must be open and draft"),
            ({"head": {"repo": {"full_name": "actor/greptimedb"}}}, "fork PRs are not admitted"),
            ({"head": {"sha": "b" * 40, "ref": "fix/draft-ci", "repo": {"full_name": "GreptimeTeam/greptimedb"}}}, "PR head changed"),
        ]:
            with self.subTest(change=change):
                Path(self.output.name).write_text("")
                pr = {"state": "open", "draft": True, "user": {"login": "actor"}, "head": {"sha": "a" * 40, "ref": "fix/draft-ci", "repo": {"full_name": "GreptimeTeam/greptimedb"}}}
                pr.update(change)
                output = self.run_main([
                    {"body": "/ci rust", "issue_url": "https://api.github.test/repos/GreptimeTeam/greptimedb/issues/42", "user": {"login": "actor"}},
                    pr,
                ])
                self.assertIn("skip=true", output)
                self.assertIn(reason, output)


if __name__ == "__main__":
    unittest.main()
