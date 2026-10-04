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
            env = {**os.environ, "PATH": directory + os.pathsep + os.environ["PATH"], "GITHUB_REPOSITORY": "GreptimeTeam/greptimedb", "PR_NUMBER": "42", "REPLY": reply, "ADMIT_OUTCOME": "success", "DISPATCH_OUTCOME": "success"}
            result = subprocess.run(["bash", "-eu", "-c", command], env=env, check=True, capture_output=True, text=True)
        self.assertEqual(json.loads(result.stdout), ["api", "--method", "POST", "/repos/GreptimeTeam/greptimedb/issues/42/comments", "-f", "body=" + reply])

    def test_failure_reply_and_partial_dispatch(self):
        workflow = (SCRIPT.parents[1] / "workflows" / "ci-slash.yml").read_text()
        dispatch = textwrap.dedent(workflow.split("      - name: Dispatch selected CI workflow\n", 1)[1].split("        run: |\n", 1)[1].split("      - name: Reply", 1)[0])
        reply = textwrap.dedent(workflow.split("      - name: Reply with command result\n", 1)[1].split("        run: |\n", 1)[1])
        with tempfile.TemporaryDirectory() as directory:
            gh = Path(directory) / "gh"
            log = Path(directory) / "calls"
            gh.write_text("#!/usr/bin/env python3\nimport json, os, sys\nwith open(os.environ['CALLS'], 'a') as f: f.write(json.dumps(sys.argv[1:]) + '\\n')\nsys.exit(1 if '/runs/8/rerun' in sys.argv[-1] else 0)\n")
            gh.chmod(0o755)
            env = {**os.environ, "PATH": directory + os.pathsep + os.environ["PATH"], "CALLS": str(log), "GITHUB_REPOSITORY": "GreptimeTeam/greptimedb", "RUN_IDS": "7,8,9", "PR_NUMBER": "42", "REPLY": "Success", "ADMIT_OUTCOME": "success", "DISPATCH_OUTCOME": "failure", "GITHUB_SERVER_URL": "https://github.com", "GITHUB_RUN_ID": "123"}
            result = subprocess.run(["bash", "-eu", "-c", dispatch], env=env, capture_output=True)
            self.assertNotEqual(result.returncode, 0)
            subprocess.run(["bash", "-eu", "-c", reply], env=env, check=True, capture_output=True)
            calls = [json.loads(line) for line in log.read_text().splitlines()]
        self.assertEqual([call[-1] for call in calls[:2]], ["/repos/GreptimeTeam/greptimedb/actions/runs/7/rerun", "/repos/GreptimeTeam/greptimedb/actions/runs/8/rerun"])
        self.assertIn("CI trigger failed; some workflows may already have been requested", calls[2][-1])
        self.assertIn("/actions/runs/123", calls[2][-1])
        self.assertIn("!cancelled()", workflow.split("      - name: Reply with command result", 1)[1])

    def test_fork_rerun_gates_cover_standard_jobs(self):
        import re
        for name, count in [("rust", 7), ("integration", 5), ("checks", 5), ("docs", 3)]:
            workflow = (SCRIPT.parents[1] / "workflows" / (name + ".yml")).read_text()
            gates = re.findall(r"^    if:.*github.run_attempt > 1.*$", workflow, re.MULTILINE)
            self.assertEqual(len(gates), count, name)
            self.assertTrue(all("always()" not in gate for gate in gates))

    def test_command_permissions(self):
        workflow = SCRIPT.parents[1] / "workflows" / "slash-command-dispatch.yml"
        config = workflow.read_text().split("config: >-", 1)[1].split("      - name:", 1)[0]
        self.assertEqual(json.loads(config), [
            {"command": "query-regression", "permission": "admin", "issue_type": "pull-request"},
            {"command": "ci", "permission": "none", "issue_type": "pull-request"},
        ])

    def test_fork_requires_writer_even_for_author(self):
        for permission in ("read", "write", "maintain", "admin"):
            with self.subTest(permission=permission):
                Path(self.output.name).write_text("")
                responses = [
                    {"body": "/ci", "issue_url": "https://api.github.test/issues/42", "user": {"login": "actor"}},
                    {"state": "open", "draft": True, "user": {"login": "actor"}, "head": {"sha": "a" * 40, "ref": "fork-branch", "repo": {"full_name": "actor/greptimedb"}}},
                    {"permission": permission},
                ]
                if permission != "read":
                    responses.append({"workflow_runs": [
                        {"id": 7, "path": ".github/workflows/rust.yml", "head_sha": "a" * 40, "head_branch": "fork-branch", "head_repository": {"full_name": "actor/greptimedb"}, "status": "completed", "conclusion": "skipped"},
                        {"id": 8, "path": ".github/workflows/checks.yml", "head_sha": "a" * 40, "head_branch": "fork-branch", "head_repository": {"full_name": "someone/greptimedb"}, "status": "completed", "conclusion": "skipped"},
                    ]})
                output = self.run_main(responses)
                if permission == "read":
                    self.assertIn("skip=true", output)
                else:
                    self.assertIn("run_ids=7\n", output)
                    self.assertNotIn("head_ref=", output)

    def test_fork_rejects_non_draft_original_run(self):
        output = self.run_main([
            {"body": "/ci rust", "issue_url": "https://api.github.test/issues/42", "user": {"login": "writer"}},
            {"state": "open", "draft": True, "head": {"sha": "a" * 40, "ref": "fork-branch", "repo": {"full_name": "actor/greptimedb"}}},
            {"permission": "write"},
            {"workflow_runs": [{"id": 7, "path": ".github/workflows/rust.yml@main", "head_sha": "a" * 40, "head_branch": "fork-branch", "head_repository": {"full_name": "actor/greptimedb"}, "status": "completed", "conclusion": "success", "run_attempt": 2}]},
            {"conclusion": "success"},
        ])
        self.assertIn("only originally skipped draft CI runs", output)
        self.assertIn("skip=true", output)

    def test_author_cannot_bypass_pr_guards(self):
        for change, reason in [
            ({"state": "closed"}, "PR must be open and draft"),
            ({"draft": False}, "PR must be open and draft"),
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
