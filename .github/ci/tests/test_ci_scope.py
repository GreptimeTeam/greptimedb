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

"""Offline tests for the integration CI admission gate."""

import contextlib
import fnmatch
import io
import json
import os
import re
import pathlib
import sys
import tempfile
import tomllib
import unittest
from urllib.parse import parse_qs, urlparse

GATE_DIR = pathlib.Path(__file__).resolve().parents[1]
WORKFLOWS = GATE_DIR.parent / "workflows"
REPO_ROOT = GATE_DIR.parents[1]
sys.path.insert(0, str(GATE_DIR))

import ci_scope  # noqa: E402
import ci_scope_rules  # noqa: E402

SCOPE_CASES = pathlib.Path(__file__).resolve().parent / "fixtures" / "scope-cases.toml"
from ci_scope_rules import (  # noqa: E402
    ALWAYS_RUN,
    PATH_GROUPS,
    SUITES,
    suite_paths,
)

def file_entry(name, patch="@@ -1 +1 @@\n- old\n+ new\n", status="modified"):
    entry = {
        "filename": name,
        "status": status,
        "additions": 1,
        "deletions": 1,
    }
    if patch is not None:
        entry["patch"] = patch
    return entry


class FakeHTTP:
    """Records requests and serves canned GitHub API responses."""

    def __init__(self):
        self.calls = []
        self.file_pages = {1: []}
        self.labels = []
        self.pull_status = 200
        self.draft = False
        self.head_sha = ""
        self.base_sha = ""
        self.file_queries = []

    def __call__(self, method, url, headers, body=None, timeout=None):
        parsed = urlparse(url)
        query = parse_qs(parsed.query)
        self.calls.append((method, parsed.path, query.get("page", ["1"])[0]))
        if parsed.path.endswith("/files"):
            self.file_queries.append(dict(query))
            page = int(query.get("page", ["1"])[0])
            per_page = int(query.get("per_page", ["100"])[0])
            items = []
            for pageno in sorted(self.file_pages):
                canned = self.file_pages[pageno]
                if isinstance(canned, int):
                    # An int stands for an HTTP error status for that page.
                    if pageno == page:
                        return canned, b"{}"
                    continue
                items.extend(canned)
            # `file_pages` is stored at per_page=100; honour the requested
            # window so a probe at per_page=1 cannot read past item 1.
            start = (page - 1) * per_page
            return 200, json.dumps(items[start : start + per_page]).encode()
        if parsed.path.endswith("/pulls/1"):
            if self.pull_status != 200:
                return self.pull_status, b"{}"
            return 200, json.dumps(
                {
                    "labels": [{"name": name} for name in self.labels],
                    "head": {"sha": self.head_sha},
                    "base": {"sha": self.base_sha},
                }
            ).encode()
        return 404, b"{}"


class ScopeTestCase(unittest.TestCase):
    def setUp(self):
        self.fake = FakeHTTP()
        self._original = ci_scope._request
        ci_scope._request = self.fake
        self._environ = dict(os.environ)
        for key in ["CI_SCOPE_FORCE_ALL"]:
            os.environ.pop(key, None)
        os.environ["GITHUB_API_URL"] = "https://api.github.com"

    def tearDown(self):
        ci_scope._request = self._original
        os.environ.clear()
        os.environ.update(self._environ)

    def plan(self, files, pages=None, event="pull_request", extra_args=(), pr=1):
        if pages is None:
            pages = {1: files}
        self.fake.file_pages = dict(pages)
        args = ci_scope.parse_args(
            ["--repo", "GreptimeTeam/greptimedb", "--pr", str(pr), "--event", event]
            + list(extra_args)
        )
        return ci_scope.build_plan(args, "token")

    def assert_all_run(self, plan):
        for suite_id, entry in plan["suites"].items():
            self.assertTrue(entry["run"], suite_id)


class GlobTests(unittest.TestCase):
    def test_double_star_crosses_directories(self):
        regex = ci_scope.glob_to_regex("src/**/*.rs")
        self.assertTrue(regex.match("src/a.rs"))
        self.assertTrue(regex.match("src/a/b/c.rs"))
        self.assertFalse(regex.match("tests/a.rs"))

    def test_single_star_stays_in_one_segment(self):
        regex = ci_scope.glob_to_regex("src/*.rs")
        self.assertTrue(regex.match("src/main.rs"))
        self.assertFalse(regex.match("src/cmd/main.rs"))
        self.assertFalse(regex.match("src/main.toml"))

    def test_question_mark_matches_one_character(self):
        regex = ci_scope.glob_to_regex("a?.rs")
        self.assertTrue(regex.match("ab.rs"))
        self.assertFalse(regex.match("abc.rs"))
        self.assertFalse(regex.match("a/.rs"))

    def test_suffix_glob(self):
        regex = ci_scope.glob_to_regex(".cargo/**")
        self.assertTrue(regex.match(".cargo/config.toml"))
        self.assertFalse(regex.match("cargo/config.toml"))

    def test_suite_groups_expand(self):
        for group in PATH_GROUPS:
            self.assertTrue(PATH_GROUPS[group])
            self.assertTrue(any(
                entry == "@" + group
                for suite in SUITES.values()
                for entry in suite["paths"]
            ), group)
        paths = suite_paths(SUITES["fuzz-distributed/repartition"])
        self.assertIn("src/mito2/**", paths)
        self.assertIn("src/meta-srv/**", paths)
        self.assertNotIn("@server", paths)

    def test_every_gate_routes_to_an_output(self):
        # Ask the producer, not a copy of its key list: a new gate must fail
        # here until `derive_outputs` routes it to a gate key.
        plan = {
            "mode": "static",
            "full": True,
            "notes": [],
            "suites": {
                suite_id: {"run": True, "source": "static", "reason": ""}
                for suite_id in SUITES
            },
        }
        outputs, _ = ci_scope.derive_outputs(plan)
        gates = {suite["gate"] for suite in SUITES.values()}
        self.assertLessEqual(gates, set(outputs))

    def test_suite_metadata(self):
        for suite_id, suite in SUITES.items():
            self.assertEqual(suite_id, suite_id.strip())
            self.assertTrue(suite["description"])
            self.assertEqual(suite_id.split("/")[0].replace("-", "_"), suite["gate"])
            if suite["gate"] in ci_scope.FUZZ_GATES:
                self.assertEqual(ci_scope.suite_group(suite_id), suite_id.split("/", 1)[1])


class ForcedModeTests(ScopeTestCase):
    def test_merge_group_forces_without_http(self):
        plan = self.plan([], event="merge_group")
        self.assertEqual(plan["mode"], "forced")
        self.assert_all_run(plan)
        self.assertEqual(self.fake.calls, [])

    def test_schedule_and_workflow_dispatch_force(self):
        for event in ("schedule", "workflow_dispatch"):
            plan = self.plan([], event=event)
            self.assertEqual(plan["mode"], "forced", event)

    def test_force_all_env_forces(self):
        os.environ["CI_SCOPE_FORCE_ALL"] = "true"
        plan = self.plan([], event="pull_request")
        self.assertEqual(plan["mode"], "forced")

    def test_ci_full_label_forces(self):
        self.fake.labels = ["ci:full"]
        plan = self.plan([file_entry("src/mito2/src/lib.rs")])
        self.assertEqual(plan["mode"], "forced")
        self.assert_all_run(plan)


class UncoveredChangeTests(ScopeTestCase):
    def test_uncovered_crate_runs_everything(self):
        # `src/common/telemetry` is mapped by the server group; a crate added
        # later is not, and must widen the gate.
        plan = self.plan([file_entry("src/common/brand-new/src/lib.rs")])
        self.assertTrue(plan["full"], plan["notes"])
        self.assertEqual(plan["suites"]["export-import-v2"]["source"], "uncovered")
        self.assertIn("uncovered build change", " ".join(plan["notes"]))

    def test_mapped_crate_keeps_the_path_decision(self):
        plan = self.plan([file_entry("src/query/src/lib.rs")])
        self.assertFalse(plan["full"])
        self.assertTrue(plan["suites"]["fuzz-standalone/logical-table"]["run"])
        # `src/query` reaches `COPY DATABASE` and the chaos scenarios too.
        self.assertTrue(plan["suites"]["export-import-v2"]["run"])
        self.assertTrue(plan["suites"]["fuzz-chaos/migration"]["run"])

    def test_exempt_trees_do_not_widen(self):
        plan = self.plan(
            [file_entry("src/promql/src/lib.rs"), file_entry("src/flow/src/lib.rs")]
        )
        self.assertFalse(plan["full"])
        for entry in plan["suites"].values():
            self.assertFalse(entry["run"], entry)

    def test_exempt_tree_is_not_claimed_by_a_broader_glob(self):
        # `@server` claims `src/query/**`; the PromQL module under it must be
        # dropped before matching, and must not count as uncovered either.
        plan = self.plan([file_entry("src/query/src/promql/src/lib.rs")])
        self.assertFalse(plan["full"])
        for entry in plan["suites"].values():
            self.assertFalse(entry["run"], entry)


class StaticDecisionTests(ScopeTestCase):
    def outputs(self, plan):
        return ci_scope.derive_outputs(plan)[0]

    def test_always_run_forces_everything(self):
        plan = self.plan([file_entry("Cargo.lock")])
        self.assertEqual(plan["mode"], "static")
        self.assertTrue(plan["full"])
        self.assert_all_run(plan)

    def test_meta_only_skips_standalone_fuzz(self):
        plan = self.plan([file_entry("src/meta-srv/src/lib.rs")])
        self.assertEqual(plan["mode"], "static")
        outputs, legs = ci_scope.derive_outputs(plan)
        self.assertEqual(outputs["fuzz_standalone"], "false")
        self.assertEqual(outputs["fuzz_unstable"], "false")
        self.assertIn("migration", legs["fuzz_chaos"])
        self.assertIn("logical-pruning", legs["fuzz_distributed"])
        self.assertEqual(outputs["export_import_v2"], "false")
        self.assertEqual(outputs["any_fuzz"], "true")
        self.assertEqual(outputs["build_ci_needed"], "true")

    def test_flow_and_promql_skip_every_gated_suite(self):
        plan = self.plan(
            [file_entry("src/flow/src/lib.rs"), file_entry("src/promql/src/lib.rs")]
        )
        self.assertEqual(plan["mode"], "static")
        for entry in plan["suites"].values():
            self.assertFalse(entry["run"], entry)
        outputs, legs = ci_scope.derive_outputs(plan)
        self.assertEqual(outputs["any_fuzz"], "false")
        self.assertEqual(outputs["build_ci_needed"], "false")
        self.assertEqual(outputs["export_import_v2"], "false")
        for gate in ci_scope.FUZZ_GATES:
            self.assertEqual(outputs[gate], "false")
        self.assertTrue(all(not values for values in legs.values()))

    def test_docs_only_short_circuits(self):
        plan = self.plan([file_entry("docs/readme.md")])
        self.assertEqual(plan["mode"], "static")
        self.assertIn("no meaningful changes", " ".join(plan["notes"]))

    def test_mapped_change_records_the_path_rule_source(self):
        plan = self.plan([file_entry("src/mito2/src/lib.rs")])
        self.assertEqual(plan["mode"], "static")
        self.assertEqual(plan["suites"]["fuzz-standalone/logical-table"]["source"], "static")

    def test_file_cap_truncates(self):
        files = [file_entry("src/mito2/f%d.rs" % index) for index in range(300)]
        extra = [file_entry("src/mito2/overflow.rs")]
        plan = self.plan(files, pages={1: files[:100], 2: files[100:200], 3: files[200:], 4: extra})
        self.assertTrue(plan["full"])
        self.assert_all_run(plan)
        self.assertTrue(all(entry["source"] == "always-run" for entry in plan["suites"].values()))

    def test_exactly_max_files_is_not_truncated(self):
        files = [file_entry("src/promql/f%d.rs" % index) for index in range(300)]
        plan = self.plan([], pages={1: files[:100], 2: files[100:200], 3: files[200:], 4: []})
        self.assertFalse(plan["full"], plan["notes"])
        self.assertNotIn("truncated", " ".join(plan["notes"]))
        self.assertEqual(len(self.fake.file_queries), 4)
        self.assertEqual(
            self.fake.file_queries[-1], {"per_page": ["100"], "page": ["4"]}
        )

    def test_force_push_after_the_run_widens_the_gate(self):
        self.fake.head_sha = "b" * 40
        plan = self.plan(
            [file_entry("src/promql/src/lib.rs")],
            extra_args=("--head-sha", "a" * 40, "--base-sha", "c" * 40),
        )
        self.assertTrue(plan["full"])
        self.assertIn("head moved from aaaaaaaaaaaa", " ".join(plan["notes"]))

    def test_matching_revisions_keep_the_static_verdict(self):
        self.fake.head_sha = "a" * 40
        self.fake.base_sha = "c" * 40
        plan = self.plan(
            [file_entry("src/promql/src/lib.rs")],
            extra_args=("--head-sha", "a" * 40, "--base-sha", "c" * 40),
        )
        self.assertFalse(plan["full"])
        self.assertNotIn("moved", " ".join(plan["notes"]))

    def test_pull_request_is_read_once_per_run(self):
        self.plan([file_entry("src/mito2/src/lib.rs")])
        pulls = [
            path
            for _, path, _ in self.fake.calls
            if path.endswith("/pulls/1")
        ]
        self.assertEqual(len(pulls), 1)

    def test_failed_completeness_probe_fails_instead_of_narrowing(self):
        # Raising leaves the gate outputs empty, which the workflow treats as
        # "run everything"; returning False here would silently classify a
        # truncated file list as complete.
        files = [file_entry("src/mito2/f%d.rs" % index) for index in range(300)]
        with self.assertRaises(ci_scope.ScopeError):
            self.plan(
                files,
                pages={1: files[:100], 2: files[100:200], 3: files[200:], 4: 500},
            )

    def test_file_list_pagination_stops_at_short_page(self):
        files = [file_entry("src/mito2/f%d.rs" % index) for index in range(120)]
        self.plan(files, pages={1: files[:100], 2: files[100:]})
        pages = [
            page for _, path, page in self.fake.calls if path.endswith("/files")
        ]
        self.assertEqual(pages, ["1", "2"])


# Crates no gated suite exercises. Changing one either runs everything through
# the always-run floor, or reaches CI that is not gated here.
UNMAPPED_CRATES = {
    "src/flow": "flow execution is covered by rust.yml and sqlness",
    "src/promql": "PromQL execution is covered by rust.yml and sqlness",
    "tests-fuzz": "in ALWAYS_RUN",
    "tests-integration": "in ALWAYS_RUN",
    "tests/runner": "the sqlness runner; sqlness is not gated",
}


def workspace_crates():
    """Every workspace member directory, so nested crates are not collapsed."""
    data = tomllib.loads((REPO_ROOT / "Cargo.toml").read_text(encoding="utf-8"))
    return sorted(member.rstrip("/") for member in data["workspace"]["members"])


def rules_doc():
    return {
        "matching": {
            "always_run": ["Cargo.toml"],
            "src_tree": "src/",
            "exempt": [{"path": "src/flow/", "reason": "test fixture"}],
        },
        "gates": {"fuzz_x": "fuzztest"},
        "groups": {"server": ["src/a/**"]},
        "suites": {
            "fuzz-x/g": {
                "gate": "fuzz_x",
                "paths": ["@server"],
                "description": "a suite",
            }
        },
    }


class RulesFileTests(unittest.TestCase):
    def test_repository_rules_are_valid(self):
        rules = ci_scope_rules.load()
        self.assertEqual(len(rules["suites"]), 13)

    def test_unknown_group_is_rejected(self):
        doc = rules_doc()
        doc["suites"]["fuzz-x/g"]["paths"] = ["@nope"]
        with self.assertRaises(ci_scope_rules.RulesError):
            ci_scope_rules.validate(doc)

    def test_group_cycle_is_rejected(self):
        doc = rules_doc()
        doc["groups"]["a"] = ["@b"]
        doc["groups"]["b"] = ["@a"]
        with self.assertRaises(ci_scope_rules.RulesError):
            ci_scope_rules.validate(doc)

    def test_gate_must_match_the_suite_prefix(self):
        doc = rules_doc()
        doc["suites"]["fuzz-x/g"]["gate"] = "fuzz_y"
        with self.assertRaises(ci_scope_rules.RulesError):
            ci_scope_rules.validate(doc)

    def test_unknown_key_in_matching_is_ignored_but_missing_is_not(self):
        doc = rules_doc()
        del doc["matching"]["src_tree"]
        with self.assertRaises(ci_scope_rules.RulesError):
            ci_scope_rules.validate(doc)

    def test_validator_entry_point_succeeds_on_the_repository_rules(self):
        out = io.StringIO()
        with contextlib.redirect_stdout(out):
            self.assertEqual(ci_scope_rules.main([]), 0)
        self.assertIn("scope rules ok", out.getvalue())

    def test_malformed_toml_names_the_file(self):
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "rules.toml"
            path.write_text("this is not toml [[[")
            with self.assertRaises(ci_scope_rules.RulesError) as ctx:
                ci_scope_rules.load(path)
            self.assertIn("not valid TOML", str(ctx.exception))


def stub_rules(**overrides):
    """A rules dict in the shape `validate` returns, naming only real paths."""
    rules = {
        "always_run": ["Cargo.toml"],
        "src_tree": "src/",
        "exempt_trees": [],
        "gates": {"fuzz_x": "fuzztest"},
        "groups": {"server": ["Cargo.toml"]},
        "suites": {
            "fuzz-x": {
                "gate": "fuzz_x",
                "paths": ["@server"],
                "description": "a suite",
            }
        },
    }
    rules.update(overrides)
    return rules


class PathCheckTests(unittest.TestCase):
    def test_repository_rules_name_only_existing_paths(self):
        self.assertEqual(ci_scope_rules.missing_paths(), [])

    def test_a_vanished_path_is_reported_with_its_prefix(self):
        doc = stub_rules()
        doc["suites"]["fuzz-x"]["paths"] = ["@server", "src/does-not-exist/**"]
        self.assertEqual(
            ci_scope_rules.missing_paths(doc),
            [("src/does-not-exist/**", "src/does-not-exist/")],
        )

    def test_patterns_without_a_fixed_prefix_are_skipped(self):
        doc = stub_rules(groups={"server": ["*.md"]}, always_run=["*.md"])
        self.assertEqual(ci_scope_rules.missing_paths(doc), [])


    def test_cli_reports_a_missing_path(self):
        text = """\
[matching]
always_run = ["Cargo.toml"]
src_tree = "src/"

[[matching.exempt]]
path = "src/flow/"
reason = "test fixture"

[[matching.exempt]]
path = "src/promql/"
reason = "test fixture"

[gates]
fuzz_x = "fuzztest"

[groups]
server = ["src/does-not-exist/**"]

[suites."fuzz-x"]
gate = "fuzz_x"
paths = ["@server"]
description = "a suite"
"""
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "scope-rules.toml"
            path.write_text(text)
            original = ci_scope_rules.RULES_PATH
            ci_scope_rules.RULES_PATH = path
            err = io.StringIO()
            try:
                with contextlib.redirect_stderr(err):
                    code = ci_scope_rules.main(["--check-paths"])
            finally:
                ci_scope_rules.RULES_PATH = original
        self.assertEqual(code, 1)
        self.assertIn("missing path: src/does-not-exist/**", err.getvalue())

    def test_cli_succeeds_on_the_repository_rules(self):
        out, err = io.StringIO(), io.StringIO()
        with contextlib.redirect_stdout(out), contextlib.redirect_stderr(err):
            code = ci_scope_rules.main(["--check-paths"])
        self.assertEqual(code, 0)
        self.assertEqual(err.getvalue(), "")



class RulesReasonTests(unittest.TestCase):
    def test_exclusion_without_a_reason_is_rejected(self):
        doc = rules_doc()
        del doc["matching"]["exempt"][0]["reason"]
        with self.assertRaises(ci_scope_rules.RulesError):
            ci_scope_rules.validate(doc)

    def test_blank_reason_is_rejected(self):
        doc = rules_doc()
        doc["matching"]["exempt"][0]["reason"] = "   "
        with self.assertRaises(ci_scope_rules.RulesError):
            ci_scope_rules.validate(doc)

    def test_repository_rules_explain_every_exclusion(self):
        raw = tomllib.loads(ci_scope_rules.RULES_PATH.read_text())
        matching = raw["matching"]
        self.assertTrue(matching["exempt"])
        for entry in matching["exempt"]:
            self.assertTrue(entry["reason"].strip(), entry)


class ScopeCaseTests(ScopeTestCase):
    """Replay `fixtures/scope-cases.toml` through the real classifier."""

    @staticmethod
    def cases():
        return tomllib.loads(SCOPE_CASES.read_text())["case"]

    def test_every_case_matches_the_classifier(self):
        for case in self.cases():
            with self.subTest(case=case["name"]):
                plan = self.plan([file_entry(path) for path in case["paths"]])
                expected = {
                    suite_id
                    for suite_id in ci_scope.SUITES
                    if any(
                        fnmatch.fnmatchcase(suite_id, pattern)
                        for pattern in case["suites"]
                    )
                }
                actual = {
                    suite_id
                    for suite_id, entry in plan["suites"].items()
                    if entry["run"]
                }
                self.assertEqual(expected, actual, plan["notes"])

    def test_cases_cover_every_suite(self):
        covered = set()
        for case in self.cases():
            covered |= {
                suite_id
                for suite_id in ci_scope.SUITES
                if any(
                    fnmatch.fnmatchcase(suite_id, pattern)
                    for pattern in case["suites"]
                )
            }
        self.assertEqual(covered, set(ci_scope.SUITES))


class SummaryTests(ScopeTestCase):
    def summary(self, plan, path):
        args = ci_scope.parse_args(
            ["--repo", "GreptimeTeam/greptimedb", "--pr", "1", "--summary", path]
        )
        ci_scope.emit(plan, args)
        return pathlib.Path(path).read_text()

    def test_summary_names_the_files_that_matched(self):
        plan = self.plan([file_entry("src/mito2/src/lib.rs")])
        with tempfile.TemporaryDirectory() as tmp:
            text = self.summary(plan, str(pathlib.Path(tmp) / "summary.md"))
        self.assertIn("### CI scope: mode=static", text)
        row = next(
            line
            for line in text.splitlines()
            if line.startswith("| `fuzz-standalone/logical-table`")
        )
        self.assertIn("| run |", row)
        self.assertIn("`src/mito2/src/lib.rs`", row)

    def test_summary_marks_a_skipped_suite_without_files(self):
        plan = self.plan([file_entry("src/cli/src/lib.rs")])
        with tempfile.TemporaryDirectory() as tmp:
            text = self.summary(plan, str(pathlib.Path(tmp) / "summary.md"))
        row = next(
            line
            for line in text.splitlines()
            if line.startswith("| `fuzz-standalone/database-and-regular-table`")
        )
        self.assertIn("| skip |", row)
        self.assertTrue(row.endswith("| — |"), row)

    def test_summary_truncates_long_file_lists(self):
        files = [file_entry(f"src/mito2/src/f{index}.rs") for index in range(5)]
        plan = self.plan(files)
        with tempfile.TemporaryDirectory() as tmp:
            text = self.summary(plan, str(pathlib.Path(tmp) / "summary.md"))
        self.assertIn("+2 more", text)

    def test_summary_appends_instead_of_overwriting(self):
        plan = self.plan([file_entry("src/mito2/src/lib.rs")])
        with tempfile.TemporaryDirectory() as tmp:
            path = pathlib.Path(tmp) / "summary.md"
            path.write_text("existing\n")
            self.summary(plan, str(path))
            text = path.read_text()
        self.assertTrue(text.startswith("existing\n"))
        self.assertEqual(text.count("### CI scope"), 1)

    def test_summary_carries_the_notes(self):
        plan = self.plan([file_entry("Cargo.lock")])
        with tempfile.TemporaryDirectory() as tmp:
            text = self.summary(plan, str(pathlib.Path(tmp) / "summary.md"))
        self.assertIn("<details><summary>notes</summary>", text)


class CrateCoverageTests(unittest.TestCase):
    """A new crate must not silently match no suite: the runtime net would
    catch it by running everything, but the rule table should be updated."""

    def test_every_workspace_crate_is_mapped_or_excused(self):
        patterns = [
            ci_scope.glob_to_regex(path)
            for suite in SUITES.values()
            for path in suite_paths(suite)
        ]
        unmapped = [
            crate
            for crate in workspace_crates()
            if crate not in UNMAPPED_CRATES
            and not any(p.match(crate + "/src/lib.rs") for p in patterns)
        ]
        self.assertEqual(unmapped, [], "crate matches no suite and no excuse")

    def test_excuses_reference_existing_crates(self):
        stale = sorted(set(UNMAPPED_CRATES) - set(workspace_crates()))
        self.assertEqual(stale, [], "excuse points at a crate that is gone")


class PathRuleCoverageTests(ScopeTestCase):
    """A rule must select every suite the change can actually break."""

    def test_copy_parser_selects_export_import(self):
        plan = self.plan([file_entry("src/sql/src/parsers/copy_parser.rs")])
        self.assertTrue(plan["suites"]["export-import-v2"]["run"])

    def test_shared_ddl_selects_standalone_fuzz(self):
        plan = self.plan([file_entry("src/common/meta/src/ddl/create_table.rs")])
        for suite in (
            "fuzz-standalone/database-and-regular-table",
            "fuzz-standalone/logical-table",
            "fuzz-unstable/standalone",
        ):
            self.assertTrue(plan["suites"][suite]["run"], suite)

    def test_shared_ddl_selects_export_import(self):
        # The E2E creates databases, tables and views and recreates them on
        # import, all through the in-process DDL manager.
        plan = self.plan([file_entry("src/common/procedure/src/lib.rs")])
        self.assertTrue(plan["suites"]["export-import-v2"]["run"])

    def test_group_references_resolve_recursively(self):
        self.assertIn("src/common/meta/**", ci_scope.suite_paths(SUITES["fuzz-chaos/migration"]))


class OutputTests(ScopeTestCase):
    def test_flat_outputs(self):
        plan = self.plan([file_entry("src/mito2/src/lib.rs")])
        outputs, legs = ci_scope.derive_outputs(plan)
        self.assertEqual(
            set(outputs),
            {
                "any_fuzz",
                "build_ci_needed",
                "fuzz_standalone",
                "fuzz_distributed",
                "fuzz_chaos",
                "fuzz_unstable",
                "export_import_v2",
                "sqlness_kvbackend",
            },
        )
        for key in outputs:
            self.assertIn(outputs[key], ("true", "false"), key)
        self.assertEqual(
            sorted(legs["fuzz_standalone"]),
            ["database-and-regular-table", "logical-table"],
        )

    def test_emit_writes_the_gate_outputs(self):
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            out_path = os.path.join(tmp, "github_output")
            args = ci_scope.parse_args(
                [
                    "--repo",
                    "GreptimeTeam/greptimedb",
                    "--pr",
                    "1",
                    "--event",
                    "merge_group",
                    "--github-output",
                    out_path,
                ]
            )
            ci_scope.emit(ci_scope.build_plan(args, "token"), args)
            with open(out_path, encoding="utf-8") as handle:
                text = handle.read()
            self.assertIn("any_fuzz=true" + "\n", text)
            self.assertIn("fuzz_unstable=true", text)
            self.assertIn("fuzz_chaos=true", text)
            self.assertEqual(text.count("\n"), 8)



class GateJobContractTests(unittest.TestCase):
    """The rules say which job each gate switches; prove the YAML agrees."""

    WORKFLOW = WORKFLOWS / "integration.yml"

    def job_blocks(self):
        blocks = {}
        current = None
        for line in self.WORKFLOW.read_text().splitlines():
            marker = re.match(r"^  ([A-Za-z0-9_-]+):$", line)
            if marker:
                current = marker.group(1)
                blocks[current] = []
            elif current:
                blocks[current].append(line)
        return {name: "\n".join(lines) for name, lines in blocks.items()}

    def test_every_gate_names_a_job_that_reads_it(self):
        blocks = self.job_blocks()
        for gate, job in ci_scope_rules.GATE_JOBS.items():
            self.assertIn(job, blocks, gate)
            self.assertIn(f"needs.scope.outputs.{gate} != 'false'", blocks[job], gate)

    def test_every_gated_job_is_named_by_a_gate(self):
        derived = {"any_fuzz", "build_ci_needed"}
        declared = set(ci_scope_rules.GATE_JOBS)
        for name, block in self.job_blocks().items():
            for gate in re.findall(r"needs\.scope\.outputs\.([a-z0-9_]+)", block):
                self.assertIn(gate, declared | derived, name)

    def test_a_suite_gate_missing_from_the_table_is_rejected(self):
        doc = rules_doc()
        del doc["gates"]["fuzz_x"]
        with self.assertRaises(ci_scope_rules.RulesError) as ctx:
            ci_scope_rules.validate(doc)
        self.assertIn("missing from [gates]", str(ctx.exception))

    def test_gates_no_suite_uses_are_rejected(self):
        doc = rules_doc()
        doc["gates"]["fuzz_y"] = "fuzztest"
        with self.assertRaises(ci_scope_rules.RulesError) as ctx:
            ci_scope_rules.validate(doc)
        self.assertIn("no suite uses", str(ctx.exception))


class WorkflowContractTests(unittest.TestCase):
    """Keep the YAML gate contract in sync with the classifier outputs."""

    WORKFLOW = WORKFLOWS / "integration.yml"

    def job_blocks(self):
        text = self.WORKFLOW.read_text()
        blocks = {}
        current = None
        for line in text.splitlines():
            marker = re.match(r"^  ([A-Za-z0-9_-]+):$", line)
            if marker:
                current = marker.group(1)
                blocks[current] = []
            elif current:
                blocks[current].append(line)
        return {name: "\n".join(lines) for name, lines in blocks.items()}

    def test_gates_treat_a_missing_output_as_run(self):
        blocks = self.job_blocks()
        gated = [
            "export-import-v2-e2e",
            "prepare-fuzz-targets",
            "fuzztest",
            "unstable-fuzztest",
            "build-greptime-ci",
            "distributed-fuzztest",
            "distributed-fuzztest-with-chaos",
            "sqlness-kvbackend",
        ]
        declared = set(
            re.findall(
                r"^      ([a-z0-9_]+): \$\{\{ steps\.classify\.outputs\.\1 \}\}$",
                blocks["scope"],
                re.MULTILINE,
            )
        )
        self.assertIn("any_fuzz", declared)
        seen = set()
        for name in gated:
            self.assertIn("scope", blocks[name], name)
            match = re.search(
                r"needs\.scope\.outputs\.([a-z0-9_]+) != 'false'", blocks[name]
            )
            self.assertTrue(match, name)
            seen.add(match.group(1))
        self.assertEqual(seen, declared)

    def test_gates_override_the_implicit_success_check(self):
        # A condition without a status function carries an implicit success()
        # over `needs`, so a scope job that times out or dies on the runner
        # would skip the gates despite the fail-open output comparison.
        blocks = self.job_blocks()
        for name in (
            "export-import-v2-e2e",
            "prepare-fuzz-targets",
            "fuzztest",
            "unstable-fuzztest",
            "build-greptime-ci",
            "distributed-fuzztest",
            "distributed-fuzztest-with-chaos",
        ):
            block = blocks[name]
            condition = re.search(r"^    if: \$\{\{ (.*?) \}\}$", block, re.M)
            self.assertTrue(condition, name)
            self.assertTrue(condition.group(1).startswith("!cancelled() && "), name)
            needs = re.search(r"^    needs: \[(.*?)\]$", block, re.M)
            self.assertTrue(needs, name)
            for dep in [item.strip() for item in needs.group(1).split(",")]:
                if not dep or dep == "scope":
                    continue
                key = f"needs['{dep}'].result" if "-" in dep else f"needs.{dep}.result"
                self.assertIn(key + " == 'success'", condition.group(1), name)

    def test_scope_job_cannot_fail_and_skip_the_gates(self):
        block = self.job_blocks()["scope"]
        self.assertNotIn("Fail-open defaults", block)
        self.assertIn("github.event.merge_group.base_sha", block)
        self.assertIn("CI_SCOPE_FORCE_ALL", block)
        # Every step that can fail must be continue-on-error: a failed scope job
        # would make GitHub skip the gated jobs instead of running them.
        steps = block.split("\n      - ")
        guarded = [
            step
            for step in steps
            if step.startswith("uses: actions/checkout")
            or step.startswith("name: Classify")
        ]
        self.assertEqual(len(guarded), 2)
        for step in guarded:
            self.assertIn("continue-on-error: true", step, step.splitlines()[0])



class LogRenderingTests(ScopeTestCase):
    """The step log is where a triage starts; the verdict must be there."""

    def sample_plan(self):
        return {
            "mode": "static",
            "head_sha": "a" * 40,
            "base_sha": "b" * 40,
            "full": False,
            "notes": ["always-run: Cargo.lock"],
            "suites": {
                "fuzz-standalone/logical-table": {
                    "run": True,
                    "source": "static",
                    "reason": "touches mito2",
                },
                "fuzz-chaos/migration": {
                    "run": False,
                    "source": "static",
                    "reason": "meta docs only",
                },
            },
        }

    def test_header_names_mode_and_full(self):
        self.assertEqual(
            ci_scope.render_log(self.sample_plan()).splitlines()[0],
            "CI scope: mode=static full=false",
        )

    def test_one_line_per_suite_with_source_and_reason(self):
        lines = ci_scope.render_log(self.sample_plan()).splitlines()
        self.assertIn("  run  fuzz-standalone/logical-table", lines[1])
        self.assertIn("static", lines[1])
        self.assertIn("touches mito2", lines[1])
        self.assertIn("  skip fuzz-chaos/migration", lines[2])
        self.assertIn("meta docs only", lines[2])

    def test_gates_and_notes_are_printed(self):
        log = ci_scope.render_log(self.sample_plan())
        self.assertIn("  gates: fuzz_standalone=true", log)
        self.assertIn("any_fuzz=true", log)
        self.assertIn("export_import_v2=false", log)
        self.assertIn("  note: always-run: Cargo.lock", log)

    def test_a_failed_pull_request_lookup_is_annotated_and_runs_everything(self):
        # Drive the real failure path: the label lookup and the revision
        # comparison share one request, so a 500 here must widen the gate and
        # still produce the warning the annotation promises.
        self.fake.pull_status = 500
        plan = self.plan([file_entry("src/promql/src/lib.rs")])
        self.assertTrue(plan["full"])
        self.assertIn("pull request lookup failed", " ".join(plan["notes"]))
        annotations = ci_scope.log_annotations(plan)
        self.assertEqual(len(annotations), 1)
        self.assertTrue(annotations[0].startswith("::warning::"), annotations)
        self.assertIn("every suite runs", annotations[0])

    def test_main_prints_the_log(self):
        self.fake.file_pages = {1: [file_entry("src/mito2/src/lib.rs")]}
        stdout = io.StringIO()
        with contextlib.redirect_stdout(stdout):
            self.assertEqual(
                ci_scope.main(
                    [
                        "--repo",
                        "GreptimeTeam/greptimedb",
                        "--pr",
                        "1",
                        "--event",
                        "pull_request",
                        "--dry-run",
                    ]
                ),
                0,
            )
        text = stdout.getvalue()
        self.assertIn("CI scope: mode=", text)
        self.assertIn("  run  fuzz-standalone/logical-table", text)
        self.assertIn("any_fuzz=true", text)


if __name__ == "__main__":
    unittest.main()
