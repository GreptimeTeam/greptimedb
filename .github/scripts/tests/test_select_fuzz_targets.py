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

import copy
import importlib.util
import json
import os
from pathlib import Path
import subprocess
import sys
import tempfile
import tomllib
import unittest
from unittest.mock import patch


ROOT = Path(__file__).resolve().parents[3]
SCRIPT = ROOT / ".github/scripts/select-fuzz-targets.py"
spec = importlib.util.spec_from_file_location("select_fuzz_targets", SCRIPT)
selector = importlib.util.module_from_spec(spec)
spec.loader.exec_module(selector)
MANIFEST = ROOT / "tests-fuzz/Cargo.toml"
TARGETS = selector.target_paths(MANIFEST)
KNOWN = set(TARGETS.values())
with (SCRIPT.parent / "fuzz-target-rules.toml").open("rb") as source:
    POLICY = tomllib.load(source)


class SelectionTest(unittest.TestCase):
    def select(self, paths, policy=None):
        return selector.select_paths(set(paths), TARGETS, POLICY if policy is None else policy)

    def test_every_declared_target_maps_to_itself(self):
        for path, target in TARGETS.items():
            with self.subTest(target=target):
                self.assertEqual([target], self.select([path])["targets"])
                self.assertTrue((ROOT / path).is_file())
        self.assertNotIn("kafka_wal_helper", KNOWN)

    def test_multiple_paths_and_helper_rules_are_unioned(self):
        result = self.select(["tests-fuzz/targets/fuzz_insert.rs",
                              "tests-fuzz/src/generator/alter_expr.rs"])
        self.assertEqual(["fuzz_alter_logical_table", "fuzz_alter_table", "fuzz_insert"], result["targets"])

    def test_rules_reference_existing_helpers_and_targets(self):
        for rule in POLICY["rules"]:
            self.assertTrue(set(rule["targets"]) <= KNOWN)
            self.assertTrue(all(any(ROOT.glob(path)) for path in rule["paths"]))

    def test_metric_engine_selects_all_metric_operations(self):
        expected = {"fuzz_create_logical_table", "fuzz_alter_logical_table", "fuzz_insert_logical_table",
                    "fuzz_repartition_metric_table", "fuzz_migrate_metric_regions",
                    "fuzz_failover_metric_regions", "fuzz_remote_wal_logical_prune"}
        for path in ["src/metric-engine/src/engine.rs", "src/metric-engine/src/engine/put.rs"]:
            self.assertEqual(expected, set(self.select([path])["targets"]))
        self.assertIsNone(self.select(["src/metric-engine/Cargo.toml"])["targets"])

    def test_manifest_rejects_missing_duplicate_or_unsafe_targets(self):
        declarations = [
            '[[bin]]\nname = "helper"\npath = "helper.rs"\n',
            '[[bin]]\nname = "fuzz_x"\npath = "../escape.rs"\n',
            ('[[bin]]\nname = "fuzz_x"\npath = "x.rs"\n' * 2),
        ]
        for content in declarations:
            with tempfile.TemporaryDirectory() as directory:
                manifest = Path(directory) / "Cargo.toml"
                manifest.write_text(content)
                with self.subTest(content=content), self.assertRaises(ValueError):
                    selector.target_paths(manifest)

    def test_overlapping_rules_are_unioned(self):
        policy = {"version": 1, "rules": [
            {"paths": ["shared.rs"], "targets": ["fuzz_insert"]},
            {"paths": ["shared.rs"], "targets": ["fuzz_alter_table"]},
        ]}
        self.assertEqual(["fuzz_alter_table", "fuzz_insert"], self.select(["shared.rs"], policy)["targets"])

    def test_repartition_helpers_include_chaos(self):
        result = self.select(["tests-fuzz/src/translator/mysql/repartition_expr.rs"])
        self.assertEqual(["fuzz_repartition_metric_table", "fuzz_repartition_table",
                          "fuzz_repartition_table_chaos"], result["targets"])

    def test_shared_unknown_and_policy_changes_run_all(self):
        for path in ["src/mito2/src/engine.rs", "src/new-crate/lib.rs", "Cargo.lock",
                     "tests-fuzz/src/context.rs", "rust-toolchain.toml", "README.md",
                     ".github/scripts/select-fuzz-targets.py", ".github/scripts/fuzz-target-rules.toml",
                     "tests-fuzz/targets/new_target.rs", "../outside", "/absolute", "bad\npath"]:
            with self.subTest(path=path):
                self.assertIsNone(self.select(["tests-fuzz/targets/fuzz_insert.rs", path])["targets"])

    def test_empty_diff_runs_all(self):
        self.assertIsNone(self.select([])["targets"])

    def test_invalid_policy_cannot_narrow(self):
        for policy in [{}, [], {"version": 2, "rules": []},
                       {"version": 1, "rules": [None]},
                       {"version": 1, "rules": [{"paths": "x", "targets": ["fuzz_insert"]}]},
                       {"version": 1, "rules": [{"paths": ["x"], "targets": "fuzz_insert"}]},
                       {"version": 1, "rules": [{"paths": ["x"], "targets": ["typo"]}]},
                       {"version": 1, "rules": [{"paths": [], "targets": ["fuzz_insert"]}]}]:
            with self.subTest(policy=policy), self.assertRaises(ValueError):
                self.select(["tests-fuzz/targets/fuzz_insert.rs"], policy)

    def test_group_keeps_order_and_first_target_as_smoke(self):
        group = "fuzz_create_database\nfuzz_create_table\nfuzz_alter_table\nfuzz_insert"
        plan = json.dumps({"targets": ["fuzz_insert", "fuzz_create_table"]})
        self.assertEqual(["fuzz_create_database", "fuzz_create_table", "fuzz_insert"],
                         selector.select_group(group, plan, KNOWN))
        self.assertEqual(["fuzz_create_database"], selector.select_group(
            group, json.dumps({"targets": ["fuzz_migrate_mito_regions"]}), KNOWN))

    def test_single_target_group_is_never_empty(self):
        self.assertEqual(["unstable_fuzz_create_table_standalone"], selector.select_group(
            "unstable_fuzz_create_table_standalone", json.dumps({"targets": ["fuzz_insert"]}), KNOWN))

    def test_bad_or_full_decision_runs_complete_group(self):
        for decision in ["", "{", "null", "[]", "{}", '{"targets":null}',
                         '{"targets":[]}', '{"targets":"fuzz_insert"}',
                         '{"targets":["unknown"]}', '{"targets":[1]}']:
            with self.subTest(decision=decision):
                self.assertEqual(["fuzz_create_table", "fuzz_insert"], selector.select_group(
                    "fuzz_create_table\r\nfuzz_insert\nfuzz_insert", decision, KNOWN))

    def test_invalid_group_fails(self):
        for group in ["", "../escape", "bad target"]:
            with self.subTest(group=group), self.assertRaises(ValueError):
                selector.select_group(group, "", KNOWN)

    def test_group_cli_emits_multiline_output(self):
        with tempfile.TemporaryDirectory() as directory:
            output = Path(directory) / "output"
            result = subprocess.run([sys.executable, str(SCRIPT), "group"], env={
                **os.environ, "GITHUB_OUTPUT": str(output), "FUZZ_TARGETS": "fuzz_create_table\nfuzz_insert",
                "FUZZ_AFFECTED_TARGETS": json.dumps({"targets": ["fuzz_alter_table"]}),
            }, capture_output=True, text=True)
            self.assertEqual(0, result.returncode, result.stderr)
            self.assertEqual("targets<<FUZZ_SELECTED_TARGETS\nfuzz_create_table\nFUZZ_SELECTED_TARGETS\n",
                             output.read_text())


class DiffTest(unittest.TestCase):
    def setUp(self):
        self.pr = {"head": {"sha": "a" * 40}, "base": {"sha": "b" * 40},
                   "changed_files": 1, "labels": []}
        self.event = {"number": 42, "pull_request": copy.deepcopy(self.pr)}
        self.file = {"filename": "tests-fuzz/targets/fuzz_insert.rs", "status": "modified"}
        self.env = patch.dict(os.environ, {"GITHUB_REPOSITORY": "GreptimeTeam/greptimedb"})
        self.env.start()
        self.addCleanup(self.env.stop)

    def paths(self, responses):
        with patch.object(selector, "api", side_effect=responses):
            return selector.changed_paths(self.event)

    def test_modified_and_deleted_files_select_their_target(self):
        for status in ["modified", "removed", "added"]:
            self.file["status"] = status
            self.assertEqual({self.file["filename"]}, self.paths([self.pr, [self.file], self.pr]))

    def test_rename_includes_both_paths(self):
        self.file.update(status="renamed", previous_filename="tests-fuzz/targets/ddl/fuzz_alter_table.rs")
        self.assertEqual({self.file["filename"], self.file["previous_filename"]},
                         self.paths([self.pr, [self.file], self.pr]))

    def test_unknown_rename_source_forces_full_selection(self):
        self.file.update(status="renamed", previous_filename="unknown.rs")
        paths = self.paths([self.pr, [self.file], self.pr])
        self.assertIsNone(selector.select_paths(paths, TARGETS, POLICY)["targets"])

    def test_stale_head_or_base_rejected(self):
        for side in ["head", "base"]:
            changed = copy.deepcopy(self.pr)
            changed[side]["sha"] = "c" * 40
            with self.subTest(side=side), self.assertRaises(ValueError):
                self.paths([changed])
            with self.subTest(side=side, phase="after"), self.assertRaises(ValueError):
                self.paths([self.pr, [self.file], changed])

    def test_force_full_label_checked_before_and_after_pagination(self):
        changed = copy.deepcopy(self.pr)
        changed["labels"] = [{"name": "ci:full"}]
        for responses in [[changed], [self.pr, [self.file], changed]]:
            with self.assertRaises(ValueError):
                self.paths(responses)

    def test_truncated_duplicate_and_changed_file_counts_rejected(self):
        changed = {**self.pr, "changed_files": 2}
        for responses in [[self.pr, [], self.pr], [self.pr, [self.file], changed],
                          [changed, [self.file, self.file], changed]]:
            with self.assertRaises(ValueError):
                self.paths(responses)

    def test_large_diff_is_full_without_fetching_pages(self):
        for count in [0, 301, 3000]:
            with self.subTest(count=count), self.assertRaises(ValueError):
                self.paths([{**self.pr, "changed_files": count}])

    def test_complete_pagination(self):
        files = [{"filename": f"file{i}.rs", "status": "modified"} for i in range(101)]
        pr = {**self.pr, "changed_files": 101}
        self.assertEqual(101, len(self.paths([pr, files[:100], files[100:], pr])))

    def test_api_request_has_timeout_and_read_only_method(self):
        with patch.dict(os.environ, {"GITHUB_API_URL": "https://api.github.test", "GITHUB_TOKEN": "test"}), \
                patch.object(selector.urllib.request, "urlopen") as open_url:
            open_url.return_value.__enter__.return_value.read.return_value = b"{}"
            selector.api("/repos/x/y/pulls/1")
            request = open_url.call_args.args[0]
            self.assertEqual("GET", request.get_method())
            self.assertEqual(20, open_url.call_args.kwargs["timeout"])

    def test_plan_success_and_api_failure_fallback(self):
        with tempfile.TemporaryDirectory() as directory:
            event = Path(directory) / "event.json"
            event.write_text(json.dumps(self.event))
            with patch.dict(os.environ, {"GITHUB_EVENT_NAME": "pull_request", "GITHUB_EVENT_PATH": str(event),
                                         "FUZZ_FORCE_ALL": "", "FUZZ_CANDIDATE_MANIFEST": str(MANIFEST)}):
                with patch.object(selector, "api", side_effect=[self.pr, [self.file], self.pr]):
                    self.assertEqual(["fuzz_insert"], selector.plan()["targets"])
                with patch.object(selector, "api", side_effect=OSError("unavailable")):
                    self.assertIsNone(selector.plan()["targets"])
                with patch.object(selector, "target_paths", side_effect=[TARGETS, {"new.rs": "fuzz_new"}]):
                    self.assertIsNone(selector.plan()["targets"])

    def test_manual_queue_schedule_and_override_run_full(self):
        for event, force in [("workflow_dispatch", ""), ("merge_group", ""), ("schedule", ""),
                             ("pull_request", "true")]:
            with patch.dict(os.environ, {"GITHUB_EVENT_NAME": event, "FUZZ_FORCE_ALL": force}), \
                    patch.object(selector, "api") as api:
                self.assertIsNone(selector.plan()["targets"])
                api.assert_not_called()


if __name__ == "__main__":
    unittest.main()
