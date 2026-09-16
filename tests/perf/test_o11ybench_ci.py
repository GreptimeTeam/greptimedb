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

"""Offline tests: no cloud instances or database processes are started."""

import importlib.util
import json
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
SPEC = importlib.util.spec_from_file_location(
    "o11ybench_ci", ROOT / ".github/scripts/o11ybench-ci.py"
)
ci = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ci)


def inputs():
    return {
        "TARGETS": "greptimedb",
        "PROFILE": "S",
        "O11YBENCH_REF": "a" * 40,
        "DB_CPUS": "4",
        "DB_MEMORY": "8g",
        "RUNTIME_IMAGE": "greptime-registry.cn-hangzhou.cr.aliyuncs.com/greptime/o11ybench-runtime:test",
        "GREPTIMEDB_TAG": "latest",
        "CLICKHOUSE_TAG": "26.6.1.1193",
        "VICTORIALOGS_TAG": "latest",
    }


class InputsTest(unittest.TestCase):
    def test_single_subset_and_all(self):
        for value, expected in [
            ("greptimedb", ["greptimedb"]),
            ("victorialogs,greptimedb", ["greptimedb", "victorialogs"]),
            ("all", list(ci.TARGETS)),
        ]:
            with self.subTest(value=value):
                self.assertEqual(ci.selected_targets(value), expected)

    def test_invalid_targets(self):
        for value in (
            "",
            "mysql",
            "greptimedb,",
            "all,greptimedb",
            "greptimedb,greptimedb",
            "greptimedb\nx=y",
        ):
            with self.subTest(value=value), self.assertRaises(ValueError):
                ci.selected_targets(value)

    def test_profiles(self):
        for profile in ("S", "P", "M"):
            self.assertEqual(
                ci.validate(dict(inputs(), PROFILE=profile)), ["greptimedb"]
            )

    def test_invalid_inputs(self):
        for key, value in (
            ("PROFILE", "X"),
            ("O11YBENCH_REF", "main"),
            ("DB_CPUS", "0"),
            ("DB_MEMORY", "$(cmd)"),
            ("RUNTIME_IMAGE", "python:latest"),
            ("RUNTIME_IMAGE", "bad.cr.aliyuncs.com/a/b:tag\nINJECT=x"),
            ("GREPTIMEDB_TAG", "latest; echo x"),
        ):
            with self.subTest(key=key), self.assertRaises(ValueError):
                ci.validate(dict(inputs(), **{key: value}))


class PrepareTest(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory()
        self.addCleanup(self.temp.cleanup)
        self.root = Path(self.temp.name)
        self.env = dict(
            inputs(),
            ARTIFACT_ROOT=str(self.root / "results"),
            O11YBENCH_DIR="checkout",
            GITHUB_ENV=str(self.root / "env"),
            MANIFEST_PATH=str(self.root / "manifest.json"),
        )
        self.image = {
            "Id": "sha256:" + "b" * 64,
            "RepoDigests": ["registry/image@sha256:" + "c" * 64],
            "Config": {"Labels": {"org.opencontainers.image.revision": "a" * 40}},
        }

    def test_only_selected_images_and_immutable_exports(self):
        with (
            patch.object(ci.subprocess, "check_output", return_value="a" * 40 + "\n"),
            patch.object(ci, "docker_image", return_value=self.image) as pull,
        ):
            ci.prepare(self.env)
        self.assertEqual(
            [c.args[0] for c in pull.call_args_list],
            [self.env["RUNTIME_IMAGE"], "greptime/greptimedb:latest"],
        )
        self.assertFalse(Path(self.env["ARTIFACT_ROOT"]).exists())
        exports = Path(self.env["GITHUB_ENV"]).read_text()
        self.assertIn("GREPTIMEDB_IMAGE=sha256:", exports)
        self.assertNotIn("CLICKHOUSE_IMAGE", exports)
        manifest = json.loads(Path(self.env["MANIFEST_PATH"]).read_text())
        self.assertEqual(
            manifest["images"]["greptimedb"]["requested"], "greptime/greptimedb:latest"
        )
        self.assertFalse(manifest["publishable"])

    def test_runtime_revision_mismatch_before_db_pull(self):
        self.image["Config"]["Labels"]["org.opencontainers.image.revision"] = "old"
        with (
            patch.object(ci.subprocess, "check_output", return_value="a" * 40),
            patch.object(ci, "docker_image", return_value=self.image) as pull,
            self.assertRaisesRegex(ValueError, "runtime image revision"),
        ):
            ci.prepare(self.env)
        self.assertEqual(pull.call_count, 1)
        self.assertFalse(Path(self.env["GITHUB_ENV"]).exists())

    def test_wrong_checkout_before_any_pull(self):
        with (
            patch.object(ci.subprocess, "check_output", return_value="wrong"),
            patch.object(ci, "docker_image") as pull,
            self.assertRaisesRegex(ValueError, "checkout"),
        ):
            ci.prepare(self.env)
        pull.assert_not_called()

    def test_existing_root_not_overwritten(self):
        Path(self.env["ARTIFACT_ROOT"]).mkdir()
        with (
            patch.object(ci, "docker_image") as pull,
            self.assertRaisesRegex(ValueError, "new absolute"),
        ):
            ci.prepare(self.env)
        pull.assert_not_called()

    def test_all_targets(self):
        self.env["TARGETS"] = "all"
        with (
            patch.object(ci.subprocess, "check_output", return_value="a" * 40),
            patch.object(ci, "docker_image", return_value=self.image) as pull,
        ):
            ci.prepare(self.env)
        self.assertEqual(pull.call_count, 4)


class SummaryTest(unittest.TestCase):
    def test_selected_missing_failed_and_health_failure(self):
        for target_selection, summary_passed, exit_code, expected in [
            ("greptimedb", True, "0", True),
            ("all", True, "0", False),
            ("greptimedb", False, "0", False),
            ("greptimedb", True, "1", False),
            ("greptimedb", True, None, False),
        ]:
            with (
                self.subTest(
                    selection=target_selection, passed=summary_passed, exit=exit_code
                ),
                tempfile.TemporaryDirectory() as temp,
            ):
                root = Path(temp)
                summary_dir = root / "greptimedb-1/greptimedb-1"
                summary_dir.mkdir(parents=True)
                (summary_dir / "measured-summary.json").write_text(
                    json.dumps(
                        {
                            "target": "greptimedb",
                            "passed": summary_passed,
                            "successful_requests": 80,
                            "p50_ms": 1,
                            "p95_ms": 2,
                            "p99_ms": 3,
                        }
                    )
                )
                if exit_code is not None:
                    (root / "greptimedb-1/exit-code.txt").write_text(exit_code)
                env = dict(
                    inputs(),
                    TARGETS=target_selection,
                    ARTIFACT_ROOT=temp,
                    GITHUB_STEP_SUMMARY=str(root / "github.md"),
                )
                if expected:
                    ci.summarize(env)
                else:
                    with self.assertRaises(ValueError):
                        ci.summarize(env)
                self.assertEqual(
                    json.loads((root / "run-summary.json").read_text())["passed"],
                    expected,
                )
                self.assertIn("P99 ms", (root / "summary.md").read_text())

    def test_all_targets_require_successful_comparison(self):
        for comparison_passed in (None, False, True):
            with (
                self.subTest(comparison=comparison_passed),
                tempfile.TemporaryDirectory() as temp,
            ):
                root = Path(temp)
                for target in ci.TARGETS:
                    directory = root / f"{target}-1" / f"{target}-1"
                    directory.mkdir(parents=True)
                    (directory / "measured-summary.json").write_text(
                        json.dumps({"target": target, "passed": True})
                    )
                    (directory.parent / "exit-code.txt").write_text("0")
                if comparison_passed is not None:
                    (root / "comparison.json").write_text(
                        json.dumps(
                            {
                                "passed": comparison_passed,
                                "cross_target_fingerprints_match": comparison_passed,
                            }
                        )
                    )
                env = dict(
                    inputs(),
                    TARGETS="all",
                    ARTIFACT_ROOT=temp,
                    GITHUB_STEP_SUMMARY=str(root / "github.md"),
                )
                if comparison_passed:
                    ci.summarize(env)
                else:
                    with self.assertRaises(ValueError):
                        ci.summarize(env)
                self.assertEqual(
                    json.loads((root / "run-summary.json").read_text())["passed"],
                    bool(comparison_passed),
                )

    def test_missing_root(self):
        with (
            tempfile.TemporaryDirectory() as temp,
            self.assertRaisesRegex(ValueError, "generation"),
        ):
            ci.summarize(dict(inputs(), ARTIFACT_ROOT=temp + "/missing"))


if __name__ == "__main__":
    unittest.main()
