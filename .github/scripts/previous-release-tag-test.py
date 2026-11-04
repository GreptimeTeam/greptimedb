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
import subprocess
import sys
import unittest
from pathlib import Path


def load_module():
    path = Path(__file__).resolve().parent / "previous-release-tag.py"
    spec = importlib.util.spec_from_file_location("previous_release_tag", path)
    assert spec is not None and spec.loader is not None
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


TAGS = [
    "v1.1.0",
    "v1.2.0",
    "v1.2.1",
    "v1.2.1^{}",
    "v1.3.0-alpha.1",
    "v1.3.0-beta.1",
    "v1.3.0-beta.2",
    "v1.2.0-nightly-20260706",
]


class PreviousReleaseTagTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        cls.mod = load_module()

    def test_beta_uses_latest_older_stable_not_an_ancestor(self) -> None:
        self.assertEqual(self.mod.previous_stable_tag("v1.3.0-beta.2", TAGS), "v1.2.1")

    def test_stable_release_skips_itself(self) -> None:
        self.assertEqual(self.mod.previous_stable_tag("v1.2.1", TAGS), "v1.2.0")

    def test_same_line_stable_is_not_baseline_for_its_prerelease(self) -> None:
        tags = TAGS + ["v1.3.0"]
        self.assertEqual(self.mod.previous_stable_tag("v1.3.0-beta.2", tags), "v1.2.1")
        self.assertEqual(self.mod.previous_stable_tag("v1.3.0", tags), "v1.2.1")

    def test_nightly_of_unreleased_version_uses_previous_stable(self) -> None:
        self.assertEqual(
            self.mod.previous_stable_tag("v1.3.0-nightly-20261013", TAGS),
            "v1.2.1",
        )

    def test_no_older_stable_tag(self) -> None:
        self.assertIsNone(self.mod.previous_stable_tag("v0.1.0", ["v0.1.0", "v1.2.1"]))

    def test_invalid_version(self) -> None:
        with self.assertRaises(ValueError):
            self.mod.previous_stable_tag("nightly-20261013", TAGS)

    def test_unordered_tags_use_numeric_order(self) -> None:
        forward = ["v1.2.1", "v1.9.0", "v1.10.0", "v0.10.0", "v1.9.9"]
        backward = list(reversed(forward))
        for tags in (forward, backward):
            self.assertEqual(self.mod.previous_stable_tag("v1.10.0", tags), "v1.9.9")
            self.assertEqual(self.mod.previous_stable_tag("v1.10.0-rc.1", tags), "v1.9.9")

    def test_multi_digit_components_compare_as_integers(self) -> None:
        tags = ["v1.9.0", "v1.10.0", "v0.9.10", "v0.10.0", "v2.9.15", "v2.10.0", "v10.2.0"]
        self.assertEqual(self.mod.previous_stable_tag("v1.10.0", tags), "v1.9.0")
        self.assertEqual(self.mod.previous_stable_tag("v0.10.0", tags), "v0.9.10")
        self.assertEqual(self.mod.previous_stable_tag("v2.10.0", tags), "v2.9.15")
        self.assertEqual(self.mod.previous_stable_tag("v10.2.0", ["v10.0.9", "v9.20.0"]), "v10.0.9")


class PreviousReleaseTagCliTest(unittest.TestCase):
    SCRIPT = Path(__file__).resolve().parent / "previous-release-tag.py"

    def run_cli(
        self, version: str, tags: list[str] | None = None, stdin: str | None = None
    ) -> subprocess.CompletedProcess[str]:
        command = [sys.executable, str(self.SCRIPT), "--version", version]
        if tags is not None:
            command.extend(tags)
        return subprocess.run(
            command,
            input=stdin,
            text=True,
            capture_output=True,
            check=False,
        )

    def test_stdin_selects_baseline_and_exits_zero(self) -> None:
        completed = self.run_cli(
            "v1.10.0",
            stdin="v1.2.1\nv1.9.0\n\nv1.10.0\nv1.9.0^{}\n",
        )
        self.assertEqual(completed.returncode, 0)
        self.assertEqual(completed.stdout, "v1.9.0\n")
        self.assertEqual(completed.stderr, "")

    def test_positional_tags_ignore_stdin(self) -> None:
        completed = self.run_cli("v1.10.0", tags=["v1.9.0"], stdin="v1.9.1\n")
        self.assertEqual(completed.returncode, 0)
        self.assertEqual(completed.stdout, "v1.9.0\n")

    def test_no_older_tag_exits_zero_with_empty_stdout(self) -> None:
        completed = self.run_cli("v0.1.0", stdin="v0.1.0\nv1.2.1\n")
        self.assertEqual(completed.returncode, 0)
        self.assertEqual(completed.stdout, "")
        self.assertEqual(completed.stderr, "")

    def test_invalid_version_exits_one(self) -> None:
        completed = self.run_cli("nightly", stdin="v1.2.1\n")
        self.assertEqual(completed.returncode, 1)
        self.assertEqual(completed.stdout, "")
        self.assertIn("not a release version: nightly", completed.stderr)


if __name__ == "__main__":
    unittest.main()
