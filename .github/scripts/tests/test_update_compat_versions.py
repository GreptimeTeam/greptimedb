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

import contextlib
import importlib.util
import io
import tempfile
import unittest
from pathlib import Path
from unittest import mock


SCRIPT = Path(__file__).resolve().parents[1] / "update-compat-versions.py"
SPEC = importlib.util.spec_from_file_location("update_compat_versions", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
UPDATER = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(UPDATER)


class UpdateCompatVersionsTest(unittest.TestCase):
    def write_case(self, root: Path, name: str, from_range: str) -> None:
        case_dir = root / "tests" / "compatibility" / "cases" / name
        case_dir.mkdir(parents=True)
        (case_dir / "case.toml").write_text(
            f'name = "{name}"\nfrom_range = {from_range}\n', encoding="utf-8"
        )

    def write_config(self, root: Path, versions: str, trailer: str = "") -> Path:
        config = root / "tests" / "compatibility" / "ci.toml"
        config.parent.mkdir(parents=True, exist_ok=True)
        config.write_text(
            "# Keep these comments when the window is regenerated.\n"
            f"from_versions = {versions}\n{trailer}",
            encoding="utf-8",
        )
        return config

    def test_stable_tags_filter_prereleases_and_nightlies(self) -> None:
        self.assertEqual(
            ["v1.0.0", "v1.0.2", "v1.1.0"],
            UPDATER.stable_tags(
                ["v1.1.0-nightly-20260101", "v1.0.2", "v1.0.0", "v1.1.0", "v1.0.0-rc.1"]
            ),
        )

    def test_sliding_versions_roll_over_to_two_newest_minor_lines(self) -> None:
        self.assertEqual(
            ["v1.9.4", "v2.0.1"],
            UPDATER.sliding_versions(["v1.8.9", "v1.9.0", "v1.9.4", "v2.0.0", "v2.0.1"]),
        )

    def test_sliding_versions_supports_one_minor_line(self) -> None:
        self.assertEqual(["v1.1.3"], UPDATER.sliding_versions(["v1.1.0", "v1.1.3"]))

    def test_sliding_versions_rejects_no_stable_tags(self) -> None:
        with self.assertRaisesRegex(UPDATER.CompatVersionError, "No stable release tags"):
            UPDATER.sliding_versions(["v1.1.0-rc.1", "v1.1.0-nightly-20260101"])

    def test_exact_anchors_are_preserved_with_sliding_versions(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write_case(root, "historical", '["=v1.0.2", ">=v1.1.0"]')
            self.write_case(root, "more_history", '["=v1.1.0", "=v1.1.1"]')
            versions = UPDATER.effective_versions(
                ["v1.0.2", "v1.1.0", "v1.1.1", "v1.1.3"],
                root / "tests" / "compatibility" / "cases",
            )
        self.assertEqual(["v1.0.2", "v1.1.0", "v1.1.1", "v1.1.3"], versions)

    def test_exact_anchor_syntaxes_are_normalized(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write_case(
                root,
                "historical",
                '[" = v1.0.2 ", " == v1.1.0 ", " v1.1.1 ", "1.1.2", "v01.01.003-rc.1"]',
            )
            anchors = UPDATER.extract_pinned_anchors(root / "tests" / "compatibility" / "cases")
        self.assertEqual(
            {"v1.0.2", "v1.1.0", "v1.1.1", "v1.1.2", "v1.1.3"}, anchors
        )

    def test_ordered_ranges_and_wildcard_are_not_anchors(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write_case(
                root,
                "non_exact",
                '[">= v1.0.0", "<=v1.1.0", ">v1.0.2", "< v2.0.0", "*"]',
            )
            anchors = UPDATER.extract_pinned_anchors(root / "tests" / "compatibility" / "cases")
        self.assertEqual(set(), anchors)

    def test_update_is_sorted_and_idempotent(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            config = self.write_config(
                root,
                '[\n  "v1.1.0",\n  "v1.0.2",\n]',
                "# This trailing comment must remain.\nother_setting = true\n",
            )
            self.write_case(root, "historical", '["=v1.0.2"]')
            tags = ["v1.0.2", "v1.1.0", "v1.1.1", "v1.1.3"]
            with mock.patch.object(UPDATER, "git_tags", return_value=tags):
                self.assertEqual(0, UPDATER.main(["--repo-root", str(root), "--update"]))
                first_update = config.read_text(encoding="utf-8")
                self.assertEqual(0, UPDATER.main(["--repo-root", str(root), "--update"]))
            self.assertEqual(first_update, config.read_text(encoding="utf-8"))
        self.assertIn('from_versions = ["v1.0.2", "v1.1.3"]', first_update)
        self.assertIn("# Keep these comments when the window is regenerated.", first_update)
        self.assertIn("# This trailing comment must remain.\nother_setting = true\n", first_update)

    def test_check_reports_a_unified_diff_for_stale_config(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write_config(root, '["v1.0.2"]')
            self.write_case(root, "historical", '["=v1.0.2"]')
            stderr = io.StringIO()
            with mock.patch.object(UPDATER, "git_tags", return_value=["v1.0.2", "v1.1.3"]):
                with contextlib.redirect_stderr(stderr):
                    result = UPDATER.main(["--repo-root", str(root), "--check"])
        self.assertEqual(1, result)
        self.assertIn("--- a/tests/compatibility/ci.toml", stderr.getvalue())
        self.assertIn("+++ b/tests/compatibility/ci.toml", stderr.getvalue())

    def test_rejects_duplicate_config_versions(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            config = self.write_config(Path(directory), '["v1.0.2", "v1.0.2"]')
            with self.assertRaisesRegex(UPDATER.CompatVersionError, "Duplicate from_versions"):
                UPDATER.load_from_versions(config)

    def test_rejects_unavailable_pinned_anchor(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write_case(root, "historical", '["=v1.0.2"]')
            with self.assertRaisesRegex(UPDATER.CompatVersionError, "unavailable"):
                UPDATER.effective_versions(
                    ["v1.1.0"], root / "tests" / "compatibility" / "cases"
                )

    def test_rejects_malformed_exact_constraint(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write_case(root, "historical", '["=v1.0"]')
            with self.assertRaisesRegex(UPDATER.CompatVersionError, "Malformed version"):
                UPDATER.extract_pinned_anchors(root / "tests" / "compatibility" / "cases")

    def test_rejects_malformed_bare_exact_constraint(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write_case(root, "historical", '["v1.0"]')
            with self.assertRaisesRegex(UPDATER.CompatVersionError, "Malformed version"):
                UPDATER.extract_pinned_anchors(root / "tests" / "compatibility" / "cases")

    def test_rejects_malformed_ordered_constraint(self) -> None:
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write_case(root, "historical", '[">=v1.0"]')
            with self.assertRaisesRegex(UPDATER.CompatVersionError, "Malformed version"):
                UPDATER.extract_pinned_anchors(root / "tests" / "compatibility" / "cases")


if __name__ == "__main__":
    unittest.main()
