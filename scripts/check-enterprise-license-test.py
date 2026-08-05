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
import tempfile
import textwrap
import unittest
from pathlib import Path

SCRIPT_PATH = Path(__file__).resolve().parent / "check-enterprise-license.py"

spec = importlib.util.spec_from_file_location("check_enterprise_license", SCRIPT_PATH)
checker = importlib.util.module_from_spec(spec)
spec.loader.exec_module(checker)


def write(root: Path, relative: str, source: str) -> Path:
    path = root / relative
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(textwrap.dedent(source).lstrip(), encoding="utf-8")
    return path


class ParseModDeclsTest(unittest.TestCase):
    def parse(self, source: str) -> dict[str, bool]:
        return dict(checker.parse_mod_decls(textwrap.dedent(source)))

    def test_gate_applies_only_to_the_next_declaration(self):
        self.assertEqual(
            self.parse(
                """
                #[cfg(feature = "enterprise")]
                pub mod trigger;
                mod plain;
                """
            ),
            {"trigger": True, "plain": False},
        )

    def test_test_only_gate_counts_as_enterprise(self):
        self.assertEqual(
            self.parse(
                """
                #[cfg(all(test, feature = "enterprise"))]
                mod recycle_bin_test;
                """
            ),
            {"recycle_bin_test": True},
        )

    def test_doc_comment_between_attribute_and_declaration(self):
        self.assertEqual(
            self.parse(
                """
                #[cfg(feature = "enterprise")]
                /// Enterprise only.
                pub(crate) mod gated;
                """
            ),
            {"gated": True},
        )

    def test_attribute_on_the_same_line(self):
        self.assertEqual(
            self.parse('#[cfg(feature = "enterprise")] mod gated;\n'),
            {"gated": True},
        )

    def test_attribute_consumed_by_another_item_does_not_leak(self):
        self.assertEqual(
            self.parse(
                """
                #[cfg(feature = "enterprise")]
                use crate::gated::Thing;
                mod plain;
                """
            ),
            {"plain": False},
        )

    def test_other_features_are_not_enterprise(self):
        self.assertEqual(
            self.parse(
                """
                #[cfg(feature = "testing")]
                mod helper;
                """
            ),
            {"helper": False},
        )

    def test_inline_module_is_ignored(self):
        self.assertEqual(
            self.parse(
                """
                #[cfg(feature = "enterprise")]
                mod inline {
                    pub const A: u8 = 1;
                }
                """
            ),
            {},
        )


class CollectGatedFilesTest(unittest.TestCase):
    def collect(self, root: Path):
        rust_files = sorted(root.rglob("*.rs"))
        return checker.collect_gated_files(rust_files)

    def test_descendants_of_a_gated_module_are_gated(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write(
                root,
                "src/lib.rs",
                """
                #[cfg(feature = "enterprise")]
                pub mod gated;
                pub mod plain;
                """,
            )
            write(root, "src/gated.rs", "pub mod nested;\n")
            write(root, "src/gated/nested.rs", "pub const A: u8 = 1;\n")
            write(root, "src/plain.rs", "pub const B: u8 = 2;\n")

            gated, unresolved = self.collect(root)

            self.assertEqual(
                gated,
                {root / "src/gated.rs", root / "src/gated/nested.rs"},
            )
            self.assertEqual(unresolved, [])

    def test_module_declared_as_directory(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            write(
                root,
                "src/lib.rs",
                """
                #[cfg(feature = "enterprise")]
                mod gated;
                """,
            )
            write(root, "src/gated/mod.rs", "mod leaf;\n")
            write(root, "src/gated/leaf.rs", "pub const A: u8 = 1;\n")

            gated, unresolved = self.collect(root)

            self.assertEqual(
                gated,
                {root / "src/gated/mod.rs", root / "src/gated/leaf.rs"},
            )
            self.assertEqual(unresolved, [])

    def test_missing_file_is_reported(self):
        with tempfile.TemporaryDirectory() as temp_dir:
            root = Path(temp_dir)
            declaring = write(
                root,
                "src/lib.rs",
                """
                #[cfg(feature = "enterprise")]
                mod elsewhere;
                """,
            )

            gated, unresolved = self.collect(root)

            self.assertEqual(gated, set())
            self.assertEqual(unresolved, [(declaring, "elsewhere")])


if __name__ == "__main__":
    unittest.main()
