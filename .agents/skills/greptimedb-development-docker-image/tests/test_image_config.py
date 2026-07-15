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

import importlib
import stat
import sys
import tempfile
import unittest
from unittest import mock
from pathlib import Path

SCRIPTS = Path(__file__).resolve().parents[1] / "scripts"
sys.path.insert(0, str(SCRIPTS))

image_config = importlib.import_module("image_config")


VALUES = {"IMAGE_REGISTRY": "registry.example.com/team", "IMAGE_REPOSITORY": "greptime-dev", "IMAGE_TAG": "dev-001"}


class ImageConfigTest(unittest.TestCase):
    def test_parse_supported_bounded_dotenv(self):
        text = """\n# comment\n export IMAGE_REGISTRY = \"registry.example.com/team\" # note\nIMAGE_REPOSITORY='greptime-dev'\nIMAGE_TAG=dev-001 # safe comment\nOTHER=$HOME\n"""
        self.assertEqual(image_config.parse_env(text), VALUES)

    def test_parser_does_not_validate_values(self):
        self.assertEqual(image_config.parse_env("IMAGE_TAG=not valid\n"), {"IMAGE_TAG": "not valid"})

    def test_parser_rejects_unmatched_and_unsupported_syntax(self):
        with self.assertRaises(ValueError):
            image_config.parse_env('IMAGE_TAG="unterminated\n')
        with self.assertRaises(ValueError):
            image_config.parse_env("IMAGE_TAG=$(date)\n")

    def test_validation_and_reference(self):
        image_config.validate_values(VALUES)
        self.assertEqual(image_config.image_reference(VALUES), "registry.example.com/team/greptime-dev:dev-001")
        local = dict(VALUES, IMAGE_REGISTRY="")
        self.assertEqual(image_config.image_reference(local), "greptime-dev:dev-001")
        image_config.validate_values(dict(VALUES, IMAGE_REGISTRY="localhost"))
        image_config.validate_values(dict(VALUES, IMAGE_REGISTRY="example.com"))
        with self.assertRaises(ValueError):
            image_config.validate_values(dict(VALUES, IMAGE_REGISTRY="registry/team"))

    def test_atomic_update_preserves_mode_and_canonicalizes_duplicates(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".env"
            path.write_text("# keep\nIMAGE_TAG=old\nIMAGE_TAG=duplicate\nOTHER=value\n")
            path.chmod(0o640)
            self.assertTrue(image_config.update_env(path, VALUES))
            self.assertEqual(stat.S_IMODE(path.stat().st_mode), 0o640)
            self.assertEqual(path.read_text(), "# keep\nIMAGE_TAG=dev-001\nOTHER=value\nIMAGE_REGISTRY=registry.example.com/team\nIMAGE_REPOSITORY=greptime-dev\n")
            self.assertFalse(image_config.update_env(path, VALUES))

    def test_update_rejects_symlink(self):
        with tempfile.TemporaryDirectory() as directory:
            target = Path(directory) / "target"
            target.write_text("IMAGE_TAG=old\n")
            link = Path(directory) / ".env"
            link.symlink_to(target)
            with self.assertRaises(ValueError):
                image_config.update_env(link, VALUES)
            self.assertEqual(target.read_text(), "IMAGE_TAG=old\n")

    def test_update_clears_omitted_registry_and_uses_private_new_file_mode(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".env"
            path.write_text("IMAGE_REGISTRY=registry.example.com\nIMAGE_REPOSITORY=old\nIMAGE_TAG=old\n")
            values = {"IMAGE_REPOSITORY": "greptime-dev", "IMAGE_TAG": "dev-001"}
            image_config.update_env(path, values)
            self.assertIn("IMAGE_REGISTRY=\n", path.read_text())
            new_path = Path(directory) / "new.env"
            image_config.update_env(new_path, VALUES)
            self.assertEqual(stat.S_IMODE(new_path.stat().st_mode), 0o600)

    def test_failed_replace_keeps_original_and_removes_temporary_file(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / ".env"
            path.write_text("OTHER=value\n")
            with mock.patch("image_config.os.replace", side_effect=OSError("no space")):
                with self.assertRaises(OSError):
                    image_config.update_env(path, VALUES)
            self.assertEqual(path.read_text(), "OTHER=value\n")
            self.assertEqual(list(Path(directory).glob("..env.*")), [])


if __name__ == "__main__":
    unittest.main()
