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

import os
import subprocess
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("release-linux-architectures.sh")
ARCHITECTURES = ("amd64", "arm64", "riscv64")
SELECTIONS = {
    "none": set(),
    "amd64": {"amd64"},
    "arm64": {"arm64"},
    "riscv64": {"riscv64"},
    "amd64,arm64": {"amd64", "arm64"},
    "amd64,riscv64": {"amd64", "riscv64"},
    "arm64,riscv64": {"arm64", "riscv64"},
    "all": set(ARCHITECTURES),
}


class ReleaseLinuxArchitecturesTest(unittest.TestCase):
    def resolve(self, selection, event="workflow_dispatch", publish=False, images=False):
        env = dict(
            os.environ,
            GITHUB_EVENT_NAME=event,
            LINUX_ARCHITECTURES=selection,
            PUBLISH_GITHUB_RELEASE=str(publish).lower(),
            RELEASE_IMAGES=str(images).lower(),
        )
        return subprocess.run(
            ["bash", str(SCRIPT)], env=env, text=True, capture_output=True, check=False
        )

    def assert_targets(self, result, targets):
        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(result.stderr, "")
        self.assertEqual(
            result.stdout,
            "".join(f"{arch}={str(arch in targets).lower()}\n" for arch in ARCHITECTURES),
        )

    def test_manual_selections_and_publishing(self):
        for selection, targets in SELECTIONS.items():
            for publish in (False, True):
                for images in (False, True):
                    with self.subTest(selection=selection, publish=publish, images=images):
                        result = self.resolve(selection, publish=publish, images=images)
                        if publish and targets != set(ARCHITECTURES):
                            self.assertNotEqual(result.returncode, 0)
                            self.assertIn("GitHub release publishing requires all", result.stderr)
                            self.assertEqual(result.stdout, "")
                        elif images and not {"amd64", "arm64"} <= targets:
                            self.assertNotEqual(result.returncode, 0)
                            self.assertIn("Image publishing requires amd64 and arm64", result.stderr)
                            self.assertEqual(result.stdout, "")
                        else:
                            self.assert_targets(result, targets)

    def test_auto_preserves_publishing_defaults(self):
        for selection in ("", "auto"):
            for publish in (False, True):
                for images in (False, True):
                    with self.subTest(selection=selection, publish=publish, images=images):
                        self.assert_targets(
                            self.resolve(selection, publish=publish, images=images),
                            set(ARCHITECTURES) if publish or images else set(),
                        )

    def test_automatic_releases_build_all(self):
        for event in ("push", "schedule"):
            for selection in ("", "auto", *SELECTIONS):
                with self.subTest(event=event, selection=selection):
                    self.assert_targets(self.resolve(selection, event), set(ARCHITECTURES))

    def test_invalid_selection(self):
        for selection in ("true", "false", "amd", "amd64,unknown", "arm64; echo injected"):
            with self.subTest(selection=selection):
                result = self.resolve(selection)
                self.assertNotEqual(result.returncode, 0)
                self.assertIn("Invalid Linux architecture selection", result.stderr)
                self.assertEqual(result.stdout, "")

    def test_unsupported_event(self):
        result = self.resolve("all", event="pull_request")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Unsupported release event", result.stderr)
        self.assertEqual(result.stdout, "")


if __name__ == "__main__":
    unittest.main()
