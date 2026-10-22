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

import itertools
import os
import subprocess
import unittest
from pathlib import Path


SCRIPT = Path(__file__).with_name("release-linux-architectures.sh")
ARCHITECTURES = ("amd64", "arm64", "riscv64")


class ReleaseLinuxArchitecturesTest(unittest.TestCase):
    def resolve(self, selected, event="workflow_dispatch", publish=False, images=False):
        env = dict(
            os.environ,
            GITHUB_EVENT_NAME=event,
            PUBLISH_GITHUB_RELEASE=str(publish).lower(),
            RELEASE_IMAGES=str(images).lower(),
        )
        for arch in ARCHITECTURES:
            key = f"BUILD_LINUX_{arch.upper()}_ARTIFACTS"
            env.pop(key, None)
            if selected is not None:
                env[key] = str(arch in selected).lower()
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

    def test_checkbox_combinations(self):
        for flags in itertools.product((False, True), repeat=3):
            selected = {arch for arch, enabled in zip(ARCHITECTURES, flags) if enabled}
            for event, publish, images in itertools.product(
                ("workflow_dispatch", "push", "schedule"), (False, True), (False, True)
            ):
                with self.subTest(selected=selected, event=event, publish=publish, images=images):
                    expected = (
                        set(ARCHITECTURES)
                        if event != "workflow_dispatch" or publish or images
                        else selected
                    )
                    self.assert_targets(self.resolve(selected, event, publish, images), expected)

    def test_default_and_omitted_inputs_preserve_previous_behavior(self):
        for selected in (None, set()):
            for event, publish, images in itertools.product(
                ("workflow_dispatch", "push", "schedule"), (False, True), (False, True)
            ):
                with self.subTest(selected=selected, event=event, publish=publish, images=images):
                    # The old build_linux_artifacts switch defaulted to false.
                    old_build_linux = publish or images or event in ("push", "schedule")
                    self.assert_targets(
                        self.resolve(selected, event, publish, images),
                        set(ARCHITECTURES) if old_build_linux else set(),
                    )

    def test_unsupported_event(self):
        result = self.resolve(set(), event="pull_request")
        self.assertNotEqual(result.returncode, 0)
        self.assertIn("Unsupported release event", result.stderr)
        self.assertEqual(result.stdout, "")


if __name__ == "__main__":
    unittest.main()
