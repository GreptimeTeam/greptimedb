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

"""Checks for helper image extraction and unchanged source-build defaults."""

import importlib.util
import json
import subprocess
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch

ROOT = Path(__file__).resolve().parents[2]
spec = importlib.util.spec_from_file_location("helpers", ROOT / ".github/scripts/query-regression-helpers.py")
helpers = importlib.util.module_from_spec(spec)
spec.loader.exec_module(helpers)
IMAGE = "registry/tools/helpers@sha256:" + "a" * 64
REVISION = "b" * 40


class HelpersTest(unittest.TestCase):
    def test_pin_and_digest_validation(self):
        with tempfile.TemporaryDirectory() as tmp:
            pin = Path(tmp) / "pin"
            pin.write_text(IMAGE + "\n")
            self.assertEqual(helpers.resolve_image("pinned", pin), IMAGE)
            for value in ("", "registry/helpers:latest", IMAGE + "\nmalicious", "--help"):
                with self.subTest(value=value), self.assertRaises(ValueError):
                    helpers.resolve_image(value)
            pin.write_text("")
            with self.assertRaises(ValueError):
                helpers.resolve_image("pinned", pin)

    def test_extraction_provenance_and_cleanup_on_failure(self):
        for fail in (False, True):
            with self.subTest(fail=fail), tempfile.TemporaryDirectory() as tmp:
                destination = Path(tmp)
                calls = []

                def run(*args):
                    calls.append(args)
                    if args[:3] == ("docker", "image", "inspect"):
                        return json.dumps([{"Id": "sha256:image", "Config": {"Labels": {
                            "org.opencontainers.image.revision": REVISION}}}])
                    if args[:2] == ("docker", "create"):
                        return "container"
                    if args[:2] == ("docker", "cp"):
                        if fail:
                            raise subprocess.CalledProcessError(1, args)
                        Path(args[-1]).write_bytes(b"binary")
                    return ""

                with patch.object(helpers, "run", side_effect=run):
                    if fail:
                        with self.assertRaises(subprocess.CalledProcessError):
                            helpers.extract(IMAGE, destination)
                        self.assertFalse((destination / "helpers-image.json").exists())
                    else:
                        self.assertEqual(helpers.extract(IMAGE, destination), {
                            "image": IMAGE, "image_id": "sha256:image", "source_revision": REVISION})
                        for binary in helpers.BINARIES:
                            self.assertIn((str(destination / binary), "--help"), calls)
                    self.assertEqual(calls[-1], ("docker", "rm", "--force", "--volumes", "container"))

    def test_missing_revision_fails_before_container_creation(self):
        with tempfile.TemporaryDirectory() as tmp, patch.object(
            helpers, "run", side_effect=["", '[{"Config": {}}]']
        ) as run:
            with self.assertRaises(ValueError):
                helpers.extract(IMAGE, Path(tmp))
            self.assertEqual(run.call_count, 2)

    def test_candidate_build_matrix(self):
        # Execute the actual workflow shell with recorder commands. Changing
        # only image inputs must preserve source defaults and skip opted-in builds.
        import os
        workflow = (ROOT / ".github/workflows/query-regression.yml").read_text()
        step = workflow.split("      - name: Build candidate greptime and query regression helpers\n", 1)[1]
        step = step.split("      - name:", 1)[0].split("        run: |\n", 1)[1]
        script = "\n".join(line[10:] for line in step.splitlines())
        for candidate in ("", "server-image" ):
            for image in ("", IMAGE):
                with self.subTest(candidate=candidate, helpers=image), tempfile.TemporaryDirectory() as tmp:
                    root = Path(tmp)
                    log = root / "commands"
                    for command in ("cargo", "cp", "sccache"):
                        path = root / command
                        path.write_text('#!/bin/sh\nprintf "%s\\n" "' + command + ' $*" >> "$COMMAND_LOG"\n')
                        path.chmod(0o755)
                    env = dict(os.environ, PATH=tmp + os.pathsep + os.environ["PATH"],
                               COMMAND_LOG=str(log), CANDIDATE_IMAGE=candidate, HELPERS_IMAGE=image,
                               CARGO_PROFILE="nightly", CARGO_TARGET_DIR=str(root / "target"),
                               GITHUB_WORKSPACE=tmp, GITHUB_STEP_SUMMARY=str(root / "summary"))
                    subprocess.run(["bash", "-c", script], env=env, check=True, capture_output=True)
                    builds = [line for line in log.read_text().splitlines() if line.startswith("cargo ")]
                    expected = ([] if candidate else ["greptime"]) + ([] if image else list(helpers.BINARIES))
                    self.assertEqual([line.split("--bin ")[1].split()[0] for line in builds], expected)


if __name__ == "__main__":
    unittest.main()
