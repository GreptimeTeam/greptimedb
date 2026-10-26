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

"""Focused tests for the external native VMbench descriptor dispatch."""

import importlib.util
import subprocess
import sys
import tempfile
import types
import unittest
from pathlib import Path
from unittest.mock import patch


RUNNER_PATH = Path(__file__).parents[2] / ".github/scripts/query-regression-run.py"
SPEC = importlib.util.spec_from_file_location("query_regression_run_vmbench", RUNNER_PATH)
assert SPEC is not None and SPEC.loader is not None
runner = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = runner
SPEC.loader.exec_module(runner)


class NativeVmbenchDispatchTest(unittest.TestCase):
    def setUp(self) -> None:
        self.temp = tempfile.TemporaryDirectory()
        self.root = Path(self.temp.name).resolve()
        self.args = types.SimpleNamespace(
            candidate_src=self.root,
            cargo_profile="release",
            base_src=self.root / "base",
            base_bin=None,
            candidate_bin=None,
            fixture_generator=None,
            runner=None,
            candidate_ref="candidate-sha",
            base_ref="base-sha",
            run_url="https://example/run/1",
        )

    def tearDown(self) -> None:
        self.temp.cleanup()

    def test_exact_descriptor_dispatches_adapter_with_absolute_paths_and_status(self) -> None:
        descriptor = self.root / "tests/perf/native_promql_vmbench_lite/benchmark.toml"
        work = self.root / "work"
        expected = [
            sys.executable,
            str((self.root / ".github/scripts/native-promql-vmbench-lite.py").resolve()),
            "--descriptor", str(descriptor.resolve()),
            "--candidate-bin", str((self.root / "target/release/greptime").resolve()),
            "--candidate-ref", "candidate-sha",
            "--base-ref", "base-sha",
            "--work-dir", str(work.resolve()),
            "--run-url", "https://example/run/1",
        ]
        with patch.object(runner.subprocess, "run", return_value=subprocess.CompletedProcess(expected, 23)) as run, patch.object(
            runner, "load_plan", side_effect=AssertionError("external descriptor entered Rust planner")
        ):
            self.assertEqual(runner.run_case(self.args, descriptor, work), 23)
        run.assert_called_once_with(expected, check=False)

    def test_noncanonical_path_keeps_rust_planner(self) -> None:
        descriptor = self.root / "tests/perf/native_promql_vmbench_lite/benchmark.toml"
        alternate = descriptor.parent / "other-benchmark.toml"
        with patch.object(runner, "load_plan", return_value={"scenario": {"kind": "nope"}}) as load_plan:
            with self.assertRaisesRegex(ValueError, "unsupported scenario"):
                runner.run_case(self.args, alternate, self.root / "work")
        load_plan.assert_called_once()


if __name__ == "__main__":
    unittest.main()
