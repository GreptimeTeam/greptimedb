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

"""Failure accounting for the metric snapshot runner."""
import importlib.util
import json
from pathlib import Path
import tempfile
import unittest
from unittest.mock import patch

SPEC = importlib.util.spec_from_file_location(
    "metric_snapshot_bench", Path(__file__).resolve().parents[1] / "metric-snapshot-s3-bench.py")
bench = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(bench)


class PhaseTests(unittest.TestCase):
    def test_docker_nanosecond_timestamps(self):
        start = bench.docker_timestamp("2026-10-14T06:06:44.72000818Z")
        end = bench.docker_timestamp("2026-10-14T06:06:46.123456789Z")
        self.assertAlmostEqual((end - start).total_seconds(), 1.403448)

    def test_resource_interference_invalidates_success_and_preserves_fixture(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = bench.Benchmark.__new__(bench.Benchmark)
            runner.round_dir = Path(directory)
            runner.server_pid = 0
            runner.phases = []
            runner.metrics = lambda: ""
            runner.summary = lambda: None
            fixture = runner.round_dir / "source-validation.json"
            fixture.write_text('{"data_sha256":"fixture"}')
            before = {"available": True, "pswpout_pages": 0, "oom_kill": 0}
            after = {"available": True, "pswpout_pages": 1, "oom_kill": 0}
            with patch.object(bench, "host_memory", side_effect=[before, after]):
                with self.assertRaisesRegex(RuntimeError, "Resource interference"):
                    runner.phase("source-validation", lambda: None)
            self.assertEqual(json.loads(fixture.read_text()), {"data_sha256": "fixture"})
            evidence = json.loads((runner.round_dir / "source-validation-resources.json").read_text())
            self.assertEqual(evidence["outcome"], "resource-invalid")
            self.assertEqual(evidence["new_swapout_pages"], 1)

    def test_action_failure_keeps_failed_resource_evidence(self):
        with tempfile.TemporaryDirectory() as directory:
            runner = bench.Benchmark.__new__(bench.Benchmark)
            runner.round_dir = Path(directory)
            runner.server_pid = 0
            runner.phases = []
            runner.metrics = lambda: ""
            runner.summary = lambda: None
            def fail():
                raise RuntimeError("export failed")
            with patch.object(bench, "host_memory", return_value={"available": False}):
                with self.assertRaisesRegex(RuntimeError, "export failed"):
                    runner.phase("export", fail)
            evidence = json.loads((runner.round_dir / "export-resources.json").read_text())
            self.assertEqual(evidence["outcome"], "failed")


if __name__ == "__main__":
    unittest.main()
