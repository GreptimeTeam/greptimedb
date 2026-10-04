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

"""Regression coverage for the outer remote-write lifecycle."""

import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


RUNNER_PATH = Path(__file__).resolve().parents[2] / ".github/scripts/query-regression-run.py"
SPEC = importlib.util.spec_from_file_location("query_regression_outer_under_test", RUNNER_PATH)
assert SPEC is not None and SPEC.loader is not None
runner = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = runner
SPEC.loader.exec_module(runner)


class RemoteWriteLifecycleTest(unittest.TestCase):
    def setUp(self) -> None:
        self.args = type("Args", (), {"http_timeout": "1"})()
        self.ports = list(range(10000, 10016))

    def test_measure_report_is_finalized_after_datanodes_stop(self) -> None:
        events: list[str] = []

        def run(command, **_kwargs):
            phase = command[1]
            events.append(phase)
            if phase == "measure":
                output = Path(command[command.index("--output") + 1])
                output.write_text(json.dumps({"targets": [], "thresholds": [], "status": "ok"}))
            return type("Result", (), {"returncode": 0})()

        def start(target, component, _procs):
            events.append(f"start:{target.name}:{component}")

        def stop(target, component, _procs):
            events.append(f"stop:{target.name}:{component}")

        with (
            tempfile.TemporaryDirectory() as tmpdir,
            patch.object(runner, "allocate_ports", return_value=self.ports),
            patch.object(runner, "start_component", side_effect=start),
            patch.object(runner, "stop_component", side_effect=stop),
            patch.object(runner.subprocess, "run", side_effect=run),
        ):
            self.assertEqual(
                runner.run_remote_case(
                    self.args, Path("case.toml"), Path(tmpdir), Path("base"), Path("candidate"), Path("fixture"), Path("runner")
                ),
                0,
            )
            self.assertTrue((Path(tmpdir) / "query-regression-report.json").exists())

        measure = events.index("measure")
        finalize = events.index("finalize-remote")
        self.assertLess(measure, events.index("stop:base:datanode"))
        self.assertLess(events.index("stop:base:datanode"), finalize)
        self.assertLess(events.index("stop:candidate:datanode"), finalize)
        self.assertIn("stop:base:frontend", events[finalize + 1:])
        self.assertIn("stop:candidate:metasrv", events[finalize + 1:])

    def test_phase_exception_still_cleans_up_without_processes_or_network(self) -> None:
        events: list[str] = []

        def run(command, **_kwargs):
            phase = command[1]
            events.append(phase)
            if phase == "prepare-remote":
                raise RuntimeError("prepare exploded")
            return type("Result", (), {"returncode": 0})()

        def start(target, component, _procs):
            events.append(f"start:{target.name}:{component}")

        def stop(target, component, _procs):
            events.append(f"stop:{target.name}:{component}")

        with (
            tempfile.TemporaryDirectory() as tmpdir,
            patch.object(runner, "allocate_ports", return_value=self.ports),
            patch.object(runner, "start_component", side_effect=start),
            patch.object(runner, "stop_component", side_effect=stop),
            patch.object(runner.subprocess, "run", side_effect=run),
        ):
            with self.assertRaisesRegex(RuntimeError, "prepare exploded"):
                runner.run_remote_case(
                    self.args, Path("case.toml"), Path(tmpdir), Path("base"), Path("candidate"), Path("fixture"), Path("runner")
                )

        self.assertIn("prepare-remote", events)
        self.assertIn("stop:base:metasrv", events)
        self.assertIn("stop:candidate:frontend", events)


if __name__ == "__main__":
    unittest.main()
