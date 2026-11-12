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

"""Regression coverage for remote-write and Docker target lifecycles."""

import importlib.util
import json
import sys
import signal
import subprocess
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import Mock, patch


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


class DockerTargetTest(unittest.TestCase):
    image_id = "sha256:" + "a" * 64

    def test_independent_target_selection_and_image_identity(self) -> None:
        for base_image, candidate_image in (("", ""), ("image:v1", ""), ("", "image:v1"), ("image:v1", "image:v1")):
            with self.subTest(base=base_image, candidate=candidate_image), tempfile.TemporaryDirectory() as tmp:
                args = SimpleNamespace(
                    base_image=base_image, candidate_image=candidate_image,
                    base_bin=Path("base"), candidate_bin=Path("candidate"),
                    base_ref="base-sha", candidate_ref="candidate-sha", work_dir=Path(tmp),
                )
                result = SimpleNamespace(returncode=0, stdout=json.dumps([
                    {"Id": self.image_id, "RepoDigests": ["image@sha256:abc"]},
                ]))
                with patch.object(runner.subprocess, "run", return_value=result) as run:
                    metadata = runner.prepare_targets(args)
                self.assertEqual(run.call_count, int(bool(base_image or candidate_image)))
                for name, image in (("base", base_image), ("candidate", candidate_image)):
                    if image:
                        launcher = getattr(args, f"{name}_bin")
                        self.assertTrue(launcher.stat().st_mode & 0o111)
                        self.assertIn(self.image_id, launcher.read_text())
                        self.assertIn('"$@"', launcher.read_text())
                        self.assertEqual(metadata[name]["image"], image)
                        self.assertEqual(metadata[name]["image_id"], self.image_id)
                    else:
                        self.assertEqual(getattr(args, f"{name}_bin"), Path(name))
                        self.assertEqual(metadata[name], {"kind": "source", "ref": f"{name}-sha"})

    def test_default_source_run_preserves_report_and_refs(self) -> None:
        payload = '{"status":"ok","targets":[],"thresholds":[]}\n'
        with tempfile.TemporaryDirectory() as tmp:
            root = Path(tmp)
            def run_case(args, _case_path, work_dir):
                self.assertEqual((args.base_image, args.candidate_image), ("", ""))
                self.assertEqual((args.base_ref, args.candidate_ref), ("main", "feature"))
                self.assertEqual(args.cargo_profile, "nightly")
                work_dir.mkdir(parents=True)
                (work_dir / "query-regression-report.json").write_text(payload)
                return 0
            argv = ["runner", "--cases", "case.toml", "--work-dir", str(root),
                    "--base-ref", "main", "--candidate-ref", "feature"]
            with (patch.dict(runner.os.environ, {}, clear=True),
                  patch.object(sys, "argv", argv),
                  patch.object(runner, "run_case", side_effect=run_case),
                  patch.object(runner, "write_summary", return_value=0),
                  patch.object(runner.subprocess, "run") as subprocess_run):
                self.assertEqual(runner.main(), 0)
                subprocess_run.assert_not_called()
            reports = list(root.rglob("query-regression-report.json"))
            self.assertEqual(len(reports), 1)
            self.assertEqual(reports[0].read_bytes(), payload.encode())

    def test_missing_image_is_pulled_and_inspected(self) -> None:
        with tempfile.TemporaryDirectory() as tmp:
            args = SimpleNamespace(base_image="image:v1", candidate_image="", base_ref="", candidate_ref="", work_dir=Path(tmp))
            results = [SimpleNamespace(returncode=1), SimpleNamespace(returncode=0),
                       SimpleNamespace(stdout=json.dumps([{"Id": self.image_id}]))]
            with patch.object(runner.subprocess, "run", side_effect=results) as run:
                runner.prepare_targets(args)
            self.assertEqual([call.args[0][:2] for call in run.call_args_list],
                             [["docker", "image"], ["docker", "pull"], ["docker", "image"]])

    def test_container_exit_code_and_cleanup(self) -> None:
        commands = []
        def run(command, **kwargs):
            commands.append(command)
            self.assertTrue(kwargs["start_new_session"])
            return SimpleNamespace(returncode=0, stdout="42\n")
        with patch.object(runner.subprocess, "run", side_effect=run):
            status = runner.docker_target(self.image_id, Path("/tmp/work space"), ["datanode", "scanbench", "--help"])
        self.assertEqual(status, 42)
        create = commands[0]
        self.assertIn("/tmp/work space:/tmp/work space", create)
        self.assertEqual(create[-4:], [self.image_id, "datanode", "scanbench", "--help"])
        self.assertEqual([cmd[1] for cmd in commands], ["create", "start", "inspect", "stop", "rm"])
        self.assertEqual(commands[-1][-1], create[3])

    def test_signal_and_failed_start_both_remove_container(self) -> None:
        for interruption in (True, False):
            commands = []
            def run(command, **kwargs):
                commands.append(command)
                if command[1] == "start":
                    if interruption:
                        signal.getsignal(signal.SIGTERM)(signal.SIGTERM, None)
                    raise subprocess.CalledProcessError(1, command)
                return SimpleNamespace(returncode=0)
            previous = signal.getsignal(signal.SIGTERM)
            with self.subTest(interruption=interruption), patch.object(runner.subprocess, "run", side_effect=run):
                with self.assertRaises(SystemExit if interruption else subprocess.CalledProcessError):
                    runner.docker_target(self.image_id, Path("/tmp/work"), ["metasrv", "start"])
            self.assertEqual([cmd[1] for cmd in commands], ["create", "start", "stop", "rm"])
            self.assertEqual(signal.getsignal(signal.SIGTERM), previous)


    def test_stop_timeout_still_force_removes(self) -> None:
        commands = []
        def run(command, **kwargs):
            commands.append(command)
            if command[1] == "stop":
                raise subprocess.TimeoutExpired(command, 8)
            return SimpleNamespace(returncode=0, stdout="0")
        with patch.object(runner.subprocess, "run", side_effect=run):
            self.assertEqual(runner.docker_target(self.image_id, Path("/tmp/work"), ["--version"]), 0)
        self.assertEqual(commands[-1][1], "rm")

    def test_failed_container_cleanup_is_not_ignored(self) -> None:
        target = runner.make_target("base", Path("launcher"), Path("/tmp/work"), list(range(8)), containerized=True)
        proc = Mock()
        proc.poll.return_value = None
        proc.returncode = 1
        with patch.object(runner.os, "killpg"):
            with self.assertRaisesRegex(RuntimeError, "cleanup failed"):
                runner.stop_component(target, "datanode", {("base", "datanode"): proc})

    def test_cleanup_failure_does_not_skip_other_components(self) -> None:
        target = runner.make_target("base", Path("launcher"), Path("/tmp/work"), list(range(8)), containerized=True)
        with patch.object(runner, "stop_component", side_effect=[RuntimeError("failed"), None, None]) as stop:
            with self.assertRaisesRegex(RuntimeError, "component cleanup failed"):
                runner.stop_all([target], {})
        self.assertEqual([call.args[1] for call in stop.call_args_list], ["frontend", "datanode", "metasrv"])


if __name__ == "__main__":
    unittest.main()
