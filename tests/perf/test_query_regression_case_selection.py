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

"""Coverage for query-regression case group selection."""

import importlib.util
import json
import sys
import unittest
from pathlib import Path


RUNNER_PATH = Path(__file__).parents[2] / ".github/scripts/query-regression-run.py"
SPEC = importlib.util.spec_from_file_location("query_regression_run_under_test", RUNNER_PATH)
assert SPEC is not None and SPEC.loader is not None
runner = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = runner
SPEC.loader.exec_module(runner)


class QueryRegressionCaseSelectionTest(unittest.TestCase):
    def test_all_selects_the_routine_default_cases(self) -> None:
        self.assertEqual(
            runner.split_cases(["all"]),
            [
                "tests/perf/query_cases/smoke_direct_sst/case.toml",
                "tests/perf/query_cases/prom_remote_write_seeded_random/case.toml",
                "tests/perf/query_cases/prom_remote_write_run_heavy/case.toml",
                "tests/perf/query_cases/prom_remote_write_mixed_every/case.toml",
                "tests/perf/query_cases/prom_remote_write_integer_counter/case.toml",
                "tests/perf/query_cases/promql_range_boundary/case.toml",
                "tests/perf/query_cases/promql_instant_last_row_9034/case.toml",
            ],
        )

    def test_heavy_selects_only_remote_write_7913(self) -> None:
        self.assertEqual(
            runner.split_cases(["heavy"]),
            ["tests/perf/query_cases/prom_remote_write_7913/case.toml"],
        )

    def test_explicit_paths_remain_selectable(self) -> None:
        case = "tests/perf/query_cases/sql_topk_order_by/case.toml"
        self.assertEqual(runner.split_cases([case]), [case])


class QueryRegressionCpuProfileTest(unittest.TestCase):
    def make_target(self, name: str, root: Path, port_offset: int) -> object:
        return runner.make_target(
            name,
            Path(f"/{name}/greptime"),
            root,
            list(range(port_offset, port_offset + 8)),
        )

    def test_direct_profiles_run_after_measurement_and_before_cleanup(self) -> None:
        from tempfile import TemporaryDirectory
        from types import SimpleNamespace
        from unittest.mock import patch

        with TemporaryDirectory() as directory:
            root = Path(directory)
            events: list[str] = []

            def run(command: list[str], **_kwargs: object) -> object:
                if "prepare-direct" in command:
                    Path(command[command.index("--output") + 1]).write_text(json.dumps({"fixtures": []}))
                if "measure" in command:
                    events.append("measure")
                    Path(command[command.index("--output") + 1]).write_text(
                        json.dumps({"targets": [{"measurements": [{"samples": ["ordinary-sample"]}]}]}),
                    )
                return SimpleNamespace(returncode=0)

            def profile(*_args: object, **_kwargs: object) -> str:
                events.append("profile")
                return "success"

            def stop(_target: object, _name: str, _procs: object) -> None:
                events.append("cleanup" if "measure" in events else "setup-stop")

            args = SimpleNamespace(http_timeout="1", allow_large_fixture="false")
            plan = {
                "scenario": {
                    "tables": [{"database": "public"}],
                    "queries": [{"name": "selected", "kind": "sql", "query": "SELECT 1", "iterations": 1}],
                    # Measurements belong to the ordinary runner report, not CPU profiling.
                    "measurements": [{"samples": ["ordinary-sample"]}],
                },
            }
            with (
                patch.object(runner, "allocate_ports", return_value=list(range(1000, 1016))),
                patch.object(runner, "start_component", side_effect=lambda *_args: events.append("start")),
                patch.object(runner, "stop_component", side_effect=stop),
                patch.object(runner.subprocess, "run", side_effect=run),
                patch.object(runner, "collect_cpu_profiles", side_effect=profile) as collect,
            ):
                status = runner.run_direct_case(
                    args, root / "case.toml", root, Path("base"), Path("candidate"), Path("fixture"), Path("runner"), plan,
                )

            self.assertEqual(0, status)
            self.assertLess(events.index("measure"), events.index("profile"))
            self.assertLess(events.index("profile"), events.index("cleanup"))
            self.assertEqual(plan["scenario"]["measurements"][0]["samples"], ["ordinary-sample"])
            report = json.loads((root / "query-regression-report.json").read_text())
            self.assertEqual(report["targets"][0]["measurements"][0]["samples"], ["ordinary-sample"])
            self.assertEqual([target.name for target in collect.call_args.args[3]], ["base", "candidate"])

    def test_cpu_profiles_write_base_and_candidate_manifest_without_network(self) -> None:
        from tempfile import TemporaryDirectory
        from types import SimpleNamespace
        from unittest.mock import patch

        with TemporaryDirectory() as directory:
            root = Path(directory)
            targets = [self.make_target("base", root, 1000), self.make_target("candidate", root, 2000)]
            seen: list[str] = []

            def collect(target: object, _query: object, work_dir: Path, target_manifest: dict[str, object], _timeout: float) -> None:
                seen.append(target.name)
                target_manifest["query_execution"] = {"attempts": 1, "successes": 1, "failures": 0, "errors": []}
                target_manifest["profiles"] = [{"component": "frontend", "path": str(work_dir / target.name), "status": "success"}]

            plan = {"scenario": {"kind": "direct_readable_sst", "tables": [{"database": "public"}], "queries": [
                {"name": "not-supported", "kind": "prom_http", "query": "metric", "iterations": 1},
                {"name": "chosen", "kind": "tql", "query": "TQL ANALYZE x", "iterations": 1},
            ]}}
            with (
                patch.object(runner, "binary_version", side_effect=lambda binary: {"path": str(binary), "version": "test"}),
                patch.object(runner, "collect_target_cpu_profiles", side_effect=collect),
            ):
                status = runner.collect_cpu_profiles(SimpleNamespace(http_timeout="1"), plan, root, targets)

            manifest = json.loads((root / runner.CPU_PROFILE_MANIFEST).read_text())
            self.assertEqual("success", status)
            self.assertEqual(["base", "candidate"], seen)
            self.assertEqual("chosen", manifest["query"]["selected"]["name"])
            self.assertEqual(["base", "candidate"], [target["role"] for target in manifest["targets"]])

    def test_constant_tag_concat_cases_time_and_profile_ordinary_tql_eval(self) -> None:
        for name in ("promql_constant_tag_concat", "promql_constant_tag_concat_ms"):
            case_path = Path(__file__).parent / "query_cases" / name / "case.toml"
            queries = [
                line.removeprefix('query = "').removesuffix('"')
                for line in case_path.read_text().splitlines()
                if line.startswith('query = "')
            ]
            self.assertTrue(queries)
            self.assertTrue(all(query.startswith("TQL EVAL ") for query in queries))

            plan = {"scenario": {"kind": "direct_readable_sst", "tables": [{"database": "public"}], "queries": [
                {"kind": "tql", "query": query, "iterations": 1} for query in queries
            ]}}
            profile_query, reason = runner.select_cpu_profile_query(plan)
            self.assertIsNone(reason)
            self.assertTrue(profile_query["query"].startswith("TQL EVAL "))

    def test_constant_tag_concat_analyze_is_postmeasurement_and_has_its_own_full_response_artifact(self) -> None:
        from tempfile import TemporaryDirectory
        from types import SimpleNamespace
        from unittest.mock import patch

        with TemporaryDirectory() as directory:
            root = Path(directory)
            work_dir = root / "work"
            case_path = root / "promql_constant_tag_concat" / "case.toml"
            events: list[str] = []
            plan = {
                "scenario": {
                    "kind": "direct_readable_sst",
                    "tables": [{"database": "public"}],
                    "queries": [{"name": "selected", "kind": "tql", "query": "TQL EVAL (1, 2, '1s') metric", "iterations": 1}],
                },
            }

            def run(command: list[str], **_kwargs: object) -> object:
                if "prepare-direct" in command:
                    Path(command[command.index("--output") + 1]).write_text(json.dumps({"fixtures": []}))
                if "measure" in command:
                    events.append("measure")
                    Path(command[command.index("--output") + 1]).write_text(json.dumps({"samples": ["ordinary"]}))
                return SimpleNamespace(returncode=0)

            def profile(*_args: object, **_kwargs: object) -> str:
                events.append("profile")
                return "success"

            def post(_target: object, query: dict[str, str], _timeout: float) -> tuple[bool, None, dict[str, object]]:
                events.append("analyze")
                self.assertEqual("TQL ANALYZE VERBOSE (1, 2, '1s') metric", query["query"])
                return True, None, {"output": [{"rows": [["all diagnostic rows"]]}]}

            def stop(*_args: object) -> None:
                events.append("cleanup")

            with (
                patch.object(runner, "allocate_ports", return_value=list(range(1000, 1016))),
                patch.object(runner, "start_component"),
                patch.object(runner, "stop_component", side_effect=stop),
                patch.object(runner.subprocess, "run", side_effect=run),
                patch.object(runner, "collect_cpu_profiles", side_effect=profile),
                patch.object(runner, "post_query", side_effect=post),
            ):
                status = runner.run_direct_case(
                    SimpleNamespace(http_timeout="1", allow_large_fixture="false"),
                    case_path, work_dir, Path("base"), Path("candidate"), Path("fixture"), Path("runner"), plan,
                )

            self.assertEqual(0, status)
            self.assertLess(events.index("measure"), events.index("profile"))
            self.assertLess(events.index("profile"), events.index("analyze"))
            self.assertLess(events.index("analyze"), len(events) - 1 - events[::-1].index("cleanup"))
            self.assertEqual(["ordinary"], json.loads((work_dir / "query-regression-report.json").read_text())["samples"])
            manifest = json.loads((work_dir / runner.POSTMEASUREMENT_ANALYZE_MANIFEST).read_text())
            self.assertEqual("success", manifest["status"])
            self.assertEqual({"base", "candidate"}, {target["role"] for target in manifest["targets"]})
            self.assertEqual(
                {"output": [{"rows": [["all diagnostic rows"]]}]},
                manifest["targets"][0]["queries"][0]["response"],
            )

    def test_constant_tag_concat_analyze_failure_is_reported_and_fails_the_case(self) -> None:
        from tempfile import TemporaryDirectory
        from unittest.mock import patch

        with TemporaryDirectory() as directory:
            root = Path(directory)
            target = self.make_target("base", root, 1000)
            plan = {"scenario": {"kind": "direct_readable_sst", "tables": [{"database": "public"}], "queries": [
                {"name": "selected", "kind": "tql", "query": "TQL EVAL (1, 2, '1s') metric", "iterations": 1},
            ]}}
            with patch.object(runner, "post_query", return_value=(False, "HTTP 500", None)):
                status = runner.collect_postmeasurement_analyze(
                    root / "promql_constant_tag_concat" / "case.toml", plan, root, [target], 1.0,
                )

            self.assertEqual("failure", status)
            manifest = json.loads((root / runner.POSTMEASUREMENT_ANALYZE_MANIFEST).read_text())
            self.assertEqual("failure", manifest["status"])
            self.assertIn("HTTP 500", manifest["errors"][0])

    def test_profile_query_response_errors_are_failures(self) -> None:
        self.assertIsNone(runner.query_response_error({"code": 0}))
        self.assertEqual("query response error: denied", runner.query_response_error({"error": "denied"}))
        self.assertEqual("query response error_code 7: bad request", runner.query_response_error({"error_code": 7, "message": "bad request"}))
        self.assertEqual("query response was not a JSON object", runner.query_response_error([]))

    def test_target_cpu_profiles_use_mocked_profile_and_query_http_helpers(self) -> None:
        from tempfile import TemporaryDirectory
        from unittest.mock import patch

        with TemporaryDirectory() as directory:
            root = Path(directory)
            target = self.make_target("base", root, 1000)
            manifest_target: dict[str, object] = {
                "query_execution": {"attempts": 0, "successes": 0, "failures": 0, "errors": []},
                "profiles": [],
            }
            ticks = iter([0.0, 0.0, float(runner.CPU_PROFILE_SECONDS)])
            query = {"name": "chosen", "kind": "sql", "query": "SELECT 1", "database": "public"}
            with (
                patch.object(runner, "download_cpu_profile") as download,
                patch.object(runner, "post_profile_query", return_value=(True, None)) as post,
                patch.object(runner.time, "monotonic", side_effect=lambda: next(ticks)),
            ):
                runner.collect_target_cpu_profiles(target, query, root, manifest_target, 1.0)

            self.assertEqual(2, download.call_count)
            self.assertEqual({target.http_port, target.datanode_http_port}, {call.args[0] for call in download.call_args_list})
            post.assert_called_once_with(target, query, 1.0)
            self.assertEqual(1, manifest_target["query_execution"]["successes"])
            self.assertTrue(all(profile["status"] == "success" for profile in manifest_target["profiles"]))

    def test_unsupported_direct_profile_prints_reason_and_manifest_and_fails(self) -> None:
        from contextlib import redirect_stdout
        from io import StringIO
        from tempfile import TemporaryDirectory
        from types import SimpleNamespace
        from unittest.mock import patch

        with TemporaryDirectory() as directory:
            root = Path(directory)
            targets = [self.make_target("base", root, 1000), self.make_target("candidate", root, 2000)]
            plan = {"scenario": {"kind": "direct_readable_sst", "tables": [{"database": "public"}], "queries": [{"kind": "prom_http", "query": "metric", "iterations": 1}]}}
            output = StringIO()
            with patch.object(runner, "binary_version", return_value={"version": "test"}), redirect_stdout(output):
                status = runner.collect_cpu_profiles(SimpleNamespace(http_timeout="300"), plan, root, targets)

            manifest_path = root / runner.CPU_PROFILE_MANIFEST
            manifest = json.loads(manifest_path.read_text())
            self.assertEqual("unsupported", status)
            self.assertEqual("unsupported", manifest["status"])
            self.assertIn("no timed SQL or TQL query is configured", output.getvalue())
            self.assertIn(str(manifest_path), output.getvalue())
            self.assertNotEqual("success", status)

    def test_unsupported_direct_case_preserves_measurement_and_returns_nonzero(self) -> None:
        from contextlib import redirect_stdout
        from io import StringIO
        from tempfile import TemporaryDirectory
        from types import SimpleNamespace
        from unittest.mock import patch

        with TemporaryDirectory() as directory:
            root = Path(directory)

            def run(command: list[str], **_kwargs: object) -> object:
                if "prepare-direct" in command:
                    Path(command[command.index("--output") + 1]).write_text(json.dumps({"fixtures": []}))
                if "measure" in command:
                    Path(command[command.index("--output") + 1]).write_text(json.dumps({"status": "ok", "samples": ["ordinary"]}))
                return SimpleNamespace(returncode=0)

            args = SimpleNamespace(http_timeout="1", allow_large_fixture="false")
            plan = {"scenario": {"kind": "direct_readable_sst", "tables": [{"database": "public"}], "queries": [{"kind": "prom_http", "query": "metric", "iterations": 1}]}}
            output = StringIO()
            with (
                patch.object(runner, "allocate_ports", return_value=list(range(1000, 1016))),
                patch.object(runner, "start_component"),
                patch.object(runner, "stop_component"),
                patch.object(runner.subprocess, "run", side_effect=run),
                patch.object(runner, "binary_version", return_value={"version": "test"}),
                redirect_stdout(output),
            ):
                status = runner.run_direct_case(
                    args, root / "case.toml", root, Path("base"), Path("candidate"), Path("fixture"), Path("runner"), plan,
                )

            manifest_path = root / runner.CPU_PROFILE_MANIFEST
            self.assertEqual(1, status)
            self.assertEqual(["ordinary"], json.loads((root / "query-regression-report.json").read_text())["samples"])
            self.assertTrue(manifest_path.exists())
            self.assertIn("no timed SQL or TQL query is configured", output.getvalue())
            self.assertIn(str(manifest_path), output.getvalue())

    def test_profile_query_near_deadline_keeps_normal_timeout_and_stops_launching(self) -> None:
        from tempfile import TemporaryDirectory
        from unittest.mock import patch

        with TemporaryDirectory() as directory:
            root = Path(directory)
            target = self.make_target("base", root, 1000)
            manifest_target: dict[str, object] = {
                "query_execution": {"attempts": 0, "successes": 0, "failures": 0, "errors": []},
                "profiles": [],
            }
            # The first query starts just before the deadline; after it completes,
            # the next loop check must not launch another query.
            ticks = iter([0.0, 9.99, 10.01])
            query = {"name": "chosen", "kind": "sql", "query": "SELECT 1", "database": "public"}
            with (
                patch.object(runner, "download_cpu_profile"),
                patch.object(runner, "post_profile_query", return_value=(True, None)) as post,
                patch.object(runner.time, "monotonic", side_effect=lambda: next(ticks)),
            ):
                runner.collect_target_cpu_profiles(target, query, root, manifest_target, 300.0)

            post.assert_called_once_with(target, query, 300.0)
            self.assertEqual(1, manifest_target["query_execution"]["attempts"])

    def test_profile_workers_join_when_query_loop_raises(self) -> None:
        from tempfile import TemporaryDirectory
        from unittest.mock import patch

        class Thread:
            instances: list["Thread"] = []

            def __init__(self, **_kwargs: object) -> None:
                self.started = False
                self.joined = False
                self.instances.append(self)

            def start(self) -> None:
                self.started = True

            def join(self) -> None:
                self.joined = True

        with TemporaryDirectory() as directory:
            root = Path(directory)
            target = self.make_target("base", root, 1000)
            manifest_target: dict[str, object] = {
                "query_execution": {"attempts": 0, "successes": 0, "failures": 0, "errors": []},
                "profiles": [],
            }
            query = {"name": "chosen", "kind": "sql", "query": "SELECT 1", "database": "public"}
            with (
                patch.object(runner.threading, "Thread", Thread),
                patch.object(runner, "post_profile_query", side_effect=RuntimeError("query loop failed")),
                patch.object(runner.time, "monotonic", side_effect=[0.0, 0.0]),
            ):
                with self.assertRaisesRegex(RuntimeError, "query loop failed"):
                    runner.collect_target_cpu_profiles(target, query, root, manifest_target, 300.0)

            self.assertEqual(2, len(Thread.instances))
            self.assertTrue(all(thread.started and thread.joined for thread in Thread.instances))

    def test_second_profile_worker_start_failure_joins_first_and_is_captured(self) -> None:
        from tempfile import TemporaryDirectory
        from types import SimpleNamespace
        from unittest.mock import patch

        class Thread:
            instances: list["Thread"] = []

            def __init__(self, **_kwargs: object) -> None:
                self.number = len(self.instances)
                self.started = False
                self.joined = False
                self.instances.append(self)

            def start(self) -> None:
                self.started = True
                if self.number == 1:
                    raise RuntimeError("second start failed")

            def join(self) -> None:
                self.joined = True

        with TemporaryDirectory() as directory:
            root = Path(directory)
            target = self.make_target("base", root, 1000)
            plan = {"scenario": {"kind": "direct_readable_sst", "tables": [{"database": "public"}], "queries": [{"kind": "sql", "query": "SELECT 1", "iterations": 1}]}}
            with (
                patch.object(runner.threading, "Thread", Thread),
                patch.object(runner, "binary_version", return_value={"version": "test"}),
            ):
                status = runner.collect_cpu_profiles(SimpleNamespace(http_timeout="300"), plan, root, [target])

            manifest = json.loads((root / runner.CPU_PROFILE_MANIFEST).read_text())
            self.assertEqual("failure", status)
            self.assertTrue(Thread.instances[0].started)
            self.assertTrue(Thread.instances[0].joined)
            self.assertTrue(Thread.instances[1].started)
            self.assertFalse(Thread.instances[1].joined)
            self.assertIn("second start failed", manifest["targets"][0]["error"])

    def test_zero_profile_query_successes_fail_manifest(self) -> None:
        from tempfile import TemporaryDirectory
        from types import SimpleNamespace
        from unittest.mock import patch

        with TemporaryDirectory() as directory:
            root = Path(directory)
            targets = [self.make_target("base", root, 1000), self.make_target("candidate", root, 2000)]

            def collect(_target: object, _query: object, _work_dir: Path, target_manifest: dict[str, object], _timeout: float) -> None:
                target_manifest["query_execution"] = {"attempts": 0, "successes": 0, "failures": 0, "errors": []}
                target_manifest["profiles"] = [{"component": "frontend", "status": "success"}]

            plan = {"scenario": {"kind": "direct_readable_sst", "tables": [{"database": "public"}], "queries": [{"kind": "sql", "query": "SELECT 1", "iterations": 1}]}}
            with (
                patch.object(runner, "binary_version", return_value={"version": "test"}),
                patch.object(runner, "collect_target_cpu_profiles", side_effect=collect),
            ):
                status = runner.collect_cpu_profiles(SimpleNamespace(http_timeout="300"), plan, root, targets)

            manifest = json.loads((root / runner.CPU_PROFILE_MANIFEST).read_text())
            self.assertEqual("failure", status)
            self.assertEqual("failure", manifest["status"])

    def test_cpu_profile_failure_writes_manifest_without_network(self) -> None:
        from tempfile import TemporaryDirectory
        from types import SimpleNamespace
        from unittest.mock import patch

        with TemporaryDirectory() as directory:
            root = Path(directory)
            targets = [self.make_target("base", root, 1000), self.make_target("candidate", root, 2000)]

            def collect(_target: object, _query: object, _work_dir: Path, target_manifest: dict[str, object], _timeout: float) -> None:
                target_manifest["query_execution"] = {"attempts": 1, "successes": 0, "failures": 1, "errors": ["query failed"]}
                target_manifest["profiles"] = [{"component": "frontend", "status": "failure", "error": "profile failed"}]

            plan = {"scenario": {"kind": "direct_readable_sst", "tables": [{"database": "public"}], "queries": [{"kind": "sql", "query": "SELECT 1", "iterations": 1}]}}
            with (
                patch.object(runner, "binary_version", return_value={"version": "test"}),
                patch.object(runner, "collect_target_cpu_profiles", side_effect=collect),
            ):
                status = runner.collect_cpu_profiles(SimpleNamespace(http_timeout="1"), plan, root, targets)

            manifest = json.loads((root / runner.CPU_PROFILE_MANIFEST).read_text())
            self.assertEqual("failure", status)
            self.assertEqual("failure", manifest["status"])
            self.assertEqual("profile failed", manifest["targets"][0]["profiles"][0]["error"])
            self.assertEqual(["base", "candidate"], [target["role"] for target in manifest["targets"]])

    def test_remote_profiles_before_finalize_and_failure_keeps_finalize(self) -> None:
        from tempfile import TemporaryDirectory
        from types import SimpleNamespace
        from unittest.mock import patch

        for profile_status in ("success", "failure"):
            with self.subTest(profile_status=profile_status), TemporaryDirectory() as directory:
                root = Path(directory)
                events: list[str] = []

                def run(command: list[str], **_kwargs: object) -> object:
                    if "measure" in command:
                        events.append("measure")
                        Path(command[command.index("--output") + 1]).write_text(json.dumps({"status": "ok"}))
                    if "finalize-remote" in command:
                        events.append("finalize")
                    return SimpleNamespace(returncode=0)

                def profile(_args: object, plan: dict[str, object], _work_dir: Path, _targets: object) -> str:
                    events.append("profile")
                    self.assertEqual("remote_db", plan["scenario"]["remote_write"]["database"])
                    return profile_status

                args = SimpleNamespace(http_timeout="1")
                plan = {"scenario": {"kind": "prom_remote_write_then_query", "remote_write": {"database": "remote_db"}, "queries": [{"kind": "tql", "query": "TQL ANALYZE x", "iterations": 1}]}}
                with (
                    patch.object(runner, "allocate_ports", return_value=list(range(1000, 1016))),
                    patch.object(runner, "start_component"),
                    patch.object(runner, "stop_component", side_effect=lambda _target, name, _procs: events.append(f"stop-{name}")),
                    patch.object(runner.subprocess, "run", side_effect=run),
                    patch.object(runner, "collect_cpu_profiles", side_effect=profile),
                ):
                    status = runner.run_remote_case(
                        args, root / "case.toml", root, Path("base"), Path("candidate"), Path("fixture"), Path("runner"), plan,
                    )

                self.assertLess(events.index("measure"), events.index("profile"))
                self.assertLess(events.index("profile"), events.index("stop-datanode"))
                self.assertLess(events.index("stop-datanode"), events.index("finalize"))
                self.assertEqual(0 if profile_status == "success" else 1, status)

    def test_remote_profile_selection_uses_remote_write_database(self) -> None:
        plan = {"scenario": {"kind": "prom_remote_write_then_query", "remote_write": {"database": "remote_db"}, "queries": [{"name": "chosen", "kind": "sql", "query": "SELECT 1", "iterations": 1}]}}
        query, reason = runner.select_cpu_profile_query(plan)
        self.assertIsNone(reason)
        self.assertEqual("remote_db", query["database"])

    def test_cpu_profile_failure_manifest_is_saved_and_direct_case_cleans_up(self) -> None:
        from tempfile import TemporaryDirectory
        from types import SimpleNamespace
        from unittest.mock import patch

        with TemporaryDirectory() as directory:
            root = Path(directory)
            events: list[str] = []

            def run(command: list[str], **_kwargs: object) -> object:
                if "prepare-direct" in command:
                    Path(command[command.index("--output") + 1]).write_text(json.dumps({"fixtures": []}))
                if "measure" in command:
                    events.append("measure")
                return SimpleNamespace(returncode=0)

            def failure(_args: object, _plan: object, work_dir: Path, _targets: object) -> str:
                manifest = work_dir / runner.CPU_PROFILE_MANIFEST
                manifest.parent.mkdir(parents=True, exist_ok=True)
                manifest.write_text(json.dumps({"status": "failure", "errors": ["profile unavailable"]}))
                return "failure"

            args = SimpleNamespace(http_timeout="1", allow_large_fixture="false")
            plan = {"scenario": {"kind": "direct_readable_sst", "tables": [{"database": "public"}], "queries": [{"kind": "sql", "query": "SELECT 1", "iterations": 1}]}}
            with (
                patch.object(runner, "allocate_ports", return_value=list(range(1000, 1016))),
                patch.object(runner, "start_component"),
                patch.object(runner, "stop_component", side_effect=lambda *_args: events.append("cleanup")),
                patch.object(runner.subprocess, "run", side_effect=run),
                patch.object(runner, "collect_cpu_profiles", side_effect=failure),
            ):
                status = runner.run_direct_case(
                    args, root / "case.toml", root, Path("base"), Path("candidate"), Path("fixture"), Path("runner"), plan,
                )

            self.assertEqual(1, status)
            self.assertIn("cleanup", events)
            self.assertEqual("failure", json.loads((root / runner.CPU_PROFILE_MANIFEST).read_text())["status"])


if __name__ == "__main__":
    unittest.main()
