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

"""Regression coverage for quiescing remote-write storage before read benches."""

import argparse
import importlib.util
import json
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


RUNNER_PATH = Path(__file__).with_name("query_regression_runner.py")
SPEC = importlib.util.spec_from_file_location("query_regression_runner_under_test", RUNNER_PATH)
assert SPEC is not None and SPEC.loader is not None
runner = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = runner
SPEC.loader.exec_module(runner)


class ReportOutputTest(unittest.TestCase):
    def test_final_report_output_defaults_to_stdout_or_writes_to_file(self) -> None:
        report = {"status": "ok"}
        expected = json.dumps(report, indent=2, sort_keys=True)

        with patch("builtins.print") as print_mock:
            runner.output_report(report, None)
        print_mock.assert_called_once_with(expected)

        with tempfile.TemporaryDirectory() as tmpdir:
            output = Path(tmpdir) / "nested" / "report.json"
            with patch("builtins.print") as print_mock:
                runner.output_report(report, output)
            print_mock.assert_not_called()
            self.assertEqual(output.read_text(), expected + "\n")


class RemoteWriteCompactionToctouTest(unittest.TestCase):
    def test_quiesces_each_target_before_storage_inspection_and_read_bench(self) -> None:
        lifecycle: dict[str, list[str]] = {"base": [], "candidate": []}
        clusters = []

        class RecordingCluster:
            def __init__(self, target):
                self.target = target
                self.active = set()
                self.stop_all_calls = 0
                clusters.append(self)

            def start_all(self, _config_path):
                self.active.update(("metasrv", "datanode", "frontend"))

            def stop_component(self, name):
                if name in self.active:
                    lifecycle[self.target.name].append(f'cluster.stop_component("{name}")')
                    self.active.remove(name)

            def stop_all(self):
                self.stop_all_calls += 1
                for name in ("frontend", "datanode", "metasrv"):
                    self.stop_component(name)

            def component_report(self):
                return {}

        def run_queries(target, _case, _tables, _http_timeout):
            lifecycle[target.name].append("query measurement completes")
            return {"validation": [], "validation_errors": [], "measurements": [], "status": "ok"}

        def inspect_storage(_helper, target, _storage, *, dry_run):
            self.assertFalse(dry_run)
            lifecycle[target.name].append("storage inspection")
            return {"status": "ok", "summary": {"summary": {}}}

        def read_bench(_binary, target, _read_bench, _inspection, *, dry_run):
            self.assertFalse(dry_run)
            lifecycle[target.name].append("read bench")
            return {"status": "ok"}

        remote_write = {
            "database": "public",
            "metric": "cpu_usage",
            "physical_table": "cpu_usage_physical",
            "series_count": 1,
            "samples_per_series": 1,
            "visibility_timeout_seconds": 1,
            "storage": {
                "inspect": True,
                "min_files": None,
                "min_files_with_column": None,
                "max_total_file_size_bytes": None,
                "max_column_compressed_size_bytes": None,
                "max_column_uncompressed_size_bytes": None,
                "require_encodings": [],
                "forbid_encodings": [],
            },
            "read_bench": {"enabled": True},
            "prom_store": {
                "pending_rows_flush_interval": "1s",
                "max_batch_rows": 1,
                "max_concurrent_flushes": 1,
                "worker_channel_capacity": 1,
                "max_inflight_requests": 1,
            },
        }
        case = {
            "scenario": {
                "kind": "prom_remote_write_then_query",
                "remote_write": remote_write,
                "queries": [],
            }
        }
        args = argparse.Namespace(
            fixture_only=False,
            fixture_generator=Path("/bin/true"),
            remote_write_generator=None,
            storage_inspector=None,
            candidate_bin=Path("/bin/true"),
            dry_run=False,
            http_timeout=1.0,
        )

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            targets = [
                runner.make_target("base", Path("/bin/true"), root, list(range(10000, 10008))),
                runner.make_target("candidate", Path("/bin/true"), root, list(range(10008, 10016))),
            ]
            report = {"targets": [], "thresholds": []}
            with (
                patch.object(runner, "DistributedCluster", RecordingCluster),
                patch.object(runner, "run_remote_write_ingestion", return_value=({"status": "ok"}, [{"ok": True}])),
                patch.object(runner, "http_post_sql", return_value={"ok": True}),
                patch.object(runner, "poll_expected_count", return_value={"ok": True, "row_count_ok": True}),
                patch.object(runner, "run_queries", side_effect=run_queries),
                patch.object(runner, "run_storage_inspection", side_effect=inspect_storage),
                patch.object(runner, "run_read_bench", side_effect=read_bench),
            ):
                runner.run_remote_write_scenario(args, case, root / "case.toml", targets, report)

        for cluster in clusters:
            self.assertEqual(cluster.stop_all_calls, 1)
            self.assertEqual(cluster.active, set())
            cluster.stop_all()
            self.assertEqual(cluster.stop_all_calls, 2)
            self.assertEqual(cluster.active, set())

        expected = [
            "query measurement completes",
            'cluster.stop_component("datanode")',
            "storage inspection",
            "read bench",
        ]
        relevant = {name: [event for event in events if event in expected] for name, events in lifecycle.items()}
        for target_name in ("base", "candidate"):
            with self.subTest(target=target_name):
                self.assertEqual(relevant[target_name], expected)


if __name__ == "__main__":
    unittest.main()
