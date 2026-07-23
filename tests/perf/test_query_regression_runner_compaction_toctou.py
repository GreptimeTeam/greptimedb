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


class RemoteWriteCompactionToctouTest(unittest.TestCase):
    def controlled_remote(self) -> dict:
        return {
            "database": "public",
            "metric": "cpu_usage",
            "physical_table": "cpu_usage_physical",
            "series_count": 2,
            "samples_per_series": 10,
            "sample_chunk_size": None,
            "visibility_timeout_seconds": 1,
            "storage": {"inspect": True, "column": "greptime_value", "include_metadata_files": False, "root_suffix": None, "planned_thresholds": [], "min_files": 1, "min_files_with_column": 1, "max_total_file_size_bytes": None, "max_column_compressed_size_bytes": None, "max_column_uncompressed_size_bytes": None, "require_encodings": [], "forbid_encodings": []},
            "read_bench": {"enabled": True, "iterations": 2, "rounds": 2, "projection": [], "projections": [{"name": "all", "columns": None}], "max_files": None, "parquetbench": True, "scanbench": True, "parquet_reader": "direct", "scan_scanner": "seq", "parallelism": 1},
            "logical_stream_verification": {"labels": ["host"], "timestamp_column": "ts", "value_column": "value"},
            "prom_store": {"pending_rows_flush_interval": "1s", "max_batch_rows": 1, "max_concurrent_flushes": 1, "worker_channel_capacity": 1, "max_inflight_requests": 1},
        }

    def controlled_args(self) -> argparse.Namespace:
        return argparse.Namespace(fixture_only=False, fixture_generator=Path("/bin/true"), remote_write_generator=None, storage_inspector=None, candidate_bin=Path("/bin/true"), dry_run=False, http_timeout=1.0, reuse_work_dir=False)

    def controlled_targets(self, root: Path):
        return [runner.make_target("base", Path("/bin/true"), root, list(range(10000, 10008))), runner.make_target("candidate", Path("/bin/true"), root, list(range(10008, 10016)))]

    def test_controlled_b4_equal_incomplete_visibility_aborts_all_timing(self) -> None:
        remote = self.controlled_remote()
        case = {"scenario": {"kind": "prom_remote_write_then_query", "remote_write": remote, "queries": []}}

        class Cluster:
            def __init__(self, target): self.target = target
            def start_all(self, _): pass
            def stop_component(self, _): pass
            def stop_all(self): pass
            def component_report(self): return {}

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            targets = self.controlled_targets(root)
            report = {"targets": [], "thresholds": []}
            incomplete = {"ok": True, "row_count_ok": True, "observed_rows": 19}
            with (
                patch.object(runner, "DistributedCluster", Cluster),
                patch.object(runner, "run_remote_write_ingestion", return_value=({"status": "ok"}, [{"ok": True}])),
                patch.object(runner, "http_post_sql", return_value={"ok": True}),
                patch.object(runner, "poll_expected_count", side_effect=[dict(incomplete), dict(incomplete)]),
                patch.object(runner, "run_storage_inspection", return_value={"status": "ok", "summary": {"files": []}}),
                patch.object(runner, "artifact_layout_evidence", return_value={"layout_hash": "a", "artifact_hash": "a", "files": []}),
                patch.object(runner, "run_logical_digests", side_effect=AssertionError("digest must not run")),
                patch.object(runner, "run_queries", side_effect=AssertionError("TQL must not run")),
                patch.object(runner, "run_paired_read_bench", side_effect=AssertionError("offline bench must not run")),
            ):
                runner.run_remote_write_scenario(self.controlled_args(), case, root / "case.toml", targets, report)
        self.assertEqual(report["status"], "failed")
        self.assertEqual(len(report["targets"]), 2)
        for target in report["targets"]:
            evidence = next(error for error in target["validation_errors"] if error["phase"] == "planned_row_visibility")
            self.assertEqual(evidence["planned_rows"], 20)
            self.assertEqual(evidence["visibility"]["observed_rows"], 19)

    def test_controlled_b4_post_tql_mutation_skips_offline_for_pair(self) -> None:
        remote = self.controlled_remote()
        case = {"scenario": {"kind": "prom_remote_write_then_query", "remote_write": remote, "queries": []}}
        clusters = []

        class Cluster:
            def __init__(self, target): self.target = target; clusters.append(self)
            def start_all(self, _): pass
            def start_datanode(self): pass
            def stop_component(self, _): pass
            def stop_all(self): pass
            def component_report(self): return {}

        query_ok = {"validation": [], "validation_errors": [], "measurements": [], "status": "ok"}
        digests = {name: {"target": name, "status": "observed", "expected_rows": 20, "report": {"rows": 20, "digest": "a" * 64}} for name in ("base", "candidate")}
        evidence = iter([
            {"layout_hash": "a", "artifact_hash": "a", "files": []},
            {"layout_hash": "a", "artifact_hash": "a", "files": []},
            {"layout_hash": "a", "artifact_hash": "changed", "files": []},
            {"layout_hash": "a", "artifact_hash": "a", "files": []},
            {"layout_hash": "a", "artifact_hash": "changed", "files": []},
            {"layout_hash": "a", "artifact_hash": "a", "files": []},
        ])
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            targets = self.controlled_targets(root)
            report = {"targets": [], "thresholds": []}
            with (
                patch.object(runner, "DistributedCluster", Cluster),
                patch.object(runner, "run_remote_write_ingestion", return_value=({"status": "ok"}, [{"ok": True}])),
                patch.object(runner, "http_post_sql", return_value={"ok": True}),
                patch.object(runner, "poll_expected_count", side_effect=[{"ok": True, "row_count_ok": True, "observed_rows": 20}, {"ok": True, "row_count_ok": True, "observed_rows": 20}]),
                patch.object(runner, "run_storage_inspection", return_value={"status": "ok", "summary": {"files": []}}),
                patch.object(runner, "artifact_layout_evidence", side_effect=lambda *_args, **_kwargs: next(evidence)),
                patch.object(runner, "run_logical_digests", return_value=digests),
                patch.object(runner, "logical_digest_equality", return_value={"status": "ok", "reports": digests, "checks": {"row_count_equal": True, "expected_rows_equal": True, "canonicalization_equal": True, "digest_equal": True}}),
                patch.object(runner, "run_queries", return_value=query_ok),
                patch.object(runner, "run_paired_read_bench", side_effect=AssertionError("offline bench must not run after mutation")),
            ):
                runner.run_remote_write_scenario(self.controlled_args(), case, root / "case.toml", targets, report)
        self.assertEqual(report["status"], "failed")
        unstable = report["targets"][0]
        self.assertEqual(unstable["status"], "failed")
        mutation = next(error for error in unstable["validation_errors"] if error["phase"] == "post_tql_artifact_stability")
        self.assertEqual(mutation["before"]["layout_hash"], mutation["after"]["layout_hash"])
        self.assertNotEqual(mutation["before"]["artifact_hash"], mutation["after"]["artifact_hash"])
        self.assertTrue(all(target["read_bench"]["status"] == "skipped" and "post-TQL mutation" in target["read_bench"]["reason"] for target in report["targets"]))

    def test_final_mutation_fails_stable_peer_after_pairwise_skip(self) -> None:
        remote = self.controlled_remote()
        case = {"scenario": {"kind": "prom_remote_write_then_query", "remote_write": remote, "queries": []}}

        class Cluster:
            def __init__(self, target): self.target = target
            def start_all(self, _): pass
            def start_datanode(self): pass
            def stop_component(self, _): pass
            def stop_all(self): pass
            def component_report(self): return {}

        query_ok = {"validation": [], "validation_errors": [], "measurements": [], "status": "ok"}
        digests = {name: {"target": name, "status": "observed", "expected_rows": 20, "report": {"rows": 20, "digest": "a" * 64}} for name in ("base", "candidate")}
        evidence = iter([
            {"layout_hash": "a", "artifact_hash": "a", "files": []}, {"layout_hash": "a", "artifact_hash": "a", "files": []},
            {"layout_hash": "a", "artifact_hash": "changed", "files": []}, {"layout_hash": "a", "artifact_hash": "a", "files": []},
            {"layout_hash": "a", "artifact_hash": "changed", "files": []}, {"layout_hash": "a", "artifact_hash": "final-change", "files": []},
        ])
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            targets = self.controlled_targets(root)
            report = {"targets": [], "thresholds": []}
            with (
                patch.object(runner, "DistributedCluster", Cluster),
                patch.object(runner, "run_remote_write_ingestion", return_value=({"status": "ok"}, [{"ok": True}])),
                patch.object(runner, "http_post_sql", return_value={"ok": True}),
                patch.object(runner, "poll_expected_count", side_effect=[{"ok": True, "row_count_ok": True, "observed_rows": 20}, {"ok": True, "row_count_ok": True, "observed_rows": 20}]),
                patch.object(runner, "run_storage_inspection", return_value={"status": "ok", "summary": {"files": []}}),
                patch.object(runner, "artifact_layout_evidence", side_effect=lambda *_args, **_kwargs: next(evidence)),
                patch.object(runner, "run_logical_digests", return_value=digests),
                patch.object(runner, "logical_digest_equality", return_value={"status": "ok", "reports": digests, "checks": {"row_count_equal": True, "expected_rows_equal": True, "canonicalization_equal": True, "digest_equal": True}}),
                patch.object(runner, "run_queries", return_value=query_ok),
                patch.object(runner, "run_paired_read_bench", side_effect=AssertionError("offline bench must not run")),
            ):
                runner.run_remote_write_scenario(self.controlled_args(), case, root / "case.toml", targets, report)
        self.assertEqual(report["status"], "failed")
        self.assertTrue(any(error["phase"] == "post_tql_artifact_stability" for error in report["targets"][0]["validation_errors"]))
        self.assertTrue(any(error["phase"] == "artifact_stability" for error in report["targets"][1]["validation_errors"]))
        self.assertTrue(all(target["status"] == "failed" and target["read_bench"]["status"] == "skipped" and "post-TQL mutation" in target["read_bench"]["reason"] for target in report["targets"]))

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
