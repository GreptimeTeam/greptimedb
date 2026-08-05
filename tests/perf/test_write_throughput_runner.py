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

"""Regression coverage for the write_throughput scenario.

Tests the pure write-measurement math (per-window RPS bucketing, p50/p99
latency, failure rate), threshold enforcement, scenario dispatch, and the
dry-run path. No live database, cluster, or generator subprocess is started.
"""

import importlib.util
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


RUNNER_PATH = Path(__file__).with_name("query_regression_runner.py")
SPEC = importlib.util.spec_from_file_location("query_regression_runner_write_throughput_under_test", RUNNER_PATH)
assert SPEC is not None and SPEC.loader is not None
runner = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = runner
SPEC.loader.exec_module(runner)


def make_remote(**overrides):
    remote = {
        "database": "public",
        "metric": "write_throughput_scheduler",
        "physical_table": "greptime_physical_table",
        "series_count": 2048,
        "samples_per_series": 3600,
        "sample_chunk_size": None,
        "flush_every_sample_chunks": 1,
        "start_unix_millis": 1_704_067_200_000,
        "step_millis": 1000,
        "chunk_series_count": 256,
        "timeout_seconds": 120,
        "value": {"pattern": "linear", "base": 0.0, "step": 0.125, "cardinality": 97, "seed": 0, "run_length": 8, "stall_every": 100, "stall_length": 16, "mixed_every": 5},
        "prom_store": {"pending_rows_flush_interval": "1s", "max_batch_rows": 1_000_000, "max_concurrent_flushes": 256, "worker_channel_capacity": 65_526, "max_inflight_requests": 3000},
    }
    remote.update(overrides)
    return remote


def make_write_measure(**overrides):
    write_measure = {
        "duration_seconds": 60,
        "window_seconds": 5,
        "target_rps": 0,
        "thresholds": {
            "max_failure_rate": 0.05,
            "max_mean_rps_regression_pct": 10.0,
            "max_p99_latency_regression_pct": 10.0,
            "min_rps_absolute": 50_000,
        },
    }
    write_measure.update(overrides)
    return write_measure


class WriteThroughputDispatchTest(unittest.TestCase):
    def test_scenario_accepts_write_throughput_kind(self) -> None:
        case = {"scenario": {"kind": "write_throughput", "remote_write": {}, "write_measure": {}}}
        self.assertEqual(runner.scenario(case)["kind"], "write_throughput")

    def test_unsupported_kind_rejected(self) -> None:
        with self.assertRaises(ValueError):
            runner.scenario({"scenario": {"kind": "not_a_real_kind"}})

    def test_case_tables_returns_metric_table(self) -> None:
        case = {"scenario": {"kind": "write_throughput", "remote_write": make_remote(), "write_measure": make_write_measure()}}
        self.assertEqual(
            runner.case_tables(case),
            [{"database": "public", "name": "write_throughput_scheduler", "engine": "metric", "validate_show_create_engine": False}],
        )

    def test_expected_rows_formula(self) -> None:
        self.assertEqual(runner.expected_remote_write_rows(make_remote()), 2048 * 3600)


class WriteThroughputMeasurementTest(unittest.TestCase):
    def test_windows_bucket_rows_within_a_window(self) -> None:
        chunks = [
            {"status": "ok", "summary": {"rows": 100, "elapsed_seconds": 1.0}},
            {"status": "ok", "summary": {"rows": 100, "elapsed_seconds": 1.0}},
        ]
        windows = runner.write_throughput_windows(chunks, duration_seconds=10, window_seconds=5)
        self.assertEqual(len(windows), 2)
        self.assertEqual(windows[0]["start_offset"], 0)
        self.assertEqual(windows[0]["rows"], 200.0)
        self.assertEqual(windows[0]["rps"], 40.0)
        self.assertEqual(windows[1]["start_offset"], 5)
        self.assertEqual(windows[1]["rows"], 0.0)
        self.assertEqual(windows[1]["rps"], 0.0)

    def test_windows_split_rows_across_window_boundaries(self) -> None:
        chunks = [{"status": "ok", "summary": {"rows": 100, "elapsed_seconds": 10.0}}]
        windows = runner.write_throughput_windows(chunks, duration_seconds=10, window_seconds=5)
        self.assertEqual([w["rows"] for w in windows], [50.0, 50.0])
        self.assertEqual([w["rps"] for w in windows], [10.0, 10.0])

    def test_windows_attribute_partial_overlaps_proportionally(self) -> None:
        chunks = [
            {"status": "ok", "summary": {"rows": 100, "elapsed_seconds": 2.0}},
            {"status": "ok", "summary": {"rows": 100, "elapsed_seconds": 2.0}},
            {"status": "ok", "summary": {"rows": 100, "elapsed_seconds": 2.0}},
        ]
        windows = runner.write_throughput_windows(chunks, duration_seconds=6, window_seconds=3)
        self.assertEqual(len(windows), 2)
        self.assertEqual([w["rows"] for w in windows], [150.0, 150.0])
        self.assertEqual([w["rps"] for w in windows], [50.0, 50.0])

    def test_windows_truncate_at_duration(self) -> None:
        chunks = [{"status": "ok", "summary": {"rows": 100, "elapsed_seconds": 10.0}}]
        windows = runner.write_throughput_windows(chunks, duration_seconds=5, window_seconds=5)
        self.assertEqual(len(windows), 1)
        self.assertEqual(windows[0]["rows"], 50.0)

    def test_measurement_computes_rps_latency_and_failure_rate(self) -> None:
        chunks = [
            {"status": "ok", "summary": {"rows": 1000, "elapsed_seconds": 0.05}},
            {"status": "ok", "summary": {"rows": 1000, "elapsed_seconds": 0.10}},
            {"status": "ok", "summary": {"rows": 1000, "elapsed_seconds": 0.15}},
        ]
        rw = {"chunks": chunks}
        measurement = runner.write_throughput_measurement(rw, make_write_measure(duration_seconds=10, window_seconds=5))
        self.assertEqual(measurement["rows"], 3000)
        self.assertAlmostEqual(measurement["elapsed_seconds"], 0.30)
        self.assertAlmostEqual(measurement["mean_rps"], 10_000.0)
        self.assertEqual(measurement["p50_latency_ms"], 100.0)
        self.assertEqual(measurement["p99_latency_ms"], 150.0)
        self.assertEqual(measurement["failed_chunks"], 0)
        self.assertEqual(measurement["total_chunks"], 3)
        self.assertEqual(measurement["failure_rate"], 0.0)

    def test_measurement_counts_failed_chunks_and_excludes_their_latency(self) -> None:
        chunks = [
            {"status": "ok", "summary": {"rows": 1000, "elapsed_seconds": 1.0}},
            {"status": "failed", "returncode": 1, "elapsed_seconds": 0.5},
        ]
        measurement = runner.write_throughput_measurement({"chunks": chunks}, make_write_measure())
        self.assertEqual(measurement["rows"], 1000)
        self.assertEqual(measurement["failed_chunks"], 1)
        self.assertEqual(measurement["failure_rate"], 0.5)
        self.assertEqual(measurement["p50_latency_ms"], 1000.0)
        self.assertEqual(measurement["p99_latency_ms"], 1000.0)

    def test_measurement_treats_single_invocation_result_as_one_chunk(self) -> None:
        rw = {"status": "ok", "returncode": 0, "summary": {"rows": 5000, "elapsed_seconds": 2.0}}
        measurement = runner.write_throughput_measurement(rw, make_write_measure(duration_seconds=10, window_seconds=5))
        self.assertEqual(measurement["rows"], 5000)
        self.assertAlmostEqual(measurement["mean_rps"], 2500.0)
        self.assertEqual(measurement["failed_chunks"], 0)
        self.assertEqual(measurement["total_chunks"], 1)
        self.assertEqual(measurement["failure_rate"], 0.0)
        self.assertEqual(measurement["p50_latency_ms"], 2000.0)
        self.assertEqual(measurement["windows"][0]["rows"], 5000.0)

    def test_planned_measurement_reports_expected_rows_and_windows(self) -> None:
        measurement = runner.planned_write_throughput_measurement(make_remote(), make_write_measure())
        self.assertEqual(measurement["status"], "planned")
        self.assertEqual(measurement["planned_rows"], 2048 * 3600)
        self.assertIsNone(measurement["mean_rps"])
        self.assertEqual(len(measurement["windows"]), 12)
        self.assertEqual(measurement["windows"][1]["start_offset"], 5)


class WriteThroughputThresholdTest(unittest.TestCase):
    def test_enforcement_passes_when_within_limits(self) -> None:
        base = {"mean_rps": 100_000, "p99_latency_ms": 50.0, "failure_rate": 0.0}
        candidate = {"mean_rps": 90_000, "p99_latency_ms": 55.0, "failure_rate": 0.02}
        results = runner.enforce_write_throughput_thresholds(make_write_measure(), base, candidate)
        by_threshold = {(r.get("target"), r["threshold"]): r for r in results}
        self.assertEqual(len(results), 6)
        self.assertEqual(by_threshold[("base", "max_failure_rate")]["status"], "passed")
        self.assertEqual(by_threshold[("candidate", "max_failure_rate")]["status"], "passed")
        self.assertEqual(by_threshold[("base", "min_rps_absolute")]["status"], "passed")
        self.assertEqual(by_threshold[("candidate", "min_rps_absolute")]["status"], "passed")
        rps = by_threshold[(None, "max_mean_rps_regression_pct")]
        self.assertEqual(rps["status"], "passed")
        self.assertAlmostEqual(rps["actual_pct"], 10.0)
        self.assertAlmostEqual(rps["limit_pct"], 10.0)
        p99 = by_threshold[(None, "max_p99_latency_regression_pct")]
        self.assertEqual(p99["status"], "passed")
        self.assertAlmostEqual(p99["actual_pct"], 10.0)

    def test_enforcement_fails_when_regression_exceeds_limits(self) -> None:
        base = {"mean_rps": 100_000, "p99_latency_ms": 50.0, "failure_rate": 0.0}
        candidate = {"mean_rps": 80_000, "p99_latency_ms": 60.0, "failure_rate": 0.10}
        results = runner.enforce_write_throughput_thresholds(make_write_measure(), base, candidate)
        by_threshold = {(r.get("target"), r["threshold"]): r for r in results}
        self.assertEqual(by_threshold[("candidate", "max_failure_rate")]["status"], "failed")
        self.assertEqual(by_threshold[(None, "max_mean_rps_regression_pct")]["status"], "failed")
        self.assertAlmostEqual(by_threshold[(None, "max_mean_rps_regression_pct")]["actual_pct"], 20.0)
        self.assertEqual(by_threshold[(None, "max_p99_latency_regression_pct")]["status"], "failed")
        self.assertAlmostEqual(by_threshold[(None, "max_p99_latency_regression_pct")]["actual_pct"], 20.0)

    def test_enforcement_fails_on_missing_base_measurement(self) -> None:
        base = {"mean_rps": None, "p99_latency_ms": None, "failure_rate": None}
        candidate = {"mean_rps": 90_000, "p99_latency_ms": 55.0, "failure_rate": 0.0}
        results = runner.enforce_write_throughput_thresholds(make_write_measure(), base, candidate)
        by_threshold = {(r.get("target"), r["threshold"]): r for r in results}
        self.assertEqual(by_threshold[(None, "max_mean_rps_regression_pct")]["status"], "failed")
        self.assertEqual(by_threshold[(None, "max_mean_rps_regression_pct")]["reason"], "missing or zero mean RPS")
        self.assertEqual(by_threshold[(None, "max_p99_latency_regression_pct")]["status"], "failed")
        self.assertEqual(by_threshold[("base", "max_failure_rate")]["status"], "failed")

    def test_planned_thresholds_include_optional_min_rps(self) -> None:
        planned = runner.planned_write_throughput_thresholds(make_write_measure())
        self.assertEqual([p["threshold"] for p in planned], ["max_failure_rate", "max_mean_rps_regression_pct", "max_p99_latency_regression_pct", "min_rps_absolute"])
        self.assertTrue(all(p["status"] == "planned" for p in planned))

        without_min = make_write_measure()
        without_min["thresholds"]["min_rps_absolute"] = None
        planned = runner.planned_write_throughput_thresholds(without_min)
        self.assertEqual([p["threshold"] for p in planned], ["max_failure_rate", "max_mean_rps_regression_pct", "max_p99_latency_regression_pct"])


class WriteThroughputScenarioDryRunTest(unittest.TestCase):
    def test_dry_run_plans_without_subprocess(self) -> None:
        events = []

        class FakeCluster:
            def __init__(self, target):
                self.target = target
                self.stopped = False
                events.append(f"create:{self.target.name}")

            def component_report(self):
                return {}

            def stop_all(self):
                if not self.stopped:
                    self.stopped = True
                    events.append(f"stop:{self.target.name}")

        args = runner.argparse.Namespace(
            fixture_only=False,
            fixture_generator=Path("query_perf_fixture"),
            remote_write_generator=None,
            dry_run=True,
            http_timeout=1.0,
        )
        case = {"scenario": {"kind": "write_throughput", "remote_write": make_remote(), "write_measure": make_write_measure()}}

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            targets = [
                runner.make_target("base", Path("/bin/true"), root, list(range(10_000, 10_008))),
                runner.make_target("candidate", Path("/bin/true"), root, list(range(10_008, 10_016))),
            ]
            report = {"targets": []}
            with (
                patch.object(runner, "DistributedCluster", FakeCluster),
                patch.object(runner, "run_command") as run_cmd,
            ):
                runner.run_write_throughput_scenario(args, case, Path("case.toml"), targets, report)

            run_cmd.assert_not_called()
            self.assertEqual(events, ["create:base", "stop:base", "create:candidate", "stop:candidate"])
            self.assertEqual(report["status"], "planned")
            self.assertEqual([t["status"] for t in report["targets"]], ["planned", "planned"])
            self.assertEqual(report["targets"][0]["write_measurement"]["status"], "planned")
            self.assertEqual(len(report["thresholds"]), 4)
            self.assertTrue(all(t["status"] == "planned" for t in report["thresholds"]))
            # The dry-run report file for each target is written under the work dir.
            self.assertTrue(targets[0].report_path.exists())


if __name__ == "__main__":
    unittest.main()
