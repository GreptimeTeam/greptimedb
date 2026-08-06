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
import time
import unittest
from pathlib import Path
from unittest.mock import patch


RUNNER_PATH = Path(__file__).with_name("query_regression_runner.py")
SPEC = importlib.util.spec_from_file_location("query_regression_runner_write_throughput_under_test", RUNNER_PATH)
assert SPEC is not None and SPEC.loader is not None
runner = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = runner
SPEC.loader.exec_module(runner)


class _FakeProc:
    """Minimal stand-in for a live subprocess (poll() returns None = running)."""

    def poll(self) -> None:
        return None


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


def make_mix(**overrides):
    mix = {
        "query_interval_ms": 100,
        "query_parallelism": 2,
        "thresholds": {
            "max_query_failure_rate": 0.05,
            "max_query_p99_regression_pct": 10.0,
        },
    }
    mix.update(overrides)
    return mix


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


class WriteThroughputSchedulerEnvTest(unittest.TestCase):
    """Scheduler on/off env derivation and datanode spawn plumbing."""

    PREFIX = "GREPTIMEDB_DATANODE__RUNTIME__EXPERIMENTAL_WORKLOAD_SCHEDULER"
    SCHEDULER = {"max_concurrent_polls": 16, "query_weight": 2, "write_weight": 8}

    def test_scheduler_env_absent_section_disables_both(self) -> None:
        self.assertEqual(runner.scheduler_env(False, None), {})
        self.assertEqual(runner.scheduler_env(True, None), {})

    def test_scheduler_env_derivation_base_disables_candidate_enables(self) -> None:
        base_env = runner.scheduler_env(False, self.SCHEDULER)
        self.assertEqual(base_env, {f"{self.PREFIX}__ENABLE": "false"})
        candidate_env = runner.scheduler_env(True, self.SCHEDULER)
        self.assertEqual(
            candidate_env,
            {
                f"{self.PREFIX}__ENABLE": "true",
                f"{self.PREFIX}__MAX_CONCURRENT_POLLS": "16",
                f"{self.PREFIX}__QUERY_WEIGHT": "2",
                f"{self.PREFIX}__WRITE_WEIGHT": "8",
            },
        )

    def test_make_target_populates_scheduler_env(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            base = runner.make_target(
                "base", Path("/bin/true"), root, list(range(10_000, 10_008)),
                scheduler_env=runner.scheduler_env(False, self.SCHEDULER),
            )
            candidate = runner.make_target(
                "candidate", Path("/bin/true"), root, list(range(10_008, 10_016)),
                scheduler_env=runner.scheduler_env(True, self.SCHEDULER),
            )
            no_scheduler = runner.make_target(
                "candidate", Path("/bin/true"), root, list(range(10_016, 10_024)),
            )
            self.assertEqual(base.scheduler_env, {f"{self.PREFIX}__ENABLE": "false"})
            self.assertEqual(candidate.scheduler_env[f"{self.PREFIX}__ENABLE"], "true")
            self.assertEqual(candidate.scheduler_env[f"{self.PREFIX}__MAX_CONCURRENT_POLLS"], "16")
            self.assertEqual(candidate.scheduler_env[f"{self.PREFIX}__QUERY_WEIGHT"], "2")
            self.assertEqual(candidate.scheduler_env[f"{self.PREFIX}__WRITE_WEIGHT"], "8")
            self.assertEqual(no_scheduler.scheduler_env, {})

    def test_start_datanode_passes_scheduler_env(self) -> None:
        def spawn_env(target: runner.RunTarget) -> dict[str, str] | None:
            cluster = runner.DistributedCluster(target)
            cluster.procs["metasrv"] = _FakeProc()
            with (
                patch.object(runner.subprocess, "Popen") as popen,
                patch.object(runner, "wait_health"),
            ):
                cluster.start_datanode()
            popen.assert_called_once()
            return popen.call_args.kwargs.get("env")

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            base = runner.make_target(
                "base", Path("/bin/true"), root, list(range(10_000, 10_008)),
                scheduler_env=runner.scheduler_env(False, self.SCHEDULER),
            )
            candidate = runner.make_target(
                "candidate", Path("/bin/true"), root, list(range(10_008, 10_016)),
                scheduler_env=runner.scheduler_env(True, self.SCHEDULER),
            )
            base_env = spawn_env(base)
            self.assertIsNotNone(base_env)
            self.assertEqual(base_env[f"{self.PREFIX}__ENABLE"], "false")
            candidate_env = spawn_env(candidate)
            self.assertIsNotNone(candidate_env)
            self.assertEqual(candidate_env[f"{self.PREFIX}__ENABLE"], "true")
            self.assertEqual(candidate_env[f"{self.PREFIX}__MAX_CONCURRENT_POLLS"], "16")
            self.assertEqual(candidate_env[f"{self.PREFIX}__QUERY_WEIGHT"], "2")
            self.assertEqual(candidate_env[f"{self.PREFIX}__WRITE_WEIGHT"], "8")
            # The scheduler env is merged over os.environ so the datanode keeps
            # PATH and friends.
            self.assertIn("PATH", candidate_env)

    def test_metasrv_and_frontend_do_not_receive_scheduler_env(self) -> None:
        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            candidate = runner.make_target(
                "candidate", Path("/bin/true"), root, list(range(10_008, 10_016)),
                scheduler_env=runner.scheduler_env(True, self.SCHEDULER),
            )
            cluster = runner.DistributedCluster(candidate)
            cluster.procs["metasrv"] = _FakeProc()
            with (
                patch.object(runner.subprocess, "Popen") as popen,
                patch.object(runner, "wait_health"),
            ):
                popen.return_value = _FakeProc()
                cluster.start_metasrv()
                cluster.start_frontend()
            self.assertEqual(popen.call_count, 2)
            for call in popen.call_args_list:
                self.assertIsNone(call.kwargs.get("env"))

    def test_scheduler_report_entry(self) -> None:
        self.assertEqual(
            runner.scheduler_report_entry("base", self.SCHEDULER),
            {"enabled": False, "max_concurrent_polls": 16, "query_weight": 2, "write_weight": 8},
        )
        self.assertEqual(
            runner.scheduler_report_entry("candidate", self.SCHEDULER),
            {"enabled": True, "max_concurrent_polls": 16, "query_weight": 2, "write_weight": 8},
        )
        disabled = {"enabled": False, "max_concurrent_polls": 0, "query_weight": 2, "write_weight": 8}
        self.assertEqual(runner.scheduler_report_entry("base", None), disabled)
        self.assertEqual(runner.scheduler_report_entry("candidate", None), disabled)


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
            # No [scenario.scheduler] section: both targets run scheduler-disabled.
            disabled = {"enabled": False, "max_concurrent_polls": 0, "query_weight": 2, "write_weight": 8}
            self.assertEqual(report["targets"][0]["scheduler"], disabled)
            self.assertEqual(report["targets"][1]["scheduler"], disabled)
            # The dry-run report file for each target is written under the work dir.
            self.assertTrue(targets[0].report_path.exists())


class WriteThroughputMixQueryTest(unittest.TestCase):
    """Pure-function coverage for the mixed read/write query loop."""

    def test_expected_attempts_math(self) -> None:
        self.assertEqual(runner.expected_mix_query_attempts(60, 100, 2), 1200)
        self.assertEqual(runner.expected_mix_query_attempts(10, 3000, 1), 4)
        self.assertEqual(runner.expected_mix_query_attempts(60, 1000, 4), 240)
        # Degenerate parallelism is clamped to 1 (interval is already
        # guaranteed > 0 by the Rust schema; the clamp is defensive).
        self.assertEqual(runner.expected_mix_query_attempts(60, 100, 0), 600)

    def test_mix_query_sql_default_and_override(self) -> None:
        remote = make_remote(physical_table="greptime_physical_table")
        mix = make_mix()
        self.assertEqual(runner.mix_query_sql(mix, remote), 'SELECT count(*) FROM "greptime_physical_table"')
        mix["query_sql"] = "SELECT max(greptime_value) FROM greptime_physical_table"
        self.assertEqual(runner.mix_query_sql(mix, remote), "SELECT max(greptime_value) FROM greptime_physical_table")

    def test_query_loop_runs_and_collects_attempts(self) -> None:
        target = runner.RunTarget("base", Path("/bin/true"), Path("/tmp/wt"), Path("/tmp/wt/data"), Path("/tmp/wt/fixture"), Path("/tmp/wt/report.json"), 4000, 4001, 4002, 4003, 4004, 4005, 4006, 4007, Path("/tmp/wt/datanode/data"))
        mix = make_mix(query_interval_ms=100, query_parallelism=2)

        def fake_http(port, sql, db, timeout):
            return {"ok": True, "status": 200, "latency_ms": 5.0, "response": {"data": []}, "sql": sql}

        with patch.object(runner, "http_post_sql", side_effect=fake_http):
            attempts = runner.run_mix_query_loop(target, mix, "public", duration_seconds=1, http_timeout=5.0, remote=make_remote())
        # 1s at 100ms per thread = 10 attempts/thread; a delayed thread may
        # drop its final query, but both threads must have run repeatedly.
        self.assertGreaterEqual(len(attempts), 12)
        self.assertLessEqual(len(attempts), 20)
        self.assertTrue(all(a["ok"] for a in attempts))
        self.assertTrue(all(a["sql"] == 'SELECT count(*) FROM "greptime_physical_table"' for a in attempts))

    def test_mixed_ingestion_and_queries_join_thread(self) -> None:
        target = runner.RunTarget("base", Path("/bin/true"), Path("/tmp/wt"), Path("/tmp/wt/data"), Path("/tmp/wt/fixture"), Path("/tmp/wt/report.json"), 4000, 4001, 4002, 4003, 4004, 4005, 4006, 4007, Path("/tmp/wt/datanode/data"))
        args = runner.argparse.Namespace(http_timeout=5.0)
        remote = make_remote()

        def fake_ingest(generator, target, remote, args, *, dry_run):
            time.sleep(0.05)
            return {"status": "ok", "chunks": []}, [{"ok": True}]

        with (
            patch.object(runner, "run_write_throughput_ingestion", side_effect=fake_ingest),
            patch.object(runner, "run_mix_query_loop", return_value=[{"ok": True, "latency_ms": 1.0}]),
        ):
            rw, flushes, attempts = runner.run_mixed_ingestion_and_queries(None, target, remote, args, make_mix(), make_write_measure(), dry_run=False)
        self.assertEqual(rw["status"], "ok")
        self.assertEqual(flushes, [{"ok": True}])
        self.assertEqual(attempts, [{"ok": True, "latency_ms": 1.0}])

    def test_mixed_dry_run_skips_threads(self) -> None:
        target = runner.RunTarget("base", Path("/bin/true"), Path("/tmp/wt"), Path("/tmp/wt/data"), Path("/tmp/wt/fixture"), Path("/tmp/wt/report.json"), 4000, 4001, 4002, 4003, 4004, 4005, 4006, 4007, Path("/tmp/wt/datanode/data"))
        args = runner.argparse.Namespace(http_timeout=5.0)
        with (
            patch.object(runner, "run_write_throughput_ingestion", return_value=({"status": "ok"}, [{"ok": True}])) as ingest,
            patch.object(runner.threading, "Thread") as thread_cls,
        ):
            rw, flushes, attempts = runner.run_mixed_ingestion_and_queries(None, target, make_remote(), args, make_mix(), make_write_measure(), dry_run=True)
        thread_cls.assert_not_called()
        ingest.assert_called_once()
        self.assertEqual(attempts, [])


class WriteThroughputMixMeasurementTest(unittest.TestCase):
    def test_measurement_aggregates_latency_and_failures(self) -> None:
        attempts = [
            {"ok": True, "latency_ms": 10.0},
            {"ok": True, "latency_ms": 20.0},
            {"ok": True, "latency_ms": 30.0},
            {"ok": False, "latency_ms": 40.0},
            {"ok": False, "latency_ms": 50.0},
        ]
        measurement = runner.mix_query_measurement(attempts)
        self.assertEqual(measurement["samples"], 5)
        self.assertEqual(measurement["failures"], 2)
        self.assertEqual(measurement["failure_rate"], 0.4)
        self.assertEqual(measurement["latency_samples"], 5)
        self.assertEqual(measurement["p50_ms"], 30.0)
        self.assertEqual(measurement["p99_ms"], 50.0)
        self.assertAlmostEqual(measurement["mean_ms"], 30.0)

    def test_measurement_empty_attempts(self) -> None:
        measurement = runner.mix_query_measurement([])
        self.assertEqual(measurement["samples"], 0)
        self.assertEqual(measurement["failures"], 0)
        self.assertIsNone(measurement["failure_rate"])
        self.assertIsNone(measurement["p50_ms"])
        self.assertIsNone(measurement["p99_ms"])
        self.assertIsNone(measurement["mean_ms"])

    def test_planned_measurement_reports_expected_attempts(self) -> None:
        measurement = runner.planned_mix_query_measurement(make_mix(), make_write_measure())
        self.assertEqual(measurement["status"], "planned")
        self.assertEqual(measurement["planned_attempts"], 1200)
        self.assertEqual(measurement["query_interval_ms"], 100)
        self.assertEqual(measurement["query_parallelism"], 2)
        self.assertIsNone(measurement["p99_ms"])


class WriteThroughputMixThresholdTest(unittest.TestCase):
    def test_combined_write_and_query_gates(self) -> None:
        write_base = {"mean_rps": 100_000, "p99_latency_ms": 50.0, "failure_rate": 0.0}
        write_candidate = {"mean_rps": 90_000, "p99_latency_ms": 55.0, "failure_rate": 0.02}
        query_base = {"p99_ms": 200.0, "failure_rate": 0.0}
        query_candidate = {"p99_ms": 220.0, "failure_rate": 0.01}
        write_results = runner.enforce_write_throughput_thresholds(make_write_measure(), write_base, write_candidate)
        query_results = runner.enforce_mix_query_thresholds(make_mix(), query_base, query_candidate)
        combined = write_results + query_results
        by_key = {(r.get("target"), r["threshold"]): r for r in combined}
        self.assertEqual(len(combined), 9)
        # Write gates unchanged.
        self.assertEqual(by_key[(None, "max_mean_rps_regression_pct")]["status"], "passed")
        self.assertEqual(by_key[(None, "max_p99_latency_regression_pct")]["status"], "passed")
        # Query gates pass within limits.
        self.assertEqual(by_key[("base", "max_query_failure_rate")]["status"], "passed")
        self.assertEqual(by_key[("candidate", "max_query_failure_rate")]["status"], "passed")
        q99 = by_key[(None, "max_query_p99_regression_pct")]
        self.assertEqual(q99["status"], "passed")
        self.assertAlmostEqual(q99["actual_pct"], 10.0)
        self.assertAlmostEqual(q99["limit_pct"], 10.0)

    def test_query_gates_fail_on_regression_and_failures(self) -> None:
        query_base = {"p99_ms": 200.0, "failure_rate": 0.0}
        query_candidate = {"p99_ms": 260.0, "failure_rate": 0.10}
        results = runner.enforce_mix_query_thresholds(make_mix(), query_base, query_candidate)
        by_key = {(r.get("target"), r["threshold"]): r for r in results}
        self.assertEqual(by_key[("candidate", "max_query_failure_rate")]["status"], "failed")
        self.assertEqual(by_key[(None, "max_query_p99_regression_pct")]["status"], "failed")
        self.assertAlmostEqual(by_key[(None, "max_query_p99_regression_pct")]["actual_pct"], 30.0)

    def test_query_gates_fail_on_missing_base(self) -> None:
        query_base = {"p99_ms": None, "failure_rate": None}
        query_candidate = {"p99_ms": 260.0, "failure_rate": 0.0}
        results = runner.enforce_mix_query_thresholds(make_mix(), query_base, query_candidate)
        by_key = {(r.get("target"), r["threshold"]): r for r in results}
        self.assertEqual(by_key[("base", "max_query_failure_rate")]["status"], "failed")
        self.assertEqual(by_key[(None, "max_query_p99_regression_pct")]["status"], "failed")
        self.assertEqual(by_key[(None, "max_query_p99_regression_pct")]["reason"], "missing or zero query p99 latency")

    def test_planned_mix_query_thresholds(self) -> None:
        planned = runner.planned_mix_query_thresholds(make_mix())
        self.assertEqual([p["threshold"] for p in planned], ["max_query_failure_rate", "max_query_p99_regression_pct"])
        self.assertTrue(all(p["status"] == "planned" for p in planned))


class SchedulerPollMetricsTest(unittest.TestCase):
    def test_parse_scheduler_poll_metrics(self) -> None:
        text = (
            "# HELP greptime_workload_scheduler_polls Cumulative task polls admitted by the workload scheduler\n"
            "# TYPE greptime_workload_scheduler_polls gauge\n"
            "greptime_workload_scheduler_polls{workload=\"query\"} 1234\n"
            "greptime_workload_scheduler_polls{workload=\"write\"} 5678\n"
            "greptime_workload_scheduler_queued_tasks{workload=\"query\"} 3\n"
            "greptime_runtime_threads_alive{thread_name=\"global\"} 8\n"
        )
        self.assertEqual(runner.parse_scheduler_poll_metrics(text), {"query": 1234, "write": 5678})
        # Scheduler disabled: no samples at all.
        self.assertEqual(runner.parse_scheduler_poll_metrics("greptime_runtime_threads_alive 8\n"), {})

    def test_scheduler_poll_deltas(self) -> None:
        before = {"values": {"query": 100, "write": 900}}
        after = {"values": {"query": 180, "write": 1780}}
        self.assertEqual(runner.scheduler_poll_deltas(after, before), {"query": 80, "write": 880})

    def test_scheduler_poll_deltas_unknown_on_missing_or_reset(self) -> None:
        before = {"values": {"query": 100}}
        after = {"values": {"query": 180, "write": 20}}
        # write absent before -> unknown; write decreased -> unknown (reset).
        self.assertEqual(runner.scheduler_poll_deltas(after, before), {"query": 80, "write": None})
        before = {"values": {"query": 100, "write": 900}}
        after = {"values": {}}
        self.assertEqual(runner.scheduler_poll_deltas(after, before), {"query": None, "write": None})


class WriteThroughputMixScenarioDryRunTest(unittest.TestCase):
    def test_dry_run_with_mix_plans_write_and_query_gates(self) -> None:
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
        write_measure = make_write_measure()
        write_measure["mix"] = make_mix()
        case = {"scenario": {"kind": "write_throughput", "remote_write": make_remote(), "write_measure": write_measure}}

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
            self.assertEqual(report["status"], "planned")
            self.assertEqual(len(report["thresholds"]), 6)
            self.assertTrue(all(t["status"] == "planned" for t in report["thresholds"]))
            self.assertEqual(
                [t["threshold"] for t in report["thresholds"]],
                ["max_failure_rate", "max_mean_rps_regression_pct", "max_p99_latency_regression_pct", "min_rps_absolute", "max_query_failure_rate", "max_query_p99_regression_pct"],
            )
            for tr in report["targets"]:
                self.assertEqual(tr["write_measurement"]["status"], "planned")
                self.assertEqual(tr["query_measurement"]["status"], "planned")
                self.assertEqual(tr["query_measurement"]["planned_attempts"], 1200)
                self.assertEqual(tr["scheduler_poll_deltas"], {"status": "planned"})
                self.assertEqual(tr["mix"]["query_parallelism"], 2)


if __name__ == "__main__":
    unittest.main()
