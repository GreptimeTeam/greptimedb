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

"""Regression coverage for the local OTLP trace load lifecycle and metrics."""

import importlib.util
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from unittest.mock import patch


RUNNER_PATH = Path(__file__).with_name("query_regression_runner.py")
SPEC = importlib.util.spec_from_file_location("query_regression_runner_otlp_under_test", RUNNER_PATH)
assert SPEC is not None and SPEC.loader is not None
runner = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = runner
SPEC.loader.exec_module(runner)


class OtlpTraceLoadTest(unittest.TestCase):
    def test_stops_base_cluster_before_creating_candidate_cluster(self) -> None:
        events = []

        class FakeCluster:
            def __init__(self, target):
                self.target = target
                self.stopped = False
                events.append(f"create:{target.name}")

            def component_report(self):
                return {}

            def stop_all(self):
                if not self.stopped:
                    self.stopped = True
                    events.append(f"stop:{self.target.name}")

        load = {
            "database": "public",
            "table": "opentelemetry_traces",
            "pipeline": "greptime_trace_v1",
            "duration_seconds": 120,
            "warmup_seconds": 60,
            "rate": 50_000,
            "workers": 4,
            "workload": "microservices",
            "exporter_shards": 4,
            "visibility_timeout_seconds": 1,
            "thresholds": {
                "max_candidate_throughput_regression_pct": 20,
                "max_candidate_mean_latency_regression_pct": 20,
                "max_failure_count": 0,
            },
        }
        args = runner.argparse.Namespace(fixture_only=False, otelgen_bin=None, dry_run=True, http_timeout=1.0)

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            targets = [
                runner.make_target("base", Path("/bin/true"), root, list(range(10_000, 10_008))),
                runner.make_target("candidate", Path("/bin/true"), root, list(range(10_008, 10_016))),
            ]
            with patch.object(runner, "DistributedCluster", FakeCluster):
                runner.run_otlp_trace_load_scenario(
                    args,
                    {"scenario": {"kind": "otlp_trace_load", "load": load}},
                    targets,
                    {"targets": []},
                )

        self.assertEqual(events, ["create:base", "stop:base", "create:candidate", "stop:candidate"])

    def test_warmup_lifecycle_and_labeled_metric_deltas(self) -> None:
        class FakeProcess:
            def __init__(self, *_args, **_kwargs):
                self.returncode = None
                self.wait_timeouts = []

            def wait(self, timeout):
                self.wait_timeouts.append(timeout)
                if len(self.wait_timeouts) == 1:
                    raise subprocess.TimeoutExpired("otelgen", timeout)
                self.returncode = 0
                return 0

            def poll(self):
                return self.returncode

            def kill(self):
                self.returncode = -9

        def snapshot(text: str, captured: float):
            return {"captured_monotonic_seconds": captured, "values": runner.parse_prometheus_metrics(text)}

        snapshots = [
            snapshot("greptime_frontend_otlp_traces_rows 10\n", 0.0),
            snapshot(
                'greptime_frontend_otlp_traces_rows 110\n'
                'greptime_servers_http_otlp_traces_elapsed_sum{db="public"} 1\n'
                'greptime_servers_http_otlp_traces_elapsed_count{db="public"} 10\n',
                5.0,
            ),
            snapshot(
                'greptime_frontend_otlp_traces_rows 310\n'
                'greptime_frontend_otlp_traces_failure_count{label="decode"} 1\n'
                'greptime_frontend_otlp_traces_failure_count{label="write"} 2\n'
                'greptime_servers_http_otlp_traces_elapsed_sum{db="public"} 3\n'
                'greptime_servers_http_otlp_traces_elapsed_count{db="public"} 30\n',
                15.0,
            ),
        ]
        load = {
            "database": "public",
            "table": "opentelemetry_traces",
            "pipeline": "greptime_trace_v1",
            "duration_seconds": 120,
            "warmup_seconds": 60,
            "rate": 50_000,
            "workers": 4,
            "workload": "microservices",
            "exporter_shards": 4,
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            target = runner.make_target("base", Path("/bin/true"), Path(tmpdir), list(range(10_000, 10_008)))
            process = FakeProcess()
            with (
                patch.object(runner, "fetch_otlp_metrics", side_effect=snapshots),
                patch.object(runner.subprocess, "Popen", return_value=process),
                patch.object(runner.time, "monotonic", side_effect=[0.0, 120.0]),
            ):
                result = runner.run_otelgen_load(Path("/bin/otelgen"), target, load, 1.0, dry_run=False)

        self.assertEqual(result["status"], "ok")
        self.assertEqual(process.wait_timeouts, [60, 120])
        metrics = runner.summarize_otlp_metrics(result)
        self.assertEqual(metrics["accepted_spans"], 300)
        self.assertEqual(metrics["measurement_accepted_spans"], 200)
        self.assertEqual(metrics["accepted_spans_per_second"], 20.0)
        self.assertEqual(metrics["http_requests"], 20)
        self.assertEqual(metrics["mean_http_latency_ms"], 100.0)
        self.assertEqual(metrics["failure_count"], 3)


if __name__ == "__main__":
    unittest.main()
