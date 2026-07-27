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

"""Regression coverage for OTLP plotting and the outer lifecycle."""

import importlib.util
import json
import os
import shutil
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch


RUNNER_PATH = Path(__file__).resolve().parents[2] / ".github/scripts/query-regression-run.py"
PLOTTER_PATH = Path(__file__).with_name("plot_otlp_trace_report.sh")
SPEC = importlib.util.spec_from_file_location("query_regression_outer_otlp_under_test", RUNNER_PATH)
assert SPEC is not None and SPEC.loader is not None
runner = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = runner
SPEC.loader.exec_module(runner)


class OtlpTraceReportPlotTest(unittest.TestCase):
    def test_plots_all_comparison_metrics_and_thresholds(self) -> None:
        if shutil.which("jq") is None:
            self.skipTest("jq is required for plot script coverage")
        report = {
            "status": "ok",
            "targets": [
                {
                    "name": name,
                    "metrics": {
                        "accepted_spans": spans,
                        "accepted_spans_per_second": rate,
                        "mean_http_latency_ms": latency,
                        "failure_count": 0,
                    },
                    "visibility": {"observed_rows": spans},
                }
                for name, spans, rate, latency in (("base", 100, 10.0, 3.0), ("candidate", 120, 12.0, 2.5))
            ],
            "thresholds": [
                {"target": "base", "threshold": "max_failure_count", "status": "passed", "actual": 0, "limit": 0},
                {
                    "threshold": "max_candidate_throughput_regression_pct",
                    "status": "passed",
                    "actual_pct": -20.0,
                    "limit_pct": 20.0,
                    "base": 10.0,
                    "candidate": 12.0,
                },
            ],
        }

        with tempfile.TemporaryDirectory() as tmpdir:
            root = Path(tmpdir)
            report_path = root / "report.json"
            report_path.write_text(json.dumps(report))
            fake_bin = root / "bin"
            fake_bin.mkdir()
            uplot = fake_bin / "uplot"
            uplot.write_text('#!/usr/bin/env bash\nprintf "CALL %s\\n" "$*" >>"$UPLOT_LOG"\ncat >>"$UPLOT_LOG"\n')
            uplot.chmod(0o755)
            log_path = root / "uplot.log"
            env = os.environ.copy()
            env["PATH"] = f"{fake_bin}{os.pathsep}{env['PATH']}"
            env["UPLOT_LOG"] = str(log_path)

            result = subprocess.run([str(PLOTTER_PATH), str(report_path)], env=env, text=True, capture_output=True, check=False)
            log = log_path.read_text()
            report["targets"][1]["visibility"]["observed_rows"] = 119
            report_path.write_text(json.dumps(report))
            mismatch = subprocess.run([str(PLOTTER_PATH), str(report_path)], env=env, text=True, capture_output=True, check=False)

        self.assertEqual(result.returncode, 0, result.stderr)
        self.assertEqual(log.count("CALL "), 5)
        for metric in ("Accepted spans", "Visible table rows", "Throughput", "Mean HTTP latency", "Failures"):
            self.assertIn(metric, log)
        self.assertIn("base\t100", log)
        self.assertIn("candidate\t12", log)
        self.assertIn("Status: ok", result.stdout)
        self.assertIn("[passed] candidate: accepted=120, visible=120", result.stdout)
        self.assertIn("max_failure_count", result.stdout)
        self.assertIn("max_candidate_throughput_regression_pct", result.stdout)
        self.assertEqual(mismatch.returncode, 1, mismatch.stderr)
        self.assertIn("[failed] candidate: accepted=120, visible=119", mismatch.stdout)


class OtlpTraceOuterLifecycleTest(unittest.TestCase):
    def test_stops_base_before_candidate_and_wires_rust_commands(self) -> None:
        events: list[str] = []
        commands: list[list[str]] = []

        def run(command, **_kwargs):
            commands.append(command)
            phase = command[1]
            events.append(phase)
            output = Path(command[command.index("--output") + 1])
            output.write_text(json.dumps({"status": "ok", "targets": []}))
            return SimpleNamespace(returncode=0)

        def start(target, component, _procs):
            events.append(f"start:{target.name}:{component}")

        def stop(target, component, _procs):
            events.append(f"stop:{target.name}:{component}")

        args = SimpleNamespace(http_timeout="1", otelgen_bin=Path("otelgen"))
        ports = list(range(10000, 10016))
        with (
            tempfile.TemporaryDirectory() as tmpdir,
            patch.object(runner, "allocate_ports", return_value=ports),
            patch.object(runner, "start_component", side_effect=start),
            patch.object(runner, "stop_component", side_effect=stop),
            patch.object(runner.subprocess, "run", side_effect=run),
        ):
            root = Path(tmpdir)
            self.assertEqual(
                runner.run_otlp_case(args, Path("case.toml"), root, Path("base-bin"), Path("candidate-bin"), Path("fixture"), Path("runner")),
                0,
            )
            expected = [
                [
                    "runner", "run-otlp-target", "--case", "case.toml", "--fixture-generator", "fixture",
                    "--otelgen-bin", "otelgen", "--http-port", "10004", "--target-name", "base",
                    "--work-dir", str(root / "base"), "--output", str(root / "base" / "report.json"), "--http-timeout", "1",
                ],
                [
                    "runner", "run-otlp-target", "--case", "case.toml", "--fixture-generator", "fixture",
                    "--otelgen-bin", "otelgen", "--http-port", "10012", "--target-name", "candidate",
                    "--work-dir", str(root / "candidate"), "--output", str(root / "candidate" / "report.json"), "--http-timeout", "1",
                ],
                [
                    "runner", "finalize-otlp", "--case", "case.toml", "--fixture-generator", "fixture",
                    "--base-result", str(root / "base" / "report.json"),
                    "--candidate-result", str(root / "candidate" / "report.json"),
                    "--output", str(root / "query-regression-report.json"),
                ],
            ]
            self.assertEqual(commands, expected)

        self.assertLess(events.index("stop:base:metasrv"), events.index("start:candidate:metasrv"))


if __name__ == "__main__":
    unittest.main()
