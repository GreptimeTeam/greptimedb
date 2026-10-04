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

"""Regression coverage for the OTLP trace GitHub summary."""

import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path


SUMMARY = Path(__file__).parents[2] / ".github/scripts/query-regression-summary.py"


class OtlpTraceSummaryTest(unittest.TestCase):
    def test_renders_ingestion_metrics_and_thresholds(self) -> None:
        report = {
            "status": "ok",
            "case": {"name": "otlp_trace_load"},
            "scenario": {"kind": "otlp_trace_load"},
            "targets": [
                {
                    "name": "base",
                    "status": "measured",
                    "metrics": {
                        "accepted_spans": 100,
                        "accepted_spans_per_second": 200.0,
                        "mean_http_latency_ms": 10.0,
                        "failure_count": 0,
                    },
                    "visibility": {"observed_rows": 100},
                },
                {
                    "name": "candidate",
                    "status": "measured",
                    "metrics": {
                        "accepted_spans": 90,
                        "accepted_spans_per_second": 180.0,
                        "mean_http_latency_ms": 11.0,
                        "failure_count": 0,
                    },
                    "visibility": {"observed_rows": 90},
                },
            ],
            "thresholds": [
                {
                    "threshold": "max_candidate_throughput_regression_pct",
                    "actual_pct": 10.0,
                    "limit_pct": 20.0,
                    "status": "passed",
                }
            ],
        }
        with tempfile.TemporaryDirectory() as tmpdir:
            report_path = Path(tmpdir) / "query-regression-report.json"
            report_path.write_text(json.dumps(report))
            result = subprocess.run(
                [sys.executable, str(SUMMARY), "--report", str(report_path)],
                check=True,
                capture_output=True,
                text=True,
            )

        self.assertIn("### OTLP trace comparison", result.stdout)
        self.assertIn("| base | 100 | 100 | 200.00 | 10.00 | 0 |", result.stdout)
        self.assertIn(
            "| max_candidate_throughput_regression_pct | base vs candidate | 10.00% | 20.00% | ✅ passed | N/A |",
            result.stdout,
        )
        self.assertNotIn("### Query comparison", result.stdout)


if __name__ == "__main__":
    unittest.main()
