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

"""Offline tests of the object store WAL S3 benchmark plan and summary."""

import importlib.util
import json
import subprocess
import sys
import tempfile
import unittest
from pathlib import Path

SCRIPTS = Path(__file__).parents[2] / ".github/scripts"


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


bench = load("oswal_s3_bench", "oswal-s3-bench.py")
summary = load("oswal_s3_bench_summary", "oswal-s3-bench-summary.py")

ACK_BUCKETS = """# TYPE greptime_logstore_object_store_wal_append_ack_seconds histogram
greptime_logstore_object_store_wal_append_ack_seconds_bucket{le="0.064"} 0
greptime_logstore_object_store_wal_append_ack_seconds_bucket{le="0.128"} 50
greptime_logstore_object_store_wal_append_ack_seconds_bucket{le="0.256"} 100
greptime_logstore_object_store_wal_append_ack_seconds_bucket{le="+Inf"} 100
greptime_logstore_object_store_wal_append_ack_seconds_sum 12
greptime_logstore_object_store_wal_append_ack_seconds_count 100
greptime_logstore_object_store_wal_created_objects_total 120
greptime_logstore_object_store_wal_stalled_appends_total 3
greptime_logstore_object_store_wal_poisoned_total 0
greptime_logstore_kafka_client_produce_elapsed_count 9
"""


class PlanTest(unittest.TestCase):
    def test_default_plan_alternates_the_order_between_rounds(self):
        plan = bench.build_plan("1,8", "2000000", "1000", "2", smoke=False)
        self.assertEqual([c["id"] for c in plan["cells"]], [
            "1-1-R", "1-1-D", "1-1-E", "1-8-R", "1-8-D", "1-8-E",
            "2-1-E", "2-1-D", "2-1-R", "2-8-E", "2-8-D", "2-8-R"])
        self.assertEqual(plan["generator_args"], ["--data-profile", "D"])
        self.assertEqual((plan["rows"], plan["batch_rows"], plan["data_profile"]), (2_000_000, 1000, "D"))
        by_config = {c["config"]: c["wal"] for c in plan["cells"]}
        self.assertEqual(by_config["R"], {"PROVIDER": "raft_engine", "SYNC_WRITE": "true", "DIR": "/wal"})
        self.assertEqual(by_config["D"]["ACK_MODE"], "durable")
        self.assertEqual(by_config["E"]["ACK_MODE"], "enqueued")
        self.assertEqual(by_config["D"]["PROVIDER"], by_config["E"]["PROVIDER"])

    def test_smoke_overrides_rows_and_rounds(self):
        plan = bench.build_plan("8", "2000000", "1000", "2", smoke=True)
        self.assertEqual((plan["rows"], plan["rounds"], plan["data_profile"]), (50_000, 1, "custom"))
        self.assertEqual(plan["generator_args"], ["--line-count", "50000"])
        self.assertEqual([c["id"] for c in plan["cells"]], ["1-8-R", "1-8-D", "1-8-E"])

    def test_invalid_inputs_are_rejected(self):
        for args in (("0", "10", "10", "1"), ("1,1", "10", "10", "1"), ("1", "abc", "10", "1"),
                     ("1", "10", "10", "11"), ("1,", "10", "10", "1")):
            with self.subTest(args=args), self.assertRaises(ValueError):
                bench.build_plan(*args, smoke=False)

    def test_nightly_tag_is_the_newest_by_date(self):
        tags = ["latest", "v1.3.0-alpha.1-nightly-20260907", "v1.4.0-nightly-20261012",
                "v1.4.0-nightly-20261005", "nightly-arm64"]
        self.assertEqual(bench.newest_nightly(tags), "v1.4.0-nightly-20261012")
        with self.assertRaises(ValueError):
            bench.newest_nightly(["latest"])
        self.assertEqual(bench.resolve_tag("v1.4.0-nightly-20261012"), "v1.4.0-nightly-20261012")
        with self.assertRaises(ValueError):
            bench.resolve_tag("bad tag")

    def test_run_script_parses(self):
        subprocess.run(["bash", "-n", str(SCRIPTS / "oswal-s3-bench-run.sh")], check=True)


class SummaryTest(unittest.TestCase):
    def test_quantile_interpolates_like_prometheus(self):
        buckets = summary.parse_metrics(ACK_BUCKETS)["buckets"]["append_ack_seconds"]
        self.assertAlmostEqual(summary.quantile(0.5, buckets), 0.128)
        self.assertAlmostEqual(summary.quantile(0.99, buckets), 0.128 + 0.128 * 49 / 50)
        self.assertAlmostEqual(summary.quantile(0.25, buckets), 0.064 + 0.064 * 25 / 50)
        self.assertIsNone(summary.quantile(0.5, []))
        overflow = [(0.1, 1.0), (float("inf"), 4.0)]
        self.assertEqual(summary.quantile(0.99, overflow), 0.1)

    def test_parse_keeps_only_object_store_wal_families(self):
        parsed = summary.parse_metrics(ACK_BUCKETS)
        self.assertEqual(parsed["samples"]["created_objects_total"], 120)
        self.assertNotIn("kafka_client_produce_elapsed_count", parsed["samples"])

    def write_cell(self, root, cell_id, rows_per_second, exit_code=0, metrics=""):
        round_number, concurrency, config = cell_id.split("-")
        path = root / "cells" / cell_id
        (path / "greptimedb-1").mkdir(parents=True)
        (path / "cell.json").write_text(json.dumps({
            "id": cell_id, "round": int(round_number), "concurrency": int(concurrency), "config": config,
            "exit_code": exit_code, "wal_device": "/dev/nvme1n1", "version": "1.4.0", "load_wall_seconds": 61.0}))
        if exit_code == 0:
            (path / "greptimedb-1" / "load-result.json").write_text(json.dumps({
                "count_after": 6000, "load_seconds": 60.0, "rows_per_second": rows_per_second}))
        (path / "metrics.prom").write_text(metrics)

    def test_summary_reports_cells_and_geometric_mean_ratios(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = bench.build_plan("8", "6000", "1000", "2", smoke=False)
            (root / "plan.json").write_text(json.dumps(plan))
            (root / "run.json").write_text(json.dumps({"rows": 6000, "batch_rows": 1000, "instance_type": "c7id.2xlarge"}))
            speeds = {"1-8-R": 100.0, "1-8-D": 80.0, "1-8-E": 90.0, "2-8-E": 99.0, "2-8-D": 50.0, "2-8-R": 110.0}
            for cell_id, speed in speeds.items():
                self.write_cell(root, cell_id, speed, metrics=ACK_BUCKETS if cell_id.endswith("D") else "")
            output = subprocess.run([sys.executable, str(SCRIPTS / "oswal-s3-bench-summary.py"), "--root", str(root)],
                                    check=True, capture_output=True, text=True).stdout
            result = json.loads((root / "summary.json").read_text())
            self.assertEqual([c["id"] for c in result["cells"]], [c["id"] for c in plan["cells"]])
            d_ratio = next(r for r in result["ratios"] if r["ratio"] == "D/R")
            self.assertAlmostEqual(d_ratio["geomean"], (0.8 * (50 / 110)) ** 0.5)
            self.assertAlmostEqual(next(r for r in result["ratios"] if r["ratio"] == "E/R")["geomean"], 0.9)
            d_cell = next(c for c in result["cells"] if c["id"] == "1-8-D")
            self.assertEqual(d_cell["creates_per_minute"], 120.0)
            self.assertEqual(d_cell["stalled_appends_total"], 3)
            self.assertIn("| 1-8-D | 80 | 60.0 | 61.0 | 6,000 | 128.0 | 253.4 | 120 | 120.0 | 3 | 0 | — | — |", output)

    def test_a_failed_cell_leaves_its_ratio_undefined(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write_cell(root, "1-1-R", 100.0)
            self.write_cell(root, "1-1-D", 0.0, exit_code=1)
            rows = [summary.load_cell(path) for path in sorted((root / "cells").iterdir())]
            ratio = next(r for r in summary.ratios(rows) if r["ratio"] == "D/R")
            self.assertEqual((ratio["per_round"], ratio["geomean"]), ([None], None))
            self.assertIn("1-1-D (failed, exit 1)", summary.render({}, rows, summary.ratios(rows)))


if __name__ == "__main__":
    unittest.main()
