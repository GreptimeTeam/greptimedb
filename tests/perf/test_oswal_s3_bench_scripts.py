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
import os
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
        self.assertEqual({c["config"]: c["wal"] for c in plan["cells"]}, {
            "R": {"PROVIDER": "raft_engine", "SYNC_WRITE": "true", "DIR": "/wal"},
            "D": {"PROVIDER": "experimental_object_store", "ACK_MODE": "durable"},
            "E": {"PROVIDER": "experimental_object_store", "ACK_MODE": "enqueued"}})

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

    def test_a_sweep_multiplies_the_object_store_cells_and_reverses_every_other_round(self):
        plan = bench.build_plan("8", "2000000", "1000", "2", smoke=False, configs="R,R0,D,E",
                                sweep="MAX_IN_FLIGHT_CREATES=1,4")
        self.assertEqual([c["id"] for c in plan["cells"]], [
            "1-8-R", "1-8-R0", "1-8-D-1", "1-8-D-4", "1-8-E-1", "1-8-E-4",
            "2-8-E-4", "2-8-E-1", "2-8-D-4", "2-8-D-1", "2-8-R0", "2-8-R"])
        cells = {c["id"]: c for c in plan["cells"]}
        self.assertEqual(cells["1-8-D-1"]["env"], {"GREPTIMEDB_OSWAL_MAX_IN_FLIGHT_CREATES": "1"})
        self.assertEqual((cells["1-8-R0"]["env"], cells["1-8-R0"]["sweep_value"]), ({}, None))
        self.assertEqual(cells["1-8-R0"]["wal"], {"PROVIDER": "raft_engine", "SYNC_WRITE": "false", "DIR": "/wal"})
        self.assertEqual(plan["sweep"], {"name": "GREPTIMEDB_OSWAL_MAX_IN_FLIGHT_CREATES", "values": [1, 4]})
        # The Raft Engine controls ignore the depths, so only the object store points are checked.
        plan = bench.build_plan("8", "6000", "1000", "1", smoke=False, configs="R,R0,D,E",
                                extra_env="GREPTIMEDB_OSWAL_MAX_SEALED_BATCHES=4", sweep="MAX_IN_FLIGHT_CREATES=1,2")
        self.assertEqual(len(plan["cells"]), 6)

    def test_extra_env_and_restart_reach_every_cell(self):
        for extra in ('{"GREPTIMEDB_OSWAL_MAX_IN_FLIGHT_CREATES": 4, "RUST_LOG": "info"}',
                      "GREPTIMEDB_OSWAL_MAX_IN_FLIGHT_CREATES=4\n\nRUST_LOG = info\n"):
            plan = bench.build_plan("8", "2000000", "1000", "1", smoke=False, configs="R,D", extra_env=extra,
                                    sweep="GREPTIMEDB_OSWAL_MAX_SEALED_BATCHES=6,32", restart=True)
            with self.subTest(extra=extra):
                self.assertEqual([c["id"] for c in plan["cells"]], ["1-8-R", "1-8-D-6", "1-8-D-32"])
                cells = {c["id"]: c for c in plan["cells"]}
                self.assertEqual(cells["1-8-D-6"]["env"], {"GREPTIMEDB_OSWAL_MAX_IN_FLIGHT_CREATES": "4",
                                                           "RUST_LOG": "info", "GREPTIMEDB_OSWAL_MAX_SEALED_BATCHES": "6"})
                self.assertEqual(cells["1-8-D-6"]["wal"]["FLUSH_INTERVAL"], "10ms")
                self.assertNotIn("FLUSH_INTERVAL", cells["1-8-R"]["wal"])
                self.assertTrue(all(c["restart"] for c in plan["cells"]))

    def test_invalid_sweeps_and_extra_env_are_rejected(self):
        for kwargs in ({"sweep": "MAX_DELETES=1,2"}, {"sweep": "RECOVERY_CONCURRENCY=0,2"},
                       {"sweep": "RECOVERY_CONCURRENCY=2,2"}, {"sweep": "MAX_SEALED_BATCHES=5,8"},
                       {"extra_env": "GREPTIMEDB_OSWAL_MAX_IN_FLIGHT_CREATES=8", "sweep": "MAX_SEALED_BATCHES=8"},
                       {"extra_env": "GREPTIMEDB_OSWAL_RECOVERY_CONCURRENCY=8", "sweep": "RECOVERY_CONCURRENCY=2"},
                       {"extra_env": "GREPTIMEDB_STANDALONE__STORAGE__BUCKET=other"}, {"extra_env": "NO_VALUE"},
                       {"extra_env": "GREPTIMEDB_STANDALONE__WAL__FLUSH_INTERVAL=100ms", "restart": True},
                       {"extra_env": "GREPTIMEDB_STANDALONE__WAL__SYNC_WRITE=false"},
                       {"extra_env": "GREPTIMEDB_STANDALONE__wal__FLUSH_INTERVAL=100ms", "restart": True},
                       {"extra_env": "greptimedb_standalone__Storage__bucket=other"},
                       {"extra_env": "GREPTIMEDB_OSWAL_MAX_SEALED_BATCHES=4", "sweep": "MAX_IN_FLIGHT_CREATES=2,3"},
                       {"extra_env": "A=1\nA=2"}, {"extra_env": '{"A": true}'}, {"extra_env": "1A=x"},
                       {"extra_env": "GREPTIMEDB_OSWAL_MAX_IN_FLIGHT_CREATES=many"}, {"configs": "R,X"},
                       {"configs": "D,D"}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                bench.build_plan("8", "6000", "1000", "1", smoke=False, **kwargs)

    def test_the_image_is_the_full_reference_or_a_greptimedb_tag(self):
        image = "docker.io/greptime/greptimedb-dev:dev-20261013-1791848986-a5a1a9f4"
        self.assertEqual(bench.resolve_image(image, "nightly"), image)
        self.assertEqual(bench.resolve_image("", "v1.4.0-nightly-20261012"), "greptime/greptimedb:v1.4.0-nightly-20261012")
        for bad in ("greptimedb", "docker.io/greptime/greptimedb", "Docker.io/x/y:1", "x/y:bad tag"):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                bench.resolve_image(bad, "nightly")

    def test_nightly_tag_is_the_newest_by_date(self):
        tags = ["latest", "v1.3.0-alpha.1-nightly-20260907", "v1.4.0-nightly-20261012",
                "v1.4.0-nightly-20261005", "nightly-arm64"]
        self.assertEqual(bench.newest_nightly(tags), "v1.4.0-nightly-20261012")
        with self.assertRaises(ValueError):
            bench.newest_nightly(["latest"])
        self.assertEqual(bench.resolve_tag("v1.4.0-nightly-20261012"), "v1.4.0-nightly-20261012")
        with self.assertRaises(ValueError):
            bench.resolve_tag("bad tag")

    def test_budgets_and_regions(self):
        bench.validate_budgets("240", "8")
        for timeout, ttl in (("360", "7"), ("0", "8"), ("361", "168"), ("240", "169")):
            with self.subTest(timeout=timeout, ttl=ttl), self.assertRaises(ValueError):
                bench.validate_budgets(timeout, ttl)
        bench.check_regions("ap-southeast-1", "ap-southeast-1")
        for bucket, runner in (("us-east-1", "ap-southeast-1"), ("", "")):
            with self.subTest(bucket=bucket), self.assertRaisesRegex(ValueError, "cross-region"):
                bench.check_regions(bucket, runner)

    def test_manifest_takes_the_sha256_digest(self):
        plan = bench.build_plan("1,8", "2000000", "1000", "2", smoke=False)
        inspect = [{"RepoDigests": ["greptime/greptimedb@sha256:" + "a" * 64]}]
        manifest = bench.build_manifest(plan, inspect, "runtime@sha256:" + "b" * 64, "c6id.2xlarge",
                                        "ap-southeast-1", "oswal-bench/1-1")
        self.assertEqual(manifest["greptimedb_image"], "greptime/greptimedb@sha256:" + "a" * 64)
        self.assertEqual((manifest["rows"], manifest["rounds"], manifest["concurrency"]), (2_000_000, 2, [1, 8]))
        with self.assertRaises(ValueError):
            bench.build_manifest(plan, [{"RepoDigests": []}], "", "", "", "")


class SummaryTest(unittest.TestCase):
    def test_quantile_interpolates_like_prometheus(self):
        buckets = summary.parse_metrics(ACK_BUCKETS)["buckets"]["append_ack_seconds"]
        self.assertAlmostEqual(summary.quantile(0.5, buckets), 0.128)
        self.assertAlmostEqual(summary.quantile(0.99, buckets), 0.128 + 0.128 * 49 / 50)
        self.assertAlmostEqual(summary.quantile(0.25, buckets), 0.064 + 0.064 * 25 / 50)
        self.assertIsNone(summary.quantile(0.5, []))
        self.assertEqual(summary.quantile(0.99, [(0.1, 1.0), (float("inf"), 4.0)]), 0.1)
        self.assertNotIn("kafka_client_produce_elapsed_count", summary.parse_metrics(ACK_BUCKETS)["samples"])

    def write_cell(self, root, cell_id, rows_per_second, exit_code=0, metrics="", **extra):
        round_number, concurrency, config, *value = cell_id.split("-")
        path = root / "cells" / cell_id
        (path / "greptimedb-1").mkdir(parents=True)
        (path / "cell.json").write_text(json.dumps({
            "id": cell_id, "round": int(round_number), "concurrency": int(concurrency), "config": config,
            "wal": bench.WAL_CONFIGS[config], "metrics_scraped": True, "sweep_value": int(value[0]) if value else None,
            "env": {"GREPTIMEDB_OSWAL_MAX_IN_FLIGHT_CREATES": value[0]} if value else {},
            "exit_code": exit_code, "wal_device": "/dev/nvme1n1", "version": "1.4.0", "load_wall_seconds": 61.0} | extra))
        if exit_code == 0:
            (path / "greptimedb-1" / "load-result.json").write_text(json.dumps({
                "count_after": 6000, "load_seconds": 60.0, "rows_per_second": rows_per_second}))
        (path / "metrics.prom").write_text(metrics)

    def test_summary_reports_cells_and_geometric_mean_ratios(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = bench.build_plan("8", "6000", "1000", "2", smoke=False)
            (root / "plan.json").write_text(json.dumps(plan))
            (root / "run.json").write_text(json.dumps({"rows": 6000, "batch_rows": 1000, "instance_type": "c6id.2xlarge"}))
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
            self.assertIn("| 1-8-D | — | 80 | 60.0 | 61.0 | 128.0 / 253.4 | — / — | — / — | 120 | 120.0 | 3 | 0 | 0 | 0 | — | — |", output)
            r_cell = next(c for c in result["cells"] if c["id"] == "1-8-R")
            self.assertIsNone(r_cell["stalled_appends_total"])

    def test_sweep_ratios_cover_both_controls_and_restart_cells_report_recovery(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            restart = {"restart": True, "restart_scraped": True, "restart_seconds": 3.5,
                       "restart_query_seconds": 4.25, "restart_count": 6000,
                       "server_cpu_seconds": 122.0, "host_cpu_busy": 0.5, "host_cpus": 8}
            self.write_cell(root, "1-8-R", 100.0)
            self.write_cell(root, "1-8-R0", 125.0)
            self.write_cell(root, "1-8-D-2", 50.0, metrics=ACK_BUCKETS, **restart)
            self.write_cell(root, "1-8-D-16", 75.0)
            (root / "cells" / "1-8-D-2" / "metrics-restart.prom").write_text(
                "greptime_logstore_object_store_wal_recovery_seconds_sum 1.5\n"
                "greptime_logstore_object_store_wal_recovery_seconds_count 1\n"
                "greptime_logstore_object_store_wal_recovered_objects_total 118\n")
            rows = [summary.load_cell(path) for path in sorted((root / "cells").iterdir())]
            pairs = {(p["ratio"], p["sweep_value"]): p["geomean"] for p in summary.ratios(rows)}
            self.assertEqual(pairs, {("D/R", 2): 0.5, ("D/R0", 2): 0.4, ("D/R", 16): 0.75, ("D/R0", 16): 0.6})
            cell = next(r for r in rows if r["id"] == "1-8-D-2")
            self.assertEqual(cell["restart"], {"wall_seconds": 3.5, "query_seconds": 4.25, "rows": 6000,
                                               "recovery_seconds": 1.5, "recovered_objects": 118})
            self.assertEqual(cell["server_cpu_cores"], 2.0)
            output = summary.render({}, rows, summary.ratios(rows))
            self.assertIn("| 1-8-D-2 | MAX_IN_FLIGHT_CREATES=2 | 120 | 3.50 | 4.25 | 1.500 | 118 | 6,000 |", output)
            self.assertIn("| 2.00 | 50% of 8 |", output)
            self.assertIn("| 8 | D/R0 | 16 | 0.600 | 0.600 |", output)

    def test_untouched_counters_are_zero_only_for_an_active_object_store_wal(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            created = "greptime_logstore_object_store_wal_created_objects_total 40\n"
            self.write_cell(root, "1-8-E", 90.0, metrics=created)
            self.write_cell(root, "1-8-D", 80.0, metrics="")
            cell_e, cell_d = (summary.load_cell(root / "cells" / i) for i in ("1-8-E", "1-8-D"))
            self.assertEqual([cell_e[c] for c in summary.COUNTERS], [40, 0, 0, 0, 0])
            self.assertEqual([cell_d[c] for c in summary.COUNTERS], [None] * 5)
            recorded = json.loads((root / "cells" / "1-8-E" / "cell.json").read_text())
            (root / "cells" / "1-8-E" / "cell.json").write_text(json.dumps(recorded | {"metrics_scraped": False}))
            unscraped = summary.load_cell(root / "cells" / "1-8-E")
            self.assertIsNone(unscraped["stalled_appends_total"])

    def test_a_planned_round_that_never_ran_leaves_the_ratio_undefined(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            plan = bench.build_plan("8", "6000", "1000", "2", smoke=False)
            (root / "plan.json").write_text(json.dumps(plan))
            for cell_id, speed in {"1-8-R": 100.0, "1-8-D": 80.0, "1-8-E": 90.0}.items():
                self.write_cell(root, cell_id, speed)
            output = subprocess.run([sys.executable, str(SCRIPTS / "oswal-s3-bench-summary.py"), "--root", str(root)],
                                    check=True, capture_output=True, text=True).stdout
            result = json.loads((root / "summary.json").read_text())
            self.assertEqual(result["missing_cells"], ["2-8-E", "2-8-D", "2-8-R"])
            self.assertEqual(result["planned_cells"], 6)
            for ratio in result["ratios"]:
                self.assertIsNone(ratio["geomean"])
                self.assertIsNone(ratio["per_round"][1])
            self.assertIn("2-8-D (not run)", output)

    def test_a_failed_cell_leaves_its_ratio_undefined(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.write_cell(root, "1-1-R", 100.0)
            self.write_cell(root, "1-1-D", 0.0, exit_code=1)
            rows = [summary.load_cell(path) for path in sorted((root / "cells").iterdir())]
            ratio = next(r for r in summary.ratios(rows) if r["ratio"] == "D/R")
            self.assertEqual((ratio["per_round"], ratio["geomean"]), ([None], None))
            self.assertIn("1-1-D (failed, exit 1)", summary.render({}, rows, summary.ratios(rows)))


FAKE_DOCKER = r"""#!/usr/bin/env bash
printf '%s\n' "docker $*" >> "$FAKE_LOG"
case "$1" in
    run)
        for arg in "$@"; do [[ "$arg" != -d ]] || { echo "keys=${GREPTIMEDB_STANDALONE__STORAGE__ACCESS_KEY_ID:+set}${GREPTIMEDB_STANDALONE__STORAGE__SECRET_ACCESS_KEY:+set}" >> "$FAKE_LOG"; echo fakecid; exit 0; }; done
        for arg in "$@"; do
            case "$arg" in type=bind,src=*,dst=/results) results=${arg#type=bind,src=}; results=${results%,dst=/results} ;; esac
        done
        [[ "$FAKE_LOAD_RC" == 0 ]] || exit "$FAKE_LOAD_RC"
        mkdir -p "$results/greptimedb-1"
        echo "{\"count_before\": 0, \"count_after\": $FAKE_ROWS, \"load_seconds\": 2, \"rows_per_second\": 3000, \"table\": \"agent_observations\"}" \
            > "$results/greptimedb-1/load-result.json" ;;
    inspect) echo true ;;
    exec) [[ "$*" == *cpu.stat* ]] && echo 'usage_usec 5000000' || echo 'version: 1.4.0-test' ;;
    logs) echo 'server log' ;;
esac
"""
FAKE_CURL = r"""#!/usr/bin/env bash
printf '%s\n' "curl $*" >> "$FAKE_LOG"
for arg in "$@"; do
    [[ "$arg" != */v1/sql* ]] || { echo "{\"output\": [{\"records\": {\"rows\": [[${FAKE_COUNT:-6000}]]}}]}"; exit 0; }
    [[ "$arg" != */metrics ]] || {
        [[ "$FAKE_METRICS_RC" == 0 ]] || exit "$FAKE_METRICS_RC"
        printf '%s\n' 'greptime_logstore_object_store_wal_poisoned_total 1' 'other_total 2'; exit 0;
    }
done
"""


class CellLifecycleTest(unittest.TestCase):  # the run script against fake docker and curl

    def run_cells(self, root, load_rc, metrics_rc=0, restart=False, count=6000):
        bin_dir = root / "bin"
        bin_dir.mkdir()
        for name, body in (("docker", FAKE_DOCKER), ("curl", FAKE_CURL)):
            (bin_dir / name).write_text(body)
            (bin_dir / name).chmod(0o755)
        plan = bench.build_plan("8", "6000", "1000", "1", smoke=False, configs="D,E", restart=restart,
                                extra_env="GREPTIMEDB_OSWAL_RECOVERY_CONCURRENCY=2")
        (root / "results").mkdir()
        (root / "results" / "plan.json").write_text(json.dumps(plan))
        env = os.environ | {
            "PATH": f"{bin_dir}:{os.environ['PATH']}", "FAKE_LOG": str(root / "fake.log"),
            "FAKE_LOAD_RC": str(load_rc), "FAKE_METRICS_RC": str(metrics_rc), "FAKE_ROWS": "6000", "FAKE_COUNT": str(count),
            "PLAN": str(root / "results" / "plan.json"),
            "RESULTS": str(root / "results"), "CORPUS_DIR": str(root / "corpus"), "OWNER": "o11ybench-oswal-1-1",
            "RUNTIME_IMAGE": "runtime@sha256:" + "b" * 64, "GREPTIMEDB_IMAGE": "greptimedb@sha256:" + "a" * 64,
            "RUN_PREFIX": "oswal-bench/1-1", "WAL_ROOT": str(root / "wal"), "S3_BUCKET": "bucket",
            "S3_REGION": "ap-southeast-1", "AWS_ACCESS_KEY_ID": "AKIAFAKEKEY", "AWS_SECRET_ACCESS_KEY": "fake-secret-value",
        }
        script = str(SCRIPTS / "oswal-s3-bench-run.sh")
        cells = subprocess.run(["bash", script, "cells"], env=env, capture_output=True, text=True, timeout=60)
        scrub = subprocess.run(["bash", script, "scrub"], env=env, capture_output=True, text=True, timeout=60)
        return cells, scrub, (root / "fake.log").read_text(), env

    def test_a_failed_load_still_scrapes_metrics_and_removes_the_server(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cells, scrub, log, _ = self.run_cells(root, load_rc=3)
            self.assertNotEqual(cells.returncode, 0)
            self.assertEqual(scrub.returncode, 0, scrub.stderr)
            for cell_id in ("1-8-D", "1-8-E"):
                cell = root / "results" / "cells" / cell_id
                self.assertEqual(json.loads((cell / "cell.json").read_text())["exit_code"], 3)
                self.assertEqual((cell / "metrics.prom").read_text(),
                                 "greptime_logstore_object_store_wal_poisoned_total 1\n")
                self.assertFalse((cell / "metrics.all").exists())
                self.assertEqual((cell / "server.log").read_text(), "server log\n")
            self.assertEqual(log.count("docker stop -t 120 fakecid"), 2)
            self.assertEqual(log.count("docker rm -fv fakecid"), 2)

    def test_an_ec2_run_without_an_instance_store_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            (Path(directory) / "lsblk").write_text("#!/usr/bin/env bash\necho '/dev/nvme0n1 Amazon Elastic Block Store'\n")
            (Path(directory) / "lsblk").chmod(0o755)
            env = os.environ | {"PATH": f"{directory}:{os.environ['PATH']}", "SMOKE": "false", "RESULTS": directory}
            done = subprocess.run(["bash", str(SCRIPTS / "oswal-s3-bench-run.sh"), "wal-disk", f"{directory}/wal"],
                                  env=env, capture_output=True, text=True, timeout=60)
            self.assertNotEqual(done.returncode, 0)
            self.assertIn("No local NVMe instance store", done.stderr)

    def test_a_failed_scrape_fails_a_successful_load(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cells, _, log, _ = self.run_cells(root, load_rc=0, metrics_rc=22)
            self.assertNotEqual(cells.returncode, 0)
            for cell_id in ("1-8-D", "1-8-E"):
                cell = root / "results" / "cells" / cell_id
                recorded = json.loads((cell / "cell.json").read_text())
                self.assertEqual((recorded["exit_code"], recorded["metrics_scraped"]), (4, False))
                self.assertFalse((cell / "metrics.prom").exists())
                self.assertFalse((cell / "metrics.all").exists())
            self.assertEqual(log.count("docker rm -fv fakecid"), 2)

    def test_a_cell_passes_the_keys_by_name_and_records_a_redacted_config(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cells, scrub, log, env = self.run_cells(root, load_rc=0)
            self.assertEqual(cells.returncode, 0, cells.stdout + cells.stderr)
            self.assertEqual(scrub.returncode, 0, scrub.stderr)
            self.assertNotIn("fake-secret-value", log)
            self.assertNotIn("AKIAFAKEKEY", log)
            self.assertEqual(log.count("keys=setset"), 2)
            self.assertIn("-e GREPTIMEDB_STANDALONE__STORAGE__SECRET_ACCESS_KEY ", log)
            cell = root / "results" / "cells" / "1-8-D"
            config = (cell / "config.env").read_text().splitlines()
            self.assertIn("GREPTIMEDB_STANDALONE__STORAGE__ROOT=oswal-bench/1-1/1-8-D", config)
            self.assertIn("GREPTIMEDB_STANDALONE__WAL__ACK_MODE=durable", config)
            self.assertIn("GREPTIMEDB_STANDALONE__STORAGE__SECRET_ACCESS_KEY=<redacted>", config)
            self.assertIn("GREPTIMEDB_OSWAL_RECOVERY_CONCURRENCY=2", config)
            self.assertIn("-e GREPTIMEDB_OSWAL_RECOVERY_CONCURRENCY=2 ", log)
            self.assertNotIn("docker kill", log)
            recorded = json.loads((cell / "cell.json").read_text())
            self.assertEqual((recorded["exit_code"], recorded["version"], recorded["metrics_scraped"]),
                             (0, "1.4.0-test", True))
            self.assertEqual(recorded["wal_device"], "s3://bucket/oswal-bench/1-1/1-8-D/wal")
            # A leaked value makes scrub refuse the results.
            (cell / "leak.txt").write_text("fake-secret-value")
            leaked = subprocess.run(["bash", str(SCRIPTS / "oswal-s3-bench-run.sh"), "scrub"], env=env,
                                    capture_output=True, text=True, timeout=60)
            self.assertNotEqual(leaked.returncode, 0)
            self.assertFalse((root / "results").exists())

    def test_a_restart_cell_kills_restarts_and_counts_the_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cells, _, log, _ = self.run_cells(root, load_rc=0, restart=True)
            self.assertEqual(cells.returncode, 0, cells.stdout + cells.stderr)
            self.assertEqual(log.count("docker kill --signal KILL fakecid"), 2)
            self.assertEqual(log.count("docker stop -t 120 fakecid"), 2)
            self.assertIn("--name o11ybench-oswal-1-1-1-8-D-restart", log)
            self.assertIn("sql=SELECT count(*) FROM agent_observations", log)
            cell = root / "results" / "cells" / "1-8-D"
            recorded = json.loads((cell / "cell.json").read_text())
            self.assertEqual((recorded["exit_code"], recorded["restart_count"], recorded["restart_scraped"]), (0, 6000, True))
            self.assertIsInstance(recorded["restart_seconds"], float)
            config = (cell / "config.env").read_text().splitlines()
            self.assertIn("GREPTIMEDB_STANDALONE__STORAGE__DATA_HOME=/oswal_home", config)
            self.assertIn("GREPTIMEDB_STANDALONE__WAL__FLUSH_INTERVAL=10ms", config)
            self.assertIn(f"type=bind,src={root}/wal/1-8-D-home,dst=/oswal_home", log)
            for name in ("metrics.prom", "metrics-restart.prom", "server.log", "server-restart.log"):
                self.assertTrue((cell / name).exists(), name)

    def test_a_restart_that_loses_rows_fails_the_cell(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cells, _, _, _ = self.run_cells(root, load_rc=0, restart=True, count=5999)
            self.assertNotEqual(cells.returncode, 0)
            recorded = json.loads((root / "results" / "cells" / "1-8-D" / "cell.json").read_text())
            self.assertEqual((recorded["exit_code"], recorded["restart_count"]), (5, 5999))


if __name__ == "__main__":
    unittest.main()
