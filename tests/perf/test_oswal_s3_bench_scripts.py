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
import re
import tempfile
import unittest
import urllib.parse
from pathlib import Path
from types import SimpleNamespace

SCRIPTS = Path(__file__).parents[2] / ".github/scripts"


def load(name, filename):
    spec = importlib.util.spec_from_file_location(name, SCRIPTS / filename)
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


bench = load("oswal_s3_bench", "oswal-s3-bench.py")
crash = load("oswal_s3_bench_crash", "oswal-s3-bench-crash.py")
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
        self.assertEqual(plan["corpora"], [{"rows": 2_000_000, "dir": "corpus-2000000", "data_profile": "D",
                                            "generator_args": ["--data-profile", "D"]}])
        self.assertEqual((plan["rows"], plan["batch_rows"]), (2_000_000, 1000))
        self.assertEqual({c["config"]: c["wal"] for c in plan["cells"]}, {
            "R": {"PROVIDER": "raft_engine", "SYNC_WRITE": "true", "DIR": "/wal"},
            "D": {"PROVIDER": "experimental_object_store", "ACK_MODE": "durable"},
            "E": {"PROVIDER": "experimental_object_store", "ACK_MODE": "enqueued"}})

    def test_smoke_overrides_rows_and_rounds(self):
        plan = bench.build_plan("8", "2000000", "1000", "2", smoke=True)
        self.assertEqual((plan["rows"], plan["rounds"], plan["corpora"][0]["data_profile"]), (50_000, 1, "custom"))
        self.assertEqual(plan["corpora"][0]["generator_args"], ["--line-count", "50000"])
        self.assertEqual([c["id"] for c in plan["cells"]], ["1-8-R", "1-8-D", "1-8-E"])

    def test_invalid_inputs_are_rejected(self):
        for args in (("0", "10", "10", "1"), ("1,1", "10", "10", "1"), ("1", "abc", "10", "1"),
                     ("1", "10", "10", "11"), ("1,", "10", "10", "1")):
            with self.subTest(args=args), self.assertRaises(ValueError):
                bench.build_plan(*args, smoke=False)

    def test_the_baselines_and_the_order_reverse_every_other_round(self):
        plan = bench.build_plan("8", "2000000", "1000", "2", smoke=False, configs="R,R0,D,E")
        self.assertEqual([c["id"] for c in plan["cells"]], [
            "1-8-R", "1-8-R0", "1-8-D", "1-8-E", "2-8-E", "2-8-D", "2-8-R0", "2-8-R"])
        cells = {c["id"]: c for c in plan["cells"]}
        self.assertEqual((cells["1-8-R0"]["env"], cells["1-8-R0"]["wal"]),
                         ({}, {"PROVIDER": "raft_engine", "SYNC_WRITE": "false", "DIR": "/wal"}))

    def test_extra_env_and_restart_reach_every_cell(self):
        for extra in ('{"RUST_BACKTRACE": 1, "RUST_LOG": "info"}', "RUST_BACKTRACE=1\n\nRUST_LOG = info\n"):
            plan = bench.build_plan("8", "2000000", "1000", "1", smoke=False, configs="R,D", extra_env=extra,
                                    restart=True)
            with self.subTest(extra=extra):
                self.assertEqual([c["id"] for c in plan["cells"]], ["1-8-R", "1-8-D"])
                cells = {c["id"]: c for c in plan["cells"]}
                self.assertEqual(cells["1-8-D"]["env"], {"RUST_BACKTRACE": "1", "RUST_LOG": "info"})
                self.assertEqual(cells["1-8-D"]["wal"]["FLUSH_INTERVAL"], "10ms")
                self.assertNotIn("FLUSH_INTERVAL", cells["1-8-R"]["wal"])
                self.assertTrue(all(c["restart"] for c in plan["cells"]))

    def test_invalid_extra_env_and_configs_are_rejected(self):
        for kwargs in ({"extra_env": "GREPTIMEDB_STANDALONE__STORAGE__BUCKET=other"}, {"extra_env": "NO_VALUE"},
                       {"extra_env": "GREPTIMEDB_STANDALONE__WAL__FLUSH_INTERVAL=100ms", "restart": True},
                       {"extra_env": "GREPTIMEDB_STANDALONE__WAL__SYNC_WRITE=false"},
                       {"extra_env": "GREPTIMEDB_STANDALONE__wal__FLUSH_INTERVAL=100ms", "restart": True},
                       {"extra_env": "greptimedb_standalone__Storage__bucket=other"},
                       {"extra_env": "A=1\nA=2"}, {"extra_env": '{"A": true}'}, {"extra_env": "1A=x"},
                       {"configs": "R,X"}, {"configs": "D,D"}):
            with self.subTest(kwargs=kwargs), self.assertRaises(ValueError):
                bench.build_plan("8", "6000", "1000", "1", smoke=False, **kwargs)

    def test_the_image_is_the_full_reference_or_a_greptimedb_tag(self):
        image = "docker.io/greptime/greptimedb-dev:dev-20261013-1791848986-a5a1a9f4"
        for valid in (image, "ghcr.io/acme/greptimedb--bench:dev", "ghcr.io/acme/greptimedb__bench:dev",
                      "localhost:5000/a/b@sha256:" + "c" * 64):
            self.assertEqual(bench.resolve_image(valid, "nightly"), valid)
        self.assertEqual(bench.resolve_image("", "v1.4.0-nightly-20261012"), "greptime/greptimedb:v1.4.0-nightly-20261012")
        for bad in ("greptimedb", "docker.io/greptime/greptimedb", "Docker.io/x/y:1", "x/y:bad tag",
                    "x/y___z:1", "x/-y:1"):
            with self.subTest(bad=bad), self.assertRaises(ValueError):
                bench.resolve_image(bad, "nightly")

    def test_groups_inherit_the_inputs_and_keep_their_own_settings(self):
        groups = [{"name": "recovery", "configs": "D,E", "rows": 4000000, "rounds": 1, "restart": True},
                  {"name": "crashe", "mode": "crash", "configs": "E", "rounds": 1, "cycles": 3, "kill_after": [5, 9]},
                  {"name": "pin", "mode": "crash", "configs": "D", "rounds": 1, "cycles": 1,
                   "kill_after": [540, 540], "pin": True},
                  {"name": "idle", "configs": "D", "rounds": 1, "idle_seconds": 180}]
        plan = bench.build_plan("8", "2000000", "1000", "2", smoke=False, groups=json.dumps(groups))
        self.assertEqual([c["id"] for c in plan["cells"]], [
            "recovery-1-8-D", "recovery-1-8-E", "crashe-1-8-E", "pin-1-8-D", "idle-1-8-D"])
        self.assertEqual([c["rows"] for c in plan["corpora"]], [4_000_000, 2_000_000])
        cells = {c["id"]: c for c in plan["cells"]}
        self.assertEqual(cells["recovery-1-8-D"]["wal"]["FLUSH_INTERVAL"], "10ms")
        self.assertEqual((cells["crashe-1-8-E"]["cycles"], cells["crashe-1-8-E"]["kill_after"]), (3, [5, 9]))
        self.assertTrue(cells["pin-1-8-D"]["pin"])
        self.assertNotIn("FLUSH_INTERVAL", cells["pin-1-8-D"]["wal"])
        self.assertEqual(cells["idle-1-8-D"]["idle_seconds"], 180)
        smoke = bench.build_plan("8", "2000000", "1000", "2", smoke=True, groups=json.dumps(groups))
        pin = next(c for c in smoke["cells"] if c["group"] == "pin")
        self.assertEqual((pin["cycles"], pin["kill_after"]), (1, [60, 60]))
        self.assertEqual([c["rows"] for c in smoke["corpora"]], [50_000])
        recovery = [{"name": "rec", "configs": "D", "restart": True, "min_objects": 6000}]
        self.assertEqual(bench.build_plan("8", "4000000", "1000", "1", smoke=False,
                                          groups=json.dumps(recovery))["cells"][0]["min_objects"], 6000)
        self.assertEqual(bench.build_plan("8", "4000000", "1000", "1", smoke=True,
                                          groups=json.dumps(recovery))["cells"][0]["min_objects"], 0)

    def test_the_workflow_passes_groups_through_bash_unchanged(self):
        workflow = (SCRIPTS.parent / "workflows" / "oswal-s3-bench.yml").read_text()
        self.assertIn("OSWAL_GROUPS: ${{ inputs.groups }}", workflow)
        self.assertEqual(workflow.count('--groups "$OSWAL_GROUPS"'), 2)
        command = (f'python3 "{SCRIPTS}/oswal-s3-bench.py" plan --smoke false --concurrency 8 --rows 6000 '
                   '--batch-rows 1000 --rounds 1 --groups "$OSWAL_GROUPS"')
        for groups, expected in (("", "3 cells"), ('[{"name": "a", "configs": "D,E"}]', "2 cells")):
            done = subprocess.run(["bash", "-c", command], env=os.environ | {"OSWAL_GROUPS": groups},
                                  capture_output=True, text=True, timeout=60)
            self.assertEqual(done.returncode, 0, done.stderr)
            self.assertTrue(done.stdout.startswith(expected), done.stdout)

    def test_a_partitioned_group_and_a_drop_group(self):
        groups = [{"name": "multi", "configs": "R,D", "concurrency": "8,32", "partitions": 8},
                  {"name": "drop", "mode": "drop", "configs": "D", "rounds": 1, "write_seconds": 45}]
        plan = bench.build_plan("8", "2000000", "1000", "1", smoke=False, groups=json.dumps(groups))
        cells = {c["id"]: c for c in plan["cells"]}
        self.assertEqual(list(cells), ["multi-1-8-R", "multi-1-8-D", "multi-1-32-R", "multi-1-32-D", "drop-1-8-D"])
        self.assertEqual({c["partitions"] for c in plan["cells"] if c["mode"] == "load"}, {8})
        self.assertEqual((cells["drop-1-8-D"]["write_seconds"], cells["drop-1-8-D"]["mode"]), (45, "drop"))
        self.assertNotIn("rows", cells["drop-1-8-D"])
        self.assertEqual([c["rows"] for c in plan["corpora"]], [2_000_000])
        smoke = bench.build_plan("8", "2000000", "1000", "1", smoke=True, groups=json.dumps(groups))
        self.assertEqual(next(c for c in smoke["cells"] if c["mode"] == "drop")["write_seconds"], 10)
        self.assertEqual(bench.build_plan("8", "6000", "1000", "1", smoke=False)["cells"][0]["partitions"], 1)

    def test_the_partitioned_table_splits_the_corpus_window_into_equal_ranges(self):
        ddl = bench.partitioned_table({"start": "2026-07-10T00:00:00Z", "end": "2026-07-10T01:00:00Z"}, 8)
        start = 1783641600000
        bounds = [int(b) for b in re.findall(r"`event_time` < (\d+)", ddl)]
        self.assertEqual(bounds, [start + 450_000 * i for i in range(1, 8)])
        self.assertIn(f"`event_time` >= {start + 450_000 * 7}\n)\nENGINE=mito", ddl)
        self.assertEqual(ddl.count("`event_time` >="), 7)
        self.assertTrue(ddl.startswith("CREATE TABLE `agent_observations` ("))
        self.assertIn("PRIMARY KEY (`tenant`, `app`, `environment`)\n)\nPARTITION ON COLUMNS (`event_time`) (", ddl)
        self.assertNotIn("{partitions}", ddl)
        with self.assertRaises(ValueError):
            bench.partitioned_table({"start": "2026-07-10T00:00:00Z", "end": "2026-07-10T00:00:00.004Z"}, 8)

    def test_invalid_groups_are_rejected(self):
        for groups in ([], [{"configs": "D"}], [{"name": "Bad"}], [{"name": "a"}, {"name": "a"}],
                       [{"name": "a", "colour": "red"}], [{"name": "a", "mode": "crash", "configs": "R"}],
                       [{"name": "a", "mode": "crash", "configs": "D", "restart": True}],
                       [{"name": "a", "mode": "crash", "configs": "D", "kill_after": [9, 5]}],
                       [{"name": "a", "mode": "soak"}], [{"name": "a", "idle_seconds": 601}],
                       [{"name": "a", "configs": "D", "min_objects": 6000}],
                       [{"name": "a", "configs": "D", "restart": True, "min_objects": -1}],
                       [{"name": "a", "configs": "D", "partitions": 65}], [{"name": "a", "configs": "D", "partitions": 0}],
                       [{"name": "a", "mode": "crash", "configs": "D", "partitions": 8}],
                       [{"name": "a", "mode": "drop", "configs": "R"}],
                       [{"name": "a", "mode": "drop", "configs": "D", "write_seconds": 0}],
                       [{"name": "a", "mode": "drop", "configs": "D", "restart": True}]):
            with self.subTest(groups=groups), self.assertRaises(ValueError):
                bench.build_plan("8", "6000", "1000", "1", smoke=False, groups=json.dumps(groups))

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

    def test_manifest_records_the_checked_images_pinned_to_their_digests(self):
        plan = bench.build_plan("1,8", "2000000", "1000", "2", smoke=False)
        checked = [{"label": "", "image": "greptime/greptimedb@sha256:" + "a" * 64,
                    "requested": "greptime/greptimedb:v1.4.0-nightly-20261012", "version": "1.4.0", "commit": "abc"}]
        manifest = bench.build_manifest(plan, checked, "runtime@sha256:" + "b" * 64, "c6id.2xlarge",
                                        "ap-southeast-1", "oswal-bench/1-1")
        self.assertEqual(manifest["greptimedb_image"], "greptime/greptimedb@sha256:" + "a" * 64)
        self.assertEqual(manifest["greptimedb_images"], checked)
        self.assertEqual((manifest["rows"], manifest["rounds"], manifest["concurrency"]), (2_000_000, 2, [1, 8]))
        for images in ([checked[0] | {"image": "greptime/greptimedb:v1.4.0"}], [checked[0] | {"label": "base"}]):
            with self.subTest(images=images), self.assertRaises(ValueError):
                bench.build_manifest(plan, images, "", "", "", "")

    def test_every_cell_runs_once_per_image_in_an_order_that_reverses_every_other_round(self):
        images = json.dumps([{"label": label, "image": f"docker.io/greptime/greptimedb-dev:{label}"}
                             for label in ("base", "pr1", "pr2")])
        plan = bench.build_plan("8", "2000000", "1000", "3", smoke=False, configs="D", images=images)
        self.assertEqual([c["id"] for c in plan["cells"]], [
            "1-8-D-base", "1-8-D-pr1", "1-8-D-pr2", "2-8-D-pr2", "2-8-D-pr1", "2-8-D-base",
            "3-8-D-base", "3-8-D-pr1", "3-8-D-pr2"])
        self.assertEqual(plan["images"], ["base", "pr1", "pr2"])
        # The configurations reverse with the images, and a warm-up runs the first load cell per image.
        groups = json.dumps([{"name": "crash", "mode": "crash", "configs": "E", "rounds": 1, "cycles": 1},
                             {"name": "single", "configs": "D,E"}])
        plan = bench.build_plan("8", "2000000", "1000", "2", smoke=False, groups=groups,
                                images=json.dumps(json.loads(images)[:2]), warmup=True)
        self.assertEqual([c["id"] for c in plan["cells"]], [
            "single-0-8-D-base", "single-0-8-D-pr1", "crash-1-8-E-base", "crash-1-8-E-pr1",
            "single-1-8-D-base", "single-1-8-D-pr1", "single-1-8-E-base", "single-1-8-E-pr1",
            "single-2-8-E-pr1", "single-2-8-E-base", "single-2-8-D-pr1", "single-2-8-D-base"])
        warmups = [c for c in plan["cells"] if c.get("warmup")]
        self.assertEqual([(c["round"], c["image"], c["mode"]) for c in warmups], [(0, "base", "load"), (0, "pr1", "load")])
        self.assertEqual((plan["rounds"], plan["warmup"]), (2, True))
        # Without images every cell keeps its id and runs the one image.
        single = bench.build_plan("8", "2000000", "1000", "1", smoke=False, configs="D")
        self.assertEqual([(c["id"], c["image"]) for c in single["cells"]], [("1-8-D", "")])
        self.assertEqual((single["images"], single["warmup"]), ([""], False))

    def test_invalid_images_and_warmups_are_rejected(self):
        ref = "docker.io/greptime/greptimedb-dev:dev"
        for images in ([{"label": "base", "image": ref}], [{"label": f"i{n}", "image": ref} for n in range(5)],
                       [{"label": "base", "image": ref}, {"label": "base", "image": ref}],
                       [{"label": "Base", "image": ref}, {"label": "pr1", "image": ref}],
                       [{"label": "ninechars", "image": ref}, {"label": "pr1", "image": ref}],
                       [{"label": 1, "image": ref}, {"label": "pr1", "image": ref}],
                       [{"label": "base", "image": "greptimedb"}, {"label": "pr1", "image": ref}],
                       [{"label": "base", "image": ref, "env": "A=1"}, {"label": "pr1", "image": ref}],
                       {"base": ref}):
            with self.subTest(images=images), self.assertRaises(ValueError):
                bench.build_plan("8", "6000", "1000", "1", smoke=False, images=json.dumps(images))
        crash_only = json.dumps([{"name": "c", "mode": "crash", "configs": "D"}])
        with self.assertRaisesRegex(ValueError, "warmup needs a load group"):
            bench.build_plan("8", "6000", "1000", "1", smoke=False, groups=crash_only, warmup=True)
        # One image twice compares it with itself.
        pair = [{"label": "base", "image": ref}, {"label": "again", "image": ref}]
        self.assertEqual(bench.resolve_images(json.dumps(pair), "", "nightly"), pair)
        with self.assertRaisesRegex(ValueError, "not both"):
            bench.resolve_images(json.dumps(pair), ref, "nightly")
        self.assertEqual(bench.resolve_images("", ref, "nightly"), [{"label": "", "image": ref}])

    def test_a_plan_longer_than_the_benchmark_timeout_is_refused(self):
        groups = json.dumps([{"name": "single", "configs": "D,E"}, {"name": "multi", "configs": "D,E", "partitions": 8}])
        images = json.dumps([{"label": label, "image": f"docker.io/greptime/greptimedb-dev:{label}"}
                             for label in ("base", "pr1", "pr2")])
        plan = bench.build_plan("8", "2000000", "1000", "5", smoke=False, groups=groups, images=images, warmup=True)
        self.assertEqual(len(plan["cells"]), 63)
        self.assertEqual(bench.estimate_minutes(plan), 75)
        cells = {c["id"]: c for c in plan["cells"]}
        # Durable at concurrency 8 writes 34,000 rows/s to one region and 26,000 to eight.
        self.assertAlmostEqual(bench.cell_seconds(cells["single-1-8-D-base"]), 5 + 2_000_000 / 34_000)
        self.assertAlmostEqual(bench.cell_seconds(cells["multi-1-8-D-pr1"]), 5 + 2_000_000 / 26_000)
        self.assertAlmostEqual(bench.cell_seconds(cells["multi-1-8-E-pr2"]), 5 + 2_000_000 / 60_000)
        durable = cells["single-1-8-D-base"]
        self.assertAlmostEqual(bench.load_rate(durable | {"concurrency": 4}), 9_000 + 25_000 * 2 / 3)
        self.assertEqual(bench.load_rate(durable | {"concurrency": 64}), 55_000)
        crash = bench.build_plan("8", "6000", "1000", "1", smoke=False, groups=json.dumps(
            [{"name": "c", "mode": "crash", "configs": "D", "cycles": 5, "kill_after": [5, 20]}]))["cells"][0]
        self.assertEqual(bench.cell_seconds(crash), 5 + 5 * (12.5 + 15))
        command = [sys.executable, str(SCRIPTS / "oswal-s3-bench.py"), "plan", "--smoke", "false", "--concurrency",
                   "8", "--rows", "2000000", "--batch-rows", "1000", "--rounds", "5", "--groups", groups,
                   "--images", images, "--warmup", "true", "--ttl-hours", "8"]
        for timeout, returncode in (("75", 0), ("74", 1)):
            done = subprocess.run(command + ["--timeout-minutes", timeout], capture_output=True, text=True, timeout=60)
            with self.subTest(timeout=timeout):
                self.assertEqual(done.returncode, returncode, done.stderr)
                self.assertIn("about 75 minutes" if returncode == 0 else "more than the 74-minute", done.stdout + done.stderr)

    def test_the_workflow_checks_every_image_before_an_instance_is_allocated(self):
        workflow = (SCRIPTS.parent / "workflows" / "oswal-s3-bench.yml").read_text()
        self.assertIn("OSWAL_IMAGES: ${{ inputs.images }}", workflow)
        self.assertEqual(workflow.count('--images "$OSWAL_IMAGES"'), 3)
        self.assertEqual(workflow.count('--warmup "$WARMUP"'), 2)
        validate = workflow[workflow.index("  validate:"):workflow.index("  provision:")]
        self.assertIn("greptimedb_images: ${{ steps.images.outputs.greptimedb_images }}", validate)
        self.assertIn("check-images \"$REQUESTED_IMAGES\"", validate)
        self.assertIn("GREPTIMEDB_IMAGES: ${{ needs.validate.outputs.greptimedb_images }}", workflow)


def write_lines(path, lines):
    path.write_text("".join(f"{line}\n" for line in lines))


def crash_cycle(root, cycle, stripes, acked, requests, exits, found=None):
    """One cycle of two writers: attempted stripes, acked batches, requests and exit times per
    writer, and what the restart after its kill found, if given."""
    directory = root / f"cycle-{cycle}"
    directory.mkdir(parents=True)
    for index in range(2):
        write_lines(directory / f"attempted-{index}.log", stripes[index])
        write_lines(directory / f"acked-{index}.log", acked[index])
        write_lines(directory / f"requests-{index}.log", requests[index])
        (directory / f"exit-{index}.log").write_text(f"{exits[index]}\n")
    if found is not None:
        (directory / "found.json").write_text(json.dumps(
            {"output": [{"records": {"rows": [[seq, count] for seq, count in found.items()]}}]}))
    return directory


class CrashOracleTest(unittest.TestCase):
    # Writer 0 got batches 0 and 2 acknowledged and lost 4 in flight; writer 1 got 1 and lost 3.
    STRIPES = ([0, 2, 4], [1, 3])
    ACKED = ([0, 2], [1])
    REQUESTS = (["0 200 10", "2 200 20", "4 000 105"], ["1 200 15", "3 000 101"])

    def check(self, root, found, mode="durable", requests=REQUESTS, exits=(106, 102)):
        crash_cycle(root, 1, self.STRIPES, self.ACKED, requests, exits)
        result = crash.check_kill(root / "cycle-1", 2, 100, 104, 2000)
        return result | crash.verify(root, found, 10, mode == "enqueued", 1)

    def test_acked_batches_present_once_and_an_in_flight_extra_pass(self):
        with tempfile.TemporaryDirectory() as directory:
            result = self.check(Path(directory), {0: 10, 1: 10, 2: 10, 4: 10})
            self.assertEqual((result["kill_errors"], result["verify_errors"]), ([], []))
            self.assertEqual((result["in_flight"], result["writers_at_kill"], result["cycle_extra_batches"],
                              result["cycle_lost_batches"], result["cycle_present_batches"]), (2, 2, 1, 0, 4))

    def test_durable_violations_are_reported(self):
        cases = {"acknowledged but absent": {0: 10, 1: 10}, "present more than once": {0: 20, 1: 10, 2: 10},
                 "present in part": {0: 10, 1: 10, 2: 7}, "never attempted": {0: 10, 1: 10, 2: 10, 9: 10}}
        for error, found in cases.items():
            with self.subTest(error=error), tempfile.TemporaryDirectory() as directory:
                result = self.check(Path(directory), found)
                self.assertTrue(any(error in e for e in result["verify_errors"]), result["verify_errors"])

    def test_enqueued_loses_acked_batches_without_failing(self):
        with tempfile.TemporaryDirectory() as directory:
            result = self.check(Path(directory), {0: 10}, mode="enqueued")
            self.assertEqual(result["verify_errors"], [])
            self.assertEqual((result["cycle_lost_batches"], result["cycle_lost_rows"], result["total_lost_batches"]),
                             (2, 20, 2))

    def test_a_later_cycle_reports_only_its_own_losses(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            crash_cycle(root, 1, self.STRIPES, self.ACKED, self.REQUESTS, (106, 102))
            crash_cycle(root, 2, ([10, 12], [11]), ([10], [11]), (["10 200 210", "12 000 305"], ["11 200 215"]),
                        (306, 302))
            # Cycle 1 lost batches 1 and 2; cycle 2 lost nothing.
            result = crash.verify(root, {0: 10, 10: 10, 11: 10}, 10, True, 2)
            self.assertEqual((result["cycle_acked_batches"], result["cycle_lost_batches"],
                              result["total_lost_batches"], result["total_acked_batches"]), (2, 0, 2, 5))

    def later_cycle(self, root):
        """Cycle 1's restart recovered batches 0, 1 and 2 and the in-flight 4; cycle 2 acknowledged
        10 and 11 and had 12 in flight."""
        crash_cycle(root, 1, self.STRIPES, self.ACKED, self.REQUESTS, (106, 102), found={0: 10, 1: 10, 2: 10, 4: 10})
        crash_cycle(root, 2, ([10, 12], [11]), ([10], [11]), (["10 200 210", "12 000 305"], ["11 200 215"]),
                    (306, 302))

    def test_a_batch_an_earlier_restart_recovered_must_stay_present(self):
        for mode in ("enqueued", "durable"):
            with self.subTest(mode=mode), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.later_cycle(root)
                # Acknowledged batch 1 and the in-flight 4 vanished after cycle 1's restart found them.
                result = crash.verify(root, {0: 10, 2: 10, 10: 10, 11: 10}, 10, mode == "enqueued", 2)
                self.assertIn("recovered by an earlier restart but absent: [1, 4]", result["verify_errors"])
                self.assertEqual(result["cycle_lost_batches"], 0)

    def test_only_the_current_cycle_may_lose_acknowledged_batches_in_enqueued_mode(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            self.later_cycle(root)
            found = {0: 10, 1: 10, 2: 10, 4: 10, 10: 10}
            result = crash.verify(root, found, 10, True, 2)
            self.assertEqual(result["verify_errors"], [])
            self.assertEqual((result["cycle_lost_batches"], result["total_lost_batches"]), (1, 1))
            durable = crash.verify(root, found, 10, False, 2)
            self.assertEqual(durable["verify_errors"], ["acknowledged but absent: [11]"])
            duplicated = crash.verify(root, found | {10: 20}, 10, True, 2)
            self.assertEqual(duplicated["verify_errors"], ["batches present more than once: [10]"])

    def test_a_connection_lost_before_the_kill_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            requests = (["0 200 10", "2 200 20", "4 000 50"], ["1 200 15", "3 500 101"])
            result = self.check(Path(directory), {0: 10, 1: 10, 2: 10}, requests=requests, exits=(51, 99))
            self.assertEqual(len(result["kill_errors"]), 3, result["kill_errors"])

    def test_the_writer_stops_each_stripe_at_its_first_unacknowledged_batch(self):
        import http.server
        import threading
        inserted, lock = [], threading.Lock()

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                body = self.rfile.read(int(self.headers["Content-Length"])).decode()
                with lock:
                    if len(inserted) >= 6:
                        self.close_connection = True
                        self.connection.close()
                        return
                    inserted.append(body)
                self.send_response(200)
                self.end_headers()

            def log_message(self, *args):
                pass

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            with tempfile.TemporaryDirectory() as directory:
                run = Path(directory)
                args = SimpleNamespace(dir=str(run / "cycle-1"), url=f"http://127.0.0.1:{server.server_port}/v1/sql",
                                       table="crash", writers=2, first_seq=0, rows=3, seconds=0)
                crash.write(args)
                found = {}
                for body in inserted:
                    for seq in set(re.findall(r"%28(\d+)%2C", body)):
                        found[int(seq)] = 3
                result = crash.verify(run, found, 3, False, 1)
                self.assertEqual((result["verify_errors"], result["total_acked_batches"]), ([], 6))
                for index in range(2):
                    attempted = (run / "cycle-1" / f"attempted-{index}.log").read_text().split()
                    self.assertEqual([int(seq) % 2 for seq in attempted], [index] * len(attempted))
        finally:
            server.shutdown()
            server.server_close()


class WriterDeadlineTest(unittest.TestCase):
    def test_writers_stop_at_the_deadline_with_every_batch_acknowledged(self):
        import http.server
        import threading
        status = [200]

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers["Content-Length"]))
                self.send_response(status[0])
                self.end_headers()

            def log_message(self, *args):
                pass

        server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        try:
            with tempfile.TemporaryDirectory() as directory:
                run = Path(directory)
                args = SimpleNamespace(dir=str(run / "cycle-1"), url=f"http://127.0.0.1:{server.server_port}/",
                                       table="dropped", writers=2, first_seq=0, rows=2, seconds=0.3)
                self.assertEqual(crash.write(args), 0)
                result = crash.verify(run, {}, 2, True, 1)
                self.assertGreater(result["cycle_acked_batches"], 0)
                self.assertEqual(result["verify_errors"], [])
                for index in range(2):
                    self.assertTrue((run / "cycle-1" / f"exit-{index}.log").exists())
                # A rejected batch ends a timed writer early, which fails the command.
                status[0] = 503
                args.dir, args.seconds = str(run / "cycle-2"), 30
                self.assertEqual(crash.write(args), 1)
        finally:
            server.shutdown()
            server.server_close()


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
        round_number, concurrency, config, *image = cell_id.split("-")
        path = root / "cells" / cell_id
        (path / "greptimedb-1").mkdir(parents=True)
        (path / "cell.json").write_text(json.dumps({
            "id": cell_id, "round": int(round_number), "concurrency": int(concurrency), "config": config,
            "image": image[0] if image else "",
            "wal": bench.WAL_CONFIGS[config], "metrics_scraped": True, "env": {},
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

    def test_ratios_cover_both_controls_and_restart_cells_report_recovery(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            restart = {"restart": True, "restart_scraped": True, "restart_seconds": 3.5, "indexed_before_kill": 117,
                       "min_objects": 6000,
                       "restart_query_seconds": 4.25, "restart_count": 6000,
                       "server_cpu_seconds": 122.0, "host_cpu_busy": 0.5, "host_cpus": 8}
            self.write_cell(root, "1-8-R", 100.0)
            self.write_cell(root, "1-8-R0", 125.0)
            self.write_cell(root, "1-8-D", 50.0, metrics=ACK_BUCKETS, env={"RUST_LOG": "info"}, **restart)
            self.write_cell(root, "1-8-E", 75.0)
            (root / "cells" / "1-8-D" / "metrics-restart.prom").write_text(
                "greptime_logstore_object_store_wal_recovery_seconds_sum 1.5\n"
                "greptime_logstore_object_store_wal_recovery_seconds_count 1\n"
                "greptime_logstore_object_store_wal_recovered_objects_total 118\n")
            rows = [summary.load_cell(path) for path in sorted((root / "cells").iterdir())]
            pairs = {p["ratio"]: p["geomean"] for p in summary.ratios(rows)}
            self.assertEqual(pairs, {"D/R": 0.5, "D/R0": 0.4, "E/R": 0.75, "E/R0": 0.6})
            cell = next(r for r in rows if r["id"] == "1-8-D")
            self.assertEqual(cell["restart"], {"wall_seconds": 3.5, "query_seconds": 4.25, "rows": 6000,
                                               "recovery_seconds": 1.5, "recovered_objects": 118,
                                               "indexed_before_kill": 117, "min_objects": 6000, "replay": None})
            self.assertEqual(cell["server_cpu_cores"], 2.0)
            output = summary.render({}, rows, summary.ratios(rows))
            self.assertIn("| 1-8-D | RUST_LOG=info | 120 | 117 (under 6,000) | 3.50 | 4.25 | 1.500 | 118 "
                          "| 6,000 |", output)
            self.assertIn("| 2.00 | 50% of 8 |", output)
            self.assertIn("| 8 | E/R0 | 0.600 | 0.600 |", output)

    def test_traffic_idle_crash_and_pinned_cells_are_summarized(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            traffic = ("greptime_logstore_object_store_wal_created_objects_total 600\n"
                       "greptime_logstore_object_store_wal_object_bytes_sum 3000000000\n"
                       "greptime_logstore_object_store_wal_object_bytes_count 600\n"
                       'opendal_operation_duration_seconds_count{scheme="s3",operation="write"} 650\n'
                       'opendal_operation_duration_seconds_count{scheme="s3",operation="read"} 40\n')
            # The final scrape follows a 180 s idle window that created 2 objects of 1 MB.
            self.write_cell(root, "1-32-E", 90.0, idle_seconds=180, group="", metrics=traffic.replace(
                " 600\n", " 602\n", 1).replace("3000000000", "3002000000").replace("count 600", "count 602")
                .replace("} 650", "} 652"))
            (root / "cells" / "1-32-E" / "metrics-idle.prom").write_text(traffic.replace("} 650", "} 648"))
            cell = summary.load_cell(root / "cells" / "1-32-E")
            self.assertEqual((cell["objects_per_second"], cell["mean_object_bytes"], cell["sealed_on_size"]),
                             (10.0, 5e6, False))
            self.assertAlmostEqual(cell["wal_mb_per_second"], 50.0)
            self.assertEqual(cell["idle"], {"seconds": 180, "creates": 2, "operations": {"write": 4}})
            path = root / "cells" / "pin-1-8-D"
            path.mkdir(parents=True)
            (path / "cell.json").write_text(json.dumps({
                "id": "pin-1-8-D", "group": "pin", "mode": "crash", "round": 1, "concurrency": 8, "config": "D",
                "wal": bench.WAL_CONFIGS["D"], "exit_code": 0, "metrics_scraped": True}))
            (path / "cycles.jsonl").write_text(json.dumps({
                "cycle": 1, "kill_after_seconds": 540.0, "load_seconds": 540.0, "cycle_acked_batches": 4000,
                "cycle_present_batches": 4003, "cycle_lost_batches": 0, "cycle_lost_rows": 0, "cycle_extra_batches": 3,
                "in_flight": 8, "total_found_batches": 4003, "total_acked_batches": 4000, "total_lost_batches": 0,
                "restart_seconds": 4.5, "pin_rows": 1, "passed": True, "metrics_complete": True,
                "kill_errors": [], "verify_errors": []}) + "\n")
            (path / "metrics-start-1.prom").write_text("greptime_logstore_object_store_wal_created_objects_total 1\n")
            (path / "metrics-cycle-1.prom").write_text(
                "greptime_logstore_object_store_wal_indexed_objects 5400\n"
                "greptime_logstore_object_store_wal_created_objects_total 5401\n"
                "greptime_logstore_object_store_wal_object_bytes_sum 10800000000\n"
                "greptime_logstore_object_store_wal_object_bytes_count 5400\n"
                'opendal_operation_duration_seconds_count{scheme="s3",operation="write"} 9000\n'
                'opendal_operation_duration_seconds_count{scheme="fs",operation="write"} 70\n')
            (path / "metrics-restart-1.prom").write_text(
                "greptime_logstore_object_store_wal_recovery_seconds_sum 2.5\n"
                "greptime_logstore_object_store_wal_recovery_seconds_count 1\n"
                "greptime_logstore_object_store_wal_recovered_objects_total 5399\n")
            (path / "objects.tsv").write_text("100 300 0 301\n130 600 0 601\n160 missing\n")
            rows = [summary.load_cell(p) for p in sorted((root / "cells").iterdir())]
            output = summary.render({}, rows, summary.ratios(rows))
            self.assertIn("| 1-32-E | 50.0 | 10.0 | 4.77 | no | read 40, write 648 | — | 180 | 2 | write 4 |", output)
            self.assertIn("| pin-1-8-D | 1 | 540.0 | 4,000 | 4,003 | 0 | 0 | 3 | 8 | 4,003 | 4,000 | 0 | 5,400 "
                          "| 4.50 | 2.500 | 5,399 | 1 | pass |", output)
            self.assertIn("| pin-1-8-D | 1 | 540.0 | 20.0 | 10.0 | 1.91 | no | write 9,000 | — |", output)
            self.assertIn("| pin-1-8-D | 300, 600, missing | 1 | 0 | 601 |", output)

    def test_partitioned_load_cells_report_client_latency_entries_and_regions(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            metrics = (ACK_BUCKETS + "greptime_logstore_object_store_wal_object_entries_sum 4000\n"
                       "greptime_logstore_object_store_wal_object_entries_count 500\n"
                       "greptime_logstore_object_store_wal_object_bytes_count 500\n")
            self.write_cell(root, "1-8-D", 80.0, metrics=metrics, partitions=8)
            path = root / "cells" / "1-8-D"
            transcript = [{"step": "create_table", "duration_seconds": 9.0}]
            transcript += [{"step": "grpc_write_batch", "status": "success", "duration_seconds": d / 1000}
                           for d in range(1, 101)]
            (path / "greptimedb-1" / "load-transcript.jsonl").write_text(
                "".join(json.dumps(r) + "\n" for r in transcript))
            (path / "regions.json").write_text(json.dumps({"output": [{"records": {"rows": [
                [f"p{i}", 4398046511104 + i, 750 + i, 1000] for i in range(8)]}}]}))
            cell = summary.load_cell(path)
            self.assertEqual((cell["client_p50"], cell["client_p99"]), (0.05, 0.099))
            self.assertEqual((cell["entries_per_object"], cell["appends_per_object"]), (8.0, 0.2))
            self.assertEqual(cell["region_rows"], [750 + i for i in range(8)])
            output = summary.render({}, [cell], [])
            self.assertIn("| 1-8-D | 50.0 / 99.0 | 8.0 | 0.20 | 8 | 750 / 757 |", output)

    def test_restarts_report_what_each_region_replayed(self):
        log = ("INFO mito2::region::opener: Replay WAL for region: 4398046511104(1024, 0), provider: ObjectStore("
               'ObjectStoreProvider { region_id: 4398046511104(1024, 0), prefix: "wal" }), rows recovered: 0, '
               "replay from entry id: 1048578, last entry id: 1048577, total timeseries replayed: 0, elapsed: 60.581µs\n"
               "INFO mito2::region::opener: Replay WAL for region: 4402341478400(1025, 0), provider: ObjectStore("
               'ObjectStoreProvider { region_id: 4402341478400(1025, 0), prefix: "wal" }), rows recovered: 12625000, '
               "replay from entry id: 4853858306, last entry id: 8164212737, total timeseries replayed: 82, "
               "elapsed: 95.150155247s\n"
               "INFO mito2::region::opener: Replay WAL for region: 4402341478401(1025, 1), provider: ObjectStore("
               'ObjectStoreProvider { region_id: 4402341478401(1025, 1), prefix: "wal" }), rows recovered: 5, '
               "replay from entry id: 8164212736, last entry id: 8164212737, total timeseries replayed: 1, "
               "elapsed: 12.5ms\n")
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory)
            (path / "server-restart.log").write_text(log)
            metrics = summary.parse_metrics("greptime_logstore_object_store_wal_read_seconds_sum 95.15\n"
                                            "greptime_logstore_object_store_wal_read_seconds_count 3\n")
            result = summary.replay(path / "server-restart.log", metrics)
            self.assertEqual((result["rows"], result["object_span"], result["read_seconds"], result["reads"]),
                             (12_625_005, 3158 + 1, 95.15, 3))
            self.assertAlmostEqual(result["seconds"], 95.150155247)
            self.assertEqual([(r["region"], r["rows"], r["object_span"]) for r in result["regions"]],
                             [(4398046511104, 0, 0), (4402341478400, 12_625_000, 3158), (4402341478401, 5, 1)])
            self.assertEqual((result["regions"][1]["first_entry_id"], result["regions"][1]["last_entry_id"]),
                             (4853858306, 8164212737))
            self.assertAlmostEqual(result["regions"][0]["seconds"], 60.581e-6)
            self.assertEqual(summary.replayed(result["regions"]),
                             "4402341478400: 12,625,000 rows, object sequences 4,629–7,786, 95.15 s; "
                             "4402341478401: 5 rows, object sequences 7,786–7,786, 0.01 s")
            self.assertIsNone(summary.replay(path / "absent.log", metrics))
            raft = log.replace("ObjectStore(", "RaftEngine(")
            (path / "server-restart.log").write_text(raft)
            self.assertIsNone(summary.replay(path / "server-restart.log", metrics)["object_span"])
            # A region flushed in object 100 that next wrote in object 200 reads one object but spans 101.
            gap = log.replace("replay from entry id: 4853858306", f"replay from entry id: {(100 << 20) + 2}") \
                .replace("last entry id: 8164212737", f"last entry id: {(200 << 20) + 1}")
            (path / "server-restart.log").write_text(gap)
            self.assertEqual(summary.replay(path / "server-restart.log", metrics)["regions"][1]["object_span"], 101)

    def test_a_drop_cell_reports_the_objects_collected_after_its_restart(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            path = root / "cells" / "drop-1-8-D"
            (path / "dropped").mkdir(parents=True)
            (path / "cell.json").write_text(json.dumps({
                "id": "drop-1-8-D", "group": "drop", "mode": "drop", "round": 1, "concurrency": 8, "config": "D",
                "wal": bench.WAL_CONFIGS["D"], "exit_code": 0, "restart_seconds": 2.5, "restart_scraped": True}))
            write_lines(path / "dropped" / "acked-0.log", [0, 8, 16])
            write_lines(path / "dropped" / "acked-1.log", [1, 9])
            (path / "metrics-written.prom").write_text("greptime_logstore_object_store_wal_indexed_objects 300\n")
            (path / "metrics-restart.prom").write_text(
                "greptime_logstore_object_store_wal_recovered_objects_total 280\n"
                "greptime_logstore_object_store_wal_deleted_objects_total 279\n")
            (path / "collected.tsv").write_text("100 281\n101 120\n104 2\n")
            cell = summary.load_cell(path)
            self.assertEqual(cell["drop"], {"acked_batches": 5, "indexed_before_drop": 300, "restart_seconds": 2.5,
                                            "recovered_objects": 280, "indexed_at_end": 2,
                                            "deleted_after_restart": 279, "seconds_to_collect": 4})
            output = summary.render({}, [cell], [])
            self.assertIn("| drop-1-8-D | 5 | 300 | 2.50 | 280 | 2 | 279 | 4 | collected |", output)

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


    def test_images_are_compared_with_the_first_in_every_round(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            images = [{"label": label, "image": f"docker.io/greptime/greptimedb-dev:{label}"} for label in ("base", "pr1")]
            plan = bench.build_plan("8", "6000", "1000", "2", smoke=False, configs="R,D", images=json.dumps(images),
                                    warmup=True)
            (root / "plan.json").write_text(json.dumps(plan))
            (root / "run.json").write_text(json.dumps({"rows": 6000, "batch_rows": 1000, "greptimedb_images": [
                i | {"image": f"greptime/greptimedb-dev@sha256:{n * 64}", "version": "1.4.0", "commit": f"{n * 7}"}
                for i, n in zip(images, "ab")]}))
            # The warm-up cells are far off and must not move any ratio.
            speeds = {"0-8-R-base": 10.0, "0-8-R-pr1": 999.0,
                      "1-8-R-base": 100.0, "1-8-R-pr1": 100.0, "1-8-D-base": 50.0, "1-8-D-pr1": 40.0,
                      "2-8-D-pr1": 66.0, "2-8-D-base": 55.0, "2-8-R-pr1": 100.0, "2-8-R-base": 110.0}
            for cell_id, speed in speeds.items():
                self.write_cell(root, cell_id, speed, warmup=cell_id.startswith("0-"),
                                host_load_start=[0.5, 0.4, 0.3], host_load_end=[1.25, 0.5, 0.3])
            with open(root / "cells" / "2-8-D-pr1" / "greptimedb-1" / "load-transcript.jsonl", "w") as out:
                for seconds in (0.2, 0.3):
                    out.write(json.dumps({"step": "grpc_write_batch", "status": "success", "duration_seconds": seconds}) + "\n")
            output = subprocess.run([sys.executable, str(SCRIPTS / "oswal-s3-bench-summary.py"), "--root", str(root)],
                                    check=True, capture_output=True, text=True).stdout
            result = json.loads((root / "summary.json").read_text())
            compared = {(c["config"], c["image"]): c for c in result["image_ratios"]}
            self.assertEqual(list(compared), [("R", "base"), ("R", "pr1"), ("D", "base"), ("D", "pr1")])
            durable = compared[("D", "pr1")]
            self.assertEqual(durable["ratio_to"], "base")
            self.assertEqual(durable["per_round"], [0.8, 1.2])
            self.assertAlmostEqual(durable["geomean"], 0.96 ** 0.5)
            self.assertEqual((durable["min"], durable["max"]), (0.8, 1.2))
            self.assertEqual([r["rows_per_second"] for r in durable["rounds"]], [40.0, 66.0])
            self.assertEqual((durable["rounds"][1]["client_p50"], durable["rounds"][1]["client_p99"]), (0.2, 0.3))
            self.assertEqual(durable["rounds"][0]["host_load_end"], [1.25, 0.5, 0.3])
            self.assertEqual((compared[("D", "base")]["per_round"], compared[("D", "base")]["geomean"]), (None, None))
            # The ratios to the Raft Engine control stay within each image.
            pairs = {(r["image"], r["ratio"]): r["per_round"] for r in result["ratios"]}
            self.assertEqual(pairs, {("base", "D/R"): [0.5, 0.5], ("pr1", "D/R"): [0.4, 0.66]})
            self.assertIn("| base | `greptime/greptimedb-dev@sha256:" + "a" * 64 + "` | 1.4.0 | `aaaaaaa` |", output)
            self.assertIn("| — | 8 | D | pr1 | 40; 66 | — / —; 200.0 / 300.0 | 0.800, 1.200 | 0.980 | 0.800 | 1.200 |", output)
            self.assertIn("| — | 8 | D | base | 50; 55 | — / —; — / — | — | — | — | — |", output)
            self.assertIn("| 0-8-R-pr1 (warm-up) |", output)
            self.assertIn("| `/dev/nvme1n1` | 0.50 / 1.25 |", output)
            self.assertIn("| — | pr1 | 8 | D/R | 0.400, 0.660 | 0.514 |", output)


FAKE_DOCKER = r"""#!/usr/bin/env bash
printf '%s\n' "docker $*" >> "$FAKE_LOG"
case "$1" in
    run)
        for arg in "$@"; do [[ "$arg" != -d ]] || { echo $(($(cat "$FAKE_LOG.runs" 2>/dev/null || echo 0) + 1)) > "$FAKE_LOG.runs"; rm -f "${FAKE_KILL_FLAG:-/nonexistent}"; echo "keys=${GREPTIMEDB_STANDALONE__STORAGE__ACCESS_KEY_ID:+set}${GREPTIMEDB_STANDALONE__STORAGE__SECRET_ACCESS_KEY:+set}" >> "$FAKE_LOG"; echo fakecid; exit 0; }; done
        for arg in "$@"; do
            case "$arg" in type=bind,src=*,dst=/results) results=${arg#type=bind,src=}; results=${results%,dst=/results} ;; esac
        done
        [[ "$FAKE_LOAD_RC" == 0 ]] || exit "$FAKE_LOAD_RC"
        mkdir -p "$results/greptimedb-1"
        echo "{\"count_before\": 0, \"count_after\": $FAKE_ROWS, \"load_seconds\": 2, \"rows_per_second\": 3000, \"table\": \"agent_observations\"}" \
            > "$results/greptimedb-1/load-result.json" ;;
    kill) touch "${FAKE_KILL_FLAG:-/dev/null}" ;;
    rm) [[ -z "${FAKE_LOAD_AFTER:-}" ]] || echo "$FAKE_LOAD_AFTER" > "$OSWAL_LOADAVG" ;;
    inspect) echo true ;;
    exec) [[ "$*" == *cpu.stat* ]] && echo 'usage_usec 5000000' || echo 'version: 1.4.0-test' ;;
    logs) echo 'server log'
        # The rows a server replayed are its start number, so each log can be told apart.
        [[ -z "${FAKE_REPLAY:-}" ]] || echo "INFO Replay WAL for region: 4398046511104(1024, 0), provider: ObjectStore(x), rows recovered: $(cat "$FAKE_LOG.runs"), replay from entry id: 1048577, last entry id: 2097153, total timeseries replayed: 1, elapsed: 1.5s" ;;
esac
"""
FAKE_CURL = r"""#!/usr/bin/env bash
printf '%s\n' "curl $*" >> "$FAKE_LOG"
[[ "$*" != *"--max-time 10"* || "${FAKE_SAMPLE_RC:-0}" == 0 ]] || exit "$FAKE_SAMPLE_RC"
for arg in "$@"; do
    [[ "$arg" != *"GROUP BY seq"* ]] || { python3 -c 'import collections, json, sys
counts = collections.Counter(int(l) for l in open(sys.argv[1]) if l.strip())
print(json.dumps({"output": [{"records": {"rows": [[k, v * int(sys.argv[2])] for k, v in counts.items()]}}]}))' \
        "$FAKE_FOUND" "$FAKE_BATCH_ROWS"; exit 0; }
done
for arg in "$@"; do
    [[ "$arg" != *region_statistics* ]] || { python3 -c 'import json, sys
print(json.dumps({"output": [{"records": {"rows": [[f"p{i}", i, int(n), 1] for i, n in enumerate(sys.argv[1].split(","))]}}]}))' \
        "$FAKE_REGION_ROWS"; exit 0; }
done
for arg in "$@"; do
    [[ "$arg" != */v1/sql* ]] || { echo "{\"output\": [{\"records\": {\"rows\": [[${FAKE_COUNT:-6000}]]}}]}"; exit 0; }
    [[ "$arg" != */metrics ]] || {
        [[ "$FAKE_METRICS_RC" == 0 ]] || exit "$FAKE_METRICS_RC"
        printf '%s\n' 'greptime_logstore_object_store_wal_poisoned_total 1' 'other_total 2'
        [[ -z "${FAKE_FULL_METRICS:-}" ]] || printf '%s\n' \
            "greptime_logstore_object_store_wal_indexed_objects $FAKE_FULL_METRICS" \
            'greptime_logstore_object_store_wal_recovery_seconds_count 1' \
            "greptime_logstore_object_store_wal_recovered_objects_total ${FAKE_RECOVERED:-0}"
        # Every server counts the objects it collects from its start: with FAKE_COLLECTING one more on
        # every scrape; with FAKE_FIRST_COLLECTS_AFTER the first server collects one object once it has
        # been scraped that many times.
        run=$(cat "$FAKE_LOG.runs") n=$(($(cat "$FAKE_LOG.scrapes.$run" 2>/dev/null || echo 0) + 1))
        echo "$n" > "$FAKE_LOG.scrapes.$run"
        [[ -z "${FAKE_COLLECTING:-}" ]] || echo "greptime_logstore_object_store_wal_deleted_objects_total $((n - 1))"
        [[ -z "${FAKE_FIRST_COLLECTS_AFTER:-}" || "$run" != 1 || "$n" -le "$FAKE_FIRST_COLLECTS_AFTER" ]] \
            || echo 'greptime_logstore_object_store_wal_deleted_objects_total 1'
        exit 0;
    }
done
"""


class CellLifecycleTest(unittest.TestCase):  # the run script against fake docker and curl

    def run_cells(self, root, load_rc, metrics_rc=0, restart=False, count=6000, groups="", images="", checked=None):
        bin_dir = root / "bin"
        bin_dir.mkdir()
        for name, body in (("docker", FAKE_DOCKER), ("curl", FAKE_CURL)):
            (bin_dir / name).write_text(body)
            (bin_dir / name).chmod(0o755)
        plan = bench.build_plan("8", "6000", "1000", "1", smoke=False, configs="D,E", restart=restart,
                                extra_env="RUST_LOG=info", groups=groups, images=images)
        if checked is None:
            checked = [{"label": label, "image": f"greptimedb{label}@sha256:" + "a" * 64} for label in plan["images"]]
        (root / "results").mkdir()
        (root / "results" / "plan.json").write_text(json.dumps(plan))
        env = os.environ | {
            "PATH": f"{bin_dir}:{os.environ['PATH']}", "FAKE_LOG": str(root / "fake.log"),
            "FAKE_LOAD_RC": str(load_rc), "FAKE_METRICS_RC": str(metrics_rc), "FAKE_ROWS": "6000", "FAKE_COUNT": str(count),
            "PLAN": str(root / "results" / "plan.json"),
            "RESULTS": str(root / "results"), "CORPUS_DIR": str(root / "corpus"), "OWNER": "o11ybench-oswal-1-1",
            "RUNTIME_IMAGE": "runtime@sha256:" + "b" * 64, "GREPTIMEDB_IMAGES": json.dumps(checked),
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
            self.assertIn("RUST_LOG=info", config)
            self.assertIn("-e RUST_LOG=info ", log)
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

    def test_an_idle_window_scrapes_before_and_after_it(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            groups = json.dumps([{"name": "idle", "configs": "D", "idle_seconds": 1}])
            cells, _, log, _ = self.run_cells(root, load_rc=0, groups=groups)
            self.assertEqual(cells.returncode, 0, cells.stdout + cells.stderr)
            cell = root / "results" / "cells" / "idle-1-8-D"
            self.assertTrue((cell / "metrics-idle.prom").exists())
            self.assertTrue((cell / "metrics.prom").exists())
            self.assertEqual(log.count("/metrics"), 2)
            self.assertIn(f"type=bind,src={root}/corpus/corpus-6000,dst=/work/corpus,readonly", log)

    def run_crash(self, root, metrics_rc=0, pin=False, sample_rc=0, full_metrics="5", groups=None, env=None,
                  status=200):
        """A crash cell, or another group that writes through the crash writer, against fake docker
        and a stub SQL endpoint that the fake kill takes down."""
        import http.server
        import threading
        flag, found = root / "killed", root / "found.log"
        found.write_text("")
        lock = threading.Lock()

        class Handler(http.server.BaseHTTPRequestHandler):
            def do_POST(self):
                body = urllib.parse.unquote_plus(self.rfile.read(int(self.headers["Content-Length"])).decode())
                if flag.exists():
                    self.close_connection = True
                    self.connection.close()
                    return
                seq = re.search(r"VALUES [(](\d+),", body).group(1)
                with lock, open(found, "a") as out:
                    out.write(seq + "\n")
                self.send_response(status)
                self.end_headers()

            def log_message(self, *args):
                pass

        class Server(http.server.ThreadingHTTPServer):
            request_queue_size = 64  # all eight writers connect at once

        server = Server(("127.0.0.1", 0), Handler)
        threading.Thread(target=server.serve_forever, daemon=True).start()
        groups = groups or json.dumps([{"name": "crash", "configs": "D", "mode": "crash", "cycles": 2,
                                        "kill_after": [1, 1], "pin": pin}])
        old = os.environ.copy()
        os.environ.update({"OSWAL_SQL_URL": f"http://127.0.0.1:{server.server_port}/v1/sql",
                           "FAKE_KILL_FLAG": str(flag), "FAKE_FOUND": str(found), "FAKE_BATCH_ROWS": "1000",
                           "OSWAL_SAMPLE_SECONDS": "0.2", "FAKE_SAMPLE_RC": str(sample_rc),
                           "FAKE_FULL_METRICS": full_metrics} | (env or {}))
        try:
            return self.run_cells(root, load_rc=0, metrics_rc=metrics_rc, groups=groups,
                                  count=(env or {}).get("FAKE_COUNT", 6000))
        finally:
            os.environ.clear()
            os.environ.update(old)
            server.shutdown()
            server.server_close()

    def test_a_crash_cell_kills_restarts_and_checks_every_cycle(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cells, _, log, _ = self.run_crash(root, env={"FAKE_REPLAY": "1"})
            self.assertEqual(cells.returncode, 0, cells.stdout + cells.stderr)
            cell = root / "results" / "cells" / "crash-1-8-D"
            # Server 1 is loaded and killed; servers 2 and 3 are the restarts of cycles 1 and 2.
            replays = [c["replay"]["rows"] for c in summary.load_cell(cell)["cycles"]]
            self.assertEqual(replays, [2, 3])
            cycles = [json.loads(line) for line in (cell / "cycles.jsonl").read_text().splitlines()]
            self.assertEqual([c["cycle"] for c in cycles], [1, 2])
            for c in cycles:
                self.assertTrue(c["passed"] and c["metrics_complete"], c)
                self.assertEqual((c["cycle_lost_batches"], c["writers_at_kill"]), (0, 8))
                self.assertGreater(c["cycle_acked_batches"], 0)
            self.assertEqual(log.count("docker kill --signal KILL fakecid"), 2)
            self.assertEqual(log.count("docker wait fakecid"), 2)
            self.assertIn("GREPTIMEDB_STANDALONE__STORAGE__DATA_HOME=/oswal_home", (cell / "config.env").read_text())
            for name in ("metrics-start-1.prom", "metrics-cycle-2.prom", "metrics-restart-2.prom", "server.log",
                         "server-restart-1.log", "server-restart-2.log"):
                self.assertTrue((cell / name).exists(), name)

    def test_failed_pinned_samples_are_missing_not_zero(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cells, _, log, _ = self.run_crash(root, pin=True, sample_rc=28, env={"FAKE_COUNT": "1"})
            self.assertEqual(cells.returncode, 0, cells.stdout + cells.stderr)
            cell = root / "results" / "cells" / "crash-1-8-D"
            samples = (cell / "objects.tsv").read_text().splitlines()
            self.assertTrue(samples and all(line.split()[1:] == ["missing"] for line in samples), samples)
            self.assertIn("sql=INSERT INTO pin VALUES (1, 0)", log)
            self.assertIn("WITH ('auto_flush_interval' = '1d')", urllib.parse.unquote_plus(log))
            self.assertEqual(len((cell / "cycles.jsonl").read_text().splitlines()), 2)

    def test_a_pinned_cell_fails_when_objects_the_pin_held_are_collected(self):
        # Each server collects two objects before its kill and no periodic sample is taken. With one
        # object below the pin the first server already exceeds it; with two, only both together do.
        for below_pin, failed_cycle in (("1", 1), ("2", 2)):
            with self.subTest(below_pin=below_pin), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                cells, _, _, _ = self.run_crash(root, pin=True, full_metrics=below_pin, env={
                    "FAKE_COUNT": "1", "FAKE_COLLECTING": "1", "OSWAL_SAMPLE_SECONDS": "30"})
                cell = root / "results" / "cells" / "crash-1-8-D"
                self.assertEqual(json.loads((cell / "cell.json").read_text())["exit_code"], 11,
                                 cells.stdout + cells.stderr)
                self.assertFalse((cell / "objects.tsv").exists())
                self.assertEqual(len((cell / "cycles.jsonl").read_text().splitlines()), failed_cycle)
                self.assertIn(f"cycle {failed_cycle} collected WAL objects the pin held", cells.stdout + cells.stderr)

    def test_a_pinned_cell_may_collect_the_objects_below_its_pin_late(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            # The start object, the one object indexed before the pin, goes only after the first cycle's
            # baseline: the pin scrape and that baseline are the first server's first two scrapes.
            cells, _, _, _ = self.run_crash(root, pin=True, full_metrics="1", env={
                "FAKE_COUNT": "1", "FAKE_FIRST_COLLECTS_AFTER": "2", "OSWAL_SAMPLE_SECONDS": "30"})
            cell = root / "results" / "cells" / "crash-1-8-D"
            self.assertEqual(json.loads((cell / "cell.json").read_text())["exit_code"], 0, cells.stdout + cells.stderr)
            self.assertNotIn("deleted_objects_total", (cell / "metrics-start-1.prom").read_text())
            self.assertIn("deleted_objects_total 1", (cell / "metrics-cycle-1.prom").read_text())
            self.assertEqual(len((cell / "cycles.jsonl").read_text().splitlines()), 2)

    def test_a_durable_pinned_cycle_fails_unless_its_pin_row_is_there_once(self):
        for count in ("0", "2"):
            with self.subTest(count=count), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                self.run_crash(root, pin=True, env={"FAKE_COUNT": count})
                cell = root / "results" / "cells" / "crash-1-8-D"
                cycle = json.loads((cell / "cycles.jsonl").read_text().splitlines()[0])
                self.assertEqual((cycle["passed"], cycle["pin_rows"]), (False, int(count)))
                self.assertIn(f"the pin table holds {count} rows, not 1", cycle["verify_errors"])
                self.assertEqual(json.loads((cell / "cell.json").read_text())["exit_code"], 6)

    def test_a_scrape_without_the_wal_samples_is_incomplete(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cells, _, _, _ = self.run_crash(root, full_metrics="")
            self.assertNotEqual(cells.returncode, 0)
            cell = root / "results" / "cells" / "crash-1-8-D"
            cycle = json.loads((cell / "cycles.jsonl").read_text().splitlines()[0])
            self.assertEqual((cycle["passed"], cycle["metrics_complete"]), (True, False))
            self.assertEqual(json.loads((cell / "cell.json").read_text())["exit_code"], 4)

    def test_a_restart_cell_must_leave_its_minimum_objects_indexed(self):
        for minimum, exit_code in ((5, 0), (6, 7)):
            with self.subTest(minimum=minimum), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                groups = json.dumps([{"name": "rec", "configs": "D", "restart": True, "min_objects": minimum}])
                old = os.environ.copy()
                os.environ["FAKE_FULL_METRICS"] = "5"
                try:
                    cells, _, _, _ = self.run_cells(root, load_rc=0, groups=groups)
                finally:
                    os.environ.clear()
                    os.environ.update(old)
                recorded = json.loads((root / "results" / "cells" / "rec-1-8-D" / "cell.json").read_text())
                self.assertEqual((recorded["exit_code"], recorded["indexed_before_kill"], recorded["restart_count"]),
                                 (exit_code, 5, 6000))

    def test_a_partitioned_cell_creates_its_table_and_checks_every_region_took_rows(self):
        for region_rows, exit_code in (("750,750,750,750,750,750,750,750", 0), ("1000,1000,1000,0,1000,1000,1000,0", 8)):
            with self.subTest(region_rows=region_rows), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                (root / "corpus" / "corpus-6000").mkdir(parents=True)
                (root / "corpus" / "corpus-6000" / "summary.json").write_text(json.dumps(
                    {"start": "2026-07-10T00:00:00Z", "end": "2026-07-10T01:00:00Z"}))
                groups = json.dumps([{"name": "multi", "configs": "D", "partitions": 8}])
                old = os.environ.copy()
                os.environ["FAKE_REGION_ROWS"] = region_rows
                try:
                    cells, _, log, _ = self.run_cells(root, load_rc=0, groups=groups)
                finally:
                    os.environ.clear()
                    os.environ.update(old)
                cell = root / "results" / "cells" / "multi-1-8-D"
                self.assertEqual(json.loads((cell / "cell.json").read_text())["exit_code"], exit_code,
                                 cells.stdout + cells.stderr)
                self.assertIn("PARTITION ON COLUMNS (`event_time`)", urllib.parse.unquote_plus(log))
                self.assertIn(f"type=bind,src={SCRIPTS}/oswal-s3-bench-load.sh,dst=/work/oswal-s3-bench-load.sh,readonly",
                              log)
                self.assertIn(" /work/oswal-s3-bench-load.sh", log)
                self.assertNotIn("greptimedb.sh load", log)
                self.assertEqual(len(json.loads((cell / "regions.json").read_text())["output"][0]["records"]["rows"]), 8)

    def test_a_drop_cell_restarts_and_waits_for_the_dropped_objects_to_go(self):
        for indexed, recovered, exit_code in (("2", "0", 0), ("50", "200", 0), ("50", "60", 9)):
            with self.subTest(indexed=indexed, recovered=recovered), tempfile.TemporaryDirectory() as directory:
                root = Path(directory)
                groups = json.dumps([{"name": "drop", "mode": "drop", "configs": "D", "write_seconds": 1}])
                cells, _, log, _ = self.run_crash(root, full_metrics=indexed, groups=groups,
                                                  env={"FAKE_RECOVERED": recovered, "OSWAL_COLLECT_SECONDS": "1"})
                cell = root / "results" / "cells" / "drop-1-8-D"
                recorded = json.loads((cell / "cell.json").read_text())
                self.assertEqual(recorded["exit_code"], exit_code, cells.stdout + cells.stderr)
                self.assertIsInstance(recorded["restart_seconds"], float)
                sql = urllib.parse.unquote_plus(log)
                for statement in ("CREATE TABLE dropped", "CREATE TABLE kept", "DROP TABLE dropped",
                                  "ADMIN FLUSH_TABLE('kept')", "INSERT INTO kept VALUES (-1, "):
                    self.assertIn(statement, sql)
                self.assertLess(sql.index("DROP TABLE dropped"), log.index("docker kill --signal KILL fakecid"))
                self.assertEqual(log.count("docker kill --signal KILL fakecid"), 1)
                self.assertTrue(list((cell / "dropped").glob("acked-*.log")))
                self.assertTrue(list((cell / "kept").glob("acked-*.log")))
                for name in ("metrics-written.prom", "metrics-restart.prom", "server.log",
                             "server-restart.log", "collected.tsv"):
                    self.assertTrue((cell / name).exists(), name)

    def test_a_drop_cell_whose_writers_fail_stops_before_the_drop(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            groups = json.dumps([{"name": "drop", "mode": "drop", "configs": "D", "write_seconds": 1}])
            cells, _, log, _ = self.run_crash(root, full_metrics="2", groups=groups, status=503)
            recorded = json.loads((root / "results" / "cells" / "drop-1-8-D" / "cell.json").read_text())
            self.assertEqual(recorded["exit_code"], 10, cells.stdout + cells.stderr)
            self.assertNotIn("DROP TABLE", urllib.parse.unquote_plus(log))
            self.assertNotIn("docker kill", log)

    def test_a_crash_cycle_without_its_scrapes_fails_the_cell(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cells, _, _, _ = self.run_crash(root, metrics_rc=22)
            self.assertNotEqual(cells.returncode, 0)
            cell = root / "results" / "cells" / "crash-1-8-D"
            cycle = json.loads((cell / "cycles.jsonl").read_text().splitlines()[0])
            self.assertEqual((cycle["passed"], cycle["metrics_complete"]), (True, False))
            self.assertEqual(json.loads((cell / "cell.json").read_text())["exit_code"], 4)

    def test_a_restart_that_loses_rows_fails_the_cell(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            cells, _, _, _ = self.run_cells(root, load_rc=0, restart=True, count=5999)
            self.assertNotEqual(cells.returncode, 0)
            recorded = json.loads((root / "results" / "cells" / "1-8-D" / "cell.json").read_text())
            self.assertEqual((recorded["exit_code"], recorded["restart_count"]), (5, 5999))


    def test_every_cell_starts_its_own_image_and_records_the_host_load(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            (root / "loadavg").write_text("0.50 0.40 0.30 1/200 99\n")
            images = json.dumps([{"label": label, "image": f"docker.io/greptime/greptimedb-dev:{label}"}
                                 for label in ("base", "pr1")])
            old = os.environ.copy()
            os.environ.update({"OSWAL_LOADAVG": str(root / "loadavg"), "FAKE_LOAD_AFTER": "1.25 1.00 0.75 1/200 99"})
            try:
                cells, _, log, _ = self.run_cells(root, load_rc=0, images=images)
            finally:
                os.environ.clear()
                os.environ.update(old)
            self.assertEqual(cells.returncode, 0, cells.stdout + cells.stderr)
            starts = [line for line in log.splitlines() if line.startswith("docker run -d")]
            self.assertEqual([re.search(r"--name o11ybench-oswal-1-1-(\S+) ", line).group(1) for line in starts],
                             ["1-8-D-base", "1-8-D-pr1", "1-8-E-base", "1-8-E-pr1"])
            for line in starts:
                label = line.split("--name ")[1].split()[0].rsplit("-", 1)[1]
                self.assertIn(f" greptimedb{label}@sha256:{'a' * 64} standalone start", line)
            self.assertIn(f"TARGET_IMAGE=greptimedbpr1@sha256:{'a' * 64}", log)
            first, second = (json.loads((root / "results" / "cells" / i / "cell.json").read_text())
                             for i in ("1-8-D-base", "1-8-D-pr1"))
            self.assertEqual((first["image"], first["host_load_start"], first["host_load_end"]),
                             ("base", [0.5, 0.4, 0.3], [1.25, 1.0, 0.75]))
            self.assertEqual(second["host_load_start"], [1.25, 1.0, 0.75])

    def test_a_cell_without_a_checked_image_fails(self):
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            images = json.dumps([{"label": label, "image": f"docker.io/greptime/greptimedb-dev:{label}"}
                                 for label in ("base", "pr1")])
            cells, _, log, _ = self.run_cells(root, load_rc=0, images=images,
                                              checked=[{"label": "base", "image": "greptimedb@sha256:" + "a" * 64}])
            self.assertNotEqual(cells.returncode, 0)
            self.assertIn("1-8-D-pr1: no image labelled 'pr1'", cells.stderr)
            self.assertEqual(log.count("docker run -d"), 2)

    def test_check_images_pins_every_image_to_its_digest_and_records_its_version(self):
        fake_docker = r"""#!/usr/bin/env bash
printf '%s\n' "docker $*" >> "$FAKE_LOG"
case "$1 $2" in
    "image inspect") printf '[{"RepoDigests": ["%s@sha256:%064d"]}]\n' "${3%:*}" "$(printf '%s' "$3" | cksum | cut -d' ' -f1)" ;;
    "run --rm") [[ "$*" == *nocommit* ]] || commit="commit: 0123456789abcdef$(printf '%s' "$4" | cksum | cut -c1-4)"
        printf 'greptime\nbranch: main\n%s\nclean: true\nversion: 1.4.0\n' "${commit:-}" ;;
esac
"""
        with tempfile.TemporaryDirectory() as directory:
            root = Path(directory)
            for name, body in (("docker", fake_docker), ("timeout", '#!/usr/bin/env bash\nshift 2\nexec "$@"\n')):
                (root / name).write_text(body)
                (root / name).chmod(0o755)
            env = os.environ | {"PATH": f"{root}:{os.environ['PATH']}", "FAKE_LOG": str(root / "fake.log"),
                                "GITHUB_OUTPUT": str(root / "output")}
            requested = [{"label": label, "image": f"docker.io/greptime/greptimedb-dev:{label}"} for label in ("base", "pr1")]
            done = subprocess.run(["bash", str(SCRIPTS / "oswal-s3-bench-run.sh"), "check-images", json.dumps(requested)],
                                  env=env, capture_output=True, text=True, timeout=60)
            self.assertEqual(done.returncode, 0, done.stderr)
            name, _, value = (root / "output").read_text().strip().partition("=")
            checked = json.loads(value)
            self.assertEqual(name, "greptimedb_images")
            self.assertEqual([(c["label"], c["requested"], c["version"]) for c in checked],
                             [(r["label"], r["image"], "1.4.0") for r in requested])
            for c in checked:
                self.assertRegex(c["image"], r"^docker\.io/greptime/greptimedb-dev@sha256:[0-9]{64}$")
                self.assertTrue(c["commit"].startswith("0123456789abcdef"), c)
                self.assertIn(f"docker run --rm --entrypoint greptime {c['image']} --version", (root / "fake.log").read_text())
            self.assertNotEqual(checked[0]["image"], checked[1]["image"])
            requested[1]["image"] = "docker.io/greptime/nocommit:pr1"
            done = subprocess.run(["bash", str(SCRIPTS / "oswal-s3-bench-run.sh"), "check-images", json.dumps(requested)],
                                  env=env, capture_output=True, text=True, timeout=60)
            self.assertNotEqual(done.returncode, 0)
            self.assertIn("reports no version or commit", done.stderr)


if __name__ == "__main__":
    unittest.main()
