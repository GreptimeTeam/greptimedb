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

"""Offline safety checks for the metric snapshot workflow's budget and cleanup."""

import importlib.util
import io
import json
from pathlib import Path
import tempfile
import unittest
from unittest import mock

SPEC = importlib.util.spec_from_file_location("snapshot_ops", Path(__file__).parents[1] / "metric-snapshot-s3-ops.py")
ops = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(ops)
PREFIX = "metric-snapshot-bench/123-1/"


class FakeS3:
    def __init__(self):
        self.objects = [{"Key": PREFIX + "current", "Size": 11}, {"Key": PREFIX + "data", "Size": 7}]
        self.deleted = []

    def get_paginator(self, operation):
        assert operation == "list_objects_v2"
        parent = self

        class Paginator:
            def paginate(self, **kwargs):
                assert kwargs == {"Bucket": "bucket", "Prefix": PREFIX}
                return iter([{"Contents": parent.objects.copy()}])

        return Paginator()

    def put_object(self, **kwargs):
        self.objects.append({"Key": kwargs["Key"], "Size": len(kwargs["Body"])})
        return {}

    def get_object(self, **kwargs):
        return {"Body": io.BytesIO(b"metric snapshot benchmark")}

    def delete_object(self, **kwargs):
        self.deleted.append(kwargs)
        self.objects = [item for item in self.objects if item["Key"] != kwargs["Key"]]


class OperationsTest(unittest.TestCase):
    def env(self, **changes):
        return dict(GREPTIMEDB_IMAGE="greptime/greptimedb-nightly@sha256:" + "a" * 64,
                    EXPECTED_COMMIT="b" * 40, LOGICAL_TABLES_PER_GROUP="10000",
                    BENCHMARK_TIMEOUT_MINUTES="240", PRIOR_SPEND_USD="0",
                    S3_REGION="ap-southeast-1", RUNNER_REGION="ap-southeast-1", S3_BUCKET="bucket",
                    RUN_PREFIX=PREFIX) | changes

    def test_budget_includes_provision_cleanup_and_other_cost_reserve(self):
        plan = ops.plan(self.env())
        self.assertEqual(plan["attempt_reserve_usd"], "6.4392")
        ops.plan(self.env(PRIOR_SPEND_USD="13.5608"))
        for changes in ({"PRIOR_SPEND_USD": "13.5609"}, {"PRIOR_SPEND_USD": "15"},
                        {"PRIOR_SPEND_USD": "NaN"}, {"PRIOR_SPEND_USD": "-1"},
                        {"BENCHMARK_TIMEOUT_MINUTES": "241"}, {"LOGICAL_TABLES_PER_GROUP": "49"},
                        {"LOGICAL_TABLES_PER_GROUP": "10001"}, {"S3_REGION": "us-east-1"},
                        {"GREPTIMEDB_IMAGE": "greptime/greptimedb:latest"},
                        {"GREPTIMEDB_IMAGE": "docker.io/greptime/greptimedb@sha256:" + "a" * 64},
                        {"GREPTIMEDB_IMAGE": "greptime/greptimedb:nightly@sha256:" + "a" * 64},
                        {"EXPECTED_COMMIT": "abcdef"}):
            with self.subTest(changes=changes), self.assertRaises(ValueError):
                ops.plan(self.env(**changes))

    def watcher_env(self):
        return self.env(GITHUB_REPOSITORY="GreptimeTeam/greptimedb", GITHUB_RUN_ID="123",
                        GITHUB_RUN_ATTEMPT="2", GH_TOKEN="test-token", BENCHMARK_TIMEOUT_MINUTES="30")

    def test_watcher_completed_job_uses_original_attempt_and_does_not_wait(self):
        for conclusion in ("success", "failure", "cancelled"):
            response = io.BytesIO(json.dumps({"jobs": [{"name": "benchmark", "status": "completed",
                                                       "conclusion": conclusion}]}).encode())
            with self.subTest(conclusion=conclusion), \
                    mock.patch.object(ops.urllib.request, "urlopen", return_value=response) as fetch, \
                    mock.patch.object(ops.time, "time", return_value=1005), \
                    mock.patch.object(ops.time, "sleep") as sleep:
                self.assertEqual(ops.wait_for_benchmark(self.watcher_env(), 1000),
                                 {"benchmark_conclusion": conclusion, "owned_attempt": "1"})
                self.assertIn("/runs/123/attempts/1/jobs?", fetch.call_args.args[0].full_url)
                sleep.assert_not_called()

    def test_watcher_queue_reaches_durable_deadline(self):
        deadline = 1000 + (45 + 30 + 10) * 60
        clock = [deadline - 31]

        def advance(seconds):
            self.assertLessEqual(seconds, 30)
            clock[0] += seconds

        def queued(*args, **kwargs):
            return io.BytesIO(b'{"jobs": [{"name": "benchmark", "status": "queued"}]}')

        with mock.patch.object(ops.urllib.request, "urlopen", side_effect=queued) as fetch, \
                mock.patch.object(ops.time, "time", side_effect=lambda: clock[0]), \
                mock.patch.object(ops.time, "sleep", side_effect=advance), \
                self.assertRaisesRegex(TimeoutError, "proceeding to resource cleanup"):
            ops.wait_for_benchmark(self.watcher_env(), 1000)
        self.assertEqual(clock[0], deadline)
        self.assertEqual(fetch.call_count, 2)

    def test_watcher_api_error_fails_immediately_for_cleanup(self):
        with mock.patch.object(ops.urllib.request, "urlopen", side_effect=OSError("API unavailable")), \
                mock.patch.object(ops.time, "time", return_value=1005), \
                mock.patch.object(ops.time, "sleep") as sleep, \
                self.assertRaises(OSError):
            ops.wait_for_benchmark(self.watcher_env(), 1000)
        sleep.assert_not_called()

    def test_cleanup_deletes_every_listed_object(self):
        s3 = FakeS3()
        result = ops.cleanup(s3, "bucket", PREFIX)
        self.assertEqual(result["objects"], 0)
        self.assertEqual(s3.deleted, [{"Bucket": "bucket", "Key": PREFIX + "current"},
                                      {"Bucket": "bucket", "Key": PREFIX + "data"}])

    def test_probe_refuses_occupied_prefix_and_removes_its_object(self):
        with self.assertRaisesRegex(ValueError, "already occupied"):
            ops.probe(FakeS3(), "bucket", PREFIX)
        s3 = FakeS3()
        s3.objects = []
        self.assertEqual(ops.probe(s3, "bucket", PREFIX)["objects"], 0)
        self.assertEqual(s3.deleted, [{"Bucket": "bucket", "Key": PREFIX + "_probe"}])

    def test_failed_probe_delete_still_has_hosted_cleanup_path(self):
        s3 = FakeS3()
        s3.objects = []
        with mock.patch.object(s3, "delete_object", side_effect=OSError("temporary delete failure")), \
                self.assertRaises(OSError):
            ops.probe(s3, "bucket", PREFIX)
        self.assertEqual([item["Key"] for item in s3.objects], [PREFIX + "_probe"])
        workflow = (Path(__file__).parents[2] / "workflows/metric-snapshot-s3-bench.yml").read_text()
        teardown = workflow.split("  teardown:\n", 1)[1]
        job_if = next(line.strip() for line in teardown.splitlines() if line.startswith("    if:"))
        self.assertEqual(job_if, "if: ${{ always() && needs.validate.outputs.run_id != '' }}")
        ec2 = teardown.split("    - name: Delete EC2, its temporary disks and runner registration\n", 1)[1]
        self.assertEqual(ec2.splitlines()[0].strip(), "if: ${{ always() && needs.provision.result != 'skipped' }}")
        self.assertEqual(ops.cleanup(s3, "bucket", PREFIX)["objects"], 0)

    def test_bad_prefix_and_out_of_scope_response_cannot_delete(self):
        for prefix in ("", "metric-snapshot-bench/", "metric-snapshot-bench/123-1", PREFIX + "../", "oswal-bench/123-1/"):
            s3 = FakeS3()
            with self.subTest(prefix=prefix), self.assertRaises(ValueError):
                ops.cleanup(s3, "bucket", prefix)
            self.assertEqual(s3.deleted, [])
        s3 = FakeS3()
        s3.objects[0]["Key"] = "other-run/current"
        with self.assertRaises(ValueError):
            ops.cleanup(s3, "bucket", PREFIX)
        self.assertEqual(s3.deleted, [])

    def test_credential_crossing_read_boundary_blocks_artifact_upload(self):
        with tempfile.TemporaryDirectory() as directory:
            logfile = Path(directory) / "server.log"
            logfile.write_bytes(b"a" * (1024 * 1024 - 4) + b"private-key" + b"tail")
            with self.assertRaisesRegex(ValueError, "upload is blocked"):
                ops.scrub(directory, {"AWS_SECRET_ACCESS_KEY": "private-key"})
            logfile.write_text("safe log\n")
            ops.scrub(directory, {"AWS_SECRET_ACCESS_KEY": "private-key"})


if __name__ == "__main__":
    unittest.main()
