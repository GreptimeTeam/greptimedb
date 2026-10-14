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
        self.uploads = [{"Key": PREFIX + "unfinished", "UploadId": "u"}]
        self.versions = [{"Key": PREFIX + "data", "VersionId": "v"}]
        self.markers = [{"Key": PREFIX + "deleted", "VersionId": "m"}]
        self.objects = [{"Key": PREFIX + "current", "Size": 11}]
        self.deleted = []

    def get_paginator(self, operation):
        parent = self

        class Paginator:
            def paginate(self, **kwargs):
                assert kwargs == {"Bucket": "bucket", "Prefix": PREFIX}
                return iter([{"Uploads": parent.uploads.copy()}] if operation == "list_multipart_uploads" else
                            [{"Versions": parent.versions.copy(), "DeleteMarkers": parent.markers.copy()}]
                            if operation == "list_object_versions" else [{"Contents": parent.objects.copy()}])

        return Paginator()

    def abort_multipart_upload(self, **kwargs):
        self.deleted.append(kwargs)
        self.uploads = []

    def delete_object(self, **kwargs):
        self.deleted.append(kwargs)
        for name in ("versions", "markers", "objects"):
            setattr(self, name, [item for item in getattr(self, name) if item["Key"] != kwargs["Key"]])


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

    def test_cleanup_aborts_multipart_and_deletes_all_versions_and_markers(self):
        s3 = FakeS3()
        result = ops.cleanup(s3, "bucket", PREFIX)
        self.assertEqual(result["objects"], 0)
        self.assertEqual(result["versions_and_delete_markers"], 0)
        self.assertEqual(result["incomplete_multipart_uploads"], 0)
        self.assertEqual(s3.deleted, [
            {"Bucket": "bucket", "Key": PREFIX + "unfinished", "UploadId": "u"},
            {"Bucket": "bucket", "Key": PREFIX + "data", "VersionId": "v"},
            {"Bucket": "bucket", "Key": PREFIX + "deleted", "VersionId": "m"},
            {"Bucket": "bucket", "Key": PREFIX + "current"}])

    def test_bad_prefix_and_out_of_scope_response_cannot_delete(self):
        for prefix in ("", "metric-snapshot-bench/", "metric-snapshot-bench/123-1", PREFIX + "../", "oswal-bench/123-1/"):
            s3 = FakeS3()
            with self.subTest(prefix=prefix), self.assertRaises(ValueError):
                ops.cleanup(s3, "bucket", prefix)
            self.assertEqual(s3.deleted, [])
        s3 = FakeS3()
        s3.uploads[0]["Key"] = "other-run/unfinished"
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
