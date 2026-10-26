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

"""Focused offline tests for the native PromQL VMBench qualification gate."""

import importlib.util
import json
import subprocess
import tempfile
import threading
import unittest
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

SCRIPT = Path(__file__).resolve().parents[2] / ".github/scripts/native-promql-vmbench-validate.py"
spec = importlib.util.spec_from_file_location("validator", SCRIPT)
validator = importlib.util.module_from_spec(spec)
spec.loader.exec_module(validator)


def matrix(series):
    return {"status": "success", "data": {"resultType": "matrix", "result": series}}


def entry(labels, values):
    return {"metric": labels, "values": values}


class ComparisonTests(unittest.TestCase):
    def test_full_matrix_order_labels_timestamps_values_and_special_numbers(self):
        left = matrix([
            entry({"a": "1"}, [[1, "NaN"], [2, "Infinity"]]),
            entry({"a": "1"}, [[1, "-0"], [3, "2"]]),
            entry({}, [[2, "-Infinity"]]),
        ])
        right = matrix([
            entry({}, [[2, "-Inf"]]),
            entry({"a": "1"}, [[1, "-0.0"], [3, "2.00000000001"]]),
            entry({"a": "1"}, [[1, "nan"], [2, "+Inf"]]),
        ])
        self.assertEqual(validator.compare_prom(left, right, 1e-10, 1e-12), (True, None))
        changed_time = matrix([
            entry({}, [[2, "-Inf"]]),
            entry({"a": "1"}, [[1, "-0"], [4, "2"]]),
            entry({"a": "1"}, [[1, "nan"], [2, "+Inf"]]),
        ])
        self.assertIn("timestamp", validator.compare_prom(left, changed_time, 0, 0)[1])
        changed_value = matrix([
            entry({}, [[2, "-Inf"]]),
            entry({"a": "1"}, [[1, "-0"], [3, "9"]]),
            entry({"a": "1"}, [[1, "nan"], [2, "+Inf"]]),
        ])
        self.assertIn("value", validator.compare_prom(left, changed_value, 0, 0)[1])

    def test_labels_name_multiplicity_empty_and_malformed(self):
        baseline = matrix([entry({"__name__": "x"}, [[1, "1"]]), entry({}, [[1, "2"]])])
        name_change = matrix([entry({"__name__": "y"}, [[1, "1"]]), entry({}, [[1, "2"]])])
        self.assertIn("metric", validator.compare_prom(baseline, name_change, 0, 0)[1])
        duplicate = matrix([entry({}, [[1, "2"]]), entry({"__name__": "x"}, [[1, "1"]])])
        self.assertEqual(validator.compare_prom(baseline, duplicate, 0, 0), (True, None))
        missing_duplicate = matrix([entry({"__name__": "x"}, [[1, "1"]])])
        self.assertIn("series count", validator.compare_prom(baseline, missing_duplicate, 0, 0)[1])
        changed_duplicate = matrix([entry({}, [[1, "3"]]), entry({"__name__": "x"}, [[1, "1"]])])
        self.assertIn("value", validator.compare_prom(baseline, changed_duplicate, 0, 0)[1])
        bad_labels = matrix([entry({"x": 1}, [[1, "1"]])])
        self.assertIn("must be strings", validator.compare_prom(bad_labels, bad_labels, 0, 0)[1])
        empty = matrix([])
        self.assertEqual(validator.normalize_prom(empty)[1], [])
        bad_timestamp = matrix([entry({}, [["bad", "1"]])])
        self.assertIn("timestamp", validator.compare_prom(bad_timestamp, bad_timestamp, 0, 0)[1])

    def test_vector_scalar_and_forecast(self):
        vector_left = {"status": "success", "data": {"resultType": "vector", "result": [{"metric": {}, "value": [1, "1"]}, {"metric": {}, "value": [1, "2"]}]}}
        vector_right = {"status": "success", "data": {"resultType": "vector", "result": list(reversed(vector_left["data"]["result"]))}}
        self.assertEqual(validator.compare_prom(vector_left, vector_right, 0, 0), (True, None))
        scalar = {"status": "success", "data": {"resultType": "scalar", "result": [1, "1"]}}
        self.assertEqual(validator.compare_prom(scalar, scalar, 0, 0), (True, None))
        self.assertEqual(9 in validator.EXPECTED_NATIVE_IDS, False)
        schema_response = {"output": [{"records": {"schema": {"column_schemas": [{"name": "FusedSeriesExec"}]}, "rows": []}}]}
        self.assertFalse(validator.inspect_plan(validator.plan_text(schema_response))[0])
        scan_line = 'SeriesScan: region=x, "partition_count":{"count":4, "mem_ranges":1, "files":0, "file_ranges":0}'
        wire_plan = {"output": [{"records": {"schema": {"column_schemas": [{"name": "Plan", "data_type": "String"}]}, "rows": [["FusedSeriesExec: x\n" + scan_line]]}}]}
        self.assertEqual(validator.inspect_plan(validator.plan_text(wire_plan))[3], 4)
        plan = 'SeriesScan: region=x, "partition_count":{"count":4, "mem_ranges":1, "files":0, "file_ranges":0}\nAggregateFinalizeExec: sum'
        parsed = validator.inspect_plan(plan)
        self.assertEqual(parsed[:4], (True, False, True, 4))
        self.assertEqual(parsed[5], 1)
        self.assertEqual(
            validator.plan_qualification_errors(1, "native", (True, True, False, 4, [], 1), 4),
            ["native aggregate plan is missing AggregateFinalizeExec"],
        )
        self.assertEqual(validator.plan_qualification_errors(7, "native", parsed, 4), [])
        self.assertEqual(validator.inspect_plan('SeriesScan: partition_count {count:4}')[3], None)
        self.assertEqual(validator.inspect_plan('FusedSeriesExec: x\nPartitions: 4')[3], None)
        self.assertEqual(9 in validator.EXPECTED_NATIVE_IDS, False)


class LoopbackTests(unittest.TestCase):
    def test_routes_artifacts_and_continues_after_failed_request(self):
        requests = []

        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                from urllib.parse import parse_qs

                params = parse_qs(self.rfile.read(int(self.headers.get("Content-Length", 0))).decode())
                native = self.headers.get("X-Greptime-Hints", "").endswith("true")
                requests.append((self.path, params, native))
                if self.path.endswith("query_range"):
                    failed = native and params.get("query") == ["q1"]
                    if failed:
                        response = {"status": "error", "errorType": "execution", "error": "fixture error"}
                    else:
                        response = matrix([entry({"__name__": "metric"}, [[1, "1"]])])
                else:
                    scan = 'SeriesScan: region=x, "partition_count":{"count":4, "mem_ranges":1, "files":0, "file_ranges":0}'
                    response = {"output": [{"records": {"schema": {"column_schemas": [{"name": "Plan"}]}, "rows": [["FusedSeriesExec: x\n" + scan]]}}]}
                raw = json.dumps(response).encode()
                self.send_response(200)
                self.send_header("Content-Type", "application/json")
                self.end_headers()
                self.wfile.write(raw)

            def log_message(self, *_):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                query_file = root / "queries.txt"
                query_file.write_text("\n".join(f"q{i}" for i in range(1, 13)))
                output = root / "artifacts"
                result = subprocess.run([
                    "python3", str(SCRIPT), "--query-file", str(query_file),
                    "--url", f"http://127.0.0.1:{server.server_port}/v1/prometheus/api/v1/query_range",
                    "--start", "2024-01-01T00:00:00Z", "--end", "2024-01-01T00:01:00Z",
                    "--output-dir", str(output),
                ], capture_output=True, text=True)
                self.assertNotEqual(result.returncode, 0)
                report = json.loads((output / "qualification.json").read_text())
                self.assertEqual(len(report["records"]), 12)
                self.assertFalse(report["records"][0]["correctness"])
                self.assertEqual(report["records"][11]["query_id"], "lite-12")
                self.assertTrue((output / "lite-12/native-plan.txt").exists())
                self.assertEqual(len(requests), 48)
                self.assertEqual({row[2] for row in requests}, {True, False})
                sql = [row for row in requests if row[0] == "/v1/sql"]
                self.assertEqual(len(sql), 24)
                self.assertTrue(all(row[1]["format"] == ["greptimedb_v1"] for row in sql))
                self.assertTrue(all("TQL ANALYZE VERBOSE" in row[1]["sql"][0] for row in sql))
                self.assertTrue(all("1704067200" in row[1]["sql"][0] for row in sql))
        finally:
            server.shutdown()
            server.server_close()
            thread.join()

    def test_unknown_native_partitions_keep_correctness_but_fail_qualification(self):
        class Handler(BaseHTTPRequestHandler):
            def do_POST(self):
                self.rfile.read(int(self.headers.get("Content-Length", 0)))
                if self.path.endswith("query_range"):
                    response = matrix([entry({"m": "v"}, [[1, "1"]])])
                else:
                    response = {"output": [{"records": {"schema": {"column_schemas": [{"name": "Plan"}]}, "rows": [["FusedSeriesExec: actual operator\nSeriesScanExec: source=primary_key, fields=1, partitions=4 metrics=[output_rows: 3]"]]}}]}
                raw = json.dumps(response).encode()
                self.send_response(200)
                self.end_headers()
                self.wfile.write(raw)

            def log_message(self, *_):
                pass

        server = ThreadingHTTPServer(("127.0.0.1", 0), Handler)
        thread = threading.Thread(target=server.serve_forever, daemon=True)
        thread.start()
        try:
            with tempfile.TemporaryDirectory() as temporary:
                root = Path(temporary)
                queries = root / "queries.txt"
                queries.write_text("\n".join(f"q{i}" for i in range(12)))
                output = root / "artifacts"
                subprocess.run([
                    "python3", str(SCRIPT), "--query-file", str(queries),
                    "--url", f"http://127.0.0.1:{server.server_port}/v1/prometheus/api/v1/query_range",
                    "--start", "2024-01-01T00:00:00Z", "--end", "2024-01-01T00:01:00Z",
                    "--output-dir", str(output),
                ], capture_output=True, check=False)
                records = json.loads((output / "qualification.json").read_text())["records"]
                native_record = records[0]
                self.assertTrue(native_record["correctness"])
                self.assertFalse(native_record["qualified"])
                self.assertFalse(native_record["eligible_for_native_statistics"])
                self.assertEqual(native_record["observed_partitions"], 4)
        finally:
            server.shutdown()
            server.server_close()
            thread.join()


if __name__ == "__main__":
    unittest.main()
