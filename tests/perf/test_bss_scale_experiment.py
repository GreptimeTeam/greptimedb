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

"""Focused unit tests for BSS scale experiment parsing and accounting."""

import importlib.util
import sys
import unittest
from types import SimpleNamespace
from pathlib import Path
from unittest.mock import patch


SCRIPT = Path(__file__).resolve().parents[2] / ".github/scripts/bss-scale-experiment.py"
SPEC = importlib.util.spec_from_file_location("bss_scale_experiment_under_test", SCRIPT)
assert SPEC is not None and SPEC.loader is not None
experiment = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = experiment
SPEC.loader.exec_module(experiment)


class BssScaleExperimentTest(unittest.TestCase):
    def test_scaled_case_and_orders(self) -> None:
        case = experiment.scaled_case(experiment.SOURCE_CASE.read_text(encoding="utf-8"), 50)
        self.assertIn("samples_per_series = 216000", case)
        self.assertIn("timeout_seconds = 3600", case)
        self.assertIn("visibility_timeout_seconds = 600", case)
        self.assertIn("iterations = 1", case)
        self.assertIn("2024-05-30 00:00:00", case)
        self.assertEqual(case.count("trigger_file_num'='10000'"), 2)
        self.assertEqual(experiment.expected_rows(10), 43_200_000)
        self.assertEqual(experiment.paired_orders(), [("base", "candidate"), ("candidate", "base"), ("base", "candidate")])

    def test_response_rows_accepts_greptime_records_and_format_json_data(self) -> None:
        records = {"code": 0, "output": [{"records": {"schema": [], "rows": [["2024-01-01 00:00:00", "1.0"], ["2024-01-01 01:00:00", 2]]}}]}
        self.assertEqual(len(experiment.response_rows(records, 2)), 2)
        self.assertEqual(experiment.response_rows({"code": "success", "data": [{"sum": 1.0}]}, 1), [{"sum": 1.0}])

    def test_real_format_json_hourly_dict_order_and_epoch_millis(self) -> None:
        rows = [{"sum(sst_float_bss.greptime_value)": 3.5, "time_window": 1_704_067_200_000},
                {"sum(sst_float_bss.greptime_value)": 4.5, "time_window": 1_704_070_800_000}]
        self.assertEqual(experiment.response_rows({"data": rows}, 2), rows)
        rows[0]["sum(sst_float_bss.greptime_value)"] = None
        with self.assertRaisesRegex(RuntimeError, "finite, non-boolean numeric sum"):
            experiment.response_rows({"data": rows}, 2)

    def test_response_rows_rejects_null_sum_despite_numeric_timestamp(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "finite, non-boolean numeric sum"):
            experiment.response_rows({"data": [[1_704_067_200_000, None]]}, 1)

    def test_response_rows_rejects_wrapper_null_and_error_with_output(self) -> None:
        with self.assertRaisesRegex(RuntimeError, "finite, non-boolean numeric sum"):
            experiment.response_rows({"output": [{"records": {"rows": [["time", None]]}}]}, 1)
        with self.assertRaisesRegex(RuntimeError, "exactly one"):
            experiment.response_rows({"output": [{"records": {"rows": [[1]]}}, {"records": {"rows": [[2]]}}]}, 2)
        self.assertTrue(experiment.response_has_error({"code": 42, "output": [{"records": {"rows": [[1]]}}]}))

    def test_reader_iteration_parser_requires_full_exact_coverage(self) -> None:
        stdout = "\x1b[32mIteration 1: 42 rows in 1ms\x1b[0m\n[iter 2] 42 rows in 2ms\n"
        self.assertEqual(experiment.parse_iterations(stdout, 42, 2), [1.0, 2.0])
        self.assertIsNone(experiment.parse_average(stdout))
        with self.assertRaisesRegex(RuntimeError, "partial"):
            experiment.parse_iterations("Iteration 1: 41 rows in 1ms", 42, 1)
        with self.assertRaisesRegex(RuntimeError, "do not equal"):
            experiment.parse_iterations("Iteration 1: 42 rows in 1ms", 42, 2)

    def test_footer_accepts_six_ssts_with_4_32m_rows(self) -> None:
        files = [
            {"relative_path": f"{index}.parquet", "num_rows": 720_000,
             "columns": [{"column_path": "greptime_value", "encodings": ["BYTE_STREAM_SPLIT"]}]}
            for index in range(6)
        ]
        report = {"targets": [{"name": "candidate", "storage_inspection": {"summary": {
            "summary": {"total_rows": 4_320_000, "file_count": 6}, "files": files}},
            "read_bench": {"parquetbench": [{"command": ["parquetbench"]}], "scanbench": []}}]}
        _, summary = experiment.footer_and_templates(report, "candidate", 4_320_000)
        self.assertEqual(summary, {"file_count": 6, "total_rows": 4_320_000})

    def test_footer_rejects_partially_bss_value_column(self) -> None:
        files = [
            {"relative_path": "bss.parquet", "num_rows": 2_160_000,
             "columns": [{"column_path": "greptime_value", "encodings": ["BYTE_STREAM_SPLIT"]}]},
            {"relative_path": "plain.parquet", "num_rows": 2_160_000,
             "columns": [{"column_path": "greptime_value", "encodings": ["PLAIN"]}]},
        ]
        report = {"targets": [{"name": "candidate", "storage_inspection": {"summary": {
            "summary": {"total_rows": 4_320_000, "file_count": 2}, "files": files}},
            "read_bench": {"parquetbench": [{"command": ["parquetbench"]}], "scanbench": []}}]}
        with self.assertRaisesRegex(RuntimeError, "not BSS"):
            experiment.footer_and_templates(report, "candidate", 4_320_000)

    def test_aggregate_runs_uses_measured_average(self) -> None:
        totals = experiment.aggregate_runs([{"kind": "scanbench", "native_average_ms": 0.0, "measured": {
            "rows_per_iteration": 10, "wall_seconds": 2.0, "cpu_seconds": 0.5,
            "average_ms_from_iterations": 3.5,
        }}])
        self.assertEqual(totals["scanbench"]["native_average_ms_sum"], 3.5)

    def test_sql_round_uses_input_row_denominator_for_all_samples(self) -> None:
        target = SimpleNamespace(name="candidate", http_port=4000)
        post_result = {"response": {"data": [{"sum": 1.0}]}}
        with (patch.object(experiment, "post_sql", return_value=post_result),
              patch.object(experiment, "target_cpu_seconds", side_effect=[10.0, 40.0]),
              patch.object(experiment.time, "perf_counter", side_effect=[1.0, 3.0])):
            result = experiment.sql_round(target, {}, [("sum", "SELECT sum(value)", 1)], 100)
        measurement = result["measurements"][0]
        self.assertEqual(len(measurement["samples"]), 15)
        self.assertEqual(measurement["block_wall_seconds"], 2.0)
        self.assertEqual(measurement["cpu_ns_per_input_row"], 20_000_000.0)

    def test_cpu_denominator_and_proc_parser(self) -> None:
        self.assertEqual(experiment.row_denominator(4_320_000, 15), 64_800_000)
        fields = ["R"] + ["0"] * 10 + ["300", "200"]
        self.assertEqual(experiment.parse_proc_cpu_seconds("123 (worker) " + " ".join(fields), 100), 5.0)
        self.assertEqual(experiment.command_with_iterations(["x", "--iterations", "1"], 7)[-1], "7")


if __name__ == "__main__":
    unittest.main()
