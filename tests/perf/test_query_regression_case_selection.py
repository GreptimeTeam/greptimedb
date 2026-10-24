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

"""Coverage for query-regression case group selection."""

import importlib.util
import re
import sys
import unittest
from pathlib import Path


RUNNER_PATH = Path(__file__).parents[2] / ".github/scripts/query-regression-run.py"
SPEC = importlib.util.spec_from_file_location("query_regression_run_under_test", RUNNER_PATH)
assert SPEC is not None and SPEC.loader is not None
runner = importlib.util.module_from_spec(SPEC)
sys.modules[SPEC.name] = runner
SPEC.loader.exec_module(runner)


class QueryRegressionCaseSelectionTest(unittest.TestCase):
    def test_all_selects_the_routine_default_cases(self) -> None:
        self.assertEqual(
            runner.split_cases(["all"]),
            [
                "tests/perf/query_cases/smoke_direct_sst/case.toml",
                "tests/perf/query_cases/sst_float_bss/case.toml",
                "tests/perf/query_cases/prom_remote_write_seeded_random/case.toml",
                "tests/perf/query_cases/prom_remote_write_run_heavy/case.toml",
                "tests/perf/query_cases/prom_remote_write_mixed_every/case.toml",
                "tests/perf/query_cases/prom_remote_write_integer_counter/case.toml",
                "tests/perf/query_cases/promql_range_boundary/case.toml",
                "tests/perf/query_cases/promql_instant_last_row_9034/case.toml",
                "tests/perf/query_cases/mito_prefilter_all_match/case.toml",
            ],
        )

    def test_implicit_selection_has_nine_routine_cases(self) -> None:
        self.assertEqual(len(runner.DEFAULT_CASES), 9)
        self.assertEqual(runner.split_cases([]), runner.DEFAULT_CASES)

    def test_heavy_selects_the_heavy_case_set(self) -> None:
        self.assertEqual(
            runner.split_cases(["heavy"]),
            [
                "tests/perf/query_cases/prom_remote_write_7913/case.toml",
                "tests/perf/query_cases/promql_constant_tag_concat_ms_10k/case.toml",
                "tests/perf/query_cases/promql_constant_tag_concat_ms_100k/case.toml",
            ],
        )

    def test_explicit_paths_remain_selectable(self) -> None:
        case = "tests/perf/query_cases/sql_topk_order_by/case.toml"
        self.assertEqual(runner.split_cases([case]), [case])

    def test_sst_float_bss_omits_base_encoding_for_older_binaries(self) -> None:
        case_path = Path(__file__).parent / "query_cases/sst_float_bss/case.toml"
        setup_sql = dict(re.findall(
            r'(base|candidate)_setup_sql = \[\s*"([^"\n]+)"',
            case_path.read_text(),
        ))
        base_setup = setup_sql["base"]
        candidate_setup = setup_sql["candidate"]
        candidate_option = "'experimental_sst_float_field_encoding'='byte_stream_split'"

        self.assertNotIn("experimental_sst_float_field_encoding", base_setup)
        self.assertIn(candidate_option, candidate_setup)
        self.assertEqual(
            base_setup,
            candidate_setup.replace(f", {candidate_option}", ""),
        )

    def test_sst_float_bss_pins_the_compacted_layout_for_both_targets(self) -> None:
        case_path = Path(__file__).parent / "query_cases/sst_float_bss/case.toml"
        case_text = case_path.read_text()
        block = re.search(r"post_ingest_sql = \[(.*?)\n\]", case_text, re.DOTALL)
        self.assertIsNotNone(block, "sst_float_bss must declare a post_ingest_sql list")
        statements = re.findall(r'"([^"\n]+)"', block.group(1))

        self.assertEqual(len(statements), 3)
        compact, assertion, logical_count = statements
        # One shared list means both targets run the same compaction and assertions.
        self.assertNotIn("base_post_ingest_sql", case_text)
        self.assertNotIn("candidate_post_ingest_sql", case_text)
        self.assertEqual(
            compact,
            "ADMIN compact_table('sst_float_bss_physical', 'strict_window', 'window=86400')",
        )
        # The baseline must parse this statement, so no time-range options here.
        self.assertNotIn("start_time", compact)
        self.assertNotIn("end_time", compact)
        # The assertion only looks at live data SSTs of the physical metric table.
        self.assertIn("region_group = 0", assertion)
        self.assertIn("visible = true", assertion)
        self.assertIn("AND table_name = 'sst_float_bss_physical'", assertion)
        # Three live daily SSTs, 1440000 rows each, 4320000 rows in total.
        for expected in ("count(*) = 3", "min(num_rows) = 1440000", "max(num_rows) = 1440000",
                         "sum(num_rows) = 4320000", "count(DISTINCT min_ts) = 3"):
            self.assertIn(expected, assertion)
        for day in ("2024-01-01", "2024-01-02", "2024-01-03"):
            self.assertIn(f"TIMESTAMP '{day} 00:00:00'", assertion)
            self.assertIn(f"TIMESTAMP '{day} 23:59:00'", assertion)
        # A mismatch must raise at execution time; a bare boolean would not fail the
        # runner's HTTP-success check.
        self.assertIn("AS BIGINT", assertion)
        # A separate post-compaction guard re-checks the logical row count.
        self.assertIn("FROM sst_float_bss", logical_count)
        self.assertIn("count(*) = 4320000", logical_count)
        self.assertIn("AS BIGINT", logical_count)

    def test_sst_float_bss_pins_the_inspected_file_count(self) -> None:
        case_path = Path(__file__).parent / "query_cases/sst_float_bss/case.toml"
        case_text = case_path.read_text()
        storage = re.search(r"\[scenario\.remote_write\.storage\](.*?)\n\[", case_text, re.DOTALL)
        self.assertIsNotNone(storage, "sst_float_bss must declare remote_write.storage")
        self.assertIn("exact_files = 3", storage.group(1))
        self.assertNotIn("exact_rows_per_file", storage.group(1))


if __name__ == "__main__":
    unittest.main()
