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

"""Coverage for query-regression case group selection and case gate structure."""

import importlib.util
import re
import sys
import unittest
from pathlib import Path

try:
    import tomllib
except ModuleNotFoundError:  # Python < 3.11
    import tomli as tomllib


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

    def test_dyn_filter_join_gates_membership_instead_of_latency_only(self) -> None:
        case = tomllib.loads(
            (Path(__file__).parent / "query_cases/dyn_filter_join/case.toml").read_text()
        )
        scenario = case["scenario"]
        query = scenario["queries"][0]
        checks = query["plan_checks"]
        thresholds = query["thresholds"]

        # Plan shapes are only available from an analyzed, verbose plan.
        self.assertIn("EXPLAIN ANALYZE VERBOSE", query["query"])

        require = checks["require"]
        self.assertTrue(any("DynamicFilter" in pattern for pattern in require), require)
        self.assertTrue(
            any("trace_id" in pattern and "IN" in pattern for pattern in require), require
        )

        forbid = checks["forbid"]
        self.assertTrue(any("empty" in pattern for pattern in forbid), forbid)
        self.assertTrue(any("true" in pattern for pattern in forbid), forbid)

        # The store-work gates must be strictly narrower than a full probe scan,
        # and the row gate must also fail a bounds-only scan that decodes one
        # whole probe row group (the shape a candidate without the wire recovery
        # produces). Otherwise the gate would pass for a candidate whose
        # membership list was degraded to bounds.
        rows_per_sst = scenario["layout"]["rows_per_sst"]
        row_group_size = scenario["layout"]["row_group_size"]
        self.assertGreater(rows_per_sst, 0)
        self.assertLess(thresholds["max_candidate_num_sst_rows"], rows_per_sst)
        self.assertLess(thresholds["max_candidate_num_sst_rows"], row_group_size)
        self.assertGreater(thresholds["max_candidate_page_size_to_fetch_store_bytes"], 0)
        self.assertLessEqual(
            thresholds["max_candidate_page_size_to_fetch_store_bytes"], 16 * 1024 * 1024
        )

        # The 1M-row probe table must be the right operand: this DataFusion
        # version builds the left hash-join input, so the other order would
        # build 1M rows and leave nothing for the membership filter to prune.
        self.assertIn(
            "FROM dyn_filter_traces b JOIN dyn_filter_logs p", query["query"]
        )

        # The latency gate still exists, so the case keeps its non-regression role.
        self.assertIn("max_candidate_latency_regression_pct", thresholds)

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


if __name__ == "__main__":
    unittest.main()
