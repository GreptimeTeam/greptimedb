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
    def test_all_selects_the_five_routine_cases(self) -> None:
        self.assertEqual(
            runner.split_cases(["all"]),
            [
                "tests/perf/query_cases/smoke_direct_sst/case.toml",
                "tests/perf/query_cases/prom_remote_write_seeded_random/case.toml",
                "tests/perf/query_cases/prom_remote_write_run_heavy/case.toml",
                "tests/perf/query_cases/prom_remote_write_mixed_every/case.toml",
                "tests/perf/query_cases/prom_remote_write_integer_counter/case.toml",
            ],
        )

    def test_heavy_selects_only_remote_write_7913(self) -> None:
        self.assertEqual(
            runner.split_cases(["heavy"]),
            ["tests/perf/query_cases/prom_remote_write_7913/case.toml"],
        )

    def test_explicit_paths_remain_selectable(self) -> None:
        case = "tests/perf/query_cases/sql_topk_order_by/case.toml"
        self.assertEqual(runner.split_cases([case]), [case])


if __name__ == "__main__":
    unittest.main()
