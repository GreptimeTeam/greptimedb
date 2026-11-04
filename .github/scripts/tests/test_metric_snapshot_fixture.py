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

import copy
import importlib.util
from pathlib import Path
import unittest

spec = importlib.util.spec_from_file_location(
    "fixture", Path(__file__).resolve().parents[1] / "metric-snapshot-fixture.py"
)
fixture = importlib.util.module_from_spec(spec)
spec.loader.exec_module(fixture)


class ValidatorTests(unittest.TestCase):
    def records(self):
        return {
            "schema": {"column_schemas": [
                {"name": "ts", "data_type": "TimestampMillisecond"},
                *[{"name": f"tag_{i:03d}", "data_type": "String"} for i in range(10)],
                {"name": "val", "data_type": "Float64"},
            ]},
            "rows": [fixture.row(0, j, 50) for j in range(32)],
        }

    def test_full_tag_coverage_and_heterogeneous_schemas(self):
        for per_group in (50, 10000):
            variants = {tuple(fixture.tags(i, per_group)) for i in range(per_group)}
            self.assertEqual(set().union(*map(set, variants)), set(range(298)))
            self.assertGreater(len(variants), 1)
            self.assertTrue(all(len(tags) == 10 for tags in variants))

    def test_accepts_engine_column_order_with_matching_values(self):
        records = self.records()
        order = [*range(1, 11), 0, 11]  # The engine returns tags, timestamp, value.
        records["schema"]["column_schemas"] = [
            records["schema"]["column_schemas"][index] for index in order
        ]
        records["rows"] = [[values[index] for index in order] for values in records["rows"]]
        fixture.check_records(0, 50, records)
        records["rows"][1][10] = 2
        with self.assertRaisesRegex(RuntimeError, "typed values"):
            fixture.check_records(0, 50, records)

    def test_rejects_column_type_and_membership_changes(self):
        fixture.check_records(0, 50, self.records())
        for mutation in ("type", "extra", "missing"):
            records = self.records()
            columns = records["schema"]["column_schemas"]
            if mutation == "type":
                columns[0]["data_type"] = "TimestampMicrosecond"
            elif mutation == "extra":
                columns.append({"name": "unexpected", "data_type": "String"})
            else:
                del columns[1]
            with self.subTest(mutation=mutation), self.assertRaisesRegex(RuntimeError, "schema"):
                fixture.check_records(0, 50, records)

    def test_rejects_null_and_numeric_type_changes(self):
        for value in ("", 0, False):
            records = self.records()
            records["rows"][0][-1] = value  # Expected SQL NULL.
            with self.subTest(value=value), self.assertRaisesRegex(RuntimeError, "typed values"):
                fixture.check_records(0, 50, records)
        records = self.records()
        records["rows"][4][-1] = 1  # Numeric equality to 1.0 must not hide a type change.
        with self.assertRaisesRegex(RuntimeError, "typed values"):
            fixture.check_records(0, 50, records)

    def test_source_comparison_requires_same_ddl_and_remapped_ids(self):
        source = {"fixture": {"tables_per_group": 50}, "schema_sha256": "schema",
                  "data_sha256": "data", "ddl_sha256": "ddl",
                  "table_ids": {"m000000": 1024, "phy_00": 1025}}
        target = copy.deepcopy(source)
        target["table_ids"] = {"m000000": 1041, "phy_00": 1042}
        fixture.compare_source(target, source)
        self.assertEqual(target["remapped_table_ids"], 2)
        for field in ("schema_sha256", "data_sha256", "ddl_sha256"):
            changed = copy.deepcopy(target)
            changed[field] = "changed"
            with self.subTest(field=field), self.assertRaisesRegex(RuntimeError, field):
                fixture.compare_source(changed, source)
        target["table_ids"]["phy_00"] = 1025
        with self.assertRaisesRegex(RuntimeError, "not all remapped"):
            fixture.compare_source(target, source)


if __name__ == "__main__":
    unittest.main()
