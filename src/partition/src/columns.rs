// Copyright 2023 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::any::Any;

use datafusion_expr::Operator;
use datatypes::value::Value;
use snafu::ensure;
use store_api::storage::RegionNumber;

use crate::error::{self, Result};
use crate::partition::{PartitionBound, PartitionExpr, PartitionRule};

/// A [RangeColumnsPartitionRule] is very similar to [RangePartitionRule](crate::range::RangePartitionRule) except that it allows
/// partitioning by multiple columns.
///
/// This rule is generated from create table request, using MySQL's syntax:
///
/// ```SQL
/// CREATE TABLE table_name (
///     columns definition
/// )
/// PARTITION BY RANGE COLUMNS(column_list) (
///     PARTITION region_name VALUES LESS THAN (value_list)[,
///     PARTITION region_name VALUES LESS THAN (value_list)][,
///     ...]
/// )
///
/// column_list:
///     column_name[, column_name][, ...]
///
/// value_list:
///     value[, value][, ...]
/// ```
///
/// Please refer to MySQL's ["RANGE COLUMNS Partitioning"](https://dev.mysql.com/doc/refman/8.0/en/partitioning-columns-range.html)
/// document for more details.
pub struct RangeColumnsPartitionRule {
    column_list: Vec<String>,
    value_lists: Vec<Vec<PartitionBound>>,
    regions: Vec<RegionNumber>,

    // TODO(LFC): Implement finding regions by all partitioning columns, not by the first one only.
    // Singled out the first partitioning column's bounds for finding regions by range.
    //
    // Theoretically, finding regions in `value_list`s should use all the partition columns values
    // as a whole in the comparison (think of how Rust's vector is compared to each other). And
    // this is how we do it if provided with concrete values (see `find_region` method).
    //
    // However, when we need to find regions by range, for example, a filter of "x < 100" defined
    // in SQL, currently I'm not quite sure how that could be implemented. Especially facing the complex
    // filter expression like "a < 1 AND (b > 2 OR c != 3)".
    //
    // So I decided to use the first partitioning column temporarily in finding regions by range,
    // and further investigate how MySQL (and others) implemented this feature in detail.
    //
    // Finding regions only using the first partitioning column is fine. It might return regions that
    // actually do not contain the range's value (causing unnecessary table scans), but will
    // not lose any data that should have been scanned.
    //
    // The following two fields are acted as caches, so we don't need to recalculate them every time.
    first_column_bounds: Vec<PartitionBound>,
    first_column_regions: Vec<Vec<RegionNumber>>,
}

impl RangeColumnsPartitionRule {
    // It's assured that input arguments are valid because they are checked in SQL parsing stage.
    // So we can skip validating them.
    pub fn new(
        column_list: Vec<String>,
        value_lists: Vec<Vec<PartitionBound>>,
        regions: Vec<RegionNumber>,
    ) -> Self {
        // An example range columns partition rule to calculate the first column bounds and regions:
        // SQL:
        //   PARTITION p1 VALUES LESS THAN (10, 'c'),
        //   PARTITION p2 VALUES LESS THAN (20, 'h'),
        //   PARTITION p3 VALUES LESS THAN (20, 'm'),
        //   PARTITION p4 VALUES LESS THAN (50, 'p'),
        //   PARTITION p5 VALUES LESS THAN (MAXVALUE, 'x'),
        //   PARTITION p6 VALUES LESS THAN (MAXVALUE, MAXVALUE),
        // first column bounds:
        //   [10, 20, 50, MAXVALUE]
        // first column regions:
        //   [[1], [2, 3], [4], [5, 6]]

        let first_column_bounds = value_lists
            .iter()
            .map(|x| &x[0])
            .collect::<Vec<&PartitionBound>>();

        let mut distinct_bounds = Vec::<PartitionBound>::new();
        distinct_bounds.push(first_column_bounds[0].clone());
        let mut first_column_regions = Vec::<Vec<RegionNumber>>::new();
        first_column_regions.push(vec![regions[0]]);

        for i in 1..first_column_bounds.len() {
            if first_column_bounds[i] == &distinct_bounds[distinct_bounds.len() - 1] {
                first_column_regions[distinct_bounds.len() - 1].push(regions[i]);
            } else {
                distinct_bounds.push(first_column_bounds[i].clone());
                first_column_regions.push(vec![regions[i]]);
            }
        }

        Self {
            column_list,
            value_lists,
            regions,
            first_column_bounds: distinct_bounds,
            first_column_regions,
        }
    }

    pub fn column_list(&self) -> &Vec<String> {
        &self.column_list
    }

    pub fn value_lists(&self) -> &Vec<Vec<PartitionBound>> {
        &self.value_lists
    }

    pub fn regions(&self) -> &Vec<RegionNumber> {
        &self.regions
    }
}

impl PartitionRule for RangeColumnsPartitionRule {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn partition_columns(&self) -> Vec<String> {
        self.column_list.clone()
    }

    fn find_region(&self, values: &[Value]) -> Result<RegionNumber> {
        ensure!(
            values.len() == self.column_list.len(),
            error::RegionKeysSizeSnafu {
                expect: self.column_list.len(),
                actual: values.len(),
            }
        );

        // How tuple is compared:
        // (a, b) < (x, y) <= (a < x) || ((a == x) && (b < y))
        let values = values
            .iter()
            .map(|v| PartitionBound::Value(v.clone()))
            .collect::<Vec<PartitionBound>>();
        Ok(match self.value_lists.binary_search(&values) {
            Ok(i) => self.regions[i + 1],
            Err(i) => self.regions[i],
        })
    }

    fn find_regions_by_exprs(&self, exprs: &[PartitionExpr]) -> Result<Vec<RegionNumber>> {
        let regions =
            if !exprs.is_empty() && exprs.iter().all(|x| self.column_list.contains(&x.column)) {
                let PartitionExpr {
                    column: _,
                    op,
                    value,
                } = exprs
                    .iter()
                    .find(|x| x.column == self.column_list[0])
                    // "unwrap" is safe because we have checked that "self.column_list" contains all columns in "exprs"
                    .unwrap();

                let regions = &self.first_column_regions;
                match self
                    .first_column_bounds
                    .binary_search(&PartitionBound::Value(value.clone()))
                {
                    Ok(i) => match op {
                        Operator::Lt => &regions[..=i],
                        Operator::LtEq => &regions[..=(i + 1)],
                        Operator::Eq => &regions[(i + 1)..=(i + 1)],
                        Operator::Gt | Operator::GtEq => &regions[(i + 1)..],
                        Operator::NotEq => &regions[..],
                        _ => unimplemented!(),
                    },
                    Err(i) => match op {
                        Operator::Lt | Operator::LtEq => &regions[..=i],
                        Operator::Eq => &regions[i..=i],
                        Operator::Gt | Operator::GtEq => &regions[i..],
                        Operator::NotEq => &regions[..],
                        _ => unimplemented!(),
                    },
                }
                .iter()
                .flatten()
                .cloned()
                .collect::<Vec<RegionNumber>>()
            } else {
                self.regions.clone()
            };
        Ok(regions)
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use super::*;
    use crate::partition::{PartitionBound, PartitionExpr};

    #[test]
    fn test_find_regions() {
        // PARTITION BY RANGE COLUMNS(a, b)
        //   PARTITION p1 VALUES LESS THAN ('hz', 10),
        //   PARTITION p2 VALUES LESS THAN ('sh', 20),
        //   PARTITION p3 VALUES LESS THAN ('sh', 50),
        //   PARTITION p4 VALUES LESS THAN ('sz', 100),
        //   PARTITION p5 VALUES LESS THAN (MAXVALUE, 200),
        //   PARTITION p6 VALUES LESS THAN (MAXVALUE, MAXVALUE),
        let rule = RangeColumnsPartitionRule::new(
            vec!["a".to_string(), "b".to_string()],
            vec![
                vec![
                    PartitionBound::Value("hz".into()),
                    PartitionBound::Value(10_i32.into()),
                ],
                vec![
                    PartitionBound::Value("sh".into()),
                    PartitionBound::Value(20_i32.into()),
                ],
                vec![
                    PartitionBound::Value("sh".into()),
                    PartitionBound::Value(50_i32.into()),
                ],
                vec![
                    PartitionBound::Value("sz".into()),
                    PartitionBound::Value(100_i32.into()),
                ],
                vec![
                    PartitionBound::MaxValue,
                    PartitionBound::Value(200_i32.into()),
                ],
                vec![PartitionBound::MaxValue, PartitionBound::MaxValue],
            ],
            vec![1, 2, 3, 4, 5, 6],
        );

        let test = |op: Operator, value: &str, expected_regions: Vec<RegionNumber>| {
            let exprs = vec![
                // Intentionally fix column b's partition expr to "b < 1". If we support finding
                // regions by both columns("a" and "b") in the future, some test cases should fail.
                PartitionExpr {
                    column: "b".to_string(),
                    op: Operator::Lt,
                    value: 1_i32.into(),
                },
                PartitionExpr {
                    column: "a".to_string(),
                    op,
                    value: value.into(),
                },
            ];
            let regions = rule.find_regions_by_exprs(&exprs).unwrap();
            assert_eq!(
                regions,
                expected_regions.into_iter().collect::<Vec<RegionNumber>>()
            );
        };

        test(Operator::NotEq, "hz", vec![1, 2, 3, 4, 5, 6]);
        test(Operator::NotEq, "what", vec![1, 2, 3, 4, 5, 6]);

        test(Operator::GtEq, "ab", vec![1, 2, 3, 4, 5, 6]);
        test(Operator::GtEq, "hz", vec![2, 3, 4, 5, 6]);
        test(Operator::GtEq, "ijk", vec![2, 3, 4, 5, 6]);
        test(Operator::GtEq, "sh", vec![4, 5, 6]);
        test(Operator::GtEq, "ssh", vec![4, 5, 6]);
        test(Operator::GtEq, "sz", vec![5, 6]);
        test(Operator::GtEq, "zz", vec![5, 6]);

        test(Operator::Gt, "ab", vec![1, 2, 3, 4, 5, 6]);
        test(Operator::Gt, "hz", vec![2, 3, 4, 5, 6]);
        test(Operator::Gt, "ijk", vec![2, 3, 4, 5, 6]);
        test(Operator::Gt, "sh", vec![4, 5, 6]);
        test(Operator::Gt, "ssh", vec![4, 5, 6]);
        test(Operator::Gt, "sz", vec![5, 6]);
        test(Operator::Gt, "zz", vec![5, 6]);

        test(Operator::Eq, "ab", vec![1]);
        test(Operator::Eq, "hz", vec![2, 3]);
        test(Operator::Eq, "ijk", vec![2, 3]);
        test(Operator::Eq, "sh", vec![4]);
        test(Operator::Eq, "ssh", vec![4]);
        test(Operator::Eq, "sz", vec![5, 6]);
        test(Operator::Eq, "zz", vec![5, 6]);

        test(Operator::Lt, "ab", vec![1]);
        test(Operator::Lt, "hz", vec![1]);
        test(Operator::Lt, "ijk", vec![1, 2, 3]);
        test(Operator::Lt, "sh", vec![1, 2, 3]);
        test(Operator::Lt, "ssh", vec![1, 2, 3, 4]);
        test(Operator::Lt, "sz", vec![1, 2, 3, 4]);
        test(Operator::Lt, "zz", vec![1, 2, 3, 4, 5, 6]);

        test(Operator::LtEq, "ab", vec![1]);
        test(Operator::LtEq, "hz", vec![1, 2, 3]);
        test(Operator::LtEq, "ijk", vec![1, 2, 3]);
        test(Operator::LtEq, "sh", vec![1, 2, 3, 4]);
        test(Operator::LtEq, "ssh", vec![1, 2, 3, 4]);
        test(Operator::LtEq, "sz", vec![1, 2, 3, 4, 5, 6]);
        test(Operator::LtEq, "zz", vec![1, 2, 3, 4, 5, 6]);

        // If trying to find regions that is not partitioning column, return all regions.
        let exprs = vec![
            PartitionExpr {
                column: "c".to_string(),
                op: Operator::Lt,
                value: 1_i32.into(),
            },
            PartitionExpr {
                column: "a".to_string(),
                op: Operator::Lt,
                value: "hz".into(),
            },
        ];
        let regions = rule.find_regions_by_exprs(&exprs).unwrap();
        assert_eq!(regions, vec![1, 2, 3, 4, 5, 6]);
    }

    #[test]
    fn test_find_region() {
        // PARTITION BY RANGE COLUMNS(a) (
        //   PARTITION r1 VALUES LESS THAN ('hz'),
        //   PARTITION r2 VALUES LESS THAN ('sh'),
        //   PARTITION r3 VALUES LESS THAN (MAXVALUE),
        // )
        let rule = RangeColumnsPartitionRule::new(
            vec!["a".to_string()],
            vec![
                vec![PartitionBound::Value("hz".into())],
                vec![PartitionBound::Value("sh".into())],
                vec![PartitionBound::MaxValue],
            ],
            vec![1, 2, 3],
        );
        assert_matches!(
            rule.find_region(&["foo".into(), 1000_i32.into()]),
            Err(error::Error::RegionKeysSize {
                expect: 1,
                actual: 2,
                ..
            })
        );
        assert_matches!(rule.find_region(&["foo".into()]), Ok(1));
        assert_matches!(rule.find_region(&["bar".into()]), Ok(1));
        assert_matches!(rule.find_region(&["hz".into()]), Ok(2));
        assert_matches!(rule.find_region(&["hzz".into()]), Ok(2));
        assert_matches!(rule.find_region(&["sh".into()]), Ok(3));
        assert_matches!(rule.find_region(&["zzzz".into()]), Ok(3));

        // PARTITION BY RANGE COLUMNS(a, b) (
        //   PARTITION r1 VALUES LESS THAN ('hz', 10),
        //   PARTITION r2 VALUES LESS THAN ('hz', 20),
        //   PARTITION r3 VALUES LESS THAN ('sh', 50),
        //   PARTITION r4 VALUES LESS THAN (MAXVALUE, MAXVALUE),
        // )
        let rule = RangeColumnsPartitionRule::new(
            vec!["a".to_string(), "b".to_string()],
            vec![
                vec![
                    PartitionBound::Value("hz".into()),
                    PartitionBound::Value(10_i32.into()),
                ],
                vec![
                    PartitionBound::Value("hz".into()),
                    PartitionBound::Value(20_i32.into()),
                ],
                vec![
                    PartitionBound::Value("sh".into()),
                    PartitionBound::Value(50_i32.into()),
                ],
                vec![PartitionBound::MaxValue, PartitionBound::MaxValue],
            ],
            vec![1, 2, 3, 4],
        );
        assert_matches!(
            rule.find_region(&["foo".into()]),
            Err(error::Error::RegionKeysSize {
                expect: 2,
                actual: 1,
                ..
            })
        );
        assert_matches!(rule.find_region(&["foo".into(), 1_i32.into()]), Ok(1));
        assert_matches!(rule.find_region(&["bar".into(), 11_i32.into()]), Ok(1));
        assert_matches!(rule.find_region(&["hz".into(), 2_i32.into()]), Ok(1));
        assert_matches!(rule.find_region(&["hz".into(), 12_i32.into()]), Ok(2));
        assert_matches!(rule.find_region(&["hz".into(), 22_i32.into()]), Ok(3));
        assert_matches!(rule.find_region(&["hz".into(), 999_i32.into()]), Ok(3));
        assert_matches!(rule.find_region(&["hzz".into(), 1_i32.into()]), Ok(3));
        assert_matches!(rule.find_region(&["hzz".into(), 999_i32.into()]), Ok(3));
        assert_matches!(rule.find_region(&["sh".into(), 49_i32.into()]), Ok(3));
        assert_matches!(rule.find_region(&["sh".into(), 50_i32.into()]), Ok(4));
        assert_matches!(rule.find_region(&["zzz".into(), 1_i32.into()]), Ok(4));
    }
}
