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
use datatypes::prelude::*;
use serde::{Deserialize, Serialize};
use snafu::OptionExt;
use store_api::storage::RegionNumber;

use crate::error::{self, Error};
use crate::partition::{PartitionExpr, PartitionRule};

/// [RangePartitionRule] manages the distribution of partitions partitioning by some column's value
/// range. It's generated from create table request, using MySQL's syntax:
///
/// ```SQL
/// CREATE TABLE table_name (
///     columns definition
/// )
/// PARTITION BY RANGE (column_name) (
///     PARTITION partition_name VALUES LESS THAN (value)[,
///     PARTITION partition_name VALUES LESS THAN (value)][,
///     ...]
/// )
/// ```
///
/// Please refer to MySQL's ["RANGE Partitioning"](https://dev.mysql.com/doc/refman/8.0/en/partitioning-range.html)
/// document for more details.
///
/// Some partition related validations like:
///   - the column used in partitioning must be defined in the create table request
///   - partition name must be unique
///   - range bounds(the "value"s) must be strictly increased
///   - the last partition range must be bounded by "MAXVALUE"
///
/// are all been done in the create table SQL parsing stage. So we can safely skip some checks on the
/// input arguments.
///
/// # Important Notes on Partition and Region
///
/// Technically, table "partition" is a concept of data sharding logically, i.e., how table's data are
/// distributed in logic. And "region" is of how data been placed physically. They should be used
/// in different ways.
///
/// However, currently we have only one region for each partition. For the sake of simplicity, the
/// terms "partition" and "region" are used interchangeably.
///
// TODO(LFC): Further clarify "partition" and "region".
// Could be creating an extra layer between partition and region.
#[derive(Debug, Serialize, Deserialize)]
pub struct RangePartitionRule {
    column_name: String,
    // Does not store the last "MAXVALUE" bound; because in this way our binary search in finding
    // partitions are easier (besides, it's hard to represent "MAXVALUE" in our `Value`).
    // Then the length of `bounds` is one less than `regions`.
    bounds: Vec<Value>,
    regions: Vec<RegionNumber>,
}

impl RangePartitionRule {
    pub fn new(
        column_name: impl Into<String>,
        bounds: Vec<Value>,
        regions: Vec<RegionNumber>,
    ) -> Self {
        Self {
            column_name: column_name.into(),
            bounds,
            regions,
        }
    }

    pub fn column_name(&self) -> &String {
        &self.column_name
    }

    pub fn all_regions(&self) -> &Vec<RegionNumber> {
        &self.regions
    }

    pub fn bounds(&self) -> &Vec<Value> {
        &self.bounds
    }
}

impl PartitionRule for RangePartitionRule {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn partition_columns(&self) -> Vec<String> {
        vec![self.column_name().to_string()]
    }

    fn find_region(&self, values: &[Value]) -> Result<RegionNumber, Error> {
        debug_assert_eq!(
            values.len(),
            1,
            "RangePartitionRule can only handle one partition value, actual {}",
            values.len()
        );
        let value = &values[0];

        Ok(match self.bounds.binary_search(value) {
            Ok(i) => self.regions[i + 1],
            Err(i) => self.regions[i],
        })
    }

    fn find_regions_by_exprs(&self, exprs: &[PartitionExpr]) -> Result<Vec<RegionNumber>, Error> {
        if exprs.is_empty() {
            return Ok(self.regions.clone());
        }
        debug_assert_eq!(
            exprs.len(),
            1,
            "RangePartitionRule can only handle one partition expr, actual {}",
            exprs.len()
        );

        let PartitionExpr { column, op, value } =
            exprs.first().context(error::FindRegionSnafu {
                reason: "no partition expr is provided",
            })?;
        let regions = if column == self.column_name() {
            // an example of bounds and regions:
            // SQL:
            //   PARTITION p1 VALUES LESS THAN (10),
            //   PARTITION p2 VALUES LESS THAN (20),
            //   PARTITION p3 VALUES LESS THAN (50),
            //   PARTITION p4 VALUES LESS THAN (MAXVALUE),
            // bounds: [10, 20, 50]
            // regions: [1, 2, 3, 4]
            match self.bounds.binary_search(value) {
                Ok(i) => match op {
                    Operator::Lt => &self.regions[..=i],
                    Operator::LtEq => &self.regions[..=(i + 1)],
                    Operator::Eq => &self.regions[(i + 1)..=(i + 1)],
                    Operator::Gt | Operator::GtEq => &self.regions[(i + 1)..],
                    Operator::NotEq => &self.regions[..],
                    _ => unimplemented!(),
                },
                Err(i) => match op {
                    Operator::Lt | Operator::LtEq => &self.regions[..=i],
                    Operator::Eq => &self.regions[i..=i],
                    Operator::Gt | Operator::GtEq => &self.regions[i..],
                    Operator::NotEq => &self.regions[..],
                    _ => unimplemented!(),
                },
            }
            .to_vec()
        } else {
            self.all_regions().clone()
        };
        Ok(regions)
    }
}

#[cfg(test)]
mod test {
    use datafusion_expr::Operator;

    use super::*;
    use crate::partition::PartitionExpr;

    #[test]
    fn test_find_regions() {
        // PARTITION BY RANGE (a) (
        //   PARTITION p1 VALUES LESS THAN ('hz'),
        //   PARTITION p2 VALUES LESS THAN ('sh'),
        //   PARTITION p3 VALUES LESS THAN ('sz'),
        //   PARTITION p4 VALUES LESS THAN (MAXVALUE),
        // )
        let rule = RangePartitionRule {
            column_name: "a".to_string(),
            bounds: vec!["hz".into(), "sh".into(), "sz".into()],
            regions: vec![1, 2, 3, 4],
        };

        let test =
            |column: &str, op: Operator, value: &str, expected_regions: Vec<RegionNumber>| {
                let expr = PartitionExpr {
                    column: column.to_string(),
                    op,
                    value: value.into(),
                };
                let regions = rule.find_regions_by_exprs(&[expr]).unwrap();
                assert_eq!(
                    regions,
                    expected_regions.into_iter().collect::<Vec<RegionNumber>>()
                );
            };

        test("a", Operator::NotEq, "hz", vec![1, 2, 3, 4]);
        test("a", Operator::NotEq, "what", vec![1, 2, 3, 4]);

        test("a", Operator::GtEq, "ab", vec![1, 2, 3, 4]);
        test("a", Operator::GtEq, "hz", vec![2, 3, 4]);
        test("a", Operator::GtEq, "ijk", vec![2, 3, 4]);
        test("a", Operator::GtEq, "sh", vec![3, 4]);
        test("a", Operator::GtEq, "ssh", vec![3, 4]);
        test("a", Operator::GtEq, "sz", vec![4]);
        test("a", Operator::GtEq, "zz", vec![4]);

        test("a", Operator::Gt, "ab", vec![1, 2, 3, 4]);
        test("a", Operator::Gt, "hz", vec![2, 3, 4]);
        test("a", Operator::Gt, "ijk", vec![2, 3, 4]);
        test("a", Operator::Gt, "sh", vec![3, 4]);
        test("a", Operator::Gt, "ssh", vec![3, 4]);
        test("a", Operator::Gt, "sz", vec![4]);
        test("a", Operator::Gt, "zz", vec![4]);

        test("a", Operator::Eq, "ab", vec![1]);
        test("a", Operator::Eq, "hz", vec![2]);
        test("a", Operator::Eq, "ijk", vec![2]);
        test("a", Operator::Eq, "sh", vec![3]);
        test("a", Operator::Eq, "ssh", vec![3]);
        test("a", Operator::Eq, "sz", vec![4]);
        test("a", Operator::Eq, "zz", vec![4]);

        test("a", Operator::Lt, "ab", vec![1]);
        test("a", Operator::Lt, "hz", vec![1]);
        test("a", Operator::Lt, "ijk", vec![1, 2]);
        test("a", Operator::Lt, "sh", vec![1, 2]);
        test("a", Operator::Lt, "ssh", vec![1, 2, 3]);
        test("a", Operator::Lt, "sz", vec![1, 2, 3]);
        test("a", Operator::Lt, "zz", vec![1, 2, 3, 4]);

        test("a", Operator::LtEq, "ab", vec![1]);
        test("a", Operator::LtEq, "hz", vec![1, 2]);
        test("a", Operator::LtEq, "ijk", vec![1, 2]);
        test("a", Operator::LtEq, "sh", vec![1, 2, 3]);
        test("a", Operator::LtEq, "ssh", vec![1, 2, 3]);
        test("a", Operator::LtEq, "sz", vec![1, 2, 3, 4]);
        test("a", Operator::LtEq, "zz", vec![1, 2, 3, 4]);

        test("b", Operator::Lt, "1", vec![1, 2, 3, 4]);
    }
}
