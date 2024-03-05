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
use std::cmp::Ordering;
use std::collections::HashMap;

use datatypes::prelude::Value;
use serde::{Deserialize, Serialize};
use snafu::ensure;
use store_api::storage::RegionNumber;

use crate::error::{self, Result};
use crate::expr::{Operand, PartitionExpr, RestrictedOp};
use crate::PartitionRule;

#[derive(Debug, Serialize, Deserialize)]
pub struct MultiDimPartitionRule {
    partition_columns: Vec<String>,
    // name to index of `partition_columns`
    name_to_index: HashMap<String, usize>,
    regions: Vec<RegionNumber>,
    exprs: Vec<PartitionExpr>,
}

impl MultiDimPartitionRule {
    pub fn new(
        partition_columns: Vec<String>,
        regions: Vec<RegionNumber>,
        exprs: Vec<PartitionExpr>,
    ) -> Self {
        let name_to_index = partition_columns
            .iter()
            .enumerate()
            .map(|(i, name)| (name.clone(), i))
            .collect::<HashMap<_, _>>();

        Self {
            partition_columns,
            name_to_index,
            regions,
            exprs,
        }
    }

    fn find_region(&self, values: &[Value]) -> Result<RegionNumber> {
        ensure!(
            values.len() == self.partition_columns.len(),
            error::RegionKeysSizeSnafu {
                expect: self.partition_columns.len(),
                actual: values.len(),
            }
        );

        for (region_index, expr) in self.exprs.iter().enumerate() {
            if self.evaluate_expr(expr, values)? {
                return Ok(self.regions[region_index]);
            }
        }

        // return the default region number
        Ok(0)
    }

    fn evaluate_expr(&self, expr: &PartitionExpr, values: &[Value]) -> Result<bool> {
        match (expr.lhs.as_ref(), expr.rhs.as_ref()) {
            (Operand::Column(name), Operand::Value(r)) => {
                let index = self.name_to_index.get(name).unwrap();
                let l = &values[*index];
                Self::perform_op(l, &expr.op, r)
            }
            (Operand::Value(l), Operand::Column(name)) => {
                let index = self.name_to_index.get(name).unwrap();
                let r = &values[*index];
                Self::perform_op(l, &expr.op, r)
            }
            (Operand::Expr(lhs), Operand::Expr(rhs)) => {
                let lhs = self.evaluate_expr(lhs, values)?;
                let rhs = self.evaluate_expr(rhs, values)?;
                match expr.op {
                    RestrictedOp::And => Ok(lhs && rhs),
                    RestrictedOp::Or => Ok(lhs || rhs),
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }

    fn perform_op(lhs: &Value, op: &RestrictedOp, rhs: &Value) -> Result<bool> {
        let result = match op {
            RestrictedOp::Eq => lhs.eq(rhs),
            RestrictedOp::NotEq => lhs.ne(rhs),
            RestrictedOp::Lt => lhs.partial_cmp(rhs) == Some(Ordering::Less),
            RestrictedOp::LtEq => {
                let result = lhs.partial_cmp(rhs);
                result == Some(Ordering::Less) || result == Some(Ordering::Equal)
            }
            RestrictedOp::Gt => lhs.partial_cmp(rhs) == Some(Ordering::Greater),
            RestrictedOp::GtEq => {
                let result = lhs.partial_cmp(rhs);
                result == Some(Ordering::Greater) || result == Some(Ordering::Equal)
            }
            RestrictedOp::And | RestrictedOp::Or => unreachable!(),
        };

        Ok(result)
    }
}

impl PartitionRule for MultiDimPartitionRule {
    fn as_any(&self) -> &dyn Any {
        self
    }

    fn partition_columns(&self) -> Vec<String> {
        self.partition_columns.clone()
    }

    fn find_region(&self, values: &[Value]) -> Result<RegionNumber> {
        self.find_region(values)
    }

    fn find_regions_by_exprs(
        &self,
        _exprs: &[crate::partition::PartitionExpr],
    ) -> Result<Vec<RegionNumber>> {
        Ok(self.regions.clone())
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use super::*;
    use crate::error;

    #[test]
    fn test_find_region() {
        // PARTITION ON COLUMNS (b) (
        //     b < 'hz',
        //     b >= 'hz' AND b < 'sh',
        //     b >= 'sh'
        // )
        let rule = MultiDimPartitionRule::new(
            vec!["b".to_string()],
            vec![1, 2, 3],
            vec![
                PartitionExpr::new(
                    Operand::Column("b".to_string()),
                    RestrictedOp::Lt,
                    Operand::Value(datatypes::value::Value::String("hz".into())),
                ),
                PartitionExpr::new(
                    Operand::Expr(PartitionExpr::new(
                        Operand::Column("b".to_string()),
                        RestrictedOp::GtEq,
                        Operand::Value(datatypes::value::Value::String("hz".into())),
                    )),
                    RestrictedOp::And,
                    Operand::Expr(PartitionExpr::new(
                        Operand::Column("b".to_string()),
                        RestrictedOp::Lt,
                        Operand::Value(datatypes::value::Value::String("sh".into())),
                    )),
                ),
                PartitionExpr::new(
                    Operand::Column("b".to_string()),
                    RestrictedOp::GtEq,
                    Operand::Value(datatypes::value::Value::String("sh".into())),
                ),
            ],
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
    }
}
