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

use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::PhysicalExpr;
use datatypes::arrow::array::{BooleanArray, BooleanBufferBuilder, RecordBatch};
use datatypes::prelude::Value;
use datatypes::vectors::{Helper, VectorRef};
use serde::{Deserialize, Serialize};
use snafu::{ensure, OptionExt, ResultExt};
use store_api::storage::RegionNumber;

use crate::error::{
    self, ConjunctExprWithNonExprSnafu, InvalidExprSnafu, Result, UnclosedValueSnafu,
    UndefinedColumnSnafu,
};
use crate::expr::{Operand, PartitionExpr, RestrictedOp};
use crate::PartitionRule;

/// Multi-Dimiension partition rule. RFC [here](https://github.com/GreptimeTeam/greptimedb/blob/main/docs/rfcs/2024-02-21-multi-dimension-partition-rule/rfc.md)
///
/// This partition rule is defined by a set of simple expressions on the partition
/// key columns. Compare to RANGE partition, which can be considered as
/// single-dimension rule, this will evaluate expression on each column separately.
#[derive(Debug, Serialize, Deserialize)]
pub struct MultiDimPartitionRule {
    /// Allow list of which columns can be used for partitioning.
    partition_columns: Vec<String>,
    /// Name to index of `partition_columns`. Used for quick lookup.
    name_to_index: HashMap<String, usize>,
    /// Region number for each partition. This list has the same length as `exprs`
    /// (dispiting the default region).
    regions: Vec<RegionNumber>,
    /// Partition expressions.
    exprs: Vec<PartitionExpr>,
}

impl MultiDimPartitionRule {
    pub fn try_new(
        partition_columns: Vec<String>,
        regions: Vec<RegionNumber>,
        exprs: Vec<PartitionExpr>,
    ) -> Result<Self> {
        let name_to_index = partition_columns
            .iter()
            .enumerate()
            .map(|(i, name)| (name.clone(), i))
            .collect::<HashMap<_, _>>();

        let rule = Self {
            partition_columns,
            name_to_index,
            regions,
            exprs,
        };

        let mut checker = RuleChecker::new(&rule);
        checker.check()?;

        Ok(rule)
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

    fn row_at(&self, cols: &[VectorRef], index: usize, row: &mut [Value]) -> Result<()> {
        for (col_idx, col) in cols.iter().enumerate() {
            row[col_idx] = col.get(index);
        }
        Ok(())
    }

    fn record_batch_to_cols(&self, record_batch: &RecordBatch) -> Result<Vec<VectorRef>> {
        self.partition_columns
            .iter()
            .map(|col_name| {
                record_batch
                    .column_by_name(col_name)
                    .context(error::UndefinedColumnSnafu { column: col_name })
                    .and_then(|array| {
                        Helper::try_into_vector(array).context(error::ConvertToVectorSnafu)
                    })
            })
            .collect::<Result<Vec<_>>>()
    }

    fn split_record_batch_naive(
        &self,
        record_batch: &RecordBatch,
    ) -> Result<HashMap<RegionNumber, BooleanArray>> {
        let num_rows = record_batch.num_rows();

        let mut result = self
            .regions
            .iter()
            .map(|region| {
                let mut builder = BooleanBufferBuilder::new(num_rows);
                builder.append_n(num_rows, false);
                (*region, builder)
            })
            .collect::<HashMap<_, _>>();

        let cols = self.record_batch_to_cols(record_batch)?;
        let mut current_row = vec![Value::Null; self.partition_columns.len()];
        for row_idx in 0..num_rows {
            self.row_at(&cols, row_idx, &mut current_row)?;
            let current_region = self.find_region(&current_row)?;
            let region_mask = result
                .get_mut(&current_region)
                .expect(&format!("Region {} must be initialized", current_region));
            region_mask.set_bit(row_idx, true);
        }

        Ok(result
            .into_iter()
            .map(|(region, mut mask)| (region, BooleanArray::new(mask.finish(), None)))
            .collect())
    }

    fn split_record_batch(
        &self,
        record_batch: &RecordBatch,
    ) -> Result<HashMap<RegionNumber, BooleanArray>> {
        Ok(self
            .exprs
            .iter()
            .zip(self.regions.iter())
            .map(|(expr, region_num)| {
                let df_expr = expr.try_as_physical_expr(&record_batch.schema()).unwrap();
                let ColumnarValue::Array(column) = df_expr.evaluate(record_batch).unwrap() else {
                    unreachable!("Expected an array")
                };
                (
                    *region_num,
                    column
                        .as_any()
                        .downcast_ref::<BooleanArray>()
                        .unwrap()
                        .clone(),
                )
            })
            .collect())
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
}

/// Helper for [RuleChecker]
type Axis = HashMap<Value, SplitPoint>;

/// Helper for [RuleChecker]
struct SplitPoint {
    is_equal: bool,
    less_than_counter: isize,
}

/// Check if the rule set covers all the possible values.
///
/// Note this checker have false-negative on duplicated exprs. E.g.:
/// `a != 20`, `a <= 20` and `a > 20`.
///
/// It works on the observation that each projected split point should be included (`is_equal`)
/// and have a balanced `<` and `>` counter.
struct RuleChecker<'a> {
    axis: Vec<Axis>,
    rule: &'a MultiDimPartitionRule,
}

impl<'a> RuleChecker<'a> {
    pub fn new(rule: &'a MultiDimPartitionRule) -> Self {
        let mut projections = Vec::with_capacity(rule.partition_columns.len());
        projections.resize_with(rule.partition_columns.len(), Default::default);

        Self {
            axis: projections,
            rule,
        }
    }

    pub fn check(&mut self) -> Result<()> {
        for expr in &self.rule.exprs {
            self.walk_expr(expr)?
        }

        self.check_axis()
    }

    #[allow(clippy::mutable_key_type)]
    fn walk_expr(&mut self, expr: &PartitionExpr) -> Result<()> {
        // recursively check the expr
        match expr.op {
            RestrictedOp::And | RestrictedOp::Or => {
                match (expr.lhs.as_ref(), expr.rhs.as_ref()) {
                    (Operand::Expr(lhs), Operand::Expr(rhs)) => {
                        self.walk_expr(lhs)?;
                        self.walk_expr(rhs)?
                    }
                    _ => ConjunctExprWithNonExprSnafu { expr: expr.clone() }.fail()?,
                }

                return Ok(());
            }
            // Not conjunction
            _ => {}
        }

        let (col, val) = match (expr.lhs.as_ref(), expr.rhs.as_ref()) {
            (Operand::Expr(_), _)
            | (_, Operand::Expr(_))
            | (Operand::Column(_), Operand::Column(_))
            | (Operand::Value(_), Operand::Value(_)) => {
                InvalidExprSnafu { expr: expr.clone() }.fail()?
            }

            (Operand::Column(col), Operand::Value(val))
            | (Operand::Value(val), Operand::Column(col)) => (col, val),
        };

        let col_index =
            *self
                .rule
                .name_to_index
                .get(col)
                .with_context(|| UndefinedColumnSnafu {
                    column: col.clone(),
                })?;
        let axis = &mut self.axis[col_index];
        let split_point = axis.entry(val.clone()).or_insert(SplitPoint {
            is_equal: false,
            less_than_counter: 0,
        });
        match expr.op {
            RestrictedOp::Eq => {
                split_point.is_equal = true;
            }
            RestrictedOp::NotEq => {
                // less_than +1 -1
            }
            RestrictedOp::Lt => {
                split_point.less_than_counter += 1;
            }
            RestrictedOp::LtEq => {
                split_point.less_than_counter += 1;
                split_point.is_equal = true;
            }
            RestrictedOp::Gt => {
                split_point.less_than_counter -= 1;
            }
            RestrictedOp::GtEq => {
                split_point.less_than_counter -= 1;
                split_point.is_equal = true;
            }
            RestrictedOp::And | RestrictedOp::Or => {
                unreachable!("conjunct expr should be handled above")
            }
        }

        Ok(())
    }

    /// Return if the rule is legal.
    fn check_axis(&self) -> Result<()> {
        for (col_index, axis) in self.axis.iter().enumerate() {
            for (val, split_point) in axis {
                if split_point.less_than_counter != 0 || !split_point.is_equal {
                    UnclosedValueSnafu {
                        value: format!("{val:?}"),
                        column: self.rule.partition_columns[col_index].clone(),
                    }
                    .fail()?;
                }
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;
    use std::sync::Arc;

    use super::*;
    use crate::error::{self, Error};

    #[test]
    fn test_find_region() {
        // PARTITION ON COLUMNS (b) (
        //     b < 'hz',
        //     b >= 'hz' AND b < 'sh',
        //     b >= 'sh'
        // )
        let rule = MultiDimPartitionRule::try_new(
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
        )
        .unwrap();
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

    #[test]
    fn invalid_expr_case_1() {
        // PARTITION ON COLUMNS (b) (
        //     b <= b >= 'hz' AND b < 'sh',
        // )
        let rule = MultiDimPartitionRule::try_new(
            vec!["a".to_string(), "b".to_string()],
            vec![1],
            vec![PartitionExpr::new(
                Operand::Column("b".to_string()),
                RestrictedOp::LtEq,
                Operand::Expr(PartitionExpr::new(
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
                )),
            )],
        );

        // check rule
        assert_matches!(rule.unwrap_err(), Error::InvalidExpr { .. });
    }

    #[test]
    fn invalid_expr_case_2() {
        // PARTITION ON COLUMNS (b) (
        //     b >= 'hz' AND 'sh',
        // )
        let rule = MultiDimPartitionRule::try_new(
            vec!["a".to_string(), "b".to_string()],
            vec![1],
            vec![PartitionExpr::new(
                Operand::Expr(PartitionExpr::new(
                    Operand::Column("b".to_string()),
                    RestrictedOp::GtEq,
                    Operand::Value(datatypes::value::Value::String("hz".into())),
                )),
                RestrictedOp::And,
                Operand::Value(datatypes::value::Value::String("sh".into())),
            )],
        );

        // check rule
        assert_matches!(rule.unwrap_err(), Error::ConjunctExprWithNonExpr { .. });
    }

    /// ```ignore
    ///          │          │               
    ///          │          │               
    /// ─────────┼──────────┼────────────► b
    ///          │          │               
    ///          │          │               
    ///      b <= h     b >= s            
    /// ```
    #[test]
    fn empty_expr_case_1() {
        // PARTITION ON COLUMNS (b) (
        //     b <= 'h',
        //     b >= 's'
        // )
        let rule = MultiDimPartitionRule::try_new(
            vec!["a".to_string(), "b".to_string()],
            vec![1, 2],
            vec![
                PartitionExpr::new(
                    Operand::Column("b".to_string()),
                    RestrictedOp::LtEq,
                    Operand::Value(datatypes::value::Value::String("h".into())),
                ),
                PartitionExpr::new(
                    Operand::Column("b".to_string()),
                    RestrictedOp::GtEq,
                    Operand::Value(datatypes::value::Value::String("s".into())),
                ),
            ],
        );

        // check rule
        assert_matches!(rule.unwrap_err(), Error::UnclosedValue { .. });
    }

    /// ```
    ///     a                                                  
    ///     ▲                                        
    ///     │                   ‖        
    ///     │                   ‖        
    /// 200 │         ┌─────────┤        
    ///     │         │         │        
    ///     │         │         │        
    ///     │         │         │        
    /// 100 │   ======┴─────────┘        
    ///     │                            
    ///     └──────────────────────────►b
    ///              10          20      
    /// ```
    #[test]
    fn empty_expr_case_2() {
        // PARTITION ON COLUMNS (b) (
        //     a >= 100 AND b <= 10  OR  a > 100 AND a <= 200 AND b <= 10  OR  a >= 200 AND b > 10 AND b <= 20  OR  a > 200 AND b <= 20
        //     a < 100 AND b <= 20  OR  a >= 100 AND b > 20
        // )
        let rule = MultiDimPartitionRule::try_new(
            vec!["a".to_string(), "b".to_string()],
            vec![1, 2],
            vec![
                PartitionExpr::new(
                    Operand::Expr(PartitionExpr::new(
                        Operand::Expr(PartitionExpr::new(
                            //  a >= 100 AND b <= 10
                            Operand::Expr(PartitionExpr::new(
                                Operand::Expr(PartitionExpr::new(
                                    Operand::Column("a".to_string()),
                                    RestrictedOp::GtEq,
                                    Operand::Value(datatypes::value::Value::Int64(100)),
                                )),
                                RestrictedOp::And,
                                Operand::Expr(PartitionExpr::new(
                                    Operand::Column("b".to_string()),
                                    RestrictedOp::LtEq,
                                    Operand::Value(datatypes::value::Value::Int64(10)),
                                )),
                            )),
                            RestrictedOp::Or,
                            // a > 100 AND a <= 200 AND b <= 10
                            Operand::Expr(PartitionExpr::new(
                                Operand::Expr(PartitionExpr::new(
                                    Operand::Expr(PartitionExpr::new(
                                        Operand::Column("a".to_string()),
                                        RestrictedOp::Gt,
                                        Operand::Value(datatypes::value::Value::Int64(100)),
                                    )),
                                    RestrictedOp::And,
                                    Operand::Expr(PartitionExpr::new(
                                        Operand::Column("a".to_string()),
                                        RestrictedOp::LtEq,
                                        Operand::Value(datatypes::value::Value::Int64(200)),
                                    )),
                                )),
                                RestrictedOp::And,
                                Operand::Expr(PartitionExpr::new(
                                    Operand::Column("b".to_string()),
                                    RestrictedOp::LtEq,
                                    Operand::Value(datatypes::value::Value::Int64(10)),
                                )),
                            )),
                        )),
                        RestrictedOp::Or,
                        // a >= 200 AND b > 10 AND b <= 20
                        Operand::Expr(PartitionExpr::new(
                            Operand::Expr(PartitionExpr::new(
                                Operand::Expr(PartitionExpr::new(
                                    Operand::Column("a".to_string()),
                                    RestrictedOp::GtEq,
                                    Operand::Value(datatypes::value::Value::Int64(200)),
                                )),
                                RestrictedOp::And,
                                Operand::Expr(PartitionExpr::new(
                                    Operand::Column("b".to_string()),
                                    RestrictedOp::Gt,
                                    Operand::Value(datatypes::value::Value::Int64(10)),
                                )),
                            )),
                            RestrictedOp::And,
                            Operand::Expr(PartitionExpr::new(
                                Operand::Column("b".to_string()),
                                RestrictedOp::LtEq,
                                Operand::Value(datatypes::value::Value::Int64(20)),
                            )),
                        )),
                    )),
                    RestrictedOp::Or,
                    // a > 200 AND b <= 20
                    Operand::Expr(PartitionExpr::new(
                        Operand::Expr(PartitionExpr::new(
                            Operand::Column("a".to_string()),
                            RestrictedOp::Gt,
                            Operand::Value(datatypes::value::Value::Int64(200)),
                        )),
                        RestrictedOp::And,
                        Operand::Expr(PartitionExpr::new(
                            Operand::Column("b".to_string()),
                            RestrictedOp::LtEq,
                            Operand::Value(datatypes::value::Value::Int64(20)),
                        )),
                    )),
                ),
                PartitionExpr::new(
                    // a < 100 AND b <= 20
                    Operand::Expr(PartitionExpr::new(
                        Operand::Expr(PartitionExpr::new(
                            Operand::Column("a".to_string()),
                            RestrictedOp::Lt,
                            Operand::Value(datatypes::value::Value::Int64(100)),
                        )),
                        RestrictedOp::And,
                        Operand::Expr(PartitionExpr::new(
                            Operand::Column("b".to_string()),
                            RestrictedOp::LtEq,
                            Operand::Value(datatypes::value::Value::Int64(20)),
                        )),
                    )),
                    RestrictedOp::Or,
                    // a >= 100 AND b > 20
                    Operand::Expr(PartitionExpr::new(
                        Operand::Expr(PartitionExpr::new(
                            Operand::Column("a".to_string()),
                            RestrictedOp::GtEq,
                            Operand::Value(datatypes::value::Value::Int64(100)),
                        )),
                        RestrictedOp::And,
                        Operand::Expr(PartitionExpr::new(
                            Operand::Column("b".to_string()),
                            RestrictedOp::GtEq,
                            Operand::Value(datatypes::value::Value::Int64(20)),
                        )),
                    )),
                ),
            ],
        );

        // check rule
        assert_matches!(rule.unwrap_err(), Error::UnclosedValue { .. });
    }

    #[test]
    fn duplicate_expr_case_1() {
        // PARTITION ON COLUMNS (a) (
        //     a <= 20,
        //     a >= 10
        // )
        let rule = MultiDimPartitionRule::try_new(
            vec!["a".to_string(), "b".to_string()],
            vec![1, 2],
            vec![
                PartitionExpr::new(
                    Operand::Column("a".to_string()),
                    RestrictedOp::LtEq,
                    Operand::Value(datatypes::value::Value::Int64(20)),
                ),
                PartitionExpr::new(
                    Operand::Column("a".to_string()),
                    RestrictedOp::GtEq,
                    Operand::Value(datatypes::value::Value::Int64(10)),
                ),
            ],
        );

        // check rule
        assert_matches!(rule.unwrap_err(), Error::UnclosedValue { .. });
    }

    #[test]
    #[ignore = "checker cannot detect this kind of duplicate for now"]
    fn duplicate_expr_case_2() {
        // PARTITION ON COLUMNS (a) (
        //     a != 20,
        //     a <= 20,
        //     a > 20,
        // )
        let rule = MultiDimPartitionRule::try_new(
            vec!["a".to_string(), "b".to_string()],
            vec![1, 2],
            vec![
                PartitionExpr::new(
                    Operand::Column("a".to_string()),
                    RestrictedOp::NotEq,
                    Operand::Value(datatypes::value::Value::Int64(20)),
                ),
                PartitionExpr::new(
                    Operand::Column("a".to_string()),
                    RestrictedOp::LtEq,
                    Operand::Value(datatypes::value::Value::Int64(20)),
                ),
                PartitionExpr::new(
                    Operand::Column("a".to_string()),
                    RestrictedOp::Gt,
                    Operand::Value(datatypes::value::Value::Int64(20)),
                ),
            ],
        );

        // check rule
        assert!(rule.is_err());
    }

    #[test]
    fn test_split_record_batch_by_one_column() {
        use datatypes::arrow::array::{Int64Array, StringArray};
        use datatypes::arrow::datatypes::{DataType, Field, Schema};
        use datatypes::arrow::record_batch::RecordBatch;

        // Create a simple MultiDimPartitionRule
        let rule = MultiDimPartitionRule::try_new(
            vec!["host".to_string(), "value".to_string()],
            vec![0, 1],
            vec![
                PartitionExpr::new(
                    Operand::Column("host".to_string()),
                    RestrictedOp::Lt,
                    Operand::Value(Value::String("server1".into())),
                ),
                PartitionExpr::new(
                    Operand::Column("host".to_string()),
                    RestrictedOp::GtEq,
                    Operand::Value(Value::String("server1".into())),
                ),
            ],
        )
        .unwrap();

        // Create a record batch with test data
        let schema = Arc::new(Schema::new(vec![
            Field::new("host", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));

        let host_array = StringArray::from(vec!["server1", "server2", "server3", "server1"]);
        let value_array = Int64Array::from(vec![10, 20, 30, 40]);

        let batch = RecordBatch::try_new(schema, vec![Arc::new(host_array), Arc::new(value_array)])
            .unwrap();

        // Split the batch
        let result = rule.split_record_batch(&batch).unwrap();
        let expected = rule.split_record_batch_naive(&batch).unwrap();
        assert_eq!(result.len(), expected.len());
        for (region, value) in &result {
            assert_eq!(
                value,
                expected.get(region).unwrap(),
                "failed on region: {}",
                region
            );
        }
    }

    #[test]
    fn test_split_record_batch_empty() {
        use datatypes::arrow::array::{Int64Array, StringArray};
        use datatypes::arrow::datatypes::{DataType, Field, Schema};
        use datatypes::arrow::record_batch::RecordBatch;

        // Create a simple MultiDimPartitionRule
        let rule = MultiDimPartitionRule::try_new(
            vec!["host".to_string()],
            vec![1],
            vec![PartitionExpr::new(
                Operand::Column("host".to_string()),
                RestrictedOp::Eq,
                Operand::Value(Value::String("server1".into())),
            )],
        )
        .unwrap();

        // Create an empty record batch
        let schema = Arc::new(Schema::new(vec![
            Field::new("host", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));

        let host_array = StringArray::from(Vec::<&str>::new());
        let value_array = Int64Array::from(Vec::<i64>::new());

        let batch = RecordBatch::try_new(schema, vec![Arc::new(host_array), Arc::new(value_array)])
            .unwrap();

        // Split the batch
        let result = rule.split_record_batch(&batch).unwrap();

        // Empty batch should result in empty map
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_split_record_batch_complex_condition() {
        use datatypes::arrow::array::{Int64Array, StringArray};
        use datatypes::arrow::datatypes::{DataType, Field, Schema};
        use datatypes::arrow::record_batch::RecordBatch;

        // Create a rule with more complex conditions
        let rule = MultiDimPartitionRule::try_new(
            vec!["host".to_string(), "value".to_string()],
            vec![1, 2],
            vec![
                // Region 1: host = 'server1' AND value > 20
                PartitionExpr::new(
                    Operand::Expr(PartitionExpr::new(
                        Operand::Column("host".to_string()),
                        RestrictedOp::Eq,
                        Operand::Value(Value::String("server1".into())),
                    )),
                    RestrictedOp::And,
                    Operand::Expr(PartitionExpr::new(
                        Operand::Column("value".into()),
                        RestrictedOp::Gt,
                        Operand::Value(Value::Int64(20)),
                    )),
                ),
                // Region 2: host = 'server2'
                PartitionExpr::new(
                    Operand::Column("host".to_string()),
                    RestrictedOp::Eq,
                    Operand::Value(Value::String("server2".into())),
                ),
            ],
        )
        .unwrap();

        // Create a record batch with test data
        let schema = Arc::new(Schema::new(vec![
            Field::new("host", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]));

        let host_array = StringArray::from(vec!["server1", "server1", "server2", "server3"]);
        let value_array = Int64Array::from(vec![10, 30, 20, 40]);

        let batch = RecordBatch::try_new(schema, vec![Arc::new(host_array), Arc::new(value_array)])
            .unwrap();

        // Split the batch
        let result = rule.split_record_batch(&batch).unwrap();

        // Check the results
        assert_eq!(result.len(), 3); // 2 defined regions + 1 default region

        // Check region 1 (server1 AND value > 20)
        let region1_array = result.get(&1).unwrap();
        assert_eq!(region1_array.len(), 1);
        assert_eq!(
            region1_array.iter().map(|b| b.unwrap()).collect::<Vec<_>>(),
            vec![true]
        );

        // Check region 2 (server2)
        assert!(result.contains_key(&2));
        let region2_batch = result.get(&2).unwrap();
        assert_eq!(region2_batch.len(), 1);

        // Check default region (server1 with value <= 20 and server3)
        assert!(result.contains_key(&0));
        let default_batch = result.get(&0).unwrap();
        assert_eq!(default_batch.len(), 2);
    }
}
