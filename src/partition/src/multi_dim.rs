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
use std::collections::hash_map::Entry;
use std::sync::{Arc, RwLock};

use datafusion_expr::ColumnarValue;
use datafusion_physical_expr::PhysicalExpr;
use datatypes::arrow;
use datatypes::arrow::array::{BooleanArray, BooleanBufferBuilder, RecordBatch};
use datatypes::arrow::buffer::BooleanBuffer;
use datatypes::arrow::datatypes::Schema;
use datatypes::prelude::Value;
use datatypes::vectors::{Helper, VectorRef};
use serde::{Deserialize, Serialize};
use snafu::{OptionExt, ResultExt, ensure};
use store_api::storage::RegionNumber;

use crate::PartitionRule;
use crate::checker::PartitionChecker;
use crate::error::{self, Result, UndefinedColumnSnafu};
use crate::expr::{Operand, PartitionExpr, RestrictedOp};
use crate::partition::RegionMask;

/// The default region number when no partition exprs are matched.
const DEFAULT_REGION: RegionNumber = 0;

type PhysicalExprCache = Option<(Vec<Arc<dyn PhysicalExpr>>, Arc<Schema>)>;

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
    /// Cache of physical expressions.
    #[serde(skip)]
    physical_expr_cache: RwLock<PhysicalExprCache>,
}

impl MultiDimPartitionRule {
    /// Create a new [`MultiDimPartitionRule`].
    ///
    /// If `check_exprs` is true, the function will check if the expressions are valid. This is
    /// required when constructing a new partition rule like `CREATE TABLE` or `ALTER TABLE`.
    pub fn try_new(
        partition_columns: Vec<String>,
        regions: Vec<RegionNumber>,
        exprs: Vec<PartitionExpr>,
        check_exprs: bool,
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
            physical_expr_cache: RwLock::new(None),
        };

        if check_exprs {
            let checker = PartitionChecker::try_new(&rule)?;
            checker.check()?;
        }

        Ok(rule)
    }

    pub fn exprs(&self) -> &[PartitionExpr] {
        &self.exprs
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
        Ok(DEFAULT_REGION)
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

    pub fn row_at(&self, cols: &[VectorRef], index: usize, row: &mut [Value]) -> Result<()> {
        for (col_idx, col) in cols.iter().enumerate() {
            row[col_idx] = col.get(index);
        }
        Ok(())
    }

    pub fn record_batch_to_cols(&self, record_batch: &RecordBatch) -> Result<Vec<VectorRef>> {
        self.partition_columns
            .iter()
            .map(|col_name| {
                record_batch
                    .column_by_name(col_name)
                    .context(UndefinedColumnSnafu { column: col_name })
                    .and_then(|array| {
                        Helper::try_into_vector(array).context(error::ConvertToVectorSnafu)
                    })
            })
            .collect::<Result<Vec<_>>>()
    }

    pub fn split_record_batch_naive(
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
                .unwrap_or_else(|| panic!("Region {} must be initialized", current_region));
            region_mask.set_bit(row_idx, true);
        }

        Ok(result
            .into_iter()
            .map(|(region, mut mask)| (region, BooleanArray::new(mask.finish(), None)))
            .collect())
    }

    pub fn split_record_batch(
        &self,
        record_batch: &RecordBatch,
    ) -> Result<HashMap<RegionNumber, RegionMask>> {
        let num_rows = record_batch.num_rows();
        if self.regions.len() == 1 {
            return Ok([(
                self.regions[0],
                RegionMask::from(BooleanArray::from(vec![true; num_rows])),
            )]
            .into_iter()
            .collect());
        }
        let physical_exprs = {
            let cache_read_guard = self.physical_expr_cache.read().unwrap();
            if let Some((cached_exprs, schema)) = cache_read_guard.as_ref()
                && schema == record_batch.schema_ref()
            {
                cached_exprs.clone()
            } else {
                drop(cache_read_guard); // Release the read lock before acquiring write lock

                let schema = record_batch.schema();
                let new_cache = self
                    .exprs
                    .iter()
                    .map(|e| e.try_as_physical_expr(&schema))
                    .collect::<Result<Vec<_>>>()?;

                let mut cache_write_guard = self.physical_expr_cache.write().unwrap();
                cache_write_guard.replace((new_cache.clone(), schema));
                new_cache
            }
        };

        let mut result: HashMap<u32, RegionMask> = physical_exprs
            .iter()
            .zip(self.regions.iter())
            .filter_map(|(expr, region_num)| {
                let col_val = match expr
                    .evaluate(record_batch)
                    .context(error::EvaluateRecordBatchSnafu)
                {
                    Ok(array) => array,
                    Err(e) => {
                        return Some(Err(e));
                    }
                };
                let array = match columnar_value_to_boolean_array(col_val, num_rows) {
                    Ok(array) => array,
                    Err(e) => {
                        return Some(Err(e));
                    }
                };
                let selected_rows = array.true_count();
                if selected_rows == 0 {
                    // skip empty region in results.
                    return None;
                }
                Some(Ok((*region_num, RegionMask::new(array, selected_rows))))
            })
            .collect::<error::Result<_>>()?;

        let selected = if result.len() == 1 {
            result.values().next().unwrap().array().clone()
        } else {
            let mut selected = BooleanArray::new(BooleanBuffer::new_unset(num_rows), None);
            for region_mask in result.values() {
                selected = arrow::compute::kernels::boolean::or(&selected, region_mask.array())
                    .context(error::ComputeArrowKernelSnafu)?;
            }
            selected
        };

        // fast path: all rows are selected
        if selected.true_count() == num_rows {
            return Ok(result);
        }

        // find unselected rows and assign to default region
        let unselected = arrow::compute::kernels::boolean::not(&selected)
            .context(error::ComputeArrowKernelSnafu)?;
        match result.entry(DEFAULT_REGION) {
            Entry::Occupied(mut o) => {
                // merge default region with unselected rows.
                let default_region_mask = RegionMask::from(
                    arrow::compute::kernels::boolean::or(o.get().array(), &unselected)
                        .context(error::ComputeArrowKernelSnafu)?,
                );
                o.insert(default_region_mask);
            }
            Entry::Vacant(v) => {
                // default region has no rows, simply put all unselected rows to default region.
                v.insert(RegionMask::from(unselected));
            }
        }
        Ok(result)
    }
}

fn columnar_value_to_boolean_array(
    col_val: ColumnarValue,
    num_rows: usize,
) -> Result<BooleanArray> {
    let column = col_val
        .into_array(num_rows)
        .context(error::EvaluateRecordBatchSnafu)?;
    let array = column
        .as_any()
        .downcast_ref::<BooleanArray>()
        .with_context(|| error::UnexpectedColumnTypeSnafu {
            data_type: column.data_type().clone(),
        })?;
    Ok(array.clone())
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

    fn split_record_batch(
        &self,
        record_batch: &RecordBatch,
    ) -> Result<HashMap<RegionNumber, RegionMask>> {
        self.split_record_batch(record_batch)
    }
}

#[cfg(test)]
mod tests {
    use std::assert_matches::assert_matches;

    use super::*;
    use crate::error::{self, Error};
    use crate::expr::col;

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
            true,
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
            true,
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
            true,
        );

        // check rule
        assert_matches!(rule.unwrap_err(), Error::InvalidExpr { .. });
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
            true,
        );

        // check rule
        assert_matches!(rule.unwrap_err(), Error::CheckpointNotCovered { .. });
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
            true,
        );

        // check rule
        assert_matches!(rule.unwrap_err(), Error::CheckpointNotCovered { .. });
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
            true,
        );

        // check rule
        assert_matches!(rule.unwrap_err(), Error::CheckpointOverlapped { .. });
    }

    #[test]
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
            true,
        );

        // check rule
        assert_matches!(rule.unwrap_err(), Error::CheckpointOverlapped { .. });
    }

    /// ```ignore
    /// value
    ///                                 │
    ///                                 │
    ///    value=10 --------------------│
    ///                                 │
    /// ────────────────────────────────┼──► host
    ///                                 │
    ///                             host=server10
    /// ```
    #[test]
    fn test_partial_divided() {
        let _rule = MultiDimPartitionRule::try_new(
            vec!["host".to_string(), "value".to_string()],
            vec![0, 1, 2, 3],
            vec![
                col("host")
                    .lt(Value::String("server10".into()))
                    .and(col("value").lt(Value::Int64(10))),
                col("host")
                    .lt(Value::String("server10".into()))
                    .and(col("value").gt_eq(Value::Int64(10))),
                col("host").gt_eq(Value::String("server10".into())),
            ],
            true,
        )
        .unwrap();
    }
}

#[cfg(test)]
mod test_split_record_batch {
    use std::sync::Arc;

    use datafusion_common::ScalarValue;
    use datatypes::arrow::array::{Int64Array, StringArray};
    use datatypes::arrow::datatypes::{DataType, Field, Schema};
    use datatypes::arrow::record_batch::RecordBatch;
    use rand::Rng;

    use super::*;
    use crate::expr::{Operand, col};

    fn test_schema() -> Arc<Schema> {
        Arc::new(Schema::new(vec![
            Field::new("host", DataType::Utf8, false),
            Field::new("value", DataType::Int64, false),
        ]))
    }

    fn generate_random_record_batch(num_rows: usize) -> RecordBatch {
        let schema = test_schema();
        let mut rng = rand::thread_rng();
        let mut host_array = Vec::with_capacity(num_rows);
        let mut value_array = Vec::with_capacity(num_rows);
        for _ in 0..num_rows {
            host_array.push(format!("server{}", rng.gen_range(0..20)));
            value_array.push(rng.gen_range(0..20));
        }
        let host_array = StringArray::from(host_array);
        let value_array = Int64Array::from(value_array);
        RecordBatch::try_new(schema, vec![Arc::new(host_array), Arc::new(value_array)]).unwrap()
    }

    #[test]
    fn test_split_record_batch_by_one_column() {
        // Create a simple MultiDimPartitionRule
        let rule = MultiDimPartitionRule::try_new(
            vec!["host".to_string(), "value".to_string()],
            vec![0, 1],
            vec![
                col("host").lt(Value::String("server1".into())),
                col("host").gt_eq(Value::String("server1".into())),
            ],
            true,
        )
        .unwrap();

        let batch = generate_random_record_batch(1000);
        // Split the batch
        let result = rule.split_record_batch(&batch).unwrap();
        let expected = rule.split_record_batch_naive(&batch).unwrap();
        assert_eq!(result.len(), expected.len());
        for (region, value) in &result {
            assert_eq!(
                value.array(),
                expected.get(region).unwrap(),
                "failed on region: {}",
                region
            );
        }
    }

    #[test]
    fn test_split_record_batch_empty() {
        // Create a simple MultiDimPartitionRule
        let rule = MultiDimPartitionRule::try_new(
            vec!["host".to_string()],
            vec![1],
            vec![
                col("host").lt(Value::String("server1".into())),
                col("host").gt_eq(Value::String("server1".into())),
            ],
            true,
        )
        .unwrap();

        let schema = test_schema();
        let host_array = StringArray::from(Vec::<&str>::new());
        let value_array = Int64Array::from(Vec::<i64>::new());
        let batch = RecordBatch::try_new(schema, vec![Arc::new(host_array), Arc::new(value_array)])
            .unwrap();

        let result = rule.split_record_batch(&batch).unwrap();
        assert_eq!(result.len(), 1);
    }

    #[test]
    fn test_split_record_batch_by_two_columns() {
        let rule = MultiDimPartitionRule::try_new(
            vec!["host".to_string(), "value".to_string()],
            vec![0, 1, 2, 3],
            vec![
                col("host")
                    .lt(Value::String("server10".into()))
                    .and(col("value").lt(Value::Int64(10))),
                col("host")
                    .lt(Value::String("server10".into()))
                    .and(col("value").gt_eq(Value::Int64(10))),
                col("host")
                    .gt_eq(Value::String("server10".into()))
                    .and(col("value").lt(Value::Int64(10))),
                col("host")
                    .gt_eq(Value::String("server10".into()))
                    .and(col("value").gt_eq(Value::Int64(10))),
            ],
            true,
        )
        .unwrap();

        let batch = generate_random_record_batch(1000);
        let result = rule.split_record_batch(&batch).unwrap();
        let expected = rule.split_record_batch_naive(&batch).unwrap();
        assert_eq!(result.len(), expected.len());
        for (region, value) in &result {
            assert_eq!(value.array(), expected.get(region).unwrap());
        }
    }

    #[test]
    fn test_all_rows_selected() {
        // Test the fast path where all rows are selected by some partition
        let rule = MultiDimPartitionRule::try_new(
            vec!["value".to_string()],
            vec![1, 2],
            vec![
                col("value").lt(Value::Int64(30)),
                col("value").gt_eq(Value::Int64(30)),
            ],
            true,
        )
        .unwrap();

        let schema = test_schema();
        let host_array = StringArray::from(vec!["server1", "server2", "server3", "server4"]);
        let value_array = Int64Array::from(vec![10, 20, 30, 40]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(host_array), Arc::new(value_array)])
            .unwrap();

        let result = rule.split_record_batch(&batch).unwrap();

        // Check that we have 2 regions and no default region
        assert_eq!(result.len(), 2);
        assert!(result.contains_key(&1));
        assert!(result.contains_key(&2));

        // Verify each region has the correct number of rows
        assert_eq!(result.get(&1).unwrap().selected_rows(), 2); // values < 30
        assert_eq!(result.get(&2).unwrap().selected_rows(), 2); // values >= 30
    }

    #[test]
    fn test_split_record_batch_with_scalar_predicate() {
        // Ensure split handles conjunctive/disjunctive predicates on the same column.
        let rule = MultiDimPartitionRule::try_new(
            vec!["host".to_string()],
            vec![0, 1],
            vec![
                PartitionExpr::new(
                    Operand::Column("host".to_string()),
                    RestrictedOp::Lt,
                    Operand::Value(Value::String("never_happen_1".into())),
                ),
                PartitionExpr::new(
                    Operand::Expr(PartitionExpr::new(
                        Operand::Column("host".to_string()),
                        RestrictedOp::GtEq,
                        Operand::Value(Value::String("never_happen_1".into())),
                    )),
                    RestrictedOp::And,
                    Operand::Value(Value::Boolean(false)),
                ),
            ],
            false,
        )
        .unwrap();

        let batch = generate_random_record_batch(8);
        let result = rule.split_record_batch(&batch).unwrap();

        assert_eq!(result.len(), 1);
        assert!(result.contains_key(&0));

        let total_rows = result.get(&0).unwrap().selected_rows();
        assert_eq!(total_rows, batch.num_rows());
    }

    #[test]
    fn test_columnar_value_to_boolean_array_scalar_false() {
        let result = columnar_value_to_boolean_array(
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(false))),
            4,
        )
        .unwrap();
        assert_eq!(result.len(), 4);
        assert_eq!(result.true_count(), 0);
    }

    #[test]
    fn test_columnar_value_to_boolean_array_scalar_true() {
        let result = columnar_value_to_boolean_array(
            ColumnarValue::Scalar(ScalarValue::Boolean(Some(true))),
            4,
        )
        .unwrap();
        assert_eq!(result.len(), 4);
        assert_eq!(result.true_count(), 4);
    }
}
