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
                let index = self
                    .name_to_index
                    .get(name)
                    .context(UndefinedColumnSnafu { column: name })?;
                let l = &values[*index];
                Self::perform_op(l, &expr.op, r)
            }
            (Operand::Value(l), Operand::Column(name)) => {
                let index = self
                    .name_to_index
                    .get(name)
                    .context(UndefinedColumnSnafu { column: name })?;
                let r = &values[*index];
                Self::perform_op(l, &expr.op, r)
            }
            (operand @ Operand::Function { .. }, Operand::Value(r)) => {
                Self::perform_op(&self.evaluate_operand(operand, values)?, &expr.op, r)
            }
            (Operand::Value(l), operand @ Operand::Function { .. }) => {
                Self::perform_op(l, &expr.op, &self.evaluate_operand(operand, values)?)
            }
            (Operand::Expr(lhs), Operand::Expr(rhs)) => {
                let lhs = self.evaluate_expr(lhs, values)?;
                match expr.op {
                    RestrictedOp::And => Ok(lhs && self.evaluate_expr(rhs, values)?),
                    RestrictedOp::Or => Ok(lhs || self.evaluate_expr(rhs, values)?),
                    _ => unreachable!(),
                }
            }
            _ => unreachable!(),
        }
    }

    fn evaluate_operand(&self, operand: &Operand, values: &[Value]) -> Result<Value> {
        match operand {
            Operand::Column(name) => {
                let index = self
                    .name_to_index
                    .get(name)
                    .context(UndefinedColumnSnafu { column: name })?;
                Ok(values[*index].clone())
            }
            Operand::Value(value) => Ok(value.clone()),
            Operand::Function { function, args } => {
                let args = args
                    .iter()
                    .map(|arg| self.evaluate_operand(arg, values))
                    .collect::<Result<Vec<_>>>()?;
                function
                    .evaluate(&args)
                    .context(error::EvaluatePartitionFunctionSnafu)
            }
            Operand::Expr(_) => error::NoExprOperandSnafu {
                operand: operand.clone(),
            }
            .fail(),
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
        let has_functions = self.exprs.iter().any(PartitionExpr::contains_function);
        // Functions can reject row values, even when every valid row belongs
        // to the same region.
        if self.regions.len() == 1 && !has_functions {
            return Ok([(
                self.regions[0],
                RegionMask::from(BooleanArray::from(vec![true; num_rows])),
            )]
            .into_iter()
            .collect());
        }
        // Short-circuit selection must not copy unrelated payload columns.
        let projected;
        let record_batch = if has_functions {
            let schema = record_batch.schema();
            let indices = self
                .partition_columns
                .iter()
                .map(|name| schema.index_of(name))
                .collect::<std::result::Result<Vec<_>, _>>()
                .context(error::ComputeArrowKernelSnafu)?;
            projected = record_batch
                .project(&indices)
                .context(error::ComputeArrowKernelSnafu)?;
            &projected
        } else {
            record_batch
        };
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

        let mut result = HashMap::new();
        // Match row routing: once a row has a region, later predicates must not
        // evaluate fallible functions for that row.
        let mut remaining =
            has_functions.then(|| BooleanArray::new(BooleanBuffer::new_set(num_rows), None));
        // TODO(dennis): Reuse identical function results across partition predicates.
        for ((expr, region_num), partition_expr) in physical_exprs
            .iter()
            .zip(self.regions.iter())
            .zip(self.exprs.iter())
        {
            let evaluated = if let Some(remaining) = &remaining {
                if remaining.true_count() == 0 {
                    break;
                }
                expr.evaluate_selection(record_batch, remaining)
            } else {
                expr.evaluate(record_batch)
            };
            let col_val = match evaluated {
                Ok(value) => value,
                Err(err) if partition_expr.contains_function() => {
                    return Err(err).context(error::EvaluatePartitionFunctionSnafu);
                }
                Err(err) => return Err(err).context(error::EvaluateRecordBatchSnafu),
            };
            let mut array = columnar_value_to_boolean_array(col_val, num_rows)?;
            if let Some(remaining) = &remaining {
                // evaluate_selection scatters NULL into rows already assigned.
                array = arrow::compute::and_kleene(remaining, &array)
                    .context(error::ComputeArrowKernelSnafu)?;
            }
            let selected_rows = array.true_count();
            if selected_rows == 0 {
                continue;
            }
            if let Some(remaining) = &mut remaining {
                *remaining = arrow::compute::and_not(remaining, &array)
                    .context(error::ComputeArrowKernelSnafu)?;
            }
            result.insert(*region_num, RegionMask::new(array, selected_rows));
        }

        let unselected = if let Some(remaining) = remaining {
            if remaining.true_count() == 0 {
                return Ok(result);
            }
            remaining
        } else {
            let selected = if result.len() == 1 {
                result.values().next().unwrap().array().clone()
            } else {
                let mut selected = BooleanArray::new(BooleanBuffer::new_unset(num_rows), None);
                for region_mask in result.values() {
                    selected = arrow::compute::or(&selected, region_mask.array())
                        .context(error::ComputeArrowKernelSnafu)?;
                }
                selected
            };
            if selected.true_count() == num_rows {
                return Ok(result);
            }
            arrow::compute::not(&selected).context(error::ComputeArrowKernelSnafu)?
        };

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

    fn partition_columns(&self) -> &[String] {
        &self.partition_columns
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
    use std::assert_matches;

    use super::*;
    use crate::error::{self, Error};
    use crate::expr::col;

    #[test]
    fn test_comparisons_match_row_evaluation() {
        use datatypes::arrow::array::{Array, StringArray};
        use datatypes::arrow::datatypes::{DataType, Field};

        use crate::function::PartitionFunction;
        let operand = Operand::Function {
            function: PartitionFunction::Substring,
            args: vec![Operand::Column("host".into()), Value::Int64(1).into()],
        };
        let inputs = [
            Value::Null,
            Value::from("a"),
            Value::from("m"),
            Value::from("z"),
        ];
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new("host", DataType::Utf8, true)])),
            vec![Arc::new(StringArray::from(vec![
                None,
                Some("a"),
                Some("m"),
                Some("z"),
            ]))],
        )
        .unwrap();
        for operand in [col("host"), operand] {
            for op in [
                RestrictedOp::Eq,
                RestrictedOp::NotEq,
                RestrictedOp::Lt,
                RestrictedOp::LtEq,
                RestrictedOp::Gt,
                RestrictedOp::GtEq,
            ] {
                for bound in [Value::Null, Value::from("m")] {
                    let expr = PartitionExpr::new(operand.clone(), op.clone(), bound.into());
                    let rule = MultiDimPartitionRule::try_new(
                        vec!["host".into()],
                        vec![1],
                        vec![expr.clone()],
                        false,
                    )
                    .unwrap();
                    let physical = expr
                        .try_as_physical_expr(&batch.schema())
                        .unwrap()
                        .evaluate(&batch)
                        .unwrap();
                    let physical =
                        columnar_value_to_boolean_array(physical, batch.num_rows()).unwrap();
                    assert_eq!(physical.null_count(), 0);
                    for (row, value) in inputs.iter().enumerate() {
                        assert_eq!(
                            rule.evaluate_expr(&expr, std::slice::from_ref(value))
                                .unwrap(),
                            physical.value(row),
                            "{expr}, input={value}"
                        );
                    }
                }
            }
        }
    }

    #[test]
    fn test_function_routing_short_circuit_and_first_match() {
        use common_error::ext::ErrorExt;
        use common_error::status_code::StatusCode;
        use datatypes::arrow::array::{Array, Int64Array, StringArray};
        use datatypes::arrow::datatypes::{DataType, Field};

        use crate::function::PartitionFunction;
        let substring = Operand::Function {
            function: PartitionFunction::Substring,
            args: vec![col("host"), Value::Int64(1).into(), col("length")],
        };
        let lower = col("host").lt(Value::from("m"));
        let upper = col("host").gt_eq(Value::from("m"));
        let short = substring.clone().lt(Value::from("m"));
        let long = substring.gt_eq(Value::from("m"));
        let schema = Arc::new(Schema::new(vec![
            Field::new("length", DataType::Int64, false),
            Field::new("host", DataType::Utf8, true),
        ]));
        for (exprs, expected_a, expected_z) in [
            (
                vec![
                    lower.clone().and(short.clone()),
                    lower.clone().and(long.clone()),
                    upper.clone(),
                ],
                1,
                3,
            ),
            (
                vec![
                    col("host").not_eq(Value::from("z")).and(short.clone()),
                    col("host").not_eq(Value::from("z")).and(long.clone()),
                    col("host").eq(Value::from("z")),
                ],
                1,
                3,
            ),
            // Later predicates start with a fallible function, without a column guard.
            (
                vec![
                    upper.clone(),
                    short.clone().and(lower.clone()),
                    long.clone().and(lower.clone()),
                ],
                2,
                1,
            ),
            (
                vec![
                    PartitionExpr::new(
                        Operand::Expr(upper),
                        RestrictedOp::Or,
                        Operand::Expr(short.and(lower.clone())),
                    ),
                    lower.and(long),
                ],
                1,
                1,
            ),
        ] {
            let rule = MultiDimPartitionRule::try_new(
                vec!["host".into(), "length".into()],
                (1..=exprs.len() as u32).collect(),
                exprs,
                true,
            )
            .unwrap();
            // Exercise uniform batches and both sides of DataFusion's AND/OR
            // pre-selection threshold, including nullable inputs.
            for valid_rows in [0, 1, 5, 9, 10] {
                let mut hosts = vec![Some("a"); valid_rows];
                hosts.extend(vec![Some("z"); 10 - valid_rows]);
                hosts.push(None);
                let mut lengths = vec![1; valid_rows];
                lengths.extend(vec![-1; 11 - valid_rows]);
                let batch = RecordBatch::try_new(
                    schema.clone(),
                    vec![
                        Arc::new(Int64Array::from(lengths.clone())),
                        Arc::new(StringArray::from(hosts.clone())),
                    ],
                )
                .unwrap();
                let masks = rule.split_record_batch(&batch).unwrap();
                for (index, host) in hosts.iter().enumerate() {
                    let expected = if *host == Some("z") {
                        expected_z
                    } else {
                        expected_a
                    };
                    assert_eq!(
                        rule.find_region(&[
                            host.map_or(Value::Null, Value::from),
                            Value::Int64(lengths[index])
                        ])
                        .unwrap(),
                        expected
                    );
                    for (region, mask) in &masks {
                        assert_eq!(mask.array().null_count(), 0);
                        assert_eq!(mask.array().value(index), *region == expected);
                    }
                    assert!(masks[&expected].array().value(index));
                }
            }
            assert!(
                rule.split_record_batch(&RecordBatch::new_empty(schema.clone()))
                    .unwrap()
                    .is_empty()
            );

            let batch = RecordBatch::try_new(
                schema.clone(),
                vec![
                    Arc::new(Int64Array::from(vec![-1, -1])),
                    Arc::new(StringArray::from(vec!["z", "a"])),
                ],
            )
            .unwrap();
            assert_eq!(
                rule.find_region(&[Value::from("a"), Value::Int64(-1)])
                    .unwrap_err()
                    .status_code(),
                StatusCode::InvalidArguments
            );
            assert_eq!(
                rule.split_record_batch(&batch).err().unwrap().status_code(),
                StatusCode::InvalidArguments
            );
        }
    }

    #[test]
    fn test_function_invalid_length_with_null_bound() {
        use common_error::ext::ErrorExt;
        use common_error::status_code::StatusCode;
        use datatypes::arrow::array::{Int64Array, StringArray};
        use datatypes::arrow::datatypes::{DataType, Field};

        use crate::function::PartitionFunction;
        let operand = Operand::Function {
            function: PartitionFunction::Substring,
            args: vec![
                Operand::Column("host".into()),
                Value::Int64(1).into(),
                Operand::Column("length".into()),
            ],
        };
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("host", DataType::Utf8, false),
                Field::new("length", DataType::Int64, false),
            ])),
            vec![
                Arc::new(StringArray::from(vec!["abc"])),
                Arc::new(Int64Array::from(vec![-1])),
            ],
        )
        .unwrap();
        let tautology = PartitionExpr::new(
            Operand::Expr(operand.clone().lt(Value::Null)),
            RestrictedOp::Or,
            Operand::Expr(operand.clone().gt_eq(Value::Null)),
        );
        let single_region = MultiDimPartitionRule::try_new(
            vec!["host".into(), "length".into()],
            vec![1],
            vec![tautology],
            true,
        )
        .unwrap();
        assert_eq!(
            single_region
                .split_record_batch(&batch)
                .err()
                .unwrap()
                .status_code(),
            StatusCode::InvalidArguments
        );
        for op in [RestrictedOp::Lt, RestrictedOp::GtEq] {
            let expr = PartitionExpr::new(operand.clone(), op, Value::Null.into());
            let rule = MultiDimPartitionRule::try_new(
                vec!["host".into(), "length".into()],
                vec![1],
                vec![expr.clone()],
                false,
            )
            .unwrap();
            assert_eq!(
                rule.evaluate_expr(&expr, &[Value::from("abc"), Value::Int64(-1)])
                    .unwrap_err()
                    .status_code(),
                StatusCode::InvalidArguments,
            );
            assert!(
                expr.try_as_physical_expr(&batch.schema())
                    .unwrap()
                    .evaluate(&batch)
                    .is_err()
            );
        }
    }

    #[test]
    fn test_function_partition_dictionary_tags() {
        use datatypes::arrow::array::{Array, StringDictionaryBuilder};
        use datatypes::arrow::datatypes::{Field, UInt32Type};

        use crate::function::PartitionFunction;
        let mut builder = StringDictionaryBuilder::<UInt32Type>::new();
        builder.append("abc").unwrap();
        builder.append("xyz").unwrap();
        builder.append_null();
        let array = Arc::new(builder.finish());
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![Field::new(
                "host",
                array.data_type().clone(),
                true,
            )])),
            vec![array],
        )
        .unwrap();
        for function in [PartitionFunction::Substring, PartitionFunction::Hash] {
            let mut args = vec![Operand::Column("host".into())];
            if function == PartitionFunction::Substring {
                args.push(Value::Int64(1).into());
            }
            let operand = Operand::Function { function, args };
            let exprs = vec![
                operand.clone().lt(Value::from("m")),
                operand.gt_eq(Value::from("m")),
            ];
            let rule = MultiDimPartitionRule::try_new(vec!["host".into()], vec![1, 2], exprs, true)
                .unwrap();
            let naive = rule.split_record_batch_naive(&batch).unwrap();
            for (region, mask) in rule.split_record_batch(&batch).unwrap() {
                assert_eq!(mask.array(), &naive[&region]);
            }
        }
    }

    #[test]
    fn test_function_partition_row_batch_and_restore() {
        use datatypes::arrow::array::StringArray;
        use datatypes::arrow::datatypes::{DataType, Field};

        use crate::function::PartitionFunction;
        let substring = Operand::Function {
            function: PartitionFunction::Substring,
            args: vec![
                Operand::Column("host".into()),
                Value::Int64(1).into(),
                Value::Int64(1).into(),
            ],
        };
        let hash = Operand::Function {
            function: PartitionFunction::Hash,
            args: vec![substring.clone(), Operand::Column("idc".into())],
        };
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("host", DataType::Utf8, true),
                Field::new("idc", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(StringArray::from(vec![
                    Some("abc"),
                    Some("z"),
                    Some("中🙂"),
                    None,
                    Some(""),
                ])),
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("bc"),
                    Some("🙂"),
                    Some("a"),
                    None,
                ])),
            ],
        )
        .unwrap();
        for (operand, boundary) in [(substring, "m"), (hash, "8")] {
            let expressions = [
                operand.clone().lt(Value::from(boundary)),
                operand.gt_eq(Value::from(boundary)),
            ];
            let expressions = expressions
                .iter()
                .map(|expr| {
                    PartitionExpr::from_json_str(&expr.as_json_str().unwrap())
                        .unwrap()
                        .unwrap()
                })
                .collect();
            let rule = MultiDimPartitionRule::try_new(
                vec!["host".into(), "idc".into()],
                vec![1, 2],
                expressions,
                true,
            )
            .unwrap();
            let naive = rule.split_record_batch_naive(&batch).unwrap();
            let physical = rule.split_record_batch(&batch).unwrap();
            assert_eq!(
                physical
                    .values()
                    .map(|mask| mask.array().true_count())
                    .sum::<usize>(),
                batch.num_rows()
            );
            for (region, mask) in physical {
                assert_eq!(mask.array(), &naive[&region]);
            }
        }
    }

    #[test]
    fn test_integer_hash_partition_row_batch_and_restore() {
        use datatypes::arrow::array::{Int64Array, StringArray, UInt64Array};
        use datatypes::arrow::datatypes::{DataType, Field};

        use crate::function::PartitionFunction;
        let hash = Operand::Function {
            function: PartitionFunction::Hash,
            args: ["tenant", "device", "host"]
                .into_iter()
                .map(|name| Operand::Column(name.into()))
                .collect(),
        };
        let expressions = [
            hash.clone().lt(Value::from("8")),
            hash.gt_eq(Value::from("8")),
        ]
        .iter()
        .map(|expr| {
            PartitionExpr::from_json_str(&expr.as_json_str().unwrap())
                .unwrap()
                .unwrap()
        })
        .collect();
        let rule = MultiDimPartitionRule::try_new(
            vec!["tenant".into(), "device".into(), "host".into()],
            vec![1, 2],
            expressions,
            true,
        )
        .unwrap();
        let batch = RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("tenant", DataType::Int64, true),
                Field::new("device", DataType::UInt64, true),
                Field::new("host", DataType::Utf8, true),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![
                    Some(42),
                    Some(-1),
                    Some(i64::MIN),
                    None,
                    Some(42),
                    Some(42),
                ])),
                Arc::new(UInt64Array::from(vec![
                    Some(42),
                    Some(u64::MAX),
                    Some(0),
                    Some(42),
                    None,
                    Some(42),
                ])),
                Arc::new(StringArray::from(vec![
                    Some("a"),
                    Some("b"),
                    Some("中"),
                    Some("a"),
                    Some("a"),
                    None,
                ])),
            ],
        )
        .unwrap();
        let columns = rule.record_batch_to_cols(&batch).unwrap();
        let physical = rule.split_record_batch(&batch).unwrap();
        for (row, expected) in [2, 2, 1, 1, 1, 1].into_iter().enumerate() {
            let values = columns
                .iter()
                .map(|column| column.get(row))
                .collect::<Vec<_>>();
            assert_eq!(rule.find_region(&values).unwrap(), expected);
            for (region, mask) in &physical {
                assert_eq!(mask.array().value(row), *region == expected);
            }
        }
    }

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
    fn test_find_region_rejects_undeclared_column_on_lhs() {
        let rule = MultiDimPartitionRule::try_new(
            vec!["host".to_string()],
            vec![1],
            vec![PartitionExpr::new(
                Operand::Column("rack".to_string()),
                RestrictedOp::Lt,
                Operand::Value(Value::String("n".into())),
            )],
            false,
        )
        .unwrap();

        assert_matches!(
            rule.find_region(&[Value::String("z".into())]),
            Err(Error::UndefinedColumn { column, .. }) if column == "rack"
        );
    }

    #[test]
    fn test_find_region_rejects_undeclared_column_on_rhs() {
        let rule = MultiDimPartitionRule::try_new(
            vec!["host".to_string()],
            vec![1],
            vec![PartitionExpr::new(
                Operand::Value(Value::String("n".into())),
                RestrictedOp::Gt,
                Operand::Column("rack".to_string()),
            )],
            false,
        )
        .unwrap();

        assert_matches!(
            rule.find_region(&[Value::String("z".into())]),
            Err(Error::UndefinedColumn { column, .. }) if column == "rack"
        );
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
