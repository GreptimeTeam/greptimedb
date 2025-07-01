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

use std::collections::{BTreeMap, HashMap};
use std::sync::Arc;

use datatypes::arrow::array::{BooleanArray, Float64Array, Float64Builder, RecordBatch};
use datatypes::arrow::datatypes::{DataType, Field, Schema};
use datatypes::value::OrderedF64;

use crate::collider::{Collider, CHECK_STEP, NORMALIZE_STEP};
use crate::error::{
    CheckpointNotCoveredSnafu, CheckpointOverlappedSnafu, DuplicateExprSnafu, Result,
    UnexpectedSnafu,
};
use crate::expr::{PartitionExpr, RestrictedOp};
use crate::multi_dim::MultiDimPartitionRule;

pub struct PartitionChecker<'a> {
    rule: &'a MultiDimPartitionRule,
    collider: Collider<'a>,
}

impl<'a> PartitionChecker<'a> {
    pub fn try_new(rule: &'a MultiDimPartitionRule) -> Result<Self> {
        let collider = Collider::new(rule.exprs())?;
        Ok(Self { rule, collider })
    }

    pub fn check(&self) -> Result<()> {
        self.run()?;
        Ok(())
    }
}

// Logic of checking rules
impl<'a> PartitionChecker<'a> {
    fn run(&self) -> Result<()> {
        // Sort atomic exprs and check uniqueness
        let mut atomic_exprs = BTreeMap::new();
        for expr in self.collider.atomic_exprs.iter() {
            let key = &expr.nucleons;
            atomic_exprs.insert(key, expr);
        }
        if atomic_exprs.len() != self.collider.atomic_exprs.len() {
            // Find the duplication for error message
            for expr in self.collider.atomic_exprs.iter() {
                if atomic_exprs.get(&expr.nucleons).unwrap().source_expr_index
                    != expr.source_expr_index
                {
                    let expr = self.rule.exprs()[expr.source_expr_index].clone();
                    return DuplicateExprSnafu { expr }.fail();
                }
            }
            // Or return a placeholder. This should never happen.
            return DuplicateExprSnafu {
                expr: PartitionExpr::new(
                    crate::expr::Operand::Column("unknown".to_string()),
                    RestrictedOp::Eq,
                    crate::expr::Operand::Column("expr".to_string()),
                ),
            }
            .fail();
        }

        // TODO(ruihang): merge atomic exprs to improve checker's performance

        // matrix test
        let mut matrix_fundation = HashMap::new();
        for (col, values) in self.collider.normalized_values.iter() {
            if values.is_empty() {
                continue;
            }

            let mut cornerstones = Vec::with_capacity(values.len() * 2 + 1);
            cornerstones.push(values[0].1 - CHECK_STEP);
            for value in values {
                cornerstones.push(value.1);
                cornerstones.push(value.1 + CHECK_STEP);
            }
            matrix_fundation.insert(col.as_str(), cornerstones);
        }
        if matrix_fundation.is_empty() {
            return UnexpectedSnafu {
                err_msg: "no valid values for partition".to_string(),
            }
            .fail();
        }
        let matrix_generator = MatrixGenerator::new(matrix_fundation);
        let batch = matrix_generator.all_points();
        let mut results = Vec::with_capacity(self.collider.atomic_exprs.len());
        for expr in self.collider.atomic_exprs.iter() {
            let physical_expr = expr.to_physical_expr(&batch.schema());
            let columnar_result = physical_expr.evaluate(&batch).unwrap();
            let array_result = columnar_result.into_array(batch.num_rows()).unwrap();
            results.push(array_result);
        }
        let boolean_results = results
            .iter()
            .map(|result| result.as_any().downcast_ref::<BooleanArray>().unwrap())
            .collect::<Vec<_>>();

        // dot product and check results
        for i in 0..batch.num_rows() {
            let mut true_count = 0;
            for result in boolean_results.iter() {
                if result.value(i) {
                    true_count += 1;
                }
            }

            if true_count == 0 {
                return CheckpointNotCoveredSnafu {
                    checkpoint: self.remap_checkpoint(i, &batch),
                }
                .fail();
            } else if true_count > 1 {
                return CheckpointOverlappedSnafu {
                    checkpoint: self.remap_checkpoint(i, &batch),
                }
                .fail();
            }
        }

        Ok(())
    }

    /// Remap the normalized checkpoint data to the original values.
    fn remap_checkpoint(&self, i: usize, batch: &RecordBatch) -> String {
        let normalized_row = batch
            .columns()
            .iter()
            .map(|col| {
                let array = col.as_any().downcast_ref::<Float64Array>().unwrap();
                array.value(i)
            })
            .collect::<Vec<_>>();

        let mut check_point = String::new();
        let schema = batch.schema();
        for (col_index, normalized_value) in normalized_row.iter().enumerate() {
            let col_name = schema.field(col_index).name();

            if col_index > 0 {
                check_point.push_str(", ");
            }

            // Check if point is on NORMALIZE_STEP or between steps
            if let Some(values) = self.collider.normalized_values.get(col_name) {
                let normalize_step = NORMALIZE_STEP.0;

                // Check if the normalized value is on a NORMALIZE_STEP boundary
                let remainder = normalized_value % normalize_step;
                let is_on_step = remainder.abs() < f64::EPSILON
                    || (normalize_step - remainder).abs() < f64::EPSILON * 2.0;

                if is_on_step {
                    let index = (normalized_value / normalize_step).round() as usize;
                    if index < values.len() {
                        let original_value = &values[index].0;
                        check_point.push_str(&format!("{}={}", col_name, original_value));
                    } else {
                        check_point.push_str(&format!("{}=unknown", col_name));
                    }
                } else {
                    let lower_index = (normalized_value / normalize_step).floor() as usize;
                    let upper_index = (normalized_value / normalize_step).ceil() as usize;

                    println!("lower_index: {}, upper_index: {}", lower_index, upper_index);

                    // Handle edge cases: value is outside the valid range
                    if lower_index == upper_index && lower_index == 0 {
                        // Value is less than the first value
                        let first_original = &values[0].0;
                        check_point.push_str(&format!("{}<{}", col_name, first_original));
                    } else if upper_index == values.len() {
                        // Value is greater than the last value
                        let last_original = &values[values.len() - 1].0;
                        check_point.push_str(&format!("{}>{}", col_name, last_original));
                    } else {
                        // Normal case: value is between two valid values
                        let lower_original = if lower_index < values.len() {
                            values[lower_index].0.to_string()
                        } else {
                            "unknown".to_string()
                        };

                        let upper_original = if upper_index < values.len() {
                            values[upper_index].0.to_string()
                        } else {
                            "unknown".to_string()
                        };

                        check_point.push_str(&format!(
                            "{}<{}<{}",
                            lower_original, col_name, upper_original
                        ));
                    }
                }
            } else {
                // Fallback if column not found in normalized values
                check_point.push_str(&format!("{}:unknown", col_name));
            }
        }

        check_point
    }
}

struct MatrixGenerator {
    matrix_fundation: HashMap<String, Vec<OrderedF64>>,
    // Iterator state
    current_index: usize,
    total_combinations: usize,
    schema: Schema,
    column_names: Vec<String>,
}

const MAX_BATCH_SIZE: usize = 8192;

impl MatrixGenerator {
    fn new(matrix_fundation: HashMap<&str, Vec<OrderedF64>>) -> Self {
        // Convert to owned HashMap to avoid lifetime issues
        let owned_matrix_fundation: HashMap<String, Vec<OrderedF64>> = matrix_fundation
            .into_iter()
            .map(|(k, v)| (k.to_string(), v))
            .collect();

        let mut fields = owned_matrix_fundation
            .keys()
            .map(|k| Field::new(k.clone(), DataType::Float64, false))
            .collect::<Vec<_>>();
        fields.sort_unstable();
        let schema = Schema::new(fields.clone());

        // Store column names in the same order as fields
        let column_names: Vec<String> = fields.iter().map(|field| field.name().clone()).collect();

        // Calculate total number of combinations
        let total_combinations: usize = column_names
            .iter()
            .map(|col_name| owned_matrix_fundation[col_name].len())
            .product();

        Self {
            matrix_fundation: owned_matrix_fundation,
            current_index: 0,
            total_combinations,
            schema,
            column_names,
        }
    }

    fn all_points(&self) -> RecordBatch {
        // For backward compatibility, generate all points at once
        // This method is now deprecated in favor of using the iterator
        self.generate_batch(0, self.total_combinations)
    }

    fn generate_batch(&self, start_index: usize, batch_size: usize) -> RecordBatch {
        let actual_batch_size = batch_size.min(self.total_combinations - start_index);

        // Create array builders
        let mut array_builders: Vec<Float64Builder> = Vec::with_capacity(self.column_names.len());
        for _ in 0..self.column_names.len() {
            array_builders.push(Float64Builder::with_capacity(actual_batch_size));
        }

        // Pre-calculate the sizes for each column
        let column_sizes: Vec<usize> = self
            .column_names
            .iter()
            .map(|col_name| self.matrix_fundation[col_name].len())
            .collect();

        // Generate combinations for this batch
        for combination_offset in 0..actual_batch_size {
            let combination_index = start_index + combination_offset;

            // For each column, determine which value to use for this combination
            for (col_idx, col_name) in self.column_names.iter().enumerate() {
                let values = &self.matrix_fundation[col_name];

                // Calculate the "stride" - product of sizes of all columns after this one
                let stride: usize = column_sizes[col_idx + 1..].iter().product();

                // Calculate which value to use for this column
                let value_index = (combination_index / stride) % values.len();

                let value = *values[value_index].as_ref();
                array_builders[col_idx].append_value(value);
            }
        }

        // Finish arrays and create RecordBatch
        let arrays: Vec<_> = array_builders
            .into_iter()
            .map(|mut builder| Arc::new(builder.finish()) as _)
            .collect();

        RecordBatch::try_new(Arc::new(self.schema.clone()), arrays)
            .expect("Failed to create RecordBatch from generated arrays")
    }
}

impl Iterator for MatrixGenerator {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        if self.current_index >= self.total_combinations {
            return None;
        }

        let remaining = self.total_combinations - self.current_index;
        let batch_size = remaining.min(MAX_BATCH_SIZE);

        let batch = self.generate_batch(self.current_index, batch_size);
        self.current_index += batch_size;

        Some(batch)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;

    use datatypes::value::Value;

    use super::*;
    use crate::expr::col;
    use crate::multi_dim::MultiDimPartitionRule;

    #[test]
    fn test_matrix_generator_single_column() {
        let mut matrix_fundation = HashMap::new();
        matrix_fundation.insert(
            "col1",
            vec![
                OrderedF64::from(1.0),
                OrderedF64::from(2.0),
                OrderedF64::from(3.0),
            ],
        );

        let generator = MatrixGenerator::new(matrix_fundation);
        let batch = generator.all_points();

        assert_eq!(batch.num_rows(), 3);
        assert_eq!(batch.num_columns(), 1);
        assert_eq!(batch.schema().field(0).name(), "col1");

        let col1_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<datatypes::arrow::array::Float64Array>()
            .unwrap();
        assert_eq!(col1_array.value(0), 1.0);
        assert_eq!(col1_array.value(1), 2.0);
        assert_eq!(col1_array.value(2), 3.0);
    }

    #[test]
    fn test_matrix_generator_three_columns_cartesian_product() {
        let mut matrix_fundation = HashMap::new();
        matrix_fundation.insert("a", vec![OrderedF64::from(1.0), OrderedF64::from(2.0)]);
        matrix_fundation.insert("b", vec![OrderedF64::from(10.0), OrderedF64::from(20.0)]);
        matrix_fundation.insert(
            "c",
            vec![
                OrderedF64::from(100.0),
                OrderedF64::from(200.0),
                OrderedF64::from(300.0),
            ],
        );

        let generator = MatrixGenerator::new(matrix_fundation);
        let batch = generator.all_points();

        // Should have 2 * 2 * 3 = 12 combinations
        assert_eq!(batch.num_rows(), 12);
        assert_eq!(batch.num_columns(), 3);

        let a_array = batch
            .column(0)
            .as_any()
            .downcast_ref::<datatypes::arrow::array::Float64Array>()
            .unwrap();
        let b_array = batch
            .column(1)
            .as_any()
            .downcast_ref::<datatypes::arrow::array::Float64Array>()
            .unwrap();
        let c_array = batch
            .column(2)
            .as_any()
            .downcast_ref::<datatypes::arrow::array::Float64Array>()
            .unwrap();

        // Verify first few combinations (a changes slowest, c changes fastest)
        let expected = vec![
            (1.0, 10.0, 100.0),
            (1.0, 10.0, 200.0),
            (1.0, 10.0, 300.0),
            (1.0, 20.0, 100.0),
            (1.0, 20.0, 200.0),
            (1.0, 20.0, 300.0),
            (2.0, 10.0, 100.0),
            (2.0, 10.0, 200.0),
            (2.0, 10.0, 300.0),
            (2.0, 20.0, 100.0),
            (2.0, 20.0, 200.0),
            (2.0, 20.0, 300.0),
        ];
        for i in 0..batch.num_rows() {
            assert_eq!(
                (a_array.value(i), b_array.value(i), c_array.value(i)),
                expected[i]
            );
        }
    }

    #[test]
    fn test_matrix_generator_iterator_small_batches() {
        let mut matrix_fundation = HashMap::new();
        matrix_fundation.insert("col1", vec![OrderedF64::from(1.0), OrderedF64::from(2.0)]);
        matrix_fundation.insert(
            "col2",
            vec![
                OrderedF64::from(10.0),
                OrderedF64::from(20.0),
                OrderedF64::from(30.0),
            ],
        );

        let mut generator = MatrixGenerator::new(matrix_fundation);

        // Total combinations should be 2 * 3 = 6
        assert_eq!(generator.total_combinations, 6);

        let mut total_rows = 0;

        while let Some(batch) = generator.next() {
            total_rows += batch.num_rows();
            assert_eq!(batch.num_columns(), 2);

            // Verify each batch is valid
            assert!(batch.num_rows() > 0);
            assert!(batch.num_rows() <= MAX_BATCH_SIZE);
        }

        assert_eq!(total_rows, 6);
    }

    #[test]
    fn test_matrix_generator_empty_column_values() {
        let mut matrix_fundation = HashMap::new();
        matrix_fundation.insert("col1", vec![]);

        let generator = MatrixGenerator::new(matrix_fundation);

        // Should have 0 total combinations when any column is empty
        assert_eq!(generator.total_combinations, 0);

        let batch = generator.all_points();
        assert_eq!(batch.num_rows(), 0);
    }

    #[test]
    fn test_matrix_generator_large_dataset_batching() {
        // Create a dataset that will exceed MAX_BATCH_SIZE (8192)
        // 20 * 20 * 21 = 8400 > 8192
        let mut matrix_fundation = HashMap::new();

        let values1: Vec<OrderedF64> = (0..20).map(|i| OrderedF64::from(i as f64)).collect();
        let values2: Vec<OrderedF64> = (0..20)
            .map(|i| OrderedF64::from(i as f64 + 100.0))
            .collect();
        let values3: Vec<OrderedF64> = (0..21)
            .map(|i| OrderedF64::from(i as f64 + 1000.0))
            .collect();

        matrix_fundation.insert("col1", values1);
        matrix_fundation.insert("col2", values2);
        matrix_fundation.insert("col3", values3);

        let mut generator = MatrixGenerator::new(matrix_fundation);

        assert_eq!(generator.total_combinations, 8400);

        let mut total_rows = 0;
        let mut batch_count = 0;
        let mut first_batch_size = None;

        while let Some(batch) = generator.next() {
            batch_count += 1;
            let batch_size = batch.num_rows();
            total_rows += batch_size;

            if first_batch_size.is_none() {
                first_batch_size = Some(batch_size);
            }

            // Each batch should not exceed MAX_BATCH_SIZE
            assert!(batch_size <= MAX_BATCH_SIZE);
            assert_eq!(batch.num_columns(), 3);
        }

        assert_eq!(total_rows, 8400);
        assert!(batch_count > 1);
        assert_eq!(first_batch_size.unwrap(), MAX_BATCH_SIZE);
    }

    #[test]
    fn test_remap_checkpoint_single_column_exact_values() {
        // Create rule with single column
        let rule = MultiDimPartitionRule::try_new(
            vec!["host".to_string(), "value".to_string()],
            vec![1, 2, 3],
            vec![
                col("host").eq(Value::Int64(0)),
                col("value").eq(Value::Int64(0)),
                col("host").eq(Value::Int64(1)),
                col("value").eq(Value::Int64(1)),
                col("host").eq(Value::Int64(2)),
                col("value").eq(Value::Int64(2)),
                col("host").eq(Value::Int64(3)),
                col("value").eq(Value::Int64(3)),
            ],
        )
        .unwrap();
        let checker = PartitionChecker::try_new(&rule).unwrap();

        let schema = Arc::new(Schema::new(vec![
            Field::new("host", DataType::Float64, false),
            Field::new("value", DataType::Float64, false),
        ]));
        let host_array = Float64Array::from(vec![-0.5, 0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5]);
        let value_array = Float64Array::from(vec![-0.5, 0.0, 0.5, 1.0, 1.5, 2.0, 2.5, 3.0, 3.5]);
        let batch = RecordBatch::try_new(schema, vec![Arc::new(host_array), Arc::new(value_array)])
            .unwrap();

        let checkpoint = checker.remap_checkpoint(0, &batch);
        assert_eq!(checkpoint, "host<0, value<0");
        let checkpoint = checker.remap_checkpoint(1, &batch);
        assert_eq!(checkpoint, "host=0, value=0");
        let checkpoint = checker.remap_checkpoint(6, &batch);
        assert_eq!(checkpoint, "2<host<3, 2<value<3");
        let checkpoint = checker.remap_checkpoint(7, &batch);
        assert_eq!(checkpoint, "host=3, value=3");
        let checkpoint = checker.remap_checkpoint(8, &batch);
        assert_eq!(checkpoint, "host>3, value>3");
    }
}
