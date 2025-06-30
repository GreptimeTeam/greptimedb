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

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::ops::Deref;
use std::sync::Arc;

use datatypes::arrow::array::{BooleanArray, Float64Builder, RecordBatch};
use datatypes::arrow::datatypes::{DataType, Field, Schema};
use datatypes::value::OrderedF64;

use crate::collider::{Collider, CHECK_STEP};
use crate::error::Result;
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

// logic of checking rules
impl<'a> PartitionChecker<'a> {
    fn run(&self) -> Result<()> {
        // sort atomic exprs and check uniqueness
        let mut atomic_exprs = BTreeMap::new();
        for expr in self.collider.atomic_exprs.iter() {
            let key = &expr.nucleons;
            atomic_exprs.insert(key, expr);
        }
        if atomic_exprs.len() != self.collider.atomic_exprs.len() {
            todo!("error: atomic exprs are not unique");
        }

        // TODO(ruihang): merge atomic exprs

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
            todo!("error: matrix fundation is empty");
        }
        let matrix_generator = MatrixGenerator { matrix_fundation };
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
            if true_count != 1 {
                todo!("error: check failed");
            }
        }

        Ok(())
    }
}

struct MatrixGenerator<'a> {
    matrix_fundation: HashMap<&'a str, Vec<OrderedF64>>,
}

impl<'a> MatrixGenerator<'a> {
    fn all_points(&self) -> RecordBatch {
        let mut fields = self
            .matrix_fundation
            .keys()
            .map(|k| Field::new(k.to_string(), DataType::Float64, false))
            .collect::<Vec<_>>();
        fields.sort_unstable();
        let schema = Schema::new(fields.clone());

        // Collect column names and their values in the same order as fields
        let mut columns_data: Vec<(&str, &Vec<OrderedF64>)> = Vec::with_capacity(fields.len());
        for field in &fields {
            let column_name = field.name().as_str();
            columns_data.push((column_name, &self.matrix_fundation[column_name]));
        }

        // Calculate total number of combinations
        let total_combinations: usize = columns_data
            .iter()
            .map(|(_, values)| values.len())
            .product();

        // Create array builders
        let mut array_builders: Vec<Float64Builder> = Vec::with_capacity(fields.len());
        for _ in 0..fields.len() {
            array_builders.push(Float64Builder::with_capacity(total_combinations));
        }

        // Generate all combinations
        for combination_index in 0..total_combinations {
            let mut temp_index = combination_index;

            // For each column, determine which value to use for this combination
            for (col_idx, (_, values)) in columns_data.iter().enumerate() {
                let value_index = temp_index % values.len();
                temp_index /= values.len();

                let value = *values[value_index].as_ref();
                array_builders[col_idx].append_value(value);
            }
        }

        // Finish arrays and create RecordBatch
        let arrays: Vec<_> = array_builders
            .into_iter()
            .map(|mut builder| Arc::new(builder.finish()) as _)
            .collect();

        RecordBatch::try_new(schema.into(), arrays)
            .expect("Failed to create RecordBatch from generated arrays")
    }
}

impl Iterator for MatrixGenerator<'_> {
    type Item = RecordBatch;

    fn next(&mut self) -> Option<Self::Item> {
        todo!()
    }
}
