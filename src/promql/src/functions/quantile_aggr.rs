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

use std::sync::Arc;

use datafusion::arrow::array::{ArrayRef, AsArray};
use datafusion::common::cast::{as_list_array, as_primitive_array, as_struct_array};
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::{Accumulator as DfAccumulator, AggregateUDF, Volatility};
use datafusion::prelude::create_udaf;
use datafusion_common::ScalarValue;
use datatypes::arrow::array::{Float64Array, ListArray, StructArray};
use datatypes::arrow::datatypes::{DataType, Field, Float64Type};

use crate::functions::quantile::quantile_impl;

const FN_NAME: &str = "quantile";

const Q_FIELD_NAME: &str = "q";
const HEAP_FIELD_NAME: &str = "heap";
const DEFAULT_LIST_FIELD_NAME: &str = "item";

#[derive(Debug, Default)]
pub struct QuantileAccumulator {
    q: f64,
    values: Vec<Option<f64>>,
}

/// Create a quantile `AggregateUDF`.
pub fn quantile_udaf() -> Arc<AggregateUDF> {
    Arc::new(create_udaf(
        FN_NAME,
        // Input type: (q, values)
        vec![DataType::Float64, DataType::Float64],
        // Output type: the φ-quantile
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        // Create the accumulator
        Arc::new(|_| Ok(Box::new(QuantileAccumulator::new()))),
        // Intermediate state types
        Arc::new(vec![DataType::Struct(
            vec![
                Field::new(Q_FIELD_NAME, DataType::Float64, false),
                Field::new(
                    HEAP_FIELD_NAME,
                    DataType::List(Arc::new(Field::new(
                        DEFAULT_LIST_FIELD_NAME,
                        DataType::Float64,
                        true,
                    ))),
                    false,
                ),
            ]
            .into(),
        )]),
    ))
}

impl QuantileAccumulator {
    pub fn new() -> Self {
        Self::default()
    }
}

impl DfAccumulator for QuantileAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> datafusion::error::Result<()> {
        self.q = values[0]
            .as_primitive::<Float64Type>()
            .iter()
            .flatten()
            .next()
            .unwrap();

        let f64_array = values[1].as_primitive::<Float64Type>();

        self.values.extend(f64_array);

        Ok(())
    }

    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        let values: Vec<_> = self.values.iter().map(|v| v.unwrap_or(0.0)).collect();

        let result = quantile_impl(&values, self.q);

        ScalarValue::new_primitive::<Float64Type>(result, &DataType::Float64)
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(&self.q) + std::mem::size_of_val(&self.values)
    }

    fn state(&mut self) -> datafusion::error::Result<Vec<ScalarValue>> {
        let q_array = Arc::new(Float64Array::from_iter([self.q]));
        let heap_array = Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(vec![
            Some(self.values.clone()),
        ]));

        let state_struct = StructArray::new(
            vec![
                Field::new(Q_FIELD_NAME, DataType::Float64, false),
                Field::new(
                    HEAP_FIELD_NAME,
                    DataType::List(Arc::new(Field::new(
                        DEFAULT_LIST_FIELD_NAME,
                        DataType::Float64,
                        true,
                    ))),
                    false,
                ),
            ]
            .into(),
            vec![q_array, heap_array],
            None,
        );

        Ok(vec![ScalarValue::Struct(Arc::new(state_struct))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> datafusion::error::Result<()> {
        if states.len() != 1 {
            return Err(DataFusionError::Internal(format!(
                "Expected 1 states for quantile, got {}",
                states.len()
            )));
        }

        for state in states {
            let state = as_struct_array(state)?;
            let q_array = as_primitive_array::<Float64Type>(state.column(0))?.clone();
            let heap_list = as_list_array(state.column(1))?.value(0);
            let f64_array = as_primitive_array::<Float64Type>(&heap_list)?.clone();

            self.q = q_array.iter().flatten().next().unwrap();

            self.values.extend(&f64_array);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    // TODO
}
