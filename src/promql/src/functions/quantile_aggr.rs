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
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::{Accumulator as DfAccumulator, AggregateUDF, Volatility};
use datafusion::prelude::create_udaf;
use datafusion_common::ScalarValue;
use datatypes::arrow::array::{ListArray, StructArray};
use datatypes::arrow::datatypes::{DataType, Field, Float64Type};

use crate::functions::quantile::quantile_impl;

const QUANTILE_NAME: &str = "quantile";

const VALUES_FIELD_NAME: &str = "values";
const DEFAULT_LIST_FIELD_NAME: &str = "item";

#[derive(Debug, Default)]
pub struct QuantileAccumulator {
    q: f64,
    values: Vec<Option<f64>>,
}

/// Create a quantile `AggregateUDF` for PromQL quantile operator,
/// which calculates φ-quantile (0 ≤ φ ≤ 1) over dimensions
pub fn quantile_udaf(q: f64) -> Arc<AggregateUDF> {
    Arc::new(create_udaf(
        QUANTILE_NAME,
        // Input type: (values)
        vec![DataType::Float64],
        // Output type: the φ-quantile
        Arc::new(DataType::Float64),
        Volatility::Immutable,
        // Create the accumulator
        Arc::new(move |_| Ok(Box::new(QuantileAccumulator::new(q)))),
        // Intermediate state types
        Arc::new(vec![DataType::Struct(
            vec![Field::new(
                VALUES_FIELD_NAME,
                DataType::List(Arc::new(Field::new(
                    DEFAULT_LIST_FIELD_NAME,
                    DataType::Float64,
                    true,
                ))),
                false,
            )]
            .into(),
        )]),
    ))
}

impl QuantileAccumulator {
    pub fn new(q: f64) -> Self {
        Self {
            q,
            ..Default::default()
        }
    }
}

impl DfAccumulator for QuantileAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        let f64_array = values[0].as_primitive::<Float64Type>();

        self.values.extend(f64_array);

        Ok(())
    }

    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        let values: Vec<_> = self.values.iter().map(|v| v.unwrap_or(0.0)).collect();

        let result = quantile_impl(&values, self.q);

        ScalarValue::new_primitive::<Float64Type>(result, &DataType::Float64)
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>() + self.values.capacity() * std::mem::size_of::<Option<f64>>()
    }

    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        let values_array = Arc::new(ListArray::from_iter_primitive::<Float64Type, _, _>(vec![
            Some(self.values.clone()),
        ]));

        let state_struct = StructArray::new(
            vec![Field::new(
                VALUES_FIELD_NAME,
                DataType::List(Arc::new(Field::new(
                    DEFAULT_LIST_FIELD_NAME,
                    DataType::Float64,
                    true,
                ))),
                false,
            )]
            .into(),
            vec![values_array],
            None,
        );

        Ok(vec![ScalarValue::Struct(Arc::new(state_struct))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        if states.is_empty() {
            return Ok(());
        }

        for state in states {
            let state = as_struct_array(state)?;

            for list in as_list_array(state.column(0))?.iter().flatten() {
                let f64_array = as_primitive_array::<Float64Type>(&list)?.clone();
                self.values.extend(&f64_array);
            }
        }

        Ok(())
    }
}
#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{ArrayRef, Float64Array};
    use datafusion_common::ScalarValue;

    use super::*;

    fn create_f64_array(values: Vec<Option<f64>>) -> ArrayRef {
        Arc::new(Float64Array::from(values)) as ArrayRef
    }

    #[test]
    fn test_quantile_accumulator_empty() {
        let mut accumulator = QuantileAccumulator::new(0.5);

        let result = accumulator.evaluate().unwrap();

        match result {
            ScalarValue::Float64(_) => (),
            _ => panic!("Expected Float64 scalar value"),
        }
    }

    #[test]
    fn test_quantile_accumulator_single_value() {
        let mut accumulator = QuantileAccumulator::new(0.5);
        let input = create_f64_array(vec![Some(10.0)]);

        accumulator.update_batch(&[input]).unwrap();
        let result = accumulator.evaluate().unwrap();

        assert_eq!(result, ScalarValue::Float64(Some(10.0)));
    }

    #[test]
    fn test_quantile_accumulator_multiple_values() {
        let mut accumulator = QuantileAccumulator::new(0.5);
        let input = create_f64_array(vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0), Some(5.0)]);

        accumulator.update_batch(&[input]).unwrap();
        let result = accumulator.evaluate().unwrap();

        assert_eq!(result, ScalarValue::Float64(Some(3.0)));
    }

    #[test]
    fn test_quantile_accumulator_with_nulls() {
        let mut accumulator = QuantileAccumulator::new(0.5);
        let input = create_f64_array(vec![Some(1.0), None, Some(3.0), Some(4.0), Some(5.0)]);

        accumulator.update_batch(&[input]).unwrap();

        let result = accumulator.evaluate().unwrap();
        assert_eq!(result, ScalarValue::Float64(Some(3.0)));
    }

    #[test]
    fn test_quantile_accumulator_multiple_batches() {
        let mut accumulator = QuantileAccumulator::new(0.5);
        let input1 = create_f64_array(vec![Some(1.0), Some(2.0)]);
        let input2 = create_f64_array(vec![Some(3.0), Some(4.0), Some(5.0)]);

        accumulator.update_batch(&[input1]).unwrap();
        accumulator.update_batch(&[input2]).unwrap();

        let result = accumulator.evaluate().unwrap();
        assert_eq!(result, ScalarValue::Float64(Some(3.0)));
    }

    #[test]
    fn test_quantile_accumulator_different_quantiles() {
        let mut min_accumulator = QuantileAccumulator::new(0.0);
        let input = create_f64_array(vec![Some(1.0), Some(2.0), Some(3.0), Some(4.0), Some(5.0)]);
        min_accumulator.update_batch(&[input.clone()]).unwrap();
        assert_eq!(
            min_accumulator.evaluate().unwrap(),
            ScalarValue::Float64(Some(1.0))
        );

        let mut q1_accumulator = QuantileAccumulator::new(0.25);
        q1_accumulator.update_batch(&[input.clone()]).unwrap();
        assert_eq!(
            q1_accumulator.evaluate().unwrap(),
            ScalarValue::Float64(Some(2.0))
        );

        let mut q3_accumulator = QuantileAccumulator::new(0.75);
        q3_accumulator.update_batch(&[input.clone()]).unwrap();
        assert_eq!(
            q3_accumulator.evaluate().unwrap(),
            ScalarValue::Float64(Some(4.0))
        );

        let mut max_accumulator = QuantileAccumulator::new(1.0);
        max_accumulator.update_batch(&[input]).unwrap();
        assert_eq!(
            max_accumulator.evaluate().unwrap(),
            ScalarValue::Float64(Some(5.0))
        );
    }

    #[test]
    fn test_quantile_accumulator_size() {
        let mut accumulator = QuantileAccumulator::new(0.5);
        let input = create_f64_array(vec![Some(1.0), Some(2.0), Some(3.0)]);

        let initial_size = accumulator.size();
        accumulator.update_batch(&[input]).unwrap();
        let after_update_size = accumulator.size();

        assert!(after_update_size >= initial_size);
    }

    #[test]
    fn test_quantile_accumulator_state_and_merge() -> DfResult<()> {
        let mut acc1 = QuantileAccumulator::new(0.5);
        let input1 = create_f64_array(vec![Some(1.0), Some(2.0)]);
        acc1.update_batch(&[input1])?;

        let state1 = acc1.state()?;

        let mut acc2 = QuantileAccumulator::new(0.5);
        let input2 = create_f64_array(vec![Some(3.0), Some(4.0), Some(5.0)]);
        acc2.update_batch(&[input2])?;

        let mut struct_builders = vec![];
        for scalar in &state1 {
            if let ScalarValue::Struct(struct_array) = scalar {
                struct_builders.push(struct_array.clone() as ArrayRef);
            }
        }

        acc2.merge_batch(&struct_builders)?;

        let result = acc2.evaluate()?;

        assert_eq!(result, ScalarValue::Float64(Some(3.0)));

        Ok(())
    }

    #[test]
    fn test_quantile_accumulator_with_extreme_values() {
        let mut accumulator = QuantileAccumulator::new(0.5);
        let input = create_f64_array(vec![Some(f64::MAX), Some(f64::MIN), Some(0.0)]);

        accumulator.update_batch(&[input]).unwrap();
        let _result = accumulator.evaluate().unwrap();
    }

    #[test]
    fn test_quantile_udaf_creation() {
        let q = 0.5;
        let udaf = quantile_udaf(q);

        assert_eq!(udaf.name(), QUANTILE_NAME);
        assert_eq!(udaf.return_type(&[]).unwrap(), DataType::Float64);
    }
}
