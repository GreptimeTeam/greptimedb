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

use common_query::prelude::*;
use common_telemetry::trace;
use datafusion::common::cast::{as_binary_array, as_primitive_array};
use datafusion::common::not_impl_err;
use datafusion::error::Result as DfResult;
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::{Accumulator as DfAccumulator, AggregateUDF};
use datafusion::physical_plan::expressions::Literal;
use datafusion::prelude::create_udaf;
use datatypes::arrow::array::ArrayRef;
use datatypes::arrow::datatypes::{DataType, Float64Type};
use uddsketch::UDDSketch;

pub const UDDSKETCH_STATE_NAME: &str = "uddsketch_state";

#[derive(Debug)]
pub struct UddSketchState {
    uddsketch: UDDSketch,
}

impl UddSketchState {
    pub fn new(bucket_size: u64, error_rate: f64) -> Self {
        Self {
            uddsketch: UDDSketch::new(bucket_size, error_rate),
        }
    }

    pub fn udf_impl() -> AggregateUDF {
        create_udaf(
            UDDSKETCH_STATE_NAME,
            vec![DataType::Int64, DataType::Float64, DataType::Float64],
            Arc::new(DataType::Binary),
            Volatility::Immutable,
            Arc::new(|args| {
                let (bucket_size, error_rate) = downcast_accumulator_args(args)?;
                Ok(Box::new(UddSketchState::new(bucket_size, error_rate)))
            }),
            Arc::new(vec![DataType::Binary]),
        )
    }

    fn update(&mut self, value: f64) {
        self.uddsketch.add_value(value);
    }

    fn merge(&mut self, raw: &[u8]) {
        if let Ok(uddsketch) = bincode::deserialize::<UDDSketch>(raw) {
            self.uddsketch.merge_sketch(&uddsketch);
        } else {
            trace!("Warning: Failed to deserialize UDDSketch from {:?}", raw);
        }
    }
}

fn downcast_accumulator_args(args: AccumulatorArgs) -> DfResult<(u64, f64)> {
    let bucket_size = match args.exprs[0]
        .as_any()
        .downcast_ref::<Literal>()
        .map(|lit| lit.value())
    {
        Some(ScalarValue::Int64(Some(value))) => *value as u64,
        _ => {
            return not_impl_err!(
                "{} not supported for bucket size: {}",
                UDDSKETCH_STATE_NAME,
                &args.exprs[0]
            )
        }
    };

    let error_rate = match args.exprs[1]
        .as_any()
        .downcast_ref::<Literal>()
        .map(|lit| lit.value())
    {
        Some(ScalarValue::Float64(Some(value))) => *value,
        _ => {
            return not_impl_err!(
                "{} not supported for error rate: {}",
                UDDSKETCH_STATE_NAME,
                &args.exprs[1]
            )
        }
    };

    Ok((bucket_size, error_rate))
}

impl DfAccumulator for UddSketchState {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        let array = &values[2];
        let f64_array = as_primitive_array::<Float64Type>(array)?;
        for v in f64_array.iter().flatten() {
            self.update(v);
        }

        Ok(())
    }

    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        Ok(ScalarValue::Binary(Some(
            bincode::serialize(&self.uddsketch).unwrap(),
        )))
    }

    fn size(&self) -> usize {
        0
    }

    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(Some(
            bincode::serialize(&self.uddsketch).unwrap(),
        ))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        let array = &states[0];
        let binary_array = as_binary_array(array)?;
        for v in binary_array.iter().flatten() {
            self.merge(v);
        }

        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{BinaryArray, Float64Array};

    use super::*;

    #[test]
    fn test_uddsketch_state_basic() {
        let mut state = UddSketchState::new(10, 0.01);
        state.update(1.0);
        state.update(2.0);
        state.update(3.0);

        let result = state.evaluate().unwrap();
        if let ScalarValue::Binary(Some(bytes)) = result {
            let deserialized: UDDSketch = bincode::deserialize(&bytes).unwrap();
            assert_eq!(deserialized.count(), 3);
        } else {
            panic!("Expected binary scalar value");
        }
    }

    #[test]
    fn test_uddsketch_state_roundtrip() {
        let mut state = UddSketchState::new(10, 0.01);
        state.update(1.0);
        state.update(2.0);

        // Serialize
        let serialized = state.evaluate().unwrap();

        // Create new state and merge the serialized data
        let mut new_state = UddSketchState::new(10, 0.01);
        if let ScalarValue::Binary(Some(bytes)) = &serialized {
            new_state.merge(bytes);

            // Verify the merged state matches original
            let new_result = new_state.evaluate().unwrap();
            assert_eq!(serialized, new_result);
        } else {
            panic!("Expected binary scalar value");
        }
    }

    #[test]
    fn test_uddsketch_state_batch_update() {
        let mut state = UddSketchState::new(10, 0.01);
        let values = vec![1.0f64, 2.0, 3.0];
        let array = Arc::new(Float64Array::from(values)) as ArrayRef;

        state
            .update_batch(&[array.clone(), array.clone(), array])
            .unwrap();

        let result = state.evaluate().unwrap();
        if let ScalarValue::Binary(Some(bytes)) = result {
            let deserialized: UDDSketch = bincode::deserialize(&bytes).unwrap();
            assert_eq!(deserialized.count(), 3);
        } else {
            panic!("Expected binary scalar value");
        }
    }

    #[test]
    fn test_uddsketch_state_merge_batch() {
        let mut state1 = UddSketchState::new(10, 0.01);
        state1.update(1.0);
        let state1_binary = state1.evaluate().unwrap();

        let mut state2 = UddSketchState::new(10, 0.01);
        state2.update(2.0);
        let state2_binary = state2.evaluate().unwrap();

        let mut merged_state = UddSketchState::new(10, 0.01);
        if let (ScalarValue::Binary(Some(bytes1)), ScalarValue::Binary(Some(bytes2))) =
            (&state1_binary, &state2_binary)
        {
            let binary_array = Arc::new(BinaryArray::from(vec![
                bytes1.as_slice(),
                bytes2.as_slice(),
            ])) as ArrayRef;
            merged_state.merge_batch(&[binary_array]).unwrap();

            let result = merged_state.evaluate().unwrap();
            if let ScalarValue::Binary(Some(bytes)) = result {
                let deserialized: UDDSketch = bincode::deserialize(&bytes).unwrap();
                assert_eq!(deserialized.count(), 2);
            } else {
                panic!("Expected binary scalar value");
            }
        } else {
            panic!("Expected binary scalar values");
        }
    }
}
