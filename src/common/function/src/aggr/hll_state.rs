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
use datafusion::arrow::array::ArrayRef;
use datafusion::common::cast::{as_binary_array, as_string_array};
use datafusion::common::not_impl_err;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::{Accumulator as DfAccumulator, AggregateUDF};
use datafusion::prelude::create_udaf;
use datatypes::arrow::datatypes::DataType;
use hyperloglogplus::{HyperLogLog, HyperLogLogPlus};

use crate::utils::FixedRandomState;

pub const HLL_STATE_NAME: &str = "hll_state";

const DEFAULT_PRECISION: u8 = 14;

pub(crate) type HllStateType = HyperLogLogPlus<String, FixedRandomState>;

pub struct HllState {
    hll: HllStateType,
}

impl std::fmt::Debug for HllState {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "HllState<Opaque>")
    }
}

impl Default for HllState {
    fn default() -> Self {
        Self::new()
    }
}

impl HllState {
    pub fn new() -> Self {
        Self {
            // Safety: the DEFAULT_PRECISION is fixed and valid
            hll: HllStateType::new(DEFAULT_PRECISION, FixedRandomState::new()).unwrap(),
        }
    }

    pub fn udf_impl() -> AggregateUDF {
        create_udaf(
            HLL_STATE_NAME,
            vec![DataType::Utf8],
            Arc::new(DataType::Binary),
            Volatility::Immutable,
            Arc::new(Self::create_accumulator),
            Arc::new(vec![DataType::Binary]),
        )
    }

    fn update(&mut self, value: &str) {
        self.hll.insert(value);
    }

    fn merge(&mut self, raw: &[u8]) {
        if let Ok(serialized) = bincode::deserialize::<HllStateType>(raw) {
            if let Ok(()) = self.hll.merge(&serialized) {
                return;
            }
        }
        trace!("Warning: Failed to merge HyperLogLog from {:?}", raw);
    }

    fn create_accumulator(acc_args: AccumulatorArgs) -> DfResult<Box<dyn DfAccumulator>> {
        let data_type = acc_args.exprs[0].data_type(acc_args.schema)?;

        match data_type {
            DataType::Utf8 => Ok(Box::new(HllState::new())),
            other => not_impl_err!("{HLL_STATE_NAME} does not support data type: {other}"),
        }
    }
}

impl DfAccumulator for HllState {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        let array = &values[0];
        let string_array = as_string_array(array)?;
        for value in string_array.iter().flatten() {
            self.update(value);
        }

        Ok(())
    }

    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        Ok(ScalarValue::Binary(Some(
            bincode::serialize(&self.hll).map_err(|e| {
                DataFusionError::Internal(format!("Failed to serialize HyperLogLog: {}", e))
            })?,
        )))
    }

    fn size(&self) -> usize {
        std::mem::size_of_val(&self.hll)
    }

    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(Some(
            bincode::serialize(&self.hll).map_err(|e| {
                DataFusionError::Internal(format!("Failed to serialize HyperLogLog: {}", e))
            })?,
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
    use datafusion::arrow::array::{BinaryArray, StringArray};

    use super::*;

    #[test]
    fn test_hll_state_basic() {
        let mut state = HllState::new();
        state.update("1");
        state.update("2");
        state.update("3");

        let result = state.evaluate().unwrap();
        if let ScalarValue::Binary(Some(bytes)) = result {
            let mut hll: HllStateType = bincode::deserialize(&bytes).unwrap();
            assert_eq!(hll.count().trunc() as u32, 3);
        } else {
            panic!("Expected binary scalar value");
        }
    }

    #[test]
    fn test_hll_state_roundtrip() {
        let mut state = HllState::new();
        state.update("1");
        state.update("2");

        // Serialize
        let serialized = state.evaluate().unwrap();

        // Create new state and merge the serialized data
        let mut new_state = HllState::new();
        if let ScalarValue::Binary(Some(bytes)) = &serialized {
            new_state.merge(bytes);

            // Verify the merged state matches original
            let result = new_state.evaluate().unwrap();
            if let ScalarValue::Binary(Some(new_bytes)) = result {
                let mut original: HllStateType = bincode::deserialize(bytes).unwrap();
                let mut merged: HllStateType = bincode::deserialize(&new_bytes).unwrap();
                assert_eq!(original.count(), merged.count());
            } else {
                panic!("Expected binary scalar value");
            }
        } else {
            panic!("Expected binary scalar value");
        }
    }

    #[test]
    fn test_hll_state_batch_update() {
        let mut state = HllState::new();

        // Test string values
        let str_values = vec!["a", "b", "c", "d", "e", "f", "g", "h", "i"];
        let str_array = Arc::new(StringArray::from(str_values)) as ArrayRef;
        state.update_batch(&[str_array]).unwrap();

        let result = state.evaluate().unwrap();
        if let ScalarValue::Binary(Some(bytes)) = result {
            let mut hll: HllStateType = bincode::deserialize(&bytes).unwrap();
            assert_eq!(hll.count().trunc() as u32, 9);
        } else {
            panic!("Expected binary scalar value");
        }
    }

    #[test]
    fn test_hll_state_merge_batch() {
        let mut state1 = HllState::new();
        state1.update("1");
        let state1_binary = state1.evaluate().unwrap();

        let mut state2 = HllState::new();
        state2.update("2");
        let state2_binary = state2.evaluate().unwrap();

        let mut merged_state = HllState::new();
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
                let mut hll: HllStateType = bincode::deserialize(&bytes).unwrap();
                assert_eq!(hll.count().trunc() as u32, 2);
            } else {
                panic!("Expected binary scalar value");
            }
        } else {
            panic!("Expected binary scalar values");
        }
    }
}
