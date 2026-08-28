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

//! Mergeable Welford state for population standard deviation.

use std::sync::Arc;

use datafusion::arrow::array::ArrayRef;
use datafusion::common::cast::{as_binary_array, as_primitive_array};
use datafusion::common::not_impl_err;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::{Accumulator as DfAccumulator, AggregateUDF, Volatility};
use datafusion::prelude::create_udaf;
use datafusion_common::ScalarValue;
use datatypes::arrow::datatypes::{DataType, Float64Type};

pub const WELFORD_STATE_NAME: &str = "welford_state";
pub const WELFORD_MERGE_NAME: &str = "welford_merge";

const ENCODED_LEN: usize = 28;
const MAGIC: &[u8; 4] = b"WLF1";

#[derive(Debug, Clone, Copy, PartialEq)]
pub(crate) struct WelfordState {
    pub(crate) count: u64,
    pub(crate) mean: f64,
    pub(crate) m2: f64,
}

impl Default for WelfordState {
    fn default() -> Self {
        Self {
            count: 0,
            mean: 0.0,
            m2: 0.0,
        }
    }
}

impl WelfordState {
    pub(crate) fn encode(&self) -> [u8; ENCODED_LEN] {
        let mut encoded = [0; ENCODED_LEN];
        encoded[..4].copy_from_slice(MAGIC);
        encoded[4..12].copy_from_slice(&self.count.to_le_bytes());
        encoded[12..20].copy_from_slice(&self.mean.to_bits().to_le_bytes());
        encoded[20..28].copy_from_slice(&self.m2.to_bits().to_le_bytes());
        encoded
    }

    pub(crate) fn decode(encoded: &[u8]) -> DfResult<Self> {
        if encoded.len() != ENCODED_LEN || &encoded[..4] != MAGIC {
            return Err(invalid_state());
        }

        let state = Self {
            count: decode_u64(encoded, 4),
            mean: decode_f64(encoded, 12),
            m2: decode_f64(encoded, 20),
        };
        if !state.is_valid() {
            return Err(invalid_state());
        }

        Ok(state)
    }

    fn is_valid(&self) -> bool {
        if self.count == 0 {
            return self.mean.to_bits() == 0 && self.m2.to_bits() == 0;
        }

        self.m2 >= 0.0 || self.m2.is_nan()
    }

    fn update(&mut self, value: f64) -> DfResult<()> {
        let count = self
            .count
            .checked_add(1)
            .ok_or_else(|| DataFusionError::Execution("Welford count overflow".to_string()))?;

        if self.count == 0 {
            self.count = count;
            self.mean = value;
            self.m2 = 0.0;
            return Ok(());
        }

        let delta = value - self.mean;
        self.mean += delta / count as f64;
        let delta2 = value - self.mean;
        self.m2 += delta * delta2;
        self.count = count;
        Ok(())
    }

    fn merge(&mut self, other: &Self) -> DfResult<()> {
        if other.count == 0 {
            return Ok(());
        }
        if self.count == 0 {
            *self = *other;
            return Ok(());
        }

        let count = self
            .count
            .checked_add(other.count)
            .ok_or_else(|| DataFusionError::Execution("Welford count overflow".to_string()))?;
        let delta = other.mean - self.mean;
        let weighted_count = self.count as f64 * other.count as f64 / count as f64;
        self.mean += delta * other.count as f64 / count as f64;
        self.m2 += other.m2 + delta * delta * weighted_count;
        self.count = count;
        Ok(())
    }

    pub(crate) fn population_stddev(&self) -> Option<f64> {
        if self.count == 0 {
            return None;
        }

        let variance = self.m2 / self.count as f64;
        Some(if variance < 0.0 { 0.0 } else { variance.sqrt() })
    }
}

/// Accumulates and merges versioned Welford states.
#[derive(Debug, Default)]
pub struct WelfordAccumulator {
    state: WelfordState,
}

impl WelfordAccumulator {
    /// Creates the `welford_state` aggregate function.
    pub fn state_udf_impl() -> AggregateUDF {
        create_udaf(
            WELFORD_STATE_NAME,
            vec![DataType::Float64],
            Arc::new(DataType::Binary),
            Volatility::Immutable,
            Arc::new(Self::create_accumulator),
            Arc::new(vec![DataType::Binary]),
        )
    }

    /// Creates the `welford_merge` aggregate function.
    pub fn merge_udf_impl() -> AggregateUDF {
        create_udaf(
            WELFORD_MERGE_NAME,
            vec![DataType::Binary],
            Arc::new(DataType::Binary),
            Volatility::Immutable,
            Arc::new(Self::create_accumulator),
            Arc::new(vec![DataType::Binary]),
        )
    }

    fn create_accumulator(_: AccumulatorArgs) -> DfResult<Box<dyn DfAccumulator>> {
        Ok(Box::new(Self::default()))
    }
}

impl DfAccumulator for WelfordAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        let array = &values[0];
        match array.data_type() {
            DataType::Float64 => {
                for value in as_primitive_array::<Float64Type>(array)?.iter().flatten() {
                    self.state.update(value)?;
                }
            }
            DataType::Binary => self.merge_batch(std::slice::from_ref(array))?,
            other => {
                return not_impl_err!("Welford functions do not support data type: {other}");
            }
        }
        Ok(())
    }

    fn evaluate(&mut self) -> DfResult<ScalarValue> {
        Ok(ScalarValue::Binary(Some(self.state.encode().to_vec())))
    }

    fn size(&self) -> usize {
        std::mem::size_of::<Self>()
    }

    fn state(&mut self) -> DfResult<Vec<ScalarValue>> {
        Ok(vec![ScalarValue::Binary(Some(
            self.state.encode().to_vec(),
        ))])
    }

    fn merge_batch(&mut self, states: &[ArrayRef]) -> DfResult<()> {
        let array = as_binary_array(&states[0])?;
        for encoded in array.iter().flatten() {
            self.state.merge(&WelfordState::decode(encoded)?)?;
        }
        Ok(())
    }
}

fn decode_u64(encoded: &[u8], offset: usize) -> u64 {
    let mut bytes = [0; 8];
    bytes.copy_from_slice(&encoded[offset..offset + 8]);
    u64::from_le_bytes(bytes)
}

fn decode_f64(encoded: &[u8], offset: usize) -> f64 {
    f64::from_bits(decode_u64(encoded, offset))
}

fn invalid_state() -> DataFusionError {
    DataFusionError::Execution("Invalid Welford state".to_string())
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datafusion::arrow::array::{ArrayRef, BinaryArray, Float64Array};
    use datafusion_common::ScalarValue;

    use super::*;

    fn state_from_values(values: &[f64]) -> WelfordState {
        let mut state = WelfordState::default();
        for value in values {
            state.update(*value).unwrap();
        }
        state
    }

    #[test]
    fn test_welford_state_roundtrip() {
        let state = WelfordState {
            count: 3,
            mean: 2.0,
            m2: 2.0,
        };

        let encoded = state.encode();

        assert_eq!(encoded.len(), 28);
        assert_eq!(&encoded[..4], b"WLF1");
        assert_eq!(WelfordState::decode(&encoded).unwrap(), state);
    }

    #[test]
    fn test_welford_state_online_update() {
        let mut state = WelfordState::default();
        for value in [1.0, 2.0, 3.0, 4.0] {
            state.update(value).unwrap();
        }

        assert_eq!(state.count, 4);
        assert_eq!(state.mean, 2.5);
        assert_eq!(state.m2, 5.0);
        assert_eq!(state.population_stddev(), Some(1.25_f64.sqrt()));
    }

    #[test]
    fn test_welford_state_empty_and_single_value() {
        let mut state = WelfordState::default();
        assert_eq!(state.population_stddev(), None);

        state.update(42.0).unwrap();
        assert_eq!(state.population_stddev(), Some(0.0));
    }

    #[test]
    fn test_welford_state_first_infinite_value() {
        let mut state = WelfordState::default();

        state.update(f64::INFINITY).unwrap();

        assert_eq!(state.count, 1);
        assert_eq!(state.mean, f64::INFINITY);
        assert_eq!(state.m2, 0.0);
        assert_eq!(state.population_stddev(), Some(0.0));
    }

    #[test]
    fn test_welford_state_rejects_malformed_encoding() {
        assert!(WelfordState::decode(b"").is_err());
        assert!(WelfordState::decode(&[0; 28]).is_err());

        let mut encoded = WelfordState::default().encode().to_vec();
        encoded.push(0);
        assert!(WelfordState::decode(&encoded).is_err());

        let noncanonical_empty = WelfordState {
            count: 0,
            mean: 1.0,
            m2: 0.0,
        };
        assert!(WelfordState::decode(&noncanonical_empty.encode()).is_err());

        let negative_m2 = WelfordState {
            count: 1,
            mean: 1.0,
            m2: -1.0,
        };
        assert!(WelfordState::decode(&negative_m2.encode()).is_err());
    }

    #[test]
    fn test_welford_state_merge_matches_one_pass_update() {
        let mut merged = state_from_values(&[1.0, 2.0]);
        merged.merge(&state_from_values(&[3.0, 4.0])).unwrap();

        assert_eq!(merged, state_from_values(&[1.0, 2.0, 3.0, 4.0]));
    }

    #[test]
    fn test_welford_state_empty_merge_identity() {
        let populated = state_from_values(&[1.0, 2.0]);
        let mut left = WelfordState::default();
        left.merge(&populated).unwrap();
        assert_eq!(left, populated);

        let mut right = populated;
        right.merge(&WelfordState::default()).unwrap();
        assert_eq!(right, populated);
    }

    #[test]
    fn test_welford_state_merge_rejects_count_overflow() {
        let mut state = WelfordState {
            count: u64::MAX,
            mean: 1.0,
            m2: 0.0,
        };
        let other = WelfordState {
            count: 1,
            mean: 1.0,
            m2: 0.0,
        };

        assert!(state.merge(&other).is_err());
    }

    #[test]
    fn test_welford_accumulator_ignores_nulls() {
        let mut accumulator = WelfordAccumulator::default();
        let array = Arc::new(Float64Array::from(vec![Some(1.0), None, Some(3.0)])) as ArrayRef;

        accumulator.update_batch(&[array]).unwrap();

        let ScalarValue::Binary(Some(encoded)) = accumulator.evaluate().unwrap() else {
            panic!("Expected binary scalar value");
        };
        assert_eq!(
            WelfordState::decode(&encoded).unwrap(),
            state_from_values(&[1.0, 3.0])
        );
    }

    #[test]
    fn test_welford_accumulator_merges_binary_states() {
        let first = state_from_values(&[1.0, 2.0]).encode();
        let second = state_from_values(&[3.0, 4.0]).encode();
        let states = Arc::new(BinaryArray::from(vec![
            Some(first.as_slice()),
            None,
            Some(second.as_slice()),
        ])) as ArrayRef;
        let mut accumulator = WelfordAccumulator::default();

        accumulator.merge_batch(&[states]).unwrap();

        let ScalarValue::Binary(Some(encoded)) = accumulator.state().unwrap().remove(0) else {
            panic!("Expected binary scalar value");
        };
        assert_eq!(
            WelfordState::decode(&encoded).unwrap(),
            state_from_values(&[1.0, 2.0, 3.0, 4.0])
        );
    }

    #[test]
    fn test_welford_accumulator_rejects_malformed_state() {
        let states = Arc::new(BinaryArray::from(vec![Some(b"invalid".as_slice())])) as ArrayRef;
        let mut accumulator = WelfordAccumulator::default();

        assert!(accumulator.merge_batch(&[states]).is_err());
    }
}
