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
//!
//! Input samples and intermediate states must contain only finite values.

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

pub const STDDEV_POP_STATE_NAME: &str = "stddev_pop_state";
pub const STDDEV_POP_MERGE_NAME: &str = "stddev_pop_merge";

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
        match self.count {
            0 => self.mean.to_bits() == 0 && self.m2.to_bits() == 0,
            1 => self.mean.is_finite() && self.m2.to_bits() == 0,
            _ => self.mean.is_finite() && self.m2.is_finite() && self.m2 >= 0.0,
        }
    }

    fn update(&mut self, sample: f64) -> DfResult<()> {
        if !sample.is_finite() {
            return Err(non_finite_input());
        }

        let count = self
            .count
            .checked_add(1)
            .ok_or_else(|| DataFusionError::Execution("Welford count overflow".to_string()))?;
        let candidate_state = if self.count == 0 {
            Self {
                count,
                mean: sample,
                m2: 0.0,
            }
        } else {
            let delta = sample - self.mean;
            if !delta.is_finite() {
                return Err(non_finite_arithmetic());
            }
            let mean = self.mean + delta / count as f64;
            let delta2 = sample - mean;
            Self {
                count,
                mean,
                m2: self.m2 + delta * delta2,
            }
        };
        self.replace_with_candidate(candidate_state)
    }

    fn merge(&mut self, other: &Self) -> DfResult<()> {
        if other.count == 0 {
            return Ok(());
        }
        if self.count == 0 {
            return self.replace_with_candidate(*other);
        }

        let count = self
            .count
            .checked_add(other.count)
            .ok_or_else(|| DataFusionError::Execution("Welford count overflow".to_string()))?;
        let delta = other.mean - self.mean;
        if !delta.is_finite() {
            return Err(non_finite_arithmetic());
        }
        let self_count = self.count as f64;
        let other_count = other.count as f64;
        let count_f64 = count as f64;
        let mean_delta = if delta.abs() <= f64::MAX / other_count {
            delta * other_count / count_f64
        } else {
            delta * (other_count / count_f64)
        };
        let weighted_count = self_count * other_count / count_f64;
        let candidate_state = Self {
            count,
            mean: self.mean + mean_delta,
            m2: self.m2 + other.m2 + checked_weighted_square(delta, weighted_count)?,
        };
        self.replace_with_candidate(candidate_state)
    }

    fn replace_with_candidate(&mut self, candidate_state: Self) -> DfResult<()> {
        if !candidate_state.is_valid() {
            return Err(non_finite_arithmetic());
        }

        *self = candidate_state;
        Ok(())
    }

    pub(crate) fn population_stddev(&self) -> Option<f64> {
        if self.count == 0 {
            return None;
        }

        Some((self.m2 / self.count as f64).sqrt())
    }
}

fn checked_weighted_square(delta: f64, weight: f64) -> DfResult<f64> {
    if delta.abs() <= f64::MAX.sqrt() {
        return Ok(delta * delta * weight);
    }
    if weight <= 1.0 {
        // Applying the weight first avoids overflow when the weighted square is representable.
        return Ok(delta * weight * delta);
    }

    Err(non_finite_arithmetic())
}

/// Accumulates and merges versioned Welford states.
#[derive(Debug, Default)]
pub struct WelfordAccumulator {
    state: WelfordState,
}

impl WelfordAccumulator {
    /// Creates the `stddev_pop_state` aggregate function.
    pub fn state_udf_impl() -> AggregateUDF {
        create_udaf(
            STDDEV_POP_STATE_NAME,
            vec![DataType::Float64],
            Arc::new(DataType::Binary),
            Volatility::Immutable,
            Arc::new(Self::create_accumulator),
            Arc::new(vec![DataType::Binary]),
        )
    }

    /// Creates the `stddev_pop_merge` aggregate function.
    pub fn merge_udf_impl() -> AggregateUDF {
        create_udaf(
            STDDEV_POP_MERGE_NAME,
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
                for sample in as_primitive_array::<Float64Type>(array)?.iter().flatten() {
                    self.state.update(sample)?;
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

fn non_finite_input() -> DataFusionError {
    DataFusionError::Execution("Welford state requires finite input values".to_string())
}

fn non_finite_arithmetic() -> DataFusionError {
    DataFusionError::Execution("Welford arithmetic produced a non-finite state".to_string())
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
    fn test_welford_state_encoding_contract() {
        let state = WelfordState {
            count: 3,
            mean: 2.0,
            m2: 6.0,
        };

        let encoded = state.encode();

        assert_eq!(
            encoded,
            [
                b'W', b'L', b'F', b'1', // magic
                3, 0, 0, 0, 0, 0, 0, 0, // count
                0, 0, 0, 0, 0, 0, 0, 64, // mean
                0, 0, 0, 0, 0, 0, 24, 64, // m2
            ]
        );
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
    fn test_welford_non_finite_values_fail_independent_of_partitioning() {
        for sample in [f64::NAN, f64::INFINITY, f64::NEG_INFINITY] {
            let mut one_pass = WelfordState::default();
            let update_failed = one_pass.update(sample).is_err();

            let mut merged = WelfordState::default();
            let non_finite_state = WelfordState {
                count: 1,
                mean: sample,
                m2: 0.0,
            };
            let merge_failed = merged.merge(&non_finite_state).is_err();

            assert_eq!((update_failed, merge_failed), (true, true));
            assert_eq!(one_pass, WelfordState::default());
            assert_eq!(merged, WelfordState::default());
        }
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
            count: 2,
            mean: 1.0,
            m2: -1.0,
        };
        assert!(WelfordState::decode(&negative_m2.encode()).is_err());

        for m2 in [1.0, -0.0] {
            let noncanonical_singleton = WelfordState {
                count: 1,
                mean: 0.0,
                m2,
            };
            assert!(WelfordState::decode(&noncanonical_singleton.encode()).is_err());
        }

        for (mean, m2) in [
            (f64::NAN, 0.0),
            (f64::INFINITY, 0.0),
            (f64::NEG_INFINITY, 0.0),
            (0.0, f64::NAN),
            (0.0, f64::INFINITY),
            (0.0, f64::NEG_INFINITY),
        ] {
            let non_finite = WelfordState { count: 1, mean, m2 };
            assert!(WelfordState::decode(&non_finite.encode()).is_err());
        }
    }

    #[test]
    fn test_welford_state_merge_matches_one_pass_update() {
        let mut merged = state_from_values(&[1.0, 2.0]);
        merged.merge(&state_from_values(&[3.0, 4.0])).unwrap();

        assert_eq!(merged, state_from_values(&[1.0, 2.0, 3.0, 4.0]));
    }

    #[test]
    fn test_welford_large_finite_variance_matches_partitioned_merge() {
        let large_sample = f64::MAX.sqrt() * 1.1;
        let one_pass = state_from_values(&[0.0, large_sample]);
        let mut merged = state_from_values(&[0.0]);

        merged.merge(&state_from_values(&[large_sample])).unwrap();

        assert_eq!(merged, one_pass);
    }

    #[test]
    fn test_welford_extreme_values_fail_independent_of_partitioning() {
        for values in [[f64::MAX, -f64::MAX], [-f64::MAX, f64::MAX]] {
            let mut one_pass = state_from_values(&values[..1]);
            let original_one_pass = one_pass;
            let update_failed = one_pass.update(values[1]).is_err();

            let mut merged = state_from_values(&values[..1]);
            let original_merged = merged;
            let merge_failed = merged.merge(&state_from_values(&values[1..])).is_err();

            assert_eq!((update_failed, merge_failed), (true, true));
            assert_eq!(one_pass, original_one_pass);
            assert_eq!(merged, original_merged);
        }
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
        let noncanonical_singleton = WelfordState {
            count: 1,
            mean: 0.0,
            m2: 1.0,
        }
        .encode();

        for encoded in [b"invalid".to_vec(), noncanonical_singleton.to_vec()] {
            let states = Arc::new(BinaryArray::from(vec![Some(encoded.as_slice())])) as ArrayRef;
            let mut accumulator = WelfordAccumulator::default();

            assert!(accumulator.merge_batch(&[states]).is_err());
        }
    }
}
