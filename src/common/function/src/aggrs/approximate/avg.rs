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

use datafusion::arrow::array::{ArrayRef, Float64Array};
use datafusion::arrow::compute::sum;
use datafusion::common::cast::{as_binary_array, as_primitive_array};
use datafusion::common::not_impl_err;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::logical_expr::function::AccumulatorArgs;
use datafusion::logical_expr::{Accumulator as DfAccumulator, AggregateUDF, Volatility};
use datafusion::prelude::create_udaf;
use datafusion_common::ScalarValue;
use datatypes::arrow::datatypes::{DataType, Float64Type};

pub const AVG_STATE_NAME: &str = "avg_state";
pub const AVG_MERGE_NAME: &str = "avg_merge";

const ENCODED_LEN: usize = 20;
const MAGIC: &[u8; 4] = b"AVG1";

/// The portable state used by the Float64 average aggregate functions.
#[derive(Debug, Clone, Copy, PartialEq)]
pub struct AvgState {
    count: u64,
    sum: f64,
}

impl Default for AvgState {
    fn default() -> Self {
        Self { count: 0, sum: 0.0 }
    }
}

impl AvgState {
    /// Returns the exact AVG1 representation of this state.
    pub(crate) fn encode(&self) -> [u8; ENCODED_LEN] {
        let mut encoded = [0; ENCODED_LEN];
        encoded[..4].copy_from_slice(MAGIC);
        encoded[4..12].copy_from_slice(&self.count.to_le_bytes());
        encoded[12..20].copy_from_slice(&self.sum.to_bits().to_le_bytes());
        encoded
    }

    /// Decodes and validates an AVG1 state.
    pub fn decode(encoded: &[u8]) -> DfResult<Self> {
        if encoded.len() != ENCODED_LEN || &encoded[..4] != MAGIC {
            return Err(invalid_state());
        }
        let count = decode_u64(encoded, 4);
        let sum = f64::from_bits(decode_u64(encoded, 12));
        if count == 0 && sum.to_bits() != 0 {
            return Err(invalid_state());
        }
        Ok(Self { count, sum })
    }

    /// Returns the number of non-null input values in this state.
    pub(crate) fn count(&self) -> u64 {
        self.count
    }

    /// Returns the average, or `None` for the canonical empty state.
    pub fn average(&self) -> Option<f64> {
        (self.count() != 0).then(|| self.sum / self.count() as f64)
    }
}

fn decode_u64(encoded: &[u8], offset: usize) -> u64 {
    let mut bytes = [0; 8];
    bytes.copy_from_slice(&encoded[offset..offset + 8]);
    u64::from_le_bytes(bytes)
}

fn invalid_state() -> DataFusionError {
    DataFusionError::Execution("Invalid AVG1 state".to_string())
}

fn count_overflow() -> DataFusionError {
    DataFusionError::Execution("AVG count overflow".to_string())
}

#[derive(Debug, Clone, Copy)]
enum InputKind {
    Float64,
    Binary,
}

/// Accumulates and merges AVG1 states.
#[derive(Debug)]
pub(crate) struct AvgAccumulator {
    state: AvgState,
    input: InputKind,
}

impl Default for AvgAccumulator {
    fn default() -> Self {
        Self {
            state: AvgState::default(),
            input: InputKind::Float64,
        }
    }
}

impl AvgAccumulator {
    pub fn state_udf_impl() -> AggregateUDF {
        create_udaf(
            AVG_STATE_NAME,
            vec![DataType::Float64],
            Arc::new(DataType::Binary),
            Volatility::Immutable,
            Arc::new(Self::create_accumulator),
            Arc::new(vec![DataType::Binary]),
        )
    }

    pub fn merge_udf_impl() -> AggregateUDF {
        create_udaf(
            AVG_MERGE_NAME,
            vec![DataType::Binary],
            Arc::new(DataType::Binary),
            Volatility::Immutable,
            Arc::new(Self::create_accumulator),
            Arc::new(vec![DataType::Binary]),
        )
    }

    fn create_accumulator(args: AccumulatorArgs) -> DfResult<Box<dyn DfAccumulator>> {
        if args.is_distinct {
            return not_impl_err!("AVG DISTINCT aggregations are not available");
        }
        let input = match args.exprs[0].data_type(args.schema)? {
            DataType::Float64 => InputKind::Float64,
            DataType::Binary => InputKind::Binary,
            data_type => return not_impl_err!("AVG functions do not support {data_type:?}"),
        };
        Ok(Box::new(Self {
            state: AvgState::default(),
            input,
        }))
    }

    fn update_float64(&mut self, array: &ArrayRef) -> DfResult<()> {
        let array = as_primitive_array::<Float64Type>(array)?;
        let mut count = self.state.count;
        for _ in array.iter().flatten() {
            count = count.checked_add(1).ok_or_else(count_overflow)?;
        }
        let sum = sum(array)
            .map(|batch_sum| self.state.sum + batch_sum)
            .unwrap_or(self.state.sum);
        self.state = AvgState { count, sum };
        Ok(())
    }

    fn merge_states(&mut self, array: &ArrayRef) -> DfResult<()> {
        let array = as_binary_array(array)?;
        let states = array
            .iter()
            .flatten()
            .map(AvgState::decode)
            .collect::<DfResult<Vec<_>>>()?;
        let count = states.iter().try_fold(self.state.count, |count, state| {
            count.checked_add(state.count).ok_or_else(count_overflow)
        })?;
        let sums = states
            .iter()
            .filter(|state| state.count != 0)
            .map(|state| Some(state.sum))
            .collect::<Vec<_>>();
        let sum = sum(&Float64Array::from(sums))
            .map(|batch_sum| self.state.sum + batch_sum)
            .unwrap_or(self.state.sum);
        self.state = AvgState { count, sum };
        Ok(())
    }
}

impl DfAccumulator for AvgAccumulator {
    fn update_batch(&mut self, values: &[ArrayRef]) -> DfResult<()> {
        let array = &values[0];
        match (self.input, array.data_type()) {
            (InputKind::Float64, DataType::Float64) => self.update_float64(array),
            (InputKind::Binary, DataType::Binary) => self.merge_states(array),
            (_, data_type) => not_impl_err!("AVG input type does not match: {data_type:?}"),
        }
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
        self.merge_states(&states[0])
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{BinaryArray, Float64Array};
    use datafusion_common::ScalarValue;
    use datafusion_common::arrow::datatypes::DataType;
    use datafusion_expr::TypeSignature;
    use datafusion_physical_expr::aggregate::AggregateExprBuilder;
    use datafusion_physical_expr::expressions::{Column, lit as physical_lit};

    use super::*;
    use crate::aggrs::aggr_wrapper::{aggr_delta_merge_func_name, aggr_state_func_name};
    use crate::function_registry::FUNCTION_REGISTRY;

    fn state(count: u64, sum: f64) -> Vec<u8> {
        AvgState { count, sum }.encode().to_vec()
    }

    #[test]
    fn codec_golden_and_roundtrip() {
        let empty = AvgState::default().encode();
        assert_eq!(empty.len(), ENCODED_LEN);
        assert_eq!(empty.as_slice(), b"AVG1\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0\0");
        let mut accumulator = AvgAccumulator::default();
        accumulator
            .update_batch(&[Arc::new(Float64Array::from(vec![Some(1.5)]))])
            .unwrap();
        let one = accumulator.state.encode();
        assert_eq!(one.len(), ENCODED_LEN);
        assert_eq!(
            one.as_slice(),
            &[
                b'A', b'V', b'G', b'1', 1, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0xf8, 0x3f,
            ]
        );
        assert_eq!(&one[12..20], &1.5f64.to_bits().to_le_bytes());
        assert_eq!(AvgState::decode(&empty).unwrap().encode(), empty);
        assert_eq!(AvgState::decode(&one).unwrap().encode(), one);
    }

    #[test]
    fn codec_rejects_malformed_states() {
        assert!(AvgState::decode(b"").is_err());
        assert!(AvgState::decode(&[0; 19]).is_err());
        assert!(AvgState::decode(&[0; 21]).is_err());
        let mut avg2 = AvgState::default().encode();
        avg2[..4].copy_from_slice(b"AVG2");
        assert!(AvgState::decode(&avg2).is_err());
        let mut wrong_magic = AvgState::default().encode();
        wrong_magic[0] = b'X';
        assert!(AvgState::decode(&wrong_magic).is_err());
        for sum in [1.0, -0.0] {
            assert!(AvgState::decode(&state(0, sum)).is_err());
        }
        let mut count = AvgState {
            count: 0x0102_0304_0506_0708,
            sum: 0.0,
        }
        .encode();
        assert_eq!(&count[4..12], &0x0102_0304_0506_0708u64.to_le_bytes());
        count[4..12].reverse();
        assert_ne!(
            AvgState::decode(&count).unwrap().count(),
            0x0102_0304_0506_0708
        );
        let mut sum = AvgState { count: 1, sum: 1.5 }.encode();
        assert_eq!(&sum[12..20], &1.5f64.to_bits().to_le_bytes());
        sum[12..20].reverse();
        assert_ne!(AvgState::decode(&sum).unwrap().average(), Some(1.5));
    }

    #[test]
    fn codec_preserves_populated_float_bits() {
        for bits in [
            0.0f64.to_bits(),
            (-0.0f64).to_bits(),
            f64::INFINITY.to_bits(),
            f64::NEG_INFINITY.to_bits(),
            0x7ff8_0000_0000_0001,
            0x7ff0_0000_0000_0001,
        ] {
            let encoded = state(1, f64::from_bits(bits));
            assert_eq!(
                AvgState::decode(&encoded).unwrap().encode().as_slice(),
                encoded
            );
        }
    }

    #[test]
    fn distinct_is_rejected() {
        let udf = AvgAccumulator::state_udf_impl();
        let schema = arrow_schema::Schema::empty();
        let expr = physical_lit(1.0f64);
        let field = Arc::new(arrow_schema::Field::new("in", DataType::Float64, true));
        let args = AccumulatorArgs {
            return_field: Arc::new(arrow_schema::Field::new("out", DataType::Binary, true)),
            schema: &schema,
            ignore_nulls: false,
            order_bys: &[],
            is_reversed: false,
            name: AVG_STATE_NAME,
            is_distinct: true,
            exprs: std::slice::from_ref(&expr),
            expr_fields: std::slice::from_ref(&field),
        };
        assert!(udf.accumulator(args).is_err());
    }

    #[test]
    fn state_counts_nulls_and_empty_is_canonical() {
        let mut accumulator = AvgAccumulator::default();
        accumulator
            .update_batch(&[Arc::new(Float64Array::from(vec![None, None]))])
            .unwrap();
        assert_eq!(accumulator.state.encode(), AvgState::default().encode());
        accumulator
            .update_batch(&[Arc::new(Float64Array::from(vec![
                Some(1.0),
                None,
                Some(3.0),
                Some(8.0),
            ]))])
            .unwrap();
        assert_eq!(accumulator.state.count(), 3);
        assert_eq!(accumulator.state.average(), Some(4.0));
    }

    #[test]
    fn merge_preserves_populated_negative_zero_for_empty_input() {
        let mut accumulator = AvgAccumulator {
            state: AvgState {
                count: 1,
                sum: -0.0,
            },
            input: InputKind::Binary,
        };
        let expected = accumulator.state.encode();
        accumulator
            .update_batch(&[Arc::new(BinaryArray::from(vec![
                None,
                Some(AvgState::default().encode().as_slice()),
            ]))])
            .unwrap();
        assert_eq!(accumulator.state.encode(), expected);
    }

    #[test]
    fn merge_ignores_nulls_and_merges_weighted_states() {
        let mut accumulator = AvgAccumulator {
            state: AvgState::default(),
            input: InputKind::Binary,
        };
        accumulator
            .update_batch(&[Arc::new(BinaryArray::from(vec![
                Some(state(2, 4.0).as_slice()),
                None,
                Some(state(3, 15.0).as_slice()),
            ]))])
            .unwrap();
        assert_eq!(accumulator.state.count(), 5);
        assert_eq!(accumulator.state.average(), Some(19.0 / 5.0));
        let before = accumulator.state;
        assert!(
            accumulator
                .update_batch(&[Arc::new(BinaryArray::from(vec![Some(&[][..])]))])
                .is_err()
        );
        assert_eq!(accumulator.state, before);
    }

    #[test]
    fn overflow_does_not_mutate_update_or_merge() {
        let mut update = AvgAccumulator {
            state: AvgState {
                count: u64::MAX,
                sum: 1.0,
            },
            input: InputKind::Float64,
        };
        let before = update.state;
        assert!(
            update
                .update_batch(&[Arc::new(Float64Array::from(vec![Some(2.0)]))])
                .is_err()
        );
        assert_eq!(update.state, before);

        let mut merge = AvgAccumulator {
            state: AvgState {
                count: u64::MAX,
                sum: 1.0,
            },
            input: InputKind::Binary,
        };
        let before = merge.state;
        assert!(
            merge
                .update_batch(&[Arc::new(BinaryArray::from(vec![Some(
                    state(1, 2.0).as_slice()
                )]))])
                .is_err()
        );
        assert_eq!(merge.state, before);
    }

    #[test]
    fn registered_delta_merge_has_four_way_and_malformed_behavior() {
        let udf = FUNCTION_REGISTRY
            .get_aggr_func(&aggr_delta_merge_func_name(AVG_STATE_NAME))
            .unwrap();
        assert_eq!(udf.name(), "__avg_state_delta_merge");
        assert_eq!(
            udf.signature().type_signature,
            TypeSignature::Exact(vec![DataType::Binary, DataType::Binary])
        );
        let schema = Arc::new(arrow_schema::Schema::new(vec![
            arrow_schema::Field::new("delta", DataType::Binary, true),
            arrow_schema::Field::new("persisted", DataType::Binary, true),
        ]));
        let expr = AggregateExprBuilder::new(
            Arc::new(udf),
            vec![
                Arc::new(Column::new("delta", 0)),
                Arc::new(Column::new("persisted", 1)),
            ],
        )
        .schema(schema)
        .alias("avg_delta_merge")
        .build()
        .unwrap();
        let delta = state(2, 3.0);
        let persisted = state(2, 7.0);
        for (left, right, expected) in [
            (
                Some(delta.as_slice()),
                None,
                AvgState { count: 2, sum: 3.0 }.encode(),
            ),
            (
                None,
                Some(persisted.as_slice()),
                AvgState { count: 2, sum: 7.0 }.encode(),
            ),
            (None, None, AvgState::default().encode()),
            (
                Some(delta.as_slice()),
                Some(persisted.as_slice()),
                AvgState {
                    count: 4,
                    sum: 10.0,
                }
                .encode(),
            ),
        ] {
            let mut accumulator = expr.create_accumulator().unwrap();
            accumulator
                .update_batch(&[
                    Arc::new(BinaryArray::from(vec![left])),
                    Arc::new(BinaryArray::from(vec![right])),
                ])
                .unwrap();
            let ScalarValue::Binary(Some(actual)) = accumulator.evaluate().unwrap() else {
                panic!("AVG delta merge state must be binary");
            };
            assert_eq!(actual.as_slice(), expected.as_slice());
        }
        let mut accumulator = expr.create_accumulator().unwrap();
        assert!(
            accumulator
                .update_batch(&[
                    Arc::new(BinaryArray::from(vec![Some(&[][..])])),
                    Arc::new(BinaryArray::from(vec![None])),
                ])
                .is_err()
        );
        let mut accumulator = expr.create_accumulator().unwrap();
        assert!(
            accumulator
                .update_batch(&[
                    Arc::new(BinaryArray::from(vec![None])),
                    Arc::new(BinaryArray::from(vec![Some(&[][..])])),
                ])
                .is_err()
        );
        let mut accumulator = expr.create_accumulator().unwrap();
        accumulator
            .update_batch(&[
                Arc::new(BinaryArray::from(vec![Some(delta.as_slice())])),
                Arc::new(BinaryArray::from(vec![None])),
            ])
            .unwrap();
        assert!(
            accumulator
                .update_batch(&[
                    Arc::new(BinaryArray::from(vec![Some(delta.as_slice())])),
                    Arc::new(BinaryArray::from(vec![Some(
                        AvgState {
                            count: u64::MAX,
                            sum: 1.0
                        }
                        .encode()
                        .as_slice()
                    )])),
                ])
                .is_err()
        );
    }

    #[test]
    fn avg_registry_does_not_replace_native_state_registry() {
        let avg = FUNCTION_REGISTRY.get_aggr_func(AVG_STATE_NAME).unwrap();
        let native = FUNCTION_REGISTRY
            .get_aggr_func(&aggr_state_func_name("avg"))
            .unwrap();
        assert_eq!(
            avg.return_type(&[DataType::Float64]).unwrap(),
            DataType::Binary
        );
        assert!(matches!(
            native.return_type(&[DataType::Float64]).unwrap(),
            DataType::Struct(_)
        ));
    }
}
