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

//! Implementation of the scalar functions `uddsketch_calc` and
//! `uddsketch_merge_state`.

use std::fmt;
use std::fmt::Display;
use std::sync::Arc;

use datafusion::logical_expr::Accumulator as _;
use datafusion_common::DataFusionError;
use datafusion_common::arrow::array::{Array, AsArray, BinaryArray, BinaryBuilder, Float64Builder};
use datafusion_common::arrow::datatypes::{DataType, Float64Type, Int64Type};
use datafusion_expr::{ColumnarValue, ScalarFunctionArgs, Signature, Volatility};
use uddsketch::UDDSketch;

use crate::aggrs::approximate::uddsketch::{UDDSKETCH_MERGE_STATE_NAME, UddSketchState};
use crate::function::{Function, extract_args};
use crate::function_registry::FunctionRegistry;

const NAME: &str = "uddsketch_calc";

/// UddSketchCalcFunction implements the scalar function `uddsketch_calc`.
///
/// It accepts two arguments:
/// 1. A percentile (as f64) for which to compute the estimated quantile (e.g. 0.95 for p95).
/// 2. The serialized UDDSketch state, as produced by the aggregator (binary).
///
/// For each row, it deserializes the sketch and returns the computed quantile value.
#[derive(Debug)]
pub(crate) struct UddSketchCalcFunction {
    signature: Signature,
}

impl UddSketchCalcFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register_scalar(UddSketchCalcFunction::default());
    }
}

impl Default for UddSketchCalcFunction {
    fn default() -> Self {
        Self {
            // First argument: percentile (float64)
            // Second argument: UDDSketch state (binary)
            signature: Signature::exact(
                vec![DataType::Float64, DataType::Binary],
                Volatility::Immutable,
            ),
        }
    }
}

impl Display for UddSketchCalcFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", NAME.to_ascii_uppercase())
    }
}

impl Function for UddSketchCalcFunction {
    fn name(&self) -> &str {
        NAME
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Float64)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [arg0, arg1] = extract_args(self.name(), &args)?;

        let Some(percentages) = arg0.as_primitive_opt::<Float64Type>() else {
            return Err(DataFusionError::Execution(format!(
                "'{}' expects 1st argument to be Float64 datatype, got {}",
                self.name(),
                arg0.data_type()
            )));
        };
        let Some(sketch_vec) = arg1.as_binary_opt::<i32>() else {
            return Err(DataFusionError::Execution(format!(
                "'{}' expects 2nd argument to be Binary datatype, got {}",
                self.name(),
                arg1.data_type()
            )));
        };
        let len = sketch_vec.len();
        let mut builder = Float64Builder::with_capacity(len);

        for i in 0..len {
            let perc_opt = percentages.is_valid(i).then(|| percentages.value(i));
            let sketch_opt = sketch_vec.is_valid(i).then(|| sketch_vec.value(i));

            if sketch_opt.is_none() || perc_opt.is_none() {
                builder.append_null();
                continue;
            }

            let sketch_bytes = sketch_opt.unwrap();
            let perc = perc_opt.unwrap();

            // Deserialize the UDDSketch from its bincode representation
            let sketch: UDDSketch = match bincode::deserialize(sketch_bytes) {
                Ok(s) => s,
                Err(e) => {
                    common_telemetry::trace!("Failed to deserialize UDDSketch: {}", e);
                    builder.append_null();
                    continue;
                }
            };

            // Check if the sketch is empty, if so, return null
            // This is important to avoid panics when calling estimate_quantile on an empty sketch
            // In practice, this will happen if input is all null
            if sketch.bucket_iter().count() == 0 {
                builder.append_null();
                continue;
            }
            // `estimate_quantile` asserts the percentile is in [0, 1]; validate
            // it up front so an out-of-range percentile is a query error
            // instead of a panic.
            if !(0.0..=1.0).contains(&perc) {
                return Err(DataFusionError::Execution(format!(
                    "'{}' expects percentile in [0.0, 1.0], got {perc}",
                    self.name()
                )));
            }
            // Compute the estimated quantile from the sketch
            let result = sketch.estimate_quantile(perc);
            builder.append_value(result);
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

/// UddSketchMergeStateFunction implements the scalar function
/// `uddsketch_merge_state`.
///
/// It accepts four arguments:
/// 1. Bucket size (as int64), same as `uddsketch_state`'s first argument.
/// 2. Error rate (as float64), same as `uddsketch_state`'s second argument.
/// 3. The first serialized UDDSketch state (binary), e.g. a delta state.
/// 4. The second serialized UDDSketch state (binary), e.g. an existing sink state.
///
/// It merges the two states into one and returns the merged state (binary).
/// The bucket size and error rate must match the parameters used to create
/// both input states; otherwise the merge fails loudly instead of silently
/// producing a wrong state.
#[derive(Debug)]
pub(crate) struct UddSketchMergeStateFunction {
    signature: Signature,
}

impl UddSketchMergeStateFunction {
    pub fn register(registry: &FunctionRegistry) {
        registry.register_scalar(UddSketchMergeStateFunction::default());
    }
}

impl Default for UddSketchMergeStateFunction {
    fn default() -> Self {
        Self {
            signature: Signature::exact(
                vec![
                    DataType::Int64,
                    DataType::Float64,
                    DataType::Binary,
                    DataType::Binary,
                ],
                Volatility::Immutable,
            ),
        }
    }
}

impl Display for UddSketchMergeStateFunction {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        write!(f, "{}", UDDSKETCH_MERGE_STATE_NAME.to_ascii_uppercase())
    }
}

impl Function for UddSketchMergeStateFunction {
    fn name(&self) -> &str {
        UDDSKETCH_MERGE_STATE_NAME
    }

    fn return_type(&self, _: &[DataType]) -> datafusion_common::Result<DataType> {
        Ok(DataType::Binary)
    }

    fn signature(&self) -> &Signature {
        &self.signature
    }

    fn invoke_with_args(
        &self,
        args: ScalarFunctionArgs,
    ) -> datafusion_common::Result<ColumnarValue> {
        let [arg0, arg1, arg2, arg3] = extract_args(self.name(), &args)?;

        let bucket_size_arr = match arg0.as_primitive_opt::<Int64Type>() {
            Some(arr) => arr,
            None => {
                return Err(DataFusionError::Execution(format!(
                    "'{}' expects 1st argument to be Int64 datatype, got {}",
                    self.name(),
                    arg0.data_type()
                )));
            }
        };
        let error_rate_arr = match arg1.as_primitive_opt::<Float64Type>() {
            Some(arr) => arr,
            None => {
                return Err(DataFusionError::Execution(format!(
                    "'{}' expects 2nd argument to be Float64 datatype, got {}",
                    self.name(),
                    arg1.data_type()
                )));
            }
        };
        let Some(delta_states) = arg2.as_binary_opt::<i32>() else {
            return Err(DataFusionError::Execution(format!(
                "'{}' expects 3rd argument to be Binary datatype, got {}",
                self.name(),
                arg2.data_type()
            )));
        };
        let Some(sink_states) = arg3.as_binary_opt::<i32>() else {
            return Err(DataFusionError::Execution(format!(
                "'{}' expects 4th argument to be Binary datatype, got {}",
                self.name(),
                arg3.data_type()
            )));
        };

        let len = delta_states.len().max(sink_states.len());
        if len == 0 {
            // Empty batch: nothing to merge, no parameters to validate.
            return Ok(ColumnarValue::Array(Arc::new(
                BinaryBuilder::new().finish(),
            )));
        }

        // The bucket size and error rate describe every state being merged, so
        // they must be non-null, valid, and identical across all rows. Reading
        // them per row keeps the function safe on arbitrary-length batches and
        // rejects mixed parameters instead of silently merging mismatched
        // states.
        let mut bucket_size: i64 = 0;
        let mut error_rate: f64 = 0.0;
        let mut params_set = false;
        for i in 0..len {
            if i >= bucket_size_arr.len() || i >= error_rate_arr.len() {
                return Err(DataFusionError::Execution(format!(
                    "'{}' expects bucket size and error rate arguments to cover every row",
                    self.name()
                )));
            }
            if !bucket_size_arr.is_valid(i) || !error_rate_arr.is_valid(i) {
                return Err(DataFusionError::Execution(format!(
                    "'{}' expects non-null bucket size and error rate arguments",
                    self.name()
                )));
            }
            let row_bucket_size = bucket_size_arr.value(i);
            let row_error_rate = error_rate_arr.value(i);
            if row_bucket_size <= 0 {
                return Err(DataFusionError::Execution(format!(
                    "'{}' expects bucket size to be positive, got {row_bucket_size}",
                    self.name()
                )));
            }
            if !(1e-12..1.0).contains(&row_error_rate) {
                return Err(DataFusionError::Execution(format!(
                    "'{}' expects error rate in [1e-12, 1.0), got {row_error_rate}",
                    self.name()
                )));
            }
            if params_set && (row_bucket_size != bucket_size || row_error_rate != error_rate) {
                return Err(DataFusionError::Execution(format!(
                    "'{}' expects constant bucket size and error rate across all rows, got ({row_bucket_size}, {row_error_rate}) after ({bucket_size}, {error_rate})",
                    self.name()
                )));
            }
            if !params_set {
                bucket_size = row_bucket_size;
                error_rate = row_error_rate;
                params_set = true;
            }
        }

        // Validates non-null state bytes against the merge parameters by
        // running them through the same deserialization and parameter check
        // the two-state merge path uses. Single-side NULL rows used to pass
        // their bytes through unchecked, letting corrupt or mismatched state
        // slip into the sink; now they are validated exactly like a merge.
        let validate_state = |state: &[u8]| -> datafusion_common::Result<()> {
            let mut acc = UddSketchState::new(bucket_size as u64, error_rate);
            let states = BinaryArray::from_opt_vec(vec![Some(state)]);
            acc.merge_batch(&[Arc::new(states)]).map_err(|e| {
                DataFusionError::Execution(format!(
                    "'{}' failed to merge UDDSketch states: {}",
                    self.name(),
                    e
                ))
            })
        };

        let mut builder = BinaryBuilder::with_capacity(len, 0);
        for i in 0..len {
            let delta = if i < delta_states.len() {
                delta_states.is_valid(i).then(|| delta_states.value(i))
            } else {
                None
            };
            let sink = if i < sink_states.len() {
                sink_states.is_valid(i).then(|| sink_states.value(i))
            } else {
                None
            };
            match (delta, sink) {
                (None, None) => builder.append_null(),
                (Some(delta), None) => {
                    validate_state(delta)?;
                    builder.append_value(delta);
                }
                (None, Some(sink)) => {
                    validate_state(sink)?;
                    builder.append_value(sink);
                }
                (Some(delta), Some(sink)) => {
                    let mut acc = UddSketchState::new(bucket_size as u64, error_rate);
                    let states = BinaryArray::from_opt_vec(vec![Some(delta), Some(sink)]);
                    acc.merge_batch(&[Arc::new(states)]).map_err(|e| {
                        DataFusionError::Execution(format!(
                            "'{}' failed to merge UDDSketch states: {}",
                            self.name(),
                            e
                        ))
                    })?;
                    let datafusion_common::ScalarValue::Binary(Some(bytes)) =
                        acc.evaluate().map_err(|e| {
                            DataFusionError::Execution(format!(
                                "'{}' failed to evaluate merged UDDSketch state: {}",
                                self.name(),
                                e
                            ))
                        })?
                    else {
                        return Err(DataFusionError::Execution(format!(
                            "'{}' expected a binary merged UDDSketch state",
                            self.name()
                        )));
                    };
                    builder.append_value(&bytes);
                }
            }
        }

        Ok(ColumnarValue::Array(Arc::new(builder.finish())))
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow_schema::Field;
    use datafusion_common::ScalarValue;
    use datafusion_common::arrow::array::{
        ArrayRef, AsArray, BinaryArray, Float64Array, Int64Array,
    };
    use datafusion_common::arrow::datatypes::Float64Type;

    use super::*;

    /// Builds a serialized `uddsketch_state` state from the given values.
    fn make_state(bucket_size: u64, error_rate: f64, values: &[f64]) -> Vec<u8> {
        let mut acc = UddSketchState::new(bucket_size, error_rate);
        let values: ArrayRef = Arc::new(Float64Array::from(values.to_vec()));
        acc.update_batch(&[values.clone(), values.clone(), values])
            .unwrap();
        let ScalarValue::Binary(Some(bytes)) = acc.evaluate().unwrap() else {
            unreachable!()
        };
        bytes
    }

    /// Invokes `uddsketch_merge_state` on a single row.
    fn invoke_merge_state(
        function: &UddSketchMergeStateFunction,
        bucket_size: i64,
        error_rate: f64,
        delta: Option<&[u8]>,
        sink: Option<&[u8]>,
    ) -> datafusion_common::Result<ColumnarValue> {
        function.invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Int64Array::from(vec![bucket_size]))),
                ColumnarValue::Array(Arc::new(Float64Array::from(vec![error_rate]))),
                ColumnarValue::Array(Arc::new(BinaryArray::from_opt_vec(vec![delta]))),
                ColumnarValue::Array(Arc::new(BinaryArray::from_opt_vec(vec![sink]))),
            ],
            arg_fields: vec![],
            number_rows: 1,
            return_field: Arc::new(Field::new("x", DataType::Binary, false)),
            config_options: Arc::new(Default::default()),
        })
    }

    #[test]
    fn test_uddsketch_calc_function() {
        let function = UddSketchCalcFunction::default();
        assert_eq!("uddsketch_calc", function.name());
        assert_eq!(
            DataType::Float64,
            function.return_type(&[DataType::Float64]).unwrap()
        );

        // Create a test sketch
        let mut sketch = UDDSketch::new(128, 0.01);
        sketch.add_value(10.0);
        sketch.add_value(20.0);
        sketch.add_value(30.0);
        sketch.add_value(40.0);
        sketch.add_value(50.0);
        sketch.add_value(60.0);
        sketch.add_value(70.0);
        sketch.add_value(80.0);
        sketch.add_value(90.0);
        sketch.add_value(100.0);

        // Get expected values directly from the sketch
        let expected_p50 = sketch.estimate_quantile(0.5);
        let expected_p90 = sketch.estimate_quantile(0.9);
        let expected_p95 = sketch.estimate_quantile(0.95);

        let serialized = bincode::serialize(&sketch).unwrap();
        let percentiles = vec![0.5, 0.9, 0.95];

        let args = vec![
            ColumnarValue::Array(Arc::new(Float64Array::from(percentiles.clone()))),
            ColumnarValue::Array(Arc::new(BinaryArray::from_iter_values(vec![serialized; 3]))),
        ];

        let result = function
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields: vec![],
                number_rows: 3,
                return_field: Arc::new(Field::new("x", DataType::Float64, false)),
                config_options: Arc::new(Default::default()),
            })
            .unwrap();
        let ColumnarValue::Array(result) = result else {
            unreachable!()
        };
        let result = result.as_primitive::<Float64Type>();
        assert_eq!(result.len(), 3);

        // Test median (p50)
        assert!((result.value(0) - expected_p50).abs() < 1e-10);
        // Test p90
        assert!((result.value(1) - expected_p90).abs() < 1e-10);
        // Test p95
        assert!((result.value(2) - expected_p95).abs() < 1e-10);
    }

    #[test]
    fn test_uddsketch_merge_state_function() {
        use datafusion::logical_expr::Accumulator as _;

        let mut state1 = UddSketchState::new(10, 0.01);
        let values1: ArrayRef = Arc::new(Float64Array::from(vec![1.0]));
        state1
            .update_batch(&[values1.clone(), values1.clone(), values1])
            .unwrap();
        let state1_binary = state1.evaluate().unwrap();

        let mut state2 = UddSketchState::new(10, 0.01);
        let values2: ArrayRef = Arc::new(Float64Array::from(vec![2.0]));
        state2
            .update_batch(&[values2.clone(), values2.clone(), values2])
            .unwrap();
        let state2_binary = state2.evaluate().unwrap();

        let function = UddSketchMergeStateFunction::default();
        assert_eq!("uddsketch_merge_state", function.name());
        assert_eq!(DataType::Binary, function.return_type(&[]).unwrap());

        let ScalarValue::Binary(Some(state1_bytes)) = state1_binary else {
            unreachable!()
        };
        let ScalarValue::Binary(Some(state2_bytes)) = state2_binary else {
            unreachable!()
        };
        let args = vec![
            ColumnarValue::Array(Arc::new(datafusion_common::arrow::array::Int64Array::from(
                vec![10],
            ))),
            ColumnarValue::Array(Arc::new(Float64Array::from(vec![0.01]))),
            ColumnarValue::Array(Arc::new(BinaryArray::from_opt_vec(vec![Some(
                state1_bytes.as_slice(),
            )]))),
            ColumnarValue::Array(Arc::new(BinaryArray::from_opt_vec(vec![Some(
                state2_bytes.as_slice(),
            )]))),
        ];

        let result = function
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields: vec![],
                number_rows: 1,
                return_field: Arc::new(Field::new("x", DataType::Binary, false)),
                config_options: Arc::new(Default::default()),
            })
            .unwrap();
        let ColumnarValue::Array(result) = result else {
            unreachable!()
        };
        let result = result.as_binary::<i32>();
        assert_eq!(result.len(), 1);
        let merged: uddsketch::UDDSketch = bincode::deserialize(result.value(0)).unwrap();
        assert_eq!(merged.count(), 2);
        assert!((merged.sum() - 3.0).abs() < 1e-10);
    }

    #[test]
    fn test_uddsketch_merge_state_function_rejects_parameter_mismatch() {
        use datafusion::logical_expr::Accumulator as _;

        // state1 uses bucket size 10 while the merge call uses bucket size 20:
        // the merge must fail loudly instead of silently producing a wrong state.
        let mut state1 = UddSketchState::new(10, 0.01);
        let values: ArrayRef = Arc::new(Float64Array::from(vec![1.0]));
        state1
            .update_batch(&[values.clone(), values.clone(), values])
            .unwrap();
        let ScalarValue::Binary(Some(state1_bytes)) = state1.evaluate().unwrap() else {
            unreachable!()
        };

        let function = UddSketchMergeStateFunction::default();
        let args = vec![
            ColumnarValue::Array(Arc::new(datafusion_common::arrow::array::Int64Array::from(
                vec![20],
            ))),
            ColumnarValue::Array(Arc::new(Float64Array::from(vec![0.01]))),
            ColumnarValue::Array(Arc::new(BinaryArray::from_opt_vec(vec![Some(
                state1_bytes.as_slice(),
            )]))),
            ColumnarValue::Array(Arc::new(BinaryArray::from_opt_vec(vec![Some(
                state1_bytes.as_slice(),
            )]))),
        ];
        let result = function.invoke_with_args(ScalarFunctionArgs {
            args,
            arg_fields: vec![],
            number_rows: 1,
            return_field: Arc::new(Field::new("x", DataType::Binary, false)),
            config_options: Arc::new(Default::default()),
        });
        assert!(
            result.is_err(),
            "merging states with different parameters must fail loudly"
        );
    }

    #[test]
    fn test_uddsketch_calc_function_errors() {
        let function = UddSketchCalcFunction::default();

        // Test with invalid number of arguments
        let result = function.invoke_with_args(ScalarFunctionArgs {
            args: vec![ColumnarValue::Array(Arc::new(Float64Array::from(vec![
                0.95,
            ])))],
            arg_fields: vec![],
            number_rows: 0,
            return_field: Arc::new(Field::new("x", DataType::Float64, false)),
            config_options: Arc::new(Default::default()),
        });
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("Execution error: uddsketch_calc function requires 2 arguments, got 1")
        );

        // Test with invalid binary data
        let args = vec![
            ColumnarValue::Array(Arc::new(Float64Array::from(vec![0.95]))),
            ColumnarValue::Array(Arc::new(BinaryArray::from_iter(vec![Some(vec![1, 2, 3])]))),
        ];
        let result = function
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields: vec![],
                number_rows: 0,
                return_field: Arc::new(Field::new("x", DataType::Float64, false)),
                config_options: Arc::new(Default::default()),
            })
            .unwrap();
        let ColumnarValue::Array(result) = result else {
            unreachable!()
        };
        let result = result.as_primitive::<Float64Type>();
        assert_eq!(result.len(), 1);
        assert!(result.is_null(0));
    }

    #[test]
    fn test_uddsketch_calc_function_null_and_empty_sketch() {
        let function = UddSketchCalcFunction::default();
        let state = make_state(10, 0.01, &[1.0, 2.0]);
        let empty = make_state(10, 0.01, &[]);

        // NULL percentile, NULL state, and empty sketch all produce NULL;
        // a valid percentile over a non-empty sketch produces a value.
        let args = vec![
            ColumnarValue::Array(Arc::new(Float64Array::from(vec![
                Some(0.5),
                None,
                Some(0.5),
                Some(0.5),
            ]))),
            ColumnarValue::Array(Arc::new(BinaryArray::from_opt_vec(vec![
                Some(state.as_slice()),
                Some(state.as_slice()),
                None,
                Some(empty.as_slice()),
            ]))),
        ];
        let result = function
            .invoke_with_args(ScalarFunctionArgs {
                args,
                arg_fields: vec![],
                number_rows: 4,
                return_field: Arc::new(Field::new("x", DataType::Float64, false)),
                config_options: Arc::new(Default::default()),
            })
            .unwrap();
        let ColumnarValue::Array(result) = result else {
            unreachable!()
        };
        let result = result.as_primitive::<Float64Type>();
        assert_eq!(result.len(), 4);
        assert!(
            result.is_valid(0),
            "valid percentile should produce a value"
        );
        assert!(result.is_null(1), "NULL percentile should produce NULL");
        assert!(result.is_null(2), "NULL state should produce NULL");
        assert!(result.is_null(3), "empty sketch should produce NULL");
    }

    #[test]
    fn test_uddsketch_calc_function_rejects_out_of_range_percentile() {
        let function = UddSketchCalcFunction::default();
        let state = make_state(10, 0.01, &[1.0, 2.0]);

        for percentile in [-0.1f64, 1.1] {
            let result = function.invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(Arc::new(Float64Array::from(vec![percentile]))),
                    ColumnarValue::Array(Arc::new(BinaryArray::from_opt_vec(vec![Some(
                        state.as_slice(),
                    )]))),
                ],
                arg_fields: vec![],
                number_rows: 1,
                return_field: Arc::new(Field::new("x", DataType::Float64, false)),
                config_options: Arc::new(Default::default()),
            });
            assert!(
                result.is_err(),
                "percentile {percentile} must be rejected instead of panicking"
            );
        }
    }

    #[test]
    fn test_uddsketch_merge_state_function_empty_batch_is_safe() {
        let function = UddSketchMergeStateFunction::default();
        let result = function
            .invoke_with_args(ScalarFunctionArgs {
                args: vec![
                    ColumnarValue::Array(Arc::new(Int64Array::from(Vec::<i64>::new()))),
                    ColumnarValue::Array(Arc::new(Float64Array::from(Vec::<f64>::new()))),
                    ColumnarValue::Array(Arc::new(BinaryArray::from_opt_vec(
                        Vec::<Option<&[u8]>>::new(),
                    ))),
                    ColumnarValue::Array(Arc::new(BinaryArray::from_opt_vec(
                        Vec::<Option<&[u8]>>::new(),
                    ))),
                ],
                arg_fields: vec![],
                number_rows: 0,
                return_field: Arc::new(Field::new("x", DataType::Binary, false)),
                config_options: Arc::new(Default::default()),
            })
            .unwrap();
        let ColumnarValue::Array(result) = result else {
            unreachable!()
        };
        let result = result.as_binary::<i32>();
        assert_eq!(result.len(), 0);
    }

    #[test]
    fn test_uddsketch_merge_state_function_null_handling() {
        let function = UddSketchMergeStateFunction::default();
        let state = make_state(10, 0.01, &[1.0, 2.0]);

        // NULL/NULL -> NULL
        let result = invoke_merge_state(&function, 10, 0.01, None, None)
            .unwrap()
            .into_array(1)
            .unwrap();
        let result = result.as_binary::<i32>();
        assert_eq!(result.len(), 1);
        assert!(result.is_null(0));

        // delta NULL -> sink state passes through
        let result = invoke_merge_state(&function, 10, 0.01, None, Some(state.as_slice()))
            .unwrap()
            .into_array(1)
            .unwrap();
        let result = result.as_binary::<i32>();
        assert_eq!(result.value(0), state.as_slice());

        // sink NULL -> delta state passes through
        let result = invoke_merge_state(&function, 10, 0.01, Some(state.as_slice()), None)
            .unwrap()
            .into_array(1)
            .unwrap();
        let result = result.as_binary::<i32>();
        assert_eq!(result.value(0), state.as_slice());
    }

    #[test]
    fn test_uddsketch_merge_state_function_null_validates_present_state() {
        let function = UddSketchMergeStateFunction::default();

        // A single-side NULL must still validate the present state: corrupt
        // bytes must error instead of passing through untouched.
        let result = invoke_merge_state(&function, 10, 0.01, Some(b"corrupt"), None);
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("failed to merge UDDSketch states"),
            "corrupt delta with NULL sink must fail loudly: {err}"
        );
        let result = invoke_merge_state(&function, 10, 0.01, None, Some(b"corrupt"));
        assert!(
            result.is_err(),
            "corrupt sink with NULL delta must fail loudly"
        );

        // Mismatched parameters with a NULL side must error as well.
        let mismatch_bucket = make_state(20, 0.01, &[1.0]);
        let result =
            invoke_merge_state(&function, 10, 0.01, Some(mismatch_bucket.as_slice()), None);
        assert!(
            result.is_err(),
            "bucket size mismatch with NULL sink must fail loudly"
        );
        let result =
            invoke_merge_state(&function, 10, 0.01, None, Some(mismatch_bucket.as_slice()));
        assert!(
            result.is_err(),
            "bucket size mismatch with NULL delta must fail loudly"
        );

        let mismatch_error = make_state(10, 0.05, &[1.0]);
        let result = invoke_merge_state(&function, 10, 0.01, Some(mismatch_error.as_slice()), None);
        assert!(
            result.is_err(),
            "error rate mismatch with NULL sink must fail loudly"
        );
        let result = invoke_merge_state(&function, 10, 0.01, None, Some(mismatch_error.as_slice()));
        assert!(
            result.is_err(),
            "error rate mismatch with NULL delta must fail loudly"
        );

        // A valid state with a NULL side returns the state's sketch: it must
        // match merging the state with an empty sketch.
        let state = make_state(10, 0.01, &[1.0, 2.0]);
        let empty = make_state(10, 0.01, &[]);
        for (delta, sink) in [
            (Some(state.as_slice()), None),
            (None, Some(state.as_slice())),
        ] {
            let result = invoke_merge_state(&function, 10, 0.01, delta, sink)
                .unwrap()
                .into_array(1)
                .unwrap();
            let result = result.as_binary::<i32>();
            assert!(
                result.is_valid(0),
                "valid state with NULL side should produce a value"
            );
            let merged: uddsketch::UDDSketch = bincode::deserialize(result.value(0)).unwrap();
            assert_eq!(merged.count(), 2);
            assert!((merged.sum() - 3.0).abs() < 1e-10);

            // Equal to merging the state with an empty sketch.
            let expected = invoke_merge_state(
                &function,
                10,
                0.01,
                Some(state.as_slice()),
                Some(empty.as_slice()),
            )
            .unwrap()
            .into_array(1)
            .unwrap();
            let expected = expected.as_binary::<i32>();
            let expected: uddsketch::UDDSketch = bincode::deserialize(expected.value(0)).unwrap();
            assert_eq!(expected.count(), merged.count());
            assert!((expected.sum() - merged.sum()).abs() < 1e-10);
            for q in [0.1, 0.5, 0.9] {
                assert!(
                    (expected.estimate_quantile(q) - merged.estimate_quantile(q)).abs() < 1e-10,
                    "quantile {q} mismatch: expected={}, got={}",
                    expected.estimate_quantile(q),
                    merged.estimate_quantile(q)
                );
            }
        }
    }

    #[test]
    fn test_uddsketch_merge_state_function_merges_empty_and_nonempty() {
        let function = UddSketchMergeStateFunction::default();
        let empty = make_state(10, 0.01, &[]);
        let nonempty = make_state(10, 0.01, &[3.0, 4.0]);

        let result = invoke_merge_state(
            &function,
            10,
            0.01,
            Some(empty.as_slice()),
            Some(nonempty.as_slice()),
        )
        .unwrap()
        .into_array(1)
        .unwrap();
        let result = result.as_binary::<i32>();
        let merged: uddsketch::UDDSketch = bincode::deserialize(result.value(0)).unwrap();
        assert_eq!(merged.count(), 2);
        assert!((merged.sum() - 7.0).abs() < 1e-10);

        // Order must not matter.
        let result = invoke_merge_state(
            &function,
            10,
            0.01,
            Some(nonempty.as_slice()),
            Some(empty.as_slice()),
        )
        .unwrap()
        .into_array(1)
        .unwrap();
        let result = result.as_binary::<i32>();
        let merged: uddsketch::UDDSketch = bincode::deserialize(result.value(0)).unwrap();
        assert_eq!(merged.count(), 2);
        assert!((merged.sum() - 7.0).abs() < 1e-10);
    }

    #[test]
    fn test_uddsketch_merge_state_function_rejects_corrupt_state() {
        let function = UddSketchMergeStateFunction::default();
        let state = make_state(10, 0.01, &[1.0]);

        let result = invoke_merge_state(
            &function,
            10,
            0.01,
            Some(b"corrupt"),
            Some(state.as_slice()),
        );
        let err = result.unwrap_err().to_string();
        assert!(
            err.contains("failed to merge UDDSketch states"),
            "corrupt state bytes must fail loudly: {err}"
        );

        let result = invoke_merge_state(
            &function,
            10,
            0.01,
            Some(state.as_slice()),
            Some(b"corrupt"),
        );
        assert!(result.is_err());
    }

    #[test]
    fn test_uddsketch_merge_state_function_rejects_state_parameter_mismatch() {
        let function = UddSketchMergeStateFunction::default();

        // Left state built with bucket size 10, right state with bucket size 20.
        let left = make_state(10, 0.01, &[1.0]);
        let right = make_state(20, 0.01, &[2.0]);
        let result = invoke_merge_state(
            &function,
            10,
            0.01,
            Some(left.as_slice()),
            Some(right.as_slice()),
        );
        assert!(result.is_err(), "bucket size mismatch must fail loudly");
        let result = invoke_merge_state(
            &function,
            20,
            0.01,
            Some(left.as_slice()),
            Some(right.as_slice()),
        );
        assert!(result.is_err());

        // Left state built with error rate 0.01, right state with 0.05.
        let left = make_state(10, 0.01, &[1.0]);
        let right = make_state(10, 0.05, &[2.0]);
        let result = invoke_merge_state(
            &function,
            10,
            0.01,
            Some(left.as_slice()),
            Some(right.as_slice()),
        );
        assert!(result.is_err(), "error rate mismatch must fail loudly");
    }

    #[test]
    fn test_uddsketch_merge_state_function_rejects_invalid_params() {
        let function = UddSketchMergeStateFunction::default();
        let state = make_state(10, 0.01, &[1.0]);

        // bucket size must be positive (a negative value would wrap to a huge
        // u64 without the check)
        for bucket_size in [0i64, -1, -100] {
            let result = invoke_merge_state(
                &function,
                bucket_size,
                0.01,
                Some(state.as_slice()),
                Some(state.as_slice()),
            );
            assert!(
                result.is_err(),
                "bucket size {bucket_size} must be rejected"
            );
            assert!(
                result.unwrap_err().to_string().contains("bucket size"),
                "bucket size error should mention bucket size"
            );
        }

        // error rate must be in [1e-12, 1.0)
        for error_rate in [0.0f64, -0.1, 1.0, 2.0] {
            let result = invoke_merge_state(
                &function,
                10,
                error_rate,
                Some(state.as_slice()),
                Some(state.as_slice()),
            );
            assert!(result.is_err(), "error rate {error_rate} must be rejected");
            assert!(
                result.unwrap_err().to_string().contains("error rate"),
                "error rate error should mention error rate"
            );
        }
    }

    #[test]
    fn test_uddsketch_merge_state_function_rejects_null_or_short_params() {
        let function = UddSketchMergeStateFunction::default();
        let state = make_state(10, 0.01, &[1.0]);

        // NULL bucket size / error rate must be rejected, not interpreted.
        let result = function.invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Int64Array::from(vec![None]))),
                ColumnarValue::Array(Arc::new(Float64Array::from(vec![Some(0.01)]))),
                ColumnarValue::Array(Arc::new(BinaryArray::from_opt_vec(vec![Some(
                    state.as_slice(),
                )]))),
                ColumnarValue::Array(Arc::new(BinaryArray::from_opt_vec(vec![Some(
                    state.as_slice(),
                )]))),
            ],
            arg_fields: vec![],
            number_rows: 1,
            return_field: Arc::new(Field::new("x", DataType::Binary, false)),
            config_options: Arc::new(Default::default()),
        });
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("non-null bucket size and error rate"),
            "NULL parameters must be rejected"
        );

        // Parameters shorter than the state rows must be rejected without
        // panicking. The DataFusion argument-coercion framework enforces this
        // before `invoke_with_args` runs: `values_to_arrays` rejects argument
        // arrays of mixed lengths.
        let result = function.invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Int64Array::from(Vec::<i64>::new()))),
                ColumnarValue::Array(Arc::new(Float64Array::from(Vec::<f64>::new()))),
                ColumnarValue::Array(Arc::new(BinaryArray::from_opt_vec(vec![Some(
                    state.as_slice(),
                )]))),
                ColumnarValue::Array(Arc::new(BinaryArray::from_opt_vec(vec![Some(
                    state.as_slice(),
                )]))),
            ],
            arg_fields: vec![],
            number_rows: 1,
            return_field: Arc::new(Field::new("x", DataType::Binary, false)),
            config_options: Arc::new(Default::default()),
        });
        assert!(result.is_err());
        assert!(
            result.unwrap_err().to_string().contains("mixed length"),
            "parameter arrays that do not cover every row must be rejected"
        );
    }

    #[test]
    fn test_uddsketch_merge_state_function_rejects_non_constant_params() {
        let function = UddSketchMergeStateFunction::default();
        let state = make_state(10, 0.01, &[1.0]);

        // Two rows with different bucket sizes must be rejected instead of
        // silently merging with the first row's parameters.
        let result = function.invoke_with_args(ScalarFunctionArgs {
            args: vec![
                ColumnarValue::Array(Arc::new(Int64Array::from(vec![10, 20]))),
                ColumnarValue::Array(Arc::new(Float64Array::from(vec![0.01, 0.01]))),
                ColumnarValue::Array(Arc::new(BinaryArray::from_opt_vec(vec![
                    Some(state.as_slice()),
                    Some(state.as_slice()),
                ]))),
                ColumnarValue::Array(Arc::new(BinaryArray::from_opt_vec(vec![
                    Some(state.as_slice()),
                    Some(state.as_slice()),
                ]))),
            ],
            arg_fields: vec![],
            number_rows: 2,
            return_field: Arc::new(Field::new("x", DataType::Binary, false)),
            config_options: Arc::new(Default::default()),
        });
        assert!(result.is_err());
        assert!(
            result
                .unwrap_err()
                .to_string()
                .contains("constant bucket size and error rate"),
            "non-constant parameters must be rejected"
        );
    }
}
