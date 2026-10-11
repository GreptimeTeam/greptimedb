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

//! Implementation of [`predict_linear`](https://prometheus.io/docs/prometheus/latest/querying/functions/#predict_linear) in PromQL. Refer to the [original
//! implementation](https://github.com/prometheus/prometheus/blob/90b2f7a540b8a70d8d81372e6692dcbb67ccbaaa/promql/functions.go#L859-L872).

use std::sync::Arc;

use datafusion::arrow::array::{Float64Array, Float64Builder, TimestampMillisecondArray};
use datafusion::arrow::datatypes::TimeUnit;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::{ScalarUDF, Volatility};
use datafusion::physical_plan::ColumnarValue;
use datafusion_common::ScalarValue;
use datafusion_expr::create_udf;
use datatypes::arrow::array::Array;
use datatypes::arrow::datatypes::DataType;

use crate::error;
use crate::functions::{extract_range_array, linear_regression_slices};
use crate::range_array::RangeArray;

pub struct PredictLinear;

impl PredictLinear {
    pub const fn name() -> &'static str {
        "prom_predict_linear"
    }

    pub fn scalar_udf() -> ScalarUDF {
        let input_types = vec![
            // time index column
            RangeArray::convert_data_type(DataType::Timestamp(TimeUnit::Millisecond, None)),
            // value column
            RangeArray::convert_data_type(DataType::Float64),
            // t
            DataType::Int64,
            // evaluation timestamp
            DataType::Timestamp(TimeUnit::Millisecond, None),
        ];
        create_udf(
            Self::name(),
            input_types,
            DataType::Float64,
            Volatility::Volatile,
            Arc::new(Self::predict_linear) as _,
        )
    }

    fn predict_linear(input: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
        error::ensure(
            input.len() == 4,
            DataFusionError::Plan("prom_predict_linear function should have 4 inputs".to_string()),
        )?;

        let t_col = &input[2];
        let eval_ts_col = &input[3];

        let ts_range = extract_range_array(&input[0])?;
        let value_range = extract_range_array(&input[1])?;
        error::ensure(
            ts_range.len() == value_range.len(),
            DataFusionError::Execution(format!(
                "{}: input arrays should have the same length, found {} and {}",
                Self::name(),
                ts_range.len(),
                value_range.len()
            )),
        )?;
        error::ensure(
            ts_range.value_type() == DataType::Timestamp(TimeUnit::Millisecond, None),
            DataFusionError::Execution(format!(
                "{}: expect TimestampMillisecond as time index array's type, found {}",
                Self::name(),
                ts_range.value_type()
            )),
        )?;
        error::ensure(
            value_range.value_type() == DataType::Float64,
            DataFusionError::Execution(format!(
                "{}: expect Float64 as value array's type, found {}",
                Self::name(),
                value_range.value_type()
            )),
        )?;

        let t_iter: Box<dyn Iterator<Item = Option<i64>>> = match t_col {
            ColumnarValue::Scalar(t_scalar) => {
                let t = if let ScalarValue::Int64(Some(t_val)) = t_scalar {
                    *t_val
                } else {
                    // For `ScalarValue::Int64(None)` or other scalar types, returns NULL array,
                    // which conforms to PromQL's behavior.
                    let null_array = Float64Array::new_null(ts_range.len());
                    return Ok(ColumnarValue::Array(Arc::new(null_array)));
                };
                Box::new((0..ts_range.len()).map(move |_| Some(t)))
            }
            ColumnarValue::Array(t_array) => {
                let t_array = t_array
                    .as_any()
                    .downcast_ref::<datafusion::arrow::array::Int64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "{}: expect Int64 as t array's type, found {}",
                            Self::name(),
                            t_array.data_type()
                        ))
                    })?;
                error::ensure(
                    t_array.len() == ts_range.len(),
                    DataFusionError::Execution(format!(
                        "{}: t array should have the same length as other columns, found {} and {}",
                        Self::name(),
                        t_array.len(),
                        ts_range.len()
                    )),
                )?;

                Box::new(t_array.iter())
            }
        };
        // The evaluation instant the window above was folded for. It is the instant the regression
        // is centered on, which is *not* the last sample's timestamp: a window may end before the
        // step being evaluated, and an `@`-anchored window is replayed at every step while keeping
        // its own end. See `PromPlanner::create_range_eval_ts_expr`.
        let eval_ts_iter: Box<dyn Iterator<Item = Option<i64>>> = match eval_ts_col {
            ColumnarValue::Scalar(eval_ts_scalar) => {
                let eval_ts = match eval_ts_scalar {
                    ScalarValue::TimestampMillisecond(Some(eval_ts), _) => *eval_ts,
                    // For a NULL or otherwise unusable evaluation timestamp, returns NULL array,
                    // which conforms to PromQL's behavior.
                    _ => {
                        let null_array = Float64Array::new_null(ts_range.len());
                        return Ok(ColumnarValue::Array(Arc::new(null_array)));
                    }
                };
                Box::new((0..ts_range.len()).map(move |_| Some(eval_ts)))
            }
            ColumnarValue::Array(eval_ts_array) => {
                error::ensure(
                    eval_ts_array.len() == ts_range.len(),
                    DataFusionError::Execution(format!(
                        "{}: evaluation timestamp array should have the same length as other columns, found {} and {}",
                        Self::name(),
                        eval_ts_array.len(),
                        ts_range.len()
                    )),
                )?;
                match eval_ts_array.data_type() {
                    DataType::Timestamp(TimeUnit::Millisecond, _) => Box::new(
                        eval_ts_array
                            .as_any()
                            .downcast_ref::<TimestampMillisecondArray>()
                            .expect("checked by data type")
                            .iter(),
                    ),
                    other => {
                        return Err(DataFusionError::Execution(format!(
                            "{}: expect TimestampMillisecond as evaluation timestamp array's type, found {}",
                            Self::name(),
                            other
                        )));
                    }
                }
            }
        };
        let all_timestamps = ts_range
            .values()
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .values();
        let all_values = value_range
            .values()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap();
        let mut result_builder = Float64Builder::with_capacity(ts_range.len());
        for (index, (t, eval_ts)) in t_iter.zip(eval_ts_iter).enumerate() {
            // A step without an evaluation instant has nothing to extrapolate to.
            let Some(eval_ts) = eval_ts else {
                result_builder.append_null();
                continue;
            };
            match predict_linear_impl(
                &ts_range,
                &value_range,
                all_timestamps,
                all_values,
                index,
                t.unwrap(),
                eval_ts,
            )? {
                Some(value) => result_builder.append_value(value),
                None => result_builder.append_null(),
            }
        }

        let result = ColumnarValue::Array(Arc::new(result_builder.finish()));
        Ok(result)
    }
}

fn predict_linear_impl(
    ts_range: &RangeArray,
    value_range: &RangeArray,
    all_timestamps: &[i64],
    all_values: &Float64Array,
    index: usize,
    t: i64,
    eval_ts: i64,
) -> Result<Option<f64>, DataFusionError> {
    let (ts_offset, ts_len) = ts_range.get_offset_length(index).unwrap();
    let (value_offset, value_len) = value_range.get_offset_length(index).unwrap();
    error::ensure(
        ts_len == value_len,
        DataFusionError::Execution(format!(
            "{}: time and value arrays in a group should have the same length, found {} and {}",
            PredictLinear::name(),
            ts_len,
            value_len
        )),
    )?;
    if ts_len < 2 {
        return Ok(None);
    }

    // Like Prometheus, the regression is centered on the evaluation timestamp: the returned
    // intercept is the value the window's trend predicts at the step being evaluated, and `t` is
    // the horizon (in seconds) that is added on top of it. Centering on the last sample instead
    // would drop the distance between the window's end and the step, which is non-zero both
    // without `@` (the window can end before the step) and with it (the same anchored window is
    // replayed at every step).
    let (slope, intercept) = linear_regression_slices(
        all_timestamps,
        ts_offset,
        all_values,
        value_offset,
        value_len,
        eval_ts,
    );

    if slope.is_none() || intercept.is_none() {
        return Ok(None);
    }

    Ok(Some(slope.unwrap() * t as f64 + intercept.unwrap()))
}

#[cfg(test)]
mod test {
    use std::vec;

    use datafusion::arrow::array::{DictionaryArray, Int64Array};
    use datatypes::arrow::datatypes::Int64Type;

    use super::*;
    use crate::functions::test_util::simple_range_udf_runner;

    // build timestamp range and value range arrays for test
    fn build_test_range_arrays() -> (RangeArray, RangeArray) {
        let ts_array = Arc::new(TimestampMillisecondArray::from_iter(
            [
                0i64, 300, 600, 900, 1200, 1500, 1800, 2100, 2400, 2700, 3000,
            ]
            .into_iter()
            .map(Some),
        ));
        let ranges = [(0, 11)];

        let values_array = Arc::new(Float64Array::from_iter([
            0.0, 10.0, 20.0, 30.0, 40.0, 0.0, 10.0, 20.0, 30.0, 40.0, 50.0,
        ]));

        let ts_range_array = RangeArray::from_ranges(ts_array, ranges).unwrap();
        let value_range_array = RangeArray::from_ranges(values_array, ranges).unwrap();

        (ts_range_array, value_range_array)
    }

    /// The evaluation timestamp at which the window built by [`build_test_range_arrays`] ends.
    fn window_end_eval_ts() -> ScalarValue {
        ScalarValue::TimestampMillisecond(Some(3000), None)
    }

    #[test]
    fn calculate_predict_linear_none() {
        let ts_array = Arc::new(TimestampMillisecondArray::from_iter(
            [0i64].into_iter().map(Some),
        ));
        let ranges = [(0, 0), (0, 1)];
        let values_array = Arc::new(Float64Array::from_iter([0.0]));
        let ts_array = RangeArray::from_ranges(ts_array, ranges).unwrap();
        let value_array = RangeArray::from_ranges(values_array, ranges).unwrap();
        simple_range_udf_runner(
            PredictLinear::scalar_udf(),
            ts_array,
            value_array,
            vec![ScalarValue::Int64(Some(0)), window_end_eval_ts()],
            vec![None, None],
        );
    }

    #[test]
    fn calculate_predict_linear_test1() {
        let (ts_array, value_array) = build_test_range_arrays();
        simple_range_udf_runner(
            PredictLinear::scalar_udf(),
            ts_array,
            value_array,
            vec![ScalarValue::Int64(Some(0)), window_end_eval_ts()],
            // value at t = 0
            vec![Some(38.63636363636364)],
        );
    }

    #[test]
    fn calculate_predict_linear_test2() {
        let (ts_array, value_array) = build_test_range_arrays();
        simple_range_udf_runner(
            PredictLinear::scalar_udf(),
            ts_array,
            value_array,
            vec![ScalarValue::Int64(Some(3000)), window_end_eval_ts()],
            // value at t = 3000
            vec![Some(31856.818181818187)],
        );
    }

    #[test]
    fn calculate_predict_linear_test3() {
        let (ts_array, value_array) = build_test_range_arrays();
        simple_range_udf_runner(
            PredictLinear::scalar_udf(),
            ts_array,
            value_array,
            vec![ScalarValue::Int64(Some(4200)), window_end_eval_ts()],
            // value at t = 4200
            vec![Some(44584.09090909091)],
        );
    }

    #[test]
    fn calculate_predict_linear_test4() {
        let (ts_array, value_array) = build_test_range_arrays();
        simple_range_udf_runner(
            PredictLinear::scalar_udf(),
            ts_array,
            value_array,
            vec![ScalarValue::Int64(Some(6600)), window_end_eval_ts()],
            // value at t = 6600
            vec![Some(70038.63636363638)],
        );
    }

    #[test]
    fn calculate_predict_linear_test5() {
        let (ts_array, value_array) = build_test_range_arrays();
        simple_range_udf_runner(
            PredictLinear::scalar_udf(),
            ts_array,
            value_array,
            vec![ScalarValue::Int64(Some(7800)), window_end_eval_ts()],
            // value at t = 7800
            vec![Some(82765.9090909091)],
        );
    }

    #[test]
    fn calculate_predict_linear_with_misaligned_offsets() {
        let ts_values = Arc::new(TimestampMillisecondArray::from_iter(
            [0i64, 1000, 2000, 3000].into_iter().map(Some),
        ));
        let value_values = Arc::new(Float64Array::from_iter([10.0, 20.0, 30.0]));
        let ts_array = RangeArray::from_ranges(ts_values, [(1, 3)]).unwrap();
        let value_array = RangeArray::from_ranges(value_values, [(0, 3)]).unwrap();

        simple_range_udf_runner(
            PredictLinear::scalar_udf(),
            ts_array,
            value_array,
            vec![ScalarValue::Int64(Some(0)), window_end_eval_ts()],
            vec![Some(30.0)],
        );
    }

    #[test]
    fn predict_linear_rejects_external_dictionary_with_null_keys() {
        let ts_values = Arc::new(TimestampMillisecondArray::from_iter(
            [0i64, 1000].into_iter().map(Some),
        ));
        let ts_keys = Int64Array::from_iter([Some(0), None]);
        let ts_dict = DictionaryArray::<Int64Type>::try_new(ts_keys, ts_values).unwrap();

        let value_values = Arc::new(Float64Array::from_iter([1.0, 2.0]));
        let value_keys = Int64Array::from_iter([Some(0), Some(1)]);
        let value_dict = DictionaryArray::<Int64Type>::try_new(value_keys, value_values).unwrap();

        let err = PredictLinear::predict_linear(&[
            ColumnarValue::Array(Arc::new(ts_dict)),
            ColumnarValue::Array(Arc::new(value_dict)),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(0))),
            ColumnarValue::Scalar(window_end_eval_ts()),
        ])
        .unwrap_err();

        assert!(err.to_string().contains("Empty range is not expected"));
    }

    /// The regression is centered on the evaluation timestamp, so the returned intercept follows
    /// the step being evaluated even though the window stays the same.
    #[test]
    fn calculate_predict_linear_centers_on_eval_ts() {
        // One sample per second with values 0, 1, 2: the trend is exactly one unit per second.
        for (eval_ts, t, expected) in [
            // The window ends at 2s: evaluating there returns its last value.
            (2000, 0, 2.0),
            // One second past the window's end the trend is at 3.
            (3000, 0, 3.0),
            // One second past the window's end, predicting another two seconds ahead.
            (3000, 2, 5.0),
        ] {
            let ts_values = Arc::new(TimestampMillisecondArray::from_iter(
                [0i64, 1000, 2000].into_iter().map(Some),
            ));
            let value_values = Arc::new(Float64Array::from_iter([0.0, 1.0, 2.0]));
            let ts_array = RangeArray::from_ranges(ts_values, [(0, 3)]).unwrap();
            let value_array = RangeArray::from_ranges(value_values, [(0, 3)]).unwrap();

            simple_range_udf_runner(
                PredictLinear::scalar_udf(),
                ts_array,
                value_array,
                vec![
                    ScalarValue::Int64(Some(t)),
                    ScalarValue::TimestampMillisecond(Some(eval_ts), None),
                ],
                vec![Some(expected)],
            );
        }
    }

    /// The evaluation timestamp arrives as a column, so the UDF accepts it as an array too.
    #[test]
    fn calculate_predict_linear_accepts_eval_ts_array() {
        let (ts_array, value_array) = build_test_range_arrays();
        let eval_ts_array = Arc::new(TimestampMillisecondArray::from_iter_values([3000]));

        let result = PredictLinear::predict_linear(&[
            ColumnarValue::Array(Arc::new(ts_array.into_dict())),
            ColumnarValue::Array(Arc::new(value_array.into_dict())),
            ColumnarValue::Scalar(ScalarValue::Int64(Some(0))),
            ColumnarValue::Array(eval_ts_array),
        ])
        .unwrap();

        let result = result
            .into_array(1)
            .unwrap()
            .as_any()
            .downcast_ref::<Float64Array>()
            .unwrap()
            .clone();
        assert_eq!(result.len(), 1);
        // The same value `calculate_predict_linear_test1` gets from a scalar evaluation timestamp.
        assert!((result.value(0) - 38.63636363636364).abs() < 0.0001);
    }
}
