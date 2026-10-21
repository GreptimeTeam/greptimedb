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

use datafusion::arrow::array::{Float64Array, Float64Builder};
use datafusion::arrow::datatypes::TimeUnit;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::{ScalarUDF, Volatility};
use datafusion::physical_plan::ColumnarValue;
use datafusion_common::ScalarValue;
use datafusion_expr::create_udf;
use datatypes::arrow::array::Array;
use datatypes::arrow::datatypes::DataType;

use crate::error;
use crate::functions::extract_array;
use crate::range_array::RangeArray;

pub struct QuantileOverTime;

impl QuantileOverTime {
    pub const fn name() -> &'static str {
        "prom_quantile_over_time"
    }

    pub fn scalar_udf() -> ScalarUDF {
        let input_types = vec![
            // time index column
            RangeArray::convert_data_type(DataType::Timestamp(TimeUnit::Millisecond, None)),
            // value column
            RangeArray::convert_data_type(DataType::Float64),
            // quantile
            DataType::Float64,
        ];
        create_udf(
            Self::name(),
            input_types,
            DataType::Float64,
            Volatility::Volatile,
            Arc::new(Self::quantile_over_time) as _,
        )
    }

    fn quantile_over_time(input: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
        error::ensure(
            input.len() == 3,
            DataFusionError::Plan(
                "prom_quantile_over_time function should have 3 inputs".to_string(),
            ),
        )?;

        let ts_array = extract_array(&input[0])?;
        let value_array = extract_array(&input[1])?;
        let quantile_col = &input[2];

        let ts_range: RangeArray = RangeArray::try_new(ts_array.to_data().into())?;
        let value_range: RangeArray = RangeArray::try_new(value_array.to_data().into())?;
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

        let value_array = value_range.values();
        let value_array = value_array.as_any().downcast_ref::<Float64Array>().unwrap();
        // A NULL field value means the series has no sample at that timestamp, so a window's
        // samples are not simply its slots.
        let has_nulls = value_array.null_count() > 0;
        let mut result_builder = Float64Builder::with_capacity(ts_range.len());
        let mut scratch = Vec::new();
        let mut samples = Vec::new();

        match quantile_col {
            ColumnarValue::Scalar(quantile_scalar) => {
                let quantile = if let ScalarValue::Float64(Some(q)) = quantile_scalar {
                    *q
                } else {
                    // For `ScalarValue::Float64(None)` or other scalar types, use NAN,
                    // which conforms to PromQL's behavior.
                    f64::NAN
                };

                for index in 0..ts_range.len() {
                    let (_, ts_len) = ts_range.get_offset_length(index).unwrap();
                    let (value_offset, value_len) = value_range.get_offset_length(index).unwrap();
                    error::ensure(
                        ts_len == value_len,
                        DataFusionError::Execution(format!(
                            "{}: time and value arrays in a group should have the same length, found {} and {}",
                            Self::name(),
                            ts_len,
                            value_len
                        )),
                    )?;

                    let window = window_samples(
                        value_array,
                        has_nulls,
                        value_offset,
                        value_len,
                        &mut samples,
                    );
                    match window_quantile(window, quantile, &mut scratch) {
                        Some(value) => result_builder.append_value(value),
                        None => result_builder.append_null(),
                    }
                }
            }
            ColumnarValue::Array(quantile_array) => {
                let quantile_array = quantile_array
                    .as_any()
                    .downcast_ref::<Float64Array>()
                    .ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "{}: expect Float64 as quantile array's type, found {}",
                            Self::name(),
                            quantile_array.data_type()
                        ))
                    })?;

                error::ensure(
                    quantile_array.len() == ts_range.len(),
                    DataFusionError::Execution(format!(
                        "{}: quantile array should have the same length as other columns, found {} and {}",
                        Self::name(),
                        quantile_array.len(),
                        ts_range.len()
                    )),
                )?;
                for index in 0..ts_range.len() {
                    let (_, ts_len) = ts_range.get_offset_length(index).unwrap();
                    let (value_offset, value_len) = value_range.get_offset_length(index).unwrap();
                    error::ensure(
                        ts_len == value_len,
                        DataFusionError::Execution(format!(
                            "{}: time and value arrays in a group should have the same length, found {} and {}",
                            Self::name(),
                            ts_len,
                            value_len
                        )),
                    )?;
                    let quantile = if quantile_array.is_null(index) {
                        f64::NAN
                    } else {
                        quantile_array.value(index)
                    };
                    let window = window_samples(
                        value_array,
                        has_nulls,
                        value_offset,
                        value_len,
                        &mut samples,
                    );
                    match window_quantile(window, quantile, &mut scratch) {
                        Some(value) => result_builder.append_value(value),
                        None => result_builder.append_null(),
                    }
                }
            }
        }

        let result = ColumnarValue::Array(Arc::new(result_builder.finish()));
        Ok(result)
    }
}

/// Returns the samples of the window `[offset, offset + len)`, collecting the non-null ones
/// into `samples` when the backing array has nulls and borrowing the slice otherwise.
fn window_samples<'a>(
    values: &'a Float64Array,
    has_nulls: bool,
    offset: usize,
    len: usize,
    samples: &'a mut Vec<f64>,
) -> &'a [f64] {
    let raw_values = values.values();
    if !has_nulls {
        return &raw_values[offset..offset + len];
    }
    samples.clear();
    samples.extend(
        (offset..offset + len)
            .filter(|index| values.is_valid(*index))
            .map(|index| raw_values[index]),
    );
    samples
}

/// Quantile of one range window, or `None` when the window holds no sample.
///
/// Prometheus returns an empty vector for a range without float samples rather than the NaN
/// that [`quantile_impl`] yields for an empty slice, so the emptiness check belongs here and
/// not in the shared kernel.
fn window_quantile(values: &[f64], quantile: f64, scratch: &mut Vec<f64>) -> Option<f64> {
    if values.is_empty() {
        return None;
    }
    quantile_with_scratch(values, quantile, scratch)
}

/// Refer to <https://github.com/prometheus/prometheus/blob/6e2905a4d4ff9b47b1f6d201333f5bd53633f921/promql/quantile.go#L357-L386>
pub(crate) fn quantile_impl(values: &[f64], quantile: f64) -> Option<f64> {
    let mut scratch = Vec::new();
    quantile_with_scratch(values, quantile, &mut scratch)
}

/// Same as [quantile_impl] but reuses a caller-provided scratch buffer to avoid
/// per-call allocation.
fn quantile_with_scratch(values: &[f64], quantile: f64, scratch: &mut Vec<f64>) -> Option<f64> {
    if quantile.is_nan() || values.is_empty() {
        return Some(f64::NAN);
    }
    if quantile < 0.0 {
        return Some(f64::NEG_INFINITY);
    }
    if quantile > 1.0 {
        return Some(f64::INFINITY);
    }

    scratch.clear();
    scratch.extend_from_slice(values);
    scratch.sort_unstable_by(f64::total_cmp);

    let length = scratch.len();
    let rank = quantile * (length - 1) as f64;

    let lower_index = rank.floor() as usize;
    let upper_index = (length - 1).min(lower_index + 1);
    let weight = rank - rank.floor();

    let result = scratch[lower_index] * (1.0 - weight) + scratch[upper_index] * weight;
    Some(result)
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::TimestampMillisecondArray;
    use datafusion::arrow::buffer::NullBuffer;

    use super::*;

    #[test]
    fn test_quantile_impl_empty() {
        let values = &[];
        let q = 0.5;
        assert!(quantile_impl(values, q).unwrap().is_nan());
    }

    #[test]
    fn test_quantile_impl_nan() {
        let values = &[1.0, 2.0, 3.0];
        let q = f64::NAN;
        assert!(quantile_impl(values, q).unwrap().is_nan());
    }

    #[test]
    fn test_quantile_impl_negative_quantile() {
        let values = &[1.0, 2.0, 3.0];
        let q = -0.5;
        assert_eq!(quantile_impl(values, q).unwrap(), f64::NEG_INFINITY);
    }

    #[test]
    fn test_quantile_impl_greater_than_one_quantile() {
        let values = &[1.0, 2.0, 3.0];
        let q = 1.5;
        assert_eq!(quantile_impl(values, q).unwrap(), f64::INFINITY);
    }

    #[test]
    fn test_quantile_impl_single_element() {
        let values = &[1.0];
        let q = 0.8;
        assert_eq!(quantile_impl(values, q).unwrap(), 1.0);
    }

    #[test]
    fn test_quantile_impl_even_length() {
        let values = &[3.0, 1.0, 5.0, 2.0];
        let q = 0.5;
        assert_eq!(quantile_impl(values, q).unwrap(), 2.5);
    }

    #[test]
    fn test_quantile_impl_odd_length() {
        let values = &[4.0, 1.0, 3.0, 2.0, 5.0];
        let q = 0.25;
        assert_eq!(quantile_impl(values, q).unwrap(), 2.0);
    }

    #[test]
    fn quantile_over_time_ranks_samples_only() {
        let ts_array = Arc::new(TimestampMillisecondArray::from_iter_values([
            0i64, 1000, 2000,
        ]));
        // Samples are 1.0 and 4.0; ranking the padding too would pull the median down.
        let values_array = Arc::new(Float64Array::new(
            vec![1.0, -100.0, 4.0].into(),
            Some(NullBuffer::from_iter([true, false, true])),
        ));
        // The second window holds no sample, the third holds no slot at all.
        let ranges = [(0, 3), (1, 1), (3, 0)];

        let input = vec![
            ColumnarValue::Array(Arc::new(
                RangeArray::from_ranges(ts_array, ranges)
                    .unwrap()
                    .into_dict(),
            )),
            ColumnarValue::Array(Arc::new(
                RangeArray::from_ranges(values_array, ranges)
                    .unwrap()
                    .into_dict(),
            )),
            ColumnarValue::Scalar(ScalarValue::Float64(Some(0.5))),
        ];
        let output = extract_array(&QuantileOverTime::quantile_over_time(&input).unwrap()).unwrap();
        let output = output.as_any().downcast_ref::<Float64Array>().unwrap();

        assert_eq!(
            output.iter().collect::<Vec<_>>(),
            vec![Some(2.5), None, None]
        );
    }

    #[test]
    fn quantile_over_time_keeps_nan_for_an_invalid_quantile() {
        let ts_array = Arc::new(TimestampMillisecondArray::from_iter_values([0i64, 1000]));
        let values_array = Arc::new(Float64Array::from_iter_values([1.0, 4.0]));
        let ranges = [(0, 2)];

        let input = vec![
            ColumnarValue::Array(Arc::new(
                RangeArray::from_ranges(ts_array, ranges)
                    .unwrap()
                    .into_dict(),
            )),
            ColumnarValue::Array(Arc::new(
                RangeArray::from_ranges(values_array, ranges)
                    .unwrap()
                    .into_dict(),
            )),
            ColumnarValue::Scalar(ScalarValue::Float64(None)),
        ];
        let output = extract_array(&QuantileOverTime::quantile_over_time(&input).unwrap()).unwrap();
        let output = output.as_any().downcast_ref::<Float64Array>().unwrap();

        assert!(output.value(0).is_nan());
    }
}
