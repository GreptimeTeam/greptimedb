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

use datafusion::arrow::array::Float64Array;
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

        // calculation
        let mut result_array = Vec::with_capacity(ts_range.len());

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
                    let timestamps = ts_range.get(index).unwrap();
                    let values = value_range.get(index).unwrap();
                    let values = values
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .values();
                    error::ensure(
                        timestamps.len() == values.len(),
                        DataFusionError::Execution(format!(
                            "{}: time and value arrays in a group should have the same length, found {} and {}",
                            Self::name(),
                            timestamps.len(),
                            values.len()
                        )),
                    )?;

                    let result = quantile_impl(values, quantile);
                    result_array.push(result);
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
                    let timestamps = ts_range.get(index).unwrap();
                    let values = value_range.get(index).unwrap();
                    let values = values
                        .as_any()
                        .downcast_ref::<Float64Array>()
                        .unwrap()
                        .values();
                    error::ensure(
                        timestamps.len() == values.len(),
                        DataFusionError::Execution(format!(
                            "{}: time and value arrays in a group should have the same length, found {} and {}",
                            Self::name(),
                            timestamps.len(),
                            values.len()
                        )),
                    )?;
                    let quantile = if quantile_array.is_null(index) {
                        f64::NAN
                    } else {
                        quantile_array.value(index)
                    };
                    let result = quantile_impl(values, quantile);
                    result_array.push(result);
                }
            }
        }

        let result = ColumnarValue::Array(Arc::new(Float64Array::from_iter(result_array)));
        Ok(result)
    }
}

/// Refer to <https://github.com/prometheus/prometheus/blob/6e2905a4d4ff9b47b1f6d201333f5bd53633f921/promql/quantile.go#L357-L386>
pub(crate) fn quantile_impl(values: &[f64], quantile: f64) -> Option<f64> {
    if quantile.is_nan() || values.is_empty() {
        return Some(f64::NAN);
    }
    if quantile < 0.0 {
        return Some(f64::NEG_INFINITY);
    }
    if quantile > 1.0 {
        return Some(f64::INFINITY);
    }

    let mut values = values.to_vec();
    values.sort_unstable_by(f64::total_cmp);

    let length = values.len();
    let rank = quantile * (length - 1) as f64;

    let lower_index = 0.max(rank.floor() as usize);
    let upper_index = (length - 1).min(lower_index + 1);
    let weight = rank - rank.floor();

    let result = values[lower_index] * (1.0 - weight) + values[upper_index] * weight;
    Some(result)
}

#[cfg(test)]
mod tests {
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
}
