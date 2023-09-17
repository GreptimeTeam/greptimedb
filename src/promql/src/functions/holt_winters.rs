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

//! Implementation of [`holt_winters`](https://prometheus.io/docs/prometheus/latest/querying/functions/#holt_winters) in PromQL. Refer to the [original
//! implementation](https://github.com/prometheus/prometheus/blob/8dba9163f1e923ec213f0f4d5c185d9648e387f0/promql/functions.go#L299).

use std::sync::Arc;

use datafusion::arrow::array::Float64Array;
use datafusion::arrow::datatypes::TimeUnit;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::{ScalarUDF, Signature, TypeSignature, Volatility};
use datafusion::physical_plan::ColumnarValue;
use datatypes::arrow::array::Array;
use datatypes::arrow::datatypes::DataType;

use crate::error;
use crate::functions::extract_array;
use crate::range_array::RangeArray;

/// There are 3 variants of smoothing functions:
/// 1) "Simple exponential smoothing": only the `level` component (the weighted average of the observations) is used to make forecasts.
///   This method is applied for time-series data that does not exhibit trend or seasonality.
/// 2) "Holt's linear method" (a.k.a. "double exponential smoothing"): `level` and `trend` components are used to make forecasts.
///   This method is applied for time-series data that exhibits trend but not seasonality.
/// 3) "Holt-Winter's method" (a.k.a. "triple exponential smoothing"): `level`, `trend`, and `seasonality` are used to make forecasts.
///   This method is applied for time-series data that exhibits both trend and seasonality.
///
/// In order to keep the parity with the Prometheus functions we had to follow the same naming ("HoltWinters"), however
/// the "Holt's linear"("double exponential smoothing") suits better and reflects implementation.
/// There's the [discussion](https://github.com/prometheus/prometheus/issues/2458) in the Prometheus Github that dates back
/// to 2017 highlighting the naming/implementation mismatch.
pub struct HoltWinters {
    sf: f64,
    tf: f64,
}

impl HoltWinters {
    fn new(sf: f64, tf: f64) -> Self {
        Self { sf, tf }
    }

    pub const fn name() -> &'static str {
        "prom_holt_winters"
    }

    // time index column and value column
    fn input_type() -> Vec<DataType> {
        vec![
            RangeArray::convert_data_type(DataType::Timestamp(TimeUnit::Millisecond, None)),
            RangeArray::convert_data_type(DataType::Float64),
        ]
    }

    fn return_type() -> DataType {
        DataType::Float64
    }

    pub fn scalar_udf(level: f64, trend: f64) -> ScalarUDF {
        ScalarUDF {
            name: Self::name().to_string(),
            signature: Signature::new(
                TypeSignature::Exact(Self::input_type()),
                Volatility::Immutable,
            ),
            return_type: Arc::new(|_| Ok(Arc::new(Self::return_type()))),
            fun: Arc::new(move |input| Self::new(level, trend).calc(input)),
        }
    }

    fn calc(&self, input: &[ColumnarValue]) -> Result<ColumnarValue, DataFusionError> {
        // construct matrix from input.
        // The third one is level param, the fourth - trend param which are included in fields.
        assert_eq!(input.len(), 2);

        let ts_array = extract_array(&input[0])?;
        let value_array = extract_array(&input[1])?;

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
                    "{}: input arrays should have the same length, found {} and {}",
                    Self::name(),
                    timestamps.len(),
                    values.len()
                )),
            )?;
            result_array.push(holt_winter_impl(values, self.sf, self.tf));
        }

        let result = ColumnarValue::Array(Arc::new(Float64Array::from_iter(result_array)));
        Ok(result)
    }
}

fn calc_trend_value(i: usize, tf: f64, s0: f64, s1: f64, b: f64) -> f64 {
    if i == 0 {
        return b;
    }
    let x = tf * (s1 - s0);
    let y = (1.0 - tf) * b;
    x + y
}

/// Refer to <https://github.com/prometheus/prometheus/blob/main/promql/functions.go#L299>
fn holt_winter_impl(values: &[f64], sf: f64, tf: f64) -> Option<f64> {
    if sf.is_nan() || tf.is_nan() || values.is_empty() {
        return Some(f64::NAN);
    }
    if sf < 0.0 || tf < 0.0 {
        return Some(f64::NEG_INFINITY);
    }
    if sf > 1.0 || tf > 1.0 {
        return Some(f64::INFINITY);
    }

    let l = values.len();
    if l <= 2 {
        // Can't do the smoothing operation with less than two points.
        return Some(f64::NAN);
    }

    let values = values.to_vec();

    let mut s0 = 0.0;
    let mut s1 = values[0];
    let mut b = values[1] - values[0];

    for (i, value) in values.iter().enumerate().skip(1) {
        // Scale the raw value against the smoothing factor.
        let x = sf * value;
        // Scale the last smoothed value with the trend at this point.
        b = calc_trend_value(i - 1, tf, s0, s1, b);
        let y = (1.0 - sf) * (s1 + b);
        s0 = s1;
        s1 = x + y;
    }
    Some(s1)
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::{Float64Array, TimestampMillisecondArray};

    use super::*;
    use crate::functions::test_util::simple_range_udf_runner;

    #[test]
    fn test_holt_winter_impl_empty() {
        let sf = 0.5;
        let tf = 0.5;
        let values = &[];
        assert!(holt_winter_impl(values, sf, tf).unwrap().is_nan());

        let values = &[1.0, 2.0];
        assert!(holt_winter_impl(values, sf, tf).unwrap().is_nan());
    }

    #[test]
    fn test_holt_winter_impl_nan() {
        let values = &[1.0, 2.0, 3.0];
        let sf = f64::NAN;
        let tf = 0.5;
        assert!(holt_winter_impl(values, sf, tf).unwrap().is_nan());

        let values = &[1.0, 2.0, 3.0];
        let sf = 0.5;
        let tf = f64::NAN;
        assert!(holt_winter_impl(values, sf, tf).unwrap().is_nan());
    }

    #[test]
    fn test_holt_winter_impl_validation_rules() {
        let values = &[1.0, 2.0, 3.0];
        let sf = -0.5;
        let tf = 0.5;
        assert_eq!(holt_winter_impl(values, sf, tf).unwrap(), f64::NEG_INFINITY);

        let values = &[1.0, 2.0, 3.0];
        let sf = 0.5;
        let tf = -0.5;
        assert_eq!(holt_winter_impl(values, sf, tf).unwrap(), f64::NEG_INFINITY);

        let values = &[1.0, 2.0, 3.0];
        let sf = 1.5;
        let tf = 0.5;
        assert_eq!(holt_winter_impl(values, sf, tf).unwrap(), f64::INFINITY);

        let values = &[1.0, 2.0, 3.0];
        let sf = 0.5;
        let tf = 1.5;
        assert_eq!(holt_winter_impl(values, sf, tf).unwrap(), f64::INFINITY);
    }

    #[test]
    fn test_holt_winter_impl() {
        let sf = 0.5;
        let tf = 0.1;
        let values = &[1.0, 2.0, 3.0, 4.0, 5.0];
        assert_eq!(holt_winter_impl(values, sf, tf), Some(5.0));
        let values = &[50.0, 52.0, 95.0, 59.0, 52.0, 45.0, 38.0, 10.0, 47.0, 40.0];
        assert_eq!(holt_winter_impl(values, sf, tf), Some(38.18119566835938));
    }

    #[test]
    fn test_prom_holt_winter_monotonic() {
        let ranges = [(0, 5)];
        let ts_array = Arc::new(TimestampMillisecondArray::from_iter(
            [1000i64, 3000, 5000, 7000, 9000, 11000, 13000, 15000, 17000]
                .into_iter()
                .map(Some),
        ));
        let values_array = Arc::new(Float64Array::from_iter([1.0, 2.0, 3.0, 4.0, 5.0]));
        let ts_range_array = RangeArray::from_ranges(ts_array, ranges).unwrap();
        let value_range_array = RangeArray::from_ranges(values_array, ranges).unwrap();
        simple_range_udf_runner(
            HoltWinters::scalar_udf(0.5, 0.1),
            ts_range_array,
            value_range_array,
            vec![Some(5.0)],
        );
    }

    #[test]
    fn test_prom_holt_winter_non_monotonic() {
        let ranges = [(0, 10)];
        let ts_array = Arc::new(TimestampMillisecondArray::from_iter(
            [
                1000i64, 3000, 5000, 7000, 9000, 11000, 13000, 15000, 17000, 19000,
            ]
            .into_iter()
            .map(Some),
        ));
        let values_array = Arc::new(Float64Array::from_iter([
            50.0, 52.0, 95.0, 59.0, 52.0, 45.0, 38.0, 10.0, 47.0, 40.0,
        ]));
        let ts_range_array = RangeArray::from_ranges(ts_array, ranges).unwrap();
        let value_range_array = RangeArray::from_ranges(values_array, ranges).unwrap();
        simple_range_udf_runner(
            HoltWinters::scalar_udf(0.5, 0.1),
            ts_range_array,
            value_range_array,
            vec![Some(38.18119566835938)],
        );
    }

    #[test]
    fn test_promql_trends() {
        let ranges = vec![(0, 801)];

        let trends = vec![
            // positive trends https://github.com/prometheus/prometheus/blob/8dba9163f1e923ec213f0f4d5c185d9648e387f0/promql/testdata/functions.test#L475
            ("0+10x1000 100+30x1000", 8000.0),
            ("0+20x1000 200+30x1000", 16000.0),
            ("0+30x1000 300+80x1000", 24000.0),
            ("0+40x2000", 32000.0),
            // negative trends https://github.com/prometheus/prometheus/blob/8dba9163f1e923ec213f0f4d5c185d9648e387f0/promql/testdata/functions.test#L488
            ("8000-10x1000", 0.0),
            ("0-20x1000", -16000.0),
            ("0+30x1000 300-80x1000", 24000.0),
            ("0-40x1000 0+40x1000", -32000.0),
        ];

        for (query, expected) in trends {
            let (ts_range_array, value_range_array) =
                create_ts_and_value_range_arrays(query, ranges.clone());
            simple_range_udf_runner(
                HoltWinters::scalar_udf(0.01, 0.1),
                ts_range_array,
                value_range_array,
                vec![Some(expected)],
            );
        }
    }

    fn create_ts_and_value_range_arrays(
        input: &str,
        ranges: Vec<(u32, u32)>,
    ) -> (RangeArray, RangeArray) {
        let promql_range = create_test_range_from_promql_series(input);
        let ts_array = Arc::new(TimestampMillisecondArray::from_iter(
            (0..(promql_range.len() as i64)).map(Some),
        ));
        let values_array = Arc::new(Float64Array::from_iter(promql_range));
        let ts_range_array = RangeArray::from_ranges(ts_array, ranges.clone()).unwrap();
        let value_range_array = RangeArray::from_ranges(values_array, ranges).unwrap();
        (ts_range_array, value_range_array)
    }

    /// Converts a prometheus functions test series into a vector of f64 element with respect to resets and trend direction   
    /// The input example: "0+10x1000 100+30x1000"
    fn create_test_range_from_promql_series(input: &str) -> Vec<f64> {
        input.split(' ').map(parse_promql_series_entry).fold(
            Vec::new(),
            |mut acc, (start, end, step, operation)| {
                if operation.eq("+") {
                    let iter = (start..=((step * end) + start))
                        .step_by(step as usize)
                        .map(|x| x as f64);
                    acc.extend(iter);
                } else {
                    let iter = (((-step * end) + start)..=start)
                        .rev()
                        .step_by(step as usize)
                        .map(|x| x as f64);
                    acc.extend(iter);
                };
                acc
            },
        )
    }

    /// Converts a prometheus functions test series entry into separate parts to create a range with a step
    /// The input example: "100+30x1000"
    fn parse_promql_series_entry(input: &str) -> (i32, i32, i32, &str) {
        let mut parts = input.split('x');
        let start_operation_step = parts.next().unwrap();
        let operation = start_operation_step
            .split(char::is_numeric)
            .find(|&x| !x.is_empty())
            .unwrap();
        let start_step = start_operation_step
            .split(operation)
            .map(|s| s.parse::<i32>().unwrap())
            .collect::<Vec<_>>();
        let start = *start_step.first().unwrap();
        let step = *start_step.last().unwrap();
        let end = parts.next().unwrap().parse::<i32>().unwrap();
        (start, end, step, operation)
    }
}
