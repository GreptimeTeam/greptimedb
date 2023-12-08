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

mod aggr_over_time;
mod changes;
mod deriv;
mod extrapolate_rate;
mod holt_winters;
mod idelta;
mod predict_linear;
mod quantile;
mod resets;
#[cfg(test)]
mod test_util;

pub use aggr_over_time::{
    AbsentOverTime, AvgOverTime, CountOverTime, LastOverTime, MaxOverTime, MinOverTime,
    PresentOverTime, StddevOverTime, StdvarOverTime, SumOverTime,
};
pub use changes::Changes;
use datafusion::arrow::array::{ArrayRef, Float64Array, TimestampMillisecondArray};
use datafusion::error::DataFusionError;
use datafusion::physical_plan::ColumnarValue;
pub use deriv::Deriv;
pub use extrapolate_rate::{Delta, Increase, Rate};
pub use holt_winters::HoltWinters;
pub use idelta::IDelta;
pub use predict_linear::PredictLinear;
pub use quantile::QuantileOverTime;
pub use resets::Resets;

pub(crate) fn extract_array(columnar_value: &ColumnarValue) -> Result<ArrayRef, DataFusionError> {
    if let ColumnarValue::Array(array) = columnar_value {
        Ok(array.clone())
    } else {
        Err(DataFusionError::Execution(
            "expect array as input, found scalar value".to_string(),
        ))
    }
}

/// compensation(Kahan) summation algorithm - a technique for reducing the numerical error
/// in floating-point arithmetic. The algorithm also includes the modification ("Neumaier improvement")
/// that reduces the numerical error further in cases
/// where the numbers being summed have a large difference in magnitude
/// Prometheus's implementation:
/// <https://github.com/prometheus/prometheus/blob/f55ab2217984770aa1eecd0f2d5f54580029b1c0/promql/functions.go#L782>
pub(crate) fn compensated_sum_inc(inc: f64, sum: f64, mut compensation: f64) -> (f64, f64) {
    let new_sum = sum + inc;
    if sum.abs() >= inc.abs() {
        compensation += (sum - new_sum) + inc;
    } else {
        compensation += (inc - new_sum) + sum;
    }
    (new_sum, compensation)
}

/// linear_regression performs a least-square linear regression analysis on the
/// times and values. It return the slope and intercept based on times and values.
/// Prometheus's implementation: <https://github.com/prometheus/prometheus/blob/90b2f7a540b8a70d8d81372e6692dcbb67ccbaaa/promql/functions.go#L793-L837>
pub(crate) fn linear_regression(
    times: &TimestampMillisecondArray,
    values: &Float64Array,
    intercept_time: i64,
) -> (Option<f64>, Option<f64>) {
    let mut count: f64 = 0.0;
    let mut sum_x: f64 = 0.0;
    let mut sum_y: f64 = 0.0;
    let mut sum_xy: f64 = 0.0;
    let mut sum_x2: f64 = 0.0;
    let mut comp_x: f64 = 0.0;
    let mut comp_y: f64 = 0.0;
    let mut comp_xy: f64 = 0.0;
    let mut comp_x2: f64 = 0.0;

    let mut const_y = true;
    let init_y: f64 = values.value(0);

    for (i, value) in values.iter().enumerate() {
        let time = times.value(i) as f64;
        if value.is_none() {
            continue;
        }
        let value = value.unwrap();
        if const_y && i > 0 && value != init_y {
            const_y = false;
        }
        count += 1.0;
        let x = (time - intercept_time as f64) / 1e3f64;
        (sum_x, comp_x) = compensated_sum_inc(x, sum_x, comp_x);
        (sum_y, comp_y) = compensated_sum_inc(value, sum_y, comp_y);
        (sum_xy, comp_xy) = compensated_sum_inc(x * value, sum_xy, comp_xy);
        (sum_x2, comp_x2) = compensated_sum_inc(x * x, sum_x2, comp_x2);
    }

    if count < 2.0 {
        return (None, None);
    }

    if const_y {
        if !init_y.is_finite() {
            return (None, None);
        }
        return (Some(0.0), Some(init_y));
    }

    sum_x += comp_x;
    sum_y += comp_y;
    sum_xy += comp_xy;
    sum_x2 += comp_x2;

    let cov_xy = sum_xy - sum_x * sum_y / count;
    let var_x = sum_x2 - sum_x * sum_x / count;

    let slope = cov_xy / var_x;
    let intercept = sum_y / count - slope * sum_x / count;

    (Some(slope), Some(intercept))
}

#[cfg(test)]
mod test {
    use super::*;

    #[test]
    fn calculate_linear_regression_none() {
        let ts_array = TimestampMillisecondArray::from_iter(
            [
                0i64, 300, 600, 900, 1200, 1500, 1800, 2100, 2400, 2700, 3000,
            ]
            .into_iter()
            .map(Some),
        );
        let values_array = Float64Array::from_iter([
            1.0 / 0.0,
            1.0 / 0.0,
            1.0 / 0.0,
            1.0 / 0.0,
            1.0 / 0.0,
            1.0 / 0.0,
            1.0 / 0.0,
            1.0 / 0.0,
            1.0 / 0.0,
            1.0 / 0.0,
        ]);
        let (slope, intercept) = linear_regression(&ts_array, &values_array, ts_array.value(0));
        assert_eq!(slope, None);
        assert_eq!(intercept, None);
    }

    #[test]
    fn calculate_linear_regression_value_is_const() {
        let ts_array = TimestampMillisecondArray::from_iter(
            [
                0i64, 300, 600, 900, 1200, 1500, 1800, 2100, 2400, 2700, 3000,
            ]
            .into_iter()
            .map(Some),
        );
        let values_array =
            Float64Array::from_iter([10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0, 10.0]);
        let (slope, intercept) = linear_regression(&ts_array, &values_array, ts_array.value(0));
        assert_eq!(slope, Some(0.0));
        assert_eq!(intercept, Some(10.0));
    }

    #[test]
    fn calculate_linear_regression() {
        let ts_array = TimestampMillisecondArray::from_iter(
            [
                0i64, 300, 600, 900, 1200, 1500, 1800, 2100, 2400, 2700, 3000,
            ]
            .into_iter()
            .map(Some),
        );
        let values_array = Float64Array::from_iter([
            0.0, 10.0, 20.0, 30.0, 40.0, 0.0, 10.0, 20.0, 30.0, 40.0, 50.0,
        ]);
        let (slope, intercept) = linear_regression(&ts_array, &values_array, ts_array.value(0));
        assert_eq!(slope, Some(10.606060606060607));
        assert_eq!(intercept, Some(6.818181818181815));

        let (slope, intercept) = linear_regression(&ts_array, &values_array, 3000);
        assert_eq!(slope, Some(10.606060606060607));
        assert_eq!(intercept, Some(38.63636363636364));
    }

    #[test]
    fn calculate_linear_regression_value_have_none() {
        let ts_array = TimestampMillisecondArray::from_iter(
            [
                0i64, 300, 600, 900, 1200, 1350, 1500, 1800, 2100, 2400, 2550, 2700, 3000,
            ]
            .into_iter()
            .map(Some),
        );
        let values_array: Float64Array = [
            Some(0.0),
            Some(10.0),
            Some(20.0),
            Some(30.0),
            Some(40.0),
            None,
            Some(0.0),
            Some(10.0),
            Some(20.0),
            Some(30.0),
            None,
            Some(40.0),
            Some(50.0),
        ]
        .into_iter()
        .collect();
        let (slope, intercept) = linear_regression(&ts_array, &values_array, ts_array.value(0));
        assert_eq!(slope, Some(10.606060606060607));
        assert_eq!(intercept, Some(6.818181818181815));
    }

    #[test]
    fn calculate_linear_regression_value_all_none() {
        let ts_array = TimestampMillisecondArray::from_iter([0i64, 300, 600].into_iter().map(Some));
        let values_array: Float64Array = [None, None, None].into_iter().collect();
        let (slope, intercept) = linear_regression(&ts_array, &values_array, ts_array.value(0));
        assert_eq!(slope, None);
        assert_eq!(intercept, None);
    }

    // From prometheus `promql/functions_test.go` case `TestKahanSum`
    #[test]
    fn test_kahan_sum() {
        let inputs = vec![1.0, 10.0f64.powf(100.0), 1.0, -1.0 * 10.0f64.powf(100.0)];

        let mut sum = 0.0;
        let mut c = 0f64;

        for v in inputs {
            (sum, c) = compensated_sum_inc(v, sum, c);
        }
        assert_eq!(sum + c, 2.0)
    }
}
