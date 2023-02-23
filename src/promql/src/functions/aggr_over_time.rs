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

use common_function_macro::range_fn;
use datafusion::arrow::array::{Float64Array, TimestampMillisecondArray};
use datafusion::arrow::datatypes::TimeUnit;
use datafusion::common::DataFusionError;
use datafusion::logical_expr::{ScalarUDF, Signature, TypeSignature, Volatility};
use datafusion::physical_plan::ColumnarValue;
use datatypes::arrow::array::Array;
use datatypes::arrow::compute;
use datatypes::arrow::datatypes::DataType;

use crate::functions::extract_array;
use crate::range_array::RangeArray;

/// The average value of all points in the specified interval.
#[range_fn(
    name = "AvgOverTime",
    ret = "Float64Array",
    display_name = "prom_avg_over_time"
)]
pub fn avg_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    compute::sum(values).map(|result| result / values.len() as f64)
}

/// The minimum value of all points in the specified interval.
#[range_fn(
    name = "MinOverTime",
    ret = "Float64Array",
    display_name = "prom_min_over_time"
)]
pub fn min_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    compute::min(values)
}

/// The maximum value of all points in the specified interval.
#[range_fn(
    name = "MaxOverTime",
    ret = "Float64Array",
    display_name = "prom_max_over_time"
)]
pub fn max_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    compute::max(values)
}

/// The sum of all values in the specified interval.
#[range_fn(
    name = "SumOverTime",
    ret = "Float64Array",
    display_name = "prom_sum_over_time"
)]
pub fn sum_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    compute::sum(values)
}

/// The count of all values in the specified interval.
#[range_fn(
    name = "CountOverTime",
    ret = "Float64Array",
    display_name = "prom_count_over_time"
)]
pub fn count_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> f64 {
    let result = values.len() as f64;
    println!("count over time result: {}", result);
    result
}

/// The most recent point value in specified interval.
#[range_fn(
    name = "LastOverTime",
    ret = "Float64Array",
    display_name = "prom_last_over_time"
)]
pub fn last_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    values.values().last().copied()
}

/// absent_over_time returns an empty vector if the range vector passed to it has any
/// elements (floats or native histograms) and a 1-element vector with the value 1 if
/// the range vector passed to it has no elements.
#[range_fn(
    name = "AbsentOverTime",
    ret = "Float64Array",
    display_name = "prom_absent_over_time"
)]
pub fn absent_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    if values.len() == 0 {
        Some(1.0)
    } else {
        None
    }
}

/// the value 1 for any series in the specified interval.
#[range_fn(
    name = "PresentOverTime",
    ret = "Float64Array",
    display_name = "prom_present_over_time"
)]
pub fn present_over_time(_: &TimestampMillisecondArray, values: &Float64Array) -> Option<f64> {
    if values.len() == 0 {
        None
    } else {
        Some(1.0)
    }
}

// TODO(ruihang): support quantile_over_time, stddev_over_time, and stdvar_over_time
