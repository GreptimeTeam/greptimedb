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

//! Float range-vector functions as window kernels, sharing their arithmetic with the range UDFs.

use datafusion::arrow::array::Float64Array;
use datafusion::arrow::buffer::ScalarBuffer;
use datafusion::arrow::compute;

use super::{FloatWindow, WindowKernel};
use crate::functions::{
    EdgeKind, add_counter_resets, count_edges, double_exponential_smoothing_impl, extrapolate,
    extremum, instant_delta, linear_regression_samples, stddev, stdvar, window_quantile,
};

/// A PromQL range-vector function over float samples.
#[derive(Clone, Copy, Debug, PartialEq)]
pub enum RangeFunction {
    Rate,
    Increase,
    Delta,
    Irate,
    Idelta,
    AvgOverTime,
    MinOverTime,
    MaxOverTime,
    SumOverTime,
    CountOverTime,
    LastOverTime,
    PresentOverTime,
    StddevOverTime,
    StdvarOverTime,
    /// `quantile_over_time` with a constant quantile.
    QuantileOverTime(f64),
    Resets,
    Changes,
    Deriv,
    /// `predict_linear` with a constant horizon in whole seconds, as the planner casts it.
    PredictLinear(i64),
    /// `double_exponential_smoothing` (`holt_winters`) with constant factors.
    DoubleExponentialSmoothing {
        smoothing: f64,
        trend: f64,
    },
}

impl RangeFunction {
    /// Returns the kernel computing this function over one window.
    pub(crate) fn kernel(self) -> WindowKernel {
        match self {
            Self::Rate => Box::new(extrapolated::<true, true>),
            Self::Increase => Box::new(extrapolated::<true, false>),
            Self::Delta => Box::new(extrapolated::<false, false>),
            Self::Irate => Box::new(last_two::<true>),
            Self::Idelta => Box::new(last_two::<false>),
            Self::AvgOverTime => {
                let mut scratch = Vec::new();
                Box::new(move |window| {
                    sum(&mut scratch, window.values).map(|sum| sum / window.values.len() as f64)
                })
            }
            Self::MinOverTime => Box::new(|window| extremum(window.values.iter().copied(), true)),
            Self::MaxOverTime => Box::new(|window| extremum(window.values.iter().copied(), false)),
            Self::SumOverTime => {
                let mut scratch = Vec::new();
                Box::new(move |window| sum(&mut scratch, window.values))
            }
            Self::CountOverTime => Box::new(|window| Some(window.values.len() as f64)),
            Self::LastOverTime => Box::new(|window| window.values.last().copied()),
            Self::PresentOverTime => Box::new(|_| Some(1.0)),
            Self::StddevOverTime => Box::new(|window| stddev(window.values.iter().copied())),
            Self::StdvarOverTime => Box::new(|window| stdvar(window.values.iter().copied())),
            Self::QuantileOverTime(quantile) => {
                let mut scratch = Vec::new();
                Box::new(move |window| window_quantile(window.values, quantile, &mut scratch))
            }
            Self::Resets => {
                Box::new(|window| Some(count_edges(window.values, EdgeKind::Resets) as f64))
            }
            Self::Changes => {
                Box::new(|window| Some(count_edges(window.values, EdgeKind::Changes) as f64))
            }
            Self::Deriv => Box::new(|window| regression(&window, window.timestamps[0]).0),
            Self::PredictLinear(horizon) => Box::new(move |window| {
                let (slope, intercept) = regression(&window, window.eval_ts);
                Some(slope? * horizon as f64 + intercept?)
            }),
            Self::DoubleExponentialSmoothing { smoothing, trend } => Box::new(move |window| {
                double_exponential_smoothing_impl(window.values, smoothing, trend)
            }),
        }
    }
}

fn regression(window: &FloatWindow<'_>, intercept_time: i64) -> (Option<f64>, Option<f64>) {
    linear_regression_samples(
        window
            .timestamps
            .iter()
            .copied()
            .zip(window.values.iter().copied()),
        intercept_time,
    )
}

/// Sums like the `sum_over_time` UDF: Arrow's lane-wise `sum` over the window's samples.
///
/// `scratch` keeps the buffer the window is copied into between calls.
fn sum(scratch: &mut Vec<f64>, values: &[f64]) -> Option<f64> {
    let mut buffer = std::mem::take(scratch);
    buffer.clear();
    buffer.extend_from_slice(values);
    let array = Float64Array::new(ScalarBuffer::from(buffer), None);
    let sum = compute::sum(&array);
    let (_, buffer, _) = array.into_parts();
    if let Ok(buffer) = buffer.into_inner().into_vec() {
        *scratch = buffer;
    }
    sum
}

fn extrapolated<const IS_COUNTER: bool, const IS_RATE: bool>(
    window: FloatWindow<'_>,
) -> Option<f64> {
    let FloatWindow {
        timestamps, values, ..
    } = window;
    if values.len() < 2 {
        return None;
    }
    let first = values[0];
    let last = values[values.len() - 1];
    let mut result = last - first;
    if IS_COUNTER {
        result = add_counter_resets(result, values);
    }
    Some(extrapolate::<IS_COUNTER, IS_RATE>(
        result,
        first,
        timestamps[0],
        timestamps[timestamps.len() - 1],
        values.len(),
        window.eval_ts,
        window.range_ms,
    ))
}

fn last_two<const IS_RATE: bool>(window: FloatWindow<'_>) -> Option<f64> {
    let [.., prev_ts, last_ts] = window.timestamps else {
        return None;
    };
    let [.., prev, last] = window.values else {
        return None;
    };
    instant_delta::<IS_RATE>((*prev_ts, *prev), (*last_ts, *last))
}
