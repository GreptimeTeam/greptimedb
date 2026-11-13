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

//! Float range-vector functions as window kernels.
//!
//! Each kernel follows the Prometheus function of the same name, including its floating point
//! operation order. Arithmetic that GreptimeDB's range UDFs compute the same way is shared with
//! them.

use crate::functions::{EdgeKind, count_edges, extremum, instant_delta, kahan_inc};
use crate::go_sort;
use crate::range_eval::{BatchKernel, FloatWindow, FloatWindows, RangeModifier, WindowKernel};

/// A PromQL range-vector function over float and native histogram samples.
#[derive(Clone, Copy, Debug, serde::Serialize, serde::Deserialize)]
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
    /// `predict_linear` with a constant horizon in seconds.
    PredictLinear(f64),
    /// Experimental in Prometheus: `double_exponential_smoothing` (`holt_winters`) with constant
    /// factors in `(0, 1)`.
    DoubleExponentialSmoothing {
        smoothing: f64,
        trend: f64,
    },
    FirstOverTime,
    /// Experimental in Prometheus.
    MadOverTime,
    /// Experimental in Prometheus.
    TsOfFirstOverTime,
    /// Experimental in Prometheus.
    TsOfLastOverTime,
    /// Experimental in Prometheus.
    TsOfMinOverTime,
    /// Experimental in Prometheus.
    TsOfMaxOverTime,
}

impl RangeFunction {
    /// Whether the result drops the metric name, as Prometheus does for every range function
    /// except `last_over_time` and `first_over_time`.
    pub fn drops_metric_name(self) -> bool {
        !matches!(self, Self::LastOverTime | Self::FirstOverTime)
    }

    /// Whether a window holding native histograms can answer with one; every other function
    /// reduces it to a float.
    pub(crate) fn yields_histograms(self) -> bool {
        matches!(
            self,
            Self::Rate
                | Self::Increase
                | Self::Delta
                | Self::Irate
                | Self::Idelta
                | Self::AvgOverTime
                | Self::SumOverTime
                | Self::LastOverTime
                | Self::FirstOverTime
        )
    }

    /// Whether the evaluator has to index counter resets for this function.
    ///
    /// This must hold for exactly the kernels that read [`FloatWindow::counter_resets`], which is
    /// the same as saying `IS_COUNTER` in `extrapolated_rate`: `rate` and `increase`. A kernel
    /// that corrects counters without the index being maintained would silently drop every
    /// correction, so a new counter function has to be added here as well.
    ///
    /// It governs both readers of the index: the per-window kernel above, and the batch kernel,
    /// which reads [`FloatWindows::resets_of`]. When it returns `false`, `resets` stays empty and
    /// `resets_of` yields nothing, so a batch kernel for a counter function would skip every
    /// correction and report `delta`-like values.
    pub(crate) fn indexes_counter_resets(self) -> bool {
        matches!(self, Self::Rate | Self::Increase)
    }

    /// The batch form of this function, when it has one.
    ///
    /// Only the counter functions have one. The rest keep the per-window form, which the
    /// evaluator falls back to, so adding a function here is what changes which shape its
    /// evaluation takes.
    pub(crate) fn batch_kernel(self) -> Option<BatchKernel> {
        match self {
            Self::Rate => Some(Box::new(extrapolated_rate_batch::<true, true>)),
            Self::Increase => Some(Box::new(extrapolated_rate_batch::<true, false>)),
            _ => None,
        }
    }

    /// Returns the kernel computing this function over windows read with `offset_ms`.
    pub(crate) fn kernel(self, offset_ms: i64) -> WindowKernel {
        match self {
            Self::Rate => Box::new(extrapolated_rate::<true, true>),
            Self::Increase => Box::new(extrapolated_rate::<true, false>),
            Self::Delta => Box::new(extrapolated_rate::<false, false>),
            Self::Irate => Box::new(last_two::<true>),
            Self::Idelta => Box::new(last_two::<false>),
            Self::AvgOverTime => Box::new(|window| Some(avg(window.values))),
            Self::MinOverTime => Box::new(|window| extremum(window.values.iter().copied(), true)),
            Self::MaxOverTime => Box::new(|window| extremum(window.values.iter().copied(), false)),
            Self::SumOverTime => Box::new(|window| Some(sum(window.values))),
            Self::CountOverTime => Box::new(|window| Some(window.values.len() as f64)),
            Self::LastOverTime => Box::new(|window| window.values.last().copied()),
            Self::PresentOverTime => Box::new(|_| Some(1.0)),
            Self::StddevOverTime => Box::new(|window| Some(variance(window.values).sqrt())),
            Self::StdvarOverTime => Box::new(|window| Some(variance(window.values))),
            Self::QuantileOverTime(q) => {
                let mut scratch = Vec::new();
                Box::new(move |window| Some(quantile(q, window.values, &mut scratch)))
            }
            Self::Resets => {
                Box::new(|window| Some(count_edges(window.values, EdgeKind::Resets) as f64))
            }
            Self::Changes => {
                Box::new(|window| Some(count_edges(window.values, EdgeKind::Changes) as f64))
            }
            Self::Deriv => Box::new(|window| {
                (window.values.len() >= 2)
                    .then(|| linear_regression(&window, window.timestamps[0]).0)
            }),
            // Prometheus regresses the unshifted samples around the evaluation time, which on the
            // shifted timeline is `eval_ts + offset`; `RangeEvaluator::try_new` checks it fits.
            Self::PredictLinear(duration) => Box::new(move |window| {
                if window.values.len() < 2 {
                    return None;
                }
                let (slope, intercept) =
                    linear_regression(&window, window.eval_ts.checked_add(offset_ms)?);
                Some(slope * duration + intercept)
            }),
            Self::DoubleExponentialSmoothing { smoothing, trend } => Box::new(move |window| {
                double_exponential_smoothing(window.values, smoothing, trend)
            }),
            Self::FirstOverTime => Box::new(|window| window.values.first().copied()),
            Self::MadOverTime => {
                let (mut deviations, mut scratch) = (Vec::new(), Vec::new());
                Box::new(move |window| {
                    Some(median_absolute_deviation(
                        window.values,
                        &mut deviations,
                        &mut scratch,
                    ))
                })
            }
            // Window timestamps are shifted by the offset; Prometheus reports the sample's own.
            Self::TsOfFirstOverTime => {
                Box::new(move |window| Some(seconds(window.timestamps[0] - offset_ms)))
            }
            // Prometheus takes the max with the histogram timestamp, 0 when there is none, so a
            // negative timestamp reports 0.
            Self::TsOfLastOverTime => Box::new(move |window| {
                let last = window.timestamps[window.timestamps.len() - 1] - offset_ms;
                Some(seconds(last.max(0)))
            }),
            Self::TsOfMinOverTime => Box::new(move |window| {
                Some(seconds(timestamp_of_extremum(&window, false) - offset_ms))
            }),
            Self::TsOfMaxOverTime => Box::new(move |window| {
                Some(seconds(timestamp_of_extremum(&window, true) - offset_ms))
            }),
        }
    }
}

impl RangeFunction {
    /// Returns the kernel computing this function over windows read with `modifier`, for the
    /// functions the modifier is allowed with (`RangeEvaluator::try_new` checks that).
    ///
    /// The window holds the samples the modifier reads around it as well, on the evaluation
    /// timeline, so the window itself is `(eval_ts - range_ms, eval_ts]`.
    pub(crate) fn modified_kernel(self, modifier: RangeModifier) -> WindowKernel {
        let smoothed = matches!(modifier, RangeModifier::Smoothed { .. });
        match self {
            Self::Rate => Box::new(move |window| extended_rate(&window, smoothed, true, true)),
            Self::Increase => Box::new(move |window| extended_rate(&window, smoothed, true, false)),
            Self::Delta => Box::new(move |window| extended_rate(&window, smoothed, false, false)),
            Self::Resets => Box::new(|window| {
                anchored_values(&window).map(|values| count_edges(values, EdgeKind::Resets) as f64)
            }),
            Self::Changes => Box::new(|window| {
                anchored_values(&window).map(|values| count_edges(values, EdgeKind::Changes) as f64)
            }),
            _ => unreachable!("RangeEvaluator::try_new rejects {self:?} with a range modifier"),
        }
    }
}

/// Go `sort.Search(n, predicate)`: the first index in `[0, n)` the predicate holds at, or `n`.
fn search(n: usize, predicate: impl Fn(usize) -> bool) -> usize {
    (0..n).find(|&i| predicate(i)).unwrap_or(n)
}

/// Prometheus `extendedRate`: `rate`, `increase` or `delta` over an `anchored` or `smoothed`
/// window. The value at the window start is the last sample at or before it, or for `smoothed`
/// the interpolation between it and the next one; the end is picked or interpolated the same way.
/// Nothing is extrapolated.
fn extended_rate(
    window: &FloatWindow<'_>,
    smoothed: bool,
    is_counter: bool,
    is_rate: bool,
) -> Option<f64> {
    let (timestamps, values) = (window.timestamps, window.values);
    let range_start = window.eval_ts - window.range_ms;
    let range_end = window.eval_ts;
    let mut last = timestamps.len() - 1;
    let first = search(last, |i| timestamps[i] > range_start).saturating_sub(1);
    if smoothed {
        last = search(last, |i| timestamps[i] >= range_end);
    }
    if timestamps[last] <= range_start || (smoothed && timestamps[first] > range_end) {
        return None;
    }
    let left = if smoothed && timestamps[first] < range_start {
        interpolate(window, first, range_start, is_counter)
    } else {
        values[first]
    };
    let right = if smoothed && last > 0 && timestamps[last] > range_end {
        interpolate(window, last - 1, range_end, is_counter)
    } else {
        values[last]
    };
    let mut result = right - left;
    if is_counter {
        // Prometheus `correctForCounterResets` over the samples strictly inside the window; the
        // boundary values already account for the resets at the edges.
        let inner_start = first + usize::from(timestamps[first] <= range_start);
        let inner_end = last + 1 - usize::from(timestamps[last] >= range_end);
        let mut correction = 0.0;
        let mut previous = left;
        for &value in &values[inner_start..inner_end] {
            if value < previous {
                correction += previous;
            }
            previous = value;
        }
        if right < previous {
            correction += previous;
        }
        result += correction;
    }
    if is_rate {
        result /= window.range_ms as f64 / 1000.0;
    }
    Some(result)
}

/// Prometheus `interpolate` between the samples at `index` and `index + 1`, at `t`: a counter
/// that reset between them restarts from zero.
fn interpolate(window: &FloatWindow<'_>, index: usize, t: i64, is_counter: bool) -> f64 {
    let (t1, t2) = (window.timestamps[index], window.timestamps[index + 1]);
    let (mut y1, y2) = (window.values[index], window.values[index + 1]);
    if is_counter && y2 < y1 {
        y1 = 0.0;
    }
    y1 + (y2 - y1) * (t - t1) as f64 / (t2 - t1) as f64
}

/// Prometheus `pickFirstSampleIndices` for an `anchored` window of floats: the values from the
/// last sample at or before the window start on, or `None` when no sample lies after the start.
fn anchored_values<'a>(window: &FloatWindow<'a>) -> Option<&'a [f64]> {
    let range_start = window.eval_ts - window.range_ms;
    let after = window
        .timestamps
        .partition_point(|&timestamp| timestamp <= range_start);
    if after >= window.timestamps.len() {
        return None;
    }
    Some(&window.values[after.saturating_sub(1)..])
}

/// Prometheus `funcSumOverTime`.
fn sum(values: &[f64]) -> f64 {
    let (sum, c) = values
        .iter()
        .fold((0.0, 0.0), |(sum, c), &value| kahan_inc(value, sum, c));
    sum + c
}

/// Prometheus `funcAvgOverTime`: the Kahan sum divided by the count, switching to an
/// incremental mean once the sum would overflow.
fn avg(values: &[f64]) -> f64 {
    let (mut sum, mut count) = (values[0], 1.0);
    let (mut mean, mut kahan_c) = (0.0, 0.0);
    let mut incremental_mean = false;
    for (i, &value) in values[1..].iter().enumerate() {
        count = (i + 2) as f64;
        if !incremental_mean {
            let (new_sum, new_c) = kahan_inc(value, sum, kahan_c);
            if !new_sum.is_infinite() {
                (sum, kahan_c) = (new_sum, new_c);
                continue;
            }
            incremental_mean = true;
            mean = sum / (count - 1.0);
            kahan_c /= count - 1.0;
        }
        let q = (count - 1.0) / count;
        (mean, kahan_c) = kahan_inc(value / count, q * mean, q * kahan_c);
    }
    if incremental_mean {
        mean + kahan_c
    } else {
        sum / count + kahan_c / count
    }
}

/// Prometheus `varianceOverTime`, the population variance.
fn variance(values: &[f64]) -> f64 {
    let mut count = 0.0;
    let (mut mean, mut c_mean) = (0.0, 0.0);
    let (mut aux, mut c_aux) = (0.0, 0.0);
    for &value in values {
        count += 1.0;
        let delta = value - (mean + c_mean);
        (mean, c_mean) = kahan_inc(delta / count, mean, c_mean);
        (aux, c_aux) = kahan_inc(delta * (value - (mean + c_mean)), aux, c_aux);
    }
    (aux + c_aux) / count
}

/// Prometheus `quantile`, which sorts NaN before every number.
pub(crate) fn quantile(q: f64, values: &[f64], sorted: &mut Vec<f64>) -> f64 {
    if q.is_nan() {
        return f64::NAN;
    }
    if q < 0.0 {
        return f64::NEG_INFINITY;
    }
    if q > 1.0 {
        return f64::INFINITY;
    }
    sorted.clear();
    sorted.extend_from_slice(values);
    // `sort.Sort` with `vectorByValueHeap.Less`: NaN first, +0 and -0 tie, and which zero
    // lands at the rank decides the sign of the result.
    go_sort::sort_by(sorted, |a, b| a.is_nan() || a < b);
    let n = sorted.len() as f64;
    let rank = q * (n - 1.0);
    let lower = rank.floor().max(0.0);
    let upper = (n - 1.0).min(lower + 1.0);
    let weight = rank - rank.floor();
    sorted[lower as usize] * (1.0 - weight) + sorted[upper as usize] * weight
}

/// Prometheus `extrapolatedRate` over floats.
fn extrapolated_rate<const IS_COUNTER: bool, const IS_RATE: bool>(
    window: FloatWindow<'_>,
) -> Option<f64> {
    let FloatWindow {
        timestamps,
        values,
        counter_resets,
        eval_ts,
        range_ms,
    } = window;
    let end = values.len().checked_sub(1).filter(|n| *n > 0)?;
    extrapolated_rate_of(
        (timestamps[0], timestamps[end]),
        (values[0], values[end]),
        end,
        counter_resets.iter().map(|reset| reset.previous_value),
        eval_ts,
        range_ms,
        (IS_COUNTER, IS_RATE),
    )
}

/// The arithmetic of [`extrapolated_rate`], over a window's endpoints and the previous values of
/// its resets.
///
/// Both callers reach it: the per-window form with a slice of the field, the batch form with the
/// field's own arrays. Keeping it in one place is what holds their results equal; the two differ
/// only in how they find the endpoints.
fn extrapolated_rate_of(
    (first_t, last_t): (i64, i64),
    (first, last): (f64, f64),
    samples_minus_one: usize,
    resets: impl Iterator<Item = f64>,
    eval_ts: i64,
    range_ms: i64,
    (is_counter, is_rate): (bool, bool),
) -> Option<f64> {
    let mut result = last - first;
    if is_counter {
        // The same additions in the same order as scanning the window for resets, reached
        // through the evaluator's per-field reset index instead of the whole window.
        result = resets.fold(result, |result, previous_value| result + previous_value);
    }
    let zero_point = (is_counter && result > 0.0 && first >= 0.0).then(|| first / result);
    let factor = extrapolation_factor(
        ExtrapolationSpan {
            // A float sample carries no start timestamp, so both intervals start at the first
            // sample.
            sampled_from: first_t,
            average_from: first_t,
            last: last_t,
            samples_minus_one,
        },
        eval_ts,
        range_ms,
        is_counter,
        is_rate,
        zero_point,
    );
    Some(result * factor)
}

/// The batch form of [`extrapolated_rate`], over consecutive windows of one field at once.
///
/// Computes exactly what calling the per-window form once per point computes -- same arithmetic,
/// same order -- but reaches each window's endpoints straight out of the field's arrays. A rate
/// needs only the first and last sample, the sample count and the resets, so no per-window slice
/// is built and no window struct crosses a boxed call per grid point.
pub(crate) fn extrapolated_rate_batch<const IS_COUNTER: bool, const IS_RATE: bool>(
    batch: &FloatWindows<'_>,
    out: &mut Vec<Option<f64>>,
) {
    out.clear();
    out.reserve(batch.windows.len());
    for (index, span) in batch.windows.iter().enumerate() {
        let Some(samples_minus_one) = span.len().checked_sub(1).filter(|n| *n > 0) else {
            out.push(None);
            continue;
        };
        let (start, last) = (span.start, span.end - 1);
        out.push(extrapolated_rate_of(
            (batch.timestamps[start], batch.timestamps[last]),
            (batch.values[start], batch.values[last]),
            samples_minus_one,
            batch
                .resets_of(index)
                .iter()
                .map(|reset| reset.previous_value),
            batch.eval_ts[index],
            batch.range_ms,
            (IS_COUNTER, IS_RATE),
        ));
    }
}

/// The span one window's extrapolation factor is measured over.
///
/// A float window has one anchor: its first sample starts both the interval the increase was
/// sampled over and the interval the average sample distance is measured over. A native histogram
/// counter can have two, because a `start_timestamp` inside the window is a synthetic zero the
/// sampled interval starts at, while the average sample distance still belongs to the real samples.
pub(super) struct ExtrapolationSpan {
    /// The first sample, or a native histogram's synthetic zero.
    pub sampled_from: i64,
    /// The first sample, always.
    pub average_from: i64,
    /// The last sample.
    pub last: i64,
    /// Intervals between samples, i.e. the sample count minus one; `0` when the window has a
    /// single sample and so no average interval to measure.
    pub samples_minus_one: usize,
}

/// The factor `extrapolatedRate` scales the sampled increase by. `zero_point` is the first value
/// over the increase, given only when Prometheus extrapolates a counter to zero: it is the share of
/// the sampled interval that lies before `span.sampled_from`, so `0` says the counter's zero point
/// is `span.sampled_from` itself, which is what a synthetic zero is.
///
/// Floats and native histograms both come through here, and this is the only place the factor is
/// computed; the two routes differ in the anchors they read, never in the arithmetic. The one
/// deliberate difference is not in the formula: a zero `sampled_interval` makes this function
/// answer `1.0` (`1 / range` for a rate), which is the identity a float window wants to multiply
/// its increase by, while the histogram caller answers no value at all before calling this. Its
/// buckets are all zero there, and an `Inf` or `NaN` rate would only poison what consumes it
/// downstream; a float window has no such leftovers to hand on.
pub(super) fn extrapolation_factor(
    span: ExtrapolationSpan,
    eval_ts: i64,
    range_ms: i64,
    is_counter: bool,
    is_rate: bool,
    zero_point: Option<f64>,
) -> f64 {
    let ExtrapolationSpan {
        sampled_from,
        average_from,
        last,
        samples_minus_one,
    } = span;
    // Millisecond differences in 64-bit arithmetic: widening to `i128` and narrowing back with
    // `as f64` compiles to a call to the `__floattidf` builtin instead of an instruction, once per
    // grid point per series. The subtractions wrap, which is what Go's `int64` arithmetic does, so
    // an out-of-range window start produces the same value Prometheus computes rather than a
    // clamped one; real timestamps are nowhere near that range, where both agree.
    let range_start = eval_ts.wrapping_sub(range_ms);
    let mut duration_to_start = average_from.wrapping_sub(range_start) as f64 / 1000.0;
    let mut duration_to_end = eval_ts.wrapping_sub(last) as f64 / 1000.0;
    let sampled_interval = last.wrapping_sub(sampled_from) as f64 / 1000.0;
    let average_interval = if samples_minus_one == 0 {
        0.0
    } else {
        last.wrapping_sub(average_from) as f64 / 1000.0 / samples_minus_one as f64
    };
    let threshold = average_interval * 1.1;
    if duration_to_start >= threshold {
        duration_to_start = average_interval / 2.0;
    }
    if is_counter {
        let mut duration_to_zero = duration_to_start;
        if let Some(zero_point) = zero_point {
            duration_to_zero = sampled_interval * zero_point;
        }
        if duration_to_zero < duration_to_start {
            duration_to_start = duration_to_zero;
        }
    }
    if duration_to_end >= threshold {
        duration_to_end = average_interval / 2.0;
    }
    let mut factor = 1.0;
    if sampled_interval != 0.0 {
        factor = (sampled_interval + duration_to_start + duration_to_end) / sampled_interval;
    }
    if is_rate {
        factor /= range_ms as f64 / 1000.0;
    }
    factor
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

/// Prometheus `linearRegression`: `(slope, intercept)` with `x` in seconds from
/// `intercept_time`. Needs at least two samples.
fn linear_regression(window: &FloatWindow<'_>, intercept_time: i64) -> (f64, f64) {
    let init_y = window.values[0];
    let mut const_y = true;
    let mut n = 0.0;
    let (mut sum_x, mut c_x) = (0.0, 0.0);
    let (mut sum_y, mut c_y) = (0.0, 0.0);
    let (mut sum_xy, mut c_xy) = (0.0, 0.0);
    let (mut sum_x2, mut c_x2) = (0.0, 0.0);
    for (i, (&t, &y)) in window.timestamps.iter().zip(window.values).enumerate() {
        if const_y && i > 0 && y != init_y {
            const_y = false;
        }
        n += 1.0;
        let x = (i128::from(t) - i128::from(intercept_time)) as f64 / 1e3;
        (sum_x, c_x) = kahan_inc(x, sum_x, c_x);
        (sum_y, c_y) = kahan_inc(y, sum_y, c_y);
        (sum_xy, c_xy) = kahan_inc(x * y, sum_xy, c_xy);
        (sum_x2, c_x2) = kahan_inc(x * x, sum_x2, c_x2);
    }
    if const_y {
        if init_y.is_infinite() {
            return (f64::NAN, f64::NAN);
        }
        return (0.0, init_y);
    }
    sum_x += c_x;
    sum_y += c_y;
    sum_xy += c_xy;
    sum_x2 += c_x2;
    let cov_xy = sum_xy - sum_x * sum_y / n;
    let var_x = sum_x2 - sum_x * sum_x / n;
    let slope = cov_xy / var_x;
    let intercept = sum_y / n - slope * sum_x / n;
    (slope, intercept)
}

/// Prometheus `funcDoubleExponentialSmoothing`; `RangeEvaluator` checks the factors.
fn double_exponential_smoothing(values: &[f64], sf: f64, tf: f64) -> Option<f64> {
    if values.len() < 2 {
        return None;
    }
    let (mut s0, mut s1) = (0.0, values[0]);
    let mut b = values[1] - values[0];
    for (i, &value) in values.iter().enumerate().skip(1) {
        let x = sf * value;
        b = if i == 1 {
            b
        } else {
            tf * (s1 - s0) + (1.0 - tf) * b
        };
        let y = (1.0 - sf) * (s1 + b);
        (s0, s1) = (s1, x + y);
    }
    Some(s1)
}

pub(super) fn seconds(timestamp_ms: i64) -> f64 {
    timestamp_ms as f64 / 1000.0
}

/// Prometheus `compareOverTime` returning the timestamp: the last sample holding the maximum
/// (or minimum), where a NaN so far is replaced by any later sample.
fn timestamp_of_extremum(window: &FloatWindow<'_>, max: bool) -> i64 {
    let (mut best, mut timestamp) = (window.values[0], window.timestamps[0]);
    for (&value, &ts) in window.values.iter().zip(window.timestamps) {
        let replace = if max { value >= best } else { value <= best };
        if replace || best.is_nan() {
            (best, timestamp) = (value, ts);
        }
    }
    timestamp
}

/// Prometheus `funcMadOverTime`: the median of the absolute deviations from the median; NaN
/// when any sample is NaN.
fn median_absolute_deviation(
    values: &[f64],
    deviations: &mut Vec<f64>,
    sorted: &mut Vec<f64>,
) -> f64 {
    if values.iter().any(|value| value.is_nan()) {
        return f64::NAN;
    }
    let median = quantile(0.5, values, sorted);
    deviations.clear();
    deviations.extend(values.iter().map(|value| (value - median).abs()));
    quantile(0.5, deviations, sorted)
}

impl PartialEq for RangeFunction {
    fn eq(&self, o: &Self) -> bool {
        match (self, o) {
            (Self::QuantileOverTime(a), Self::QuantileOverTime(b))
            | (Self::PredictLinear(a), Self::PredictLinear(b)) => a.to_bits() == b.to_bits(),
            (
                Self::DoubleExponentialSmoothing {
                    smoothing: a,
                    trend: b,
                },
                Self::DoubleExponentialSmoothing {
                    smoothing: c,
                    trend: d,
                },
            ) => a.to_bits() == c.to_bits() && b.to_bits() == d.to_bits(),
            _ => std::mem::discriminant(self) == std::mem::discriminant(o),
        }
    }
}
impl Eq for RangeFunction {}
impl std::hash::Hash for RangeFunction {
    fn hash<H: std::hash::Hasher>(&self, h: &mut H) {
        std::mem::discriminant(self).hash(h);
        match self {
            Self::QuantileOverTime(x) | Self::PredictLinear(x) => x.to_bits().hash(h),
            Self::DoubleExponentialSmoothing { smoothing, trend } => {
                smoothing.to_bits().hash(h);
                trend.to_bits().hash(h)
            }
            _ => (),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::{ExtrapolationSpan, extrapolation_factor};

    /// One window as both range-function routes read it: `sampled_from` starts the interval the
    /// increase was sampled over, `average_from` the interval the average sample distance is
    /// measured over. They are the same timestamp except for a native histogram counter with a
    /// synthetic zero, whose sampled interval starts at the zero.
    #[derive(Clone, Copy, Debug)]
    struct Window {
        sampled_from: i64,
        average_from: i64,
        last: i64,
        samples_minus_one: usize,
        eval_ts: i64,
        range_ms: i64,
        is_counter: bool,
        is_rate: bool,
        zero_point: Option<f64>,
    }

    impl Window {
        /// The factor the float route computes. A float sample carries no start timestamp, so its
        /// single anchor starts both intervals and `sampled_from` plays no part.
        fn as_float(self) -> f64 {
            extrapolation_factor(
                ExtrapolationSpan {
                    sampled_from: self.average_from,
                    average_from: self.average_from,
                    last: self.last,
                    samples_minus_one: self.samples_minus_one,
                },
                self.eval_ts,
                self.range_ms,
                self.is_counter,
                self.is_rate,
                self.zero_point,
            )
        }

        /// The factor the native histogram route computes, with both anchors as its call site
        /// builds them.
        fn as_histogram(self) -> f64 {
            extrapolation_factor(
                ExtrapolationSpan {
                    sampled_from: self.sampled_from,
                    average_from: self.average_from,
                    last: self.last,
                    samples_minus_one: self.samples_minus_one,
                },
                self.eval_ts,
                self.range_ms,
                self.is_counter,
                self.is_rate,
                self.zero_point,
            )
        }
    }

    /// Both routes reach one function, so their factors can only differ in how each fills the span.
    /// A window without a synthetic zero is filled the same way by both, and the factors have to
    /// agree to the bit -- the assertion that fails as soon as one of them grows arithmetic of its
    /// own again.
    #[test]
    fn both_routes_compute_one_extrapolation_factor() {
        // Evaluation at 6500ms over a 6000ms range: the window opens at 500ms and the last sample
        // sits 1500ms inside its closed end.
        let windows = [
            Window {
                sampled_from: 2_000,
                average_from: 2_000,
                last: 5_000,
                samples_minus_one: 1,
                eval_ts: 6_500,
                range_ms: 6_000,
                is_counter: true,
                is_rate: false,
                zero_point: None,
            },
            Window {
                sampled_from: 2_000,
                average_from: 2_000,
                last: 5_000,
                samples_minus_one: 2,
                eval_ts: 6_500,
                range_ms: 6_000,
                is_counter: true,
                is_rate: true,
                zero_point: None,
            },
            Window {
                sampled_from: 2_000,
                average_from: 2_000,
                last: 5_000,
                samples_minus_one: 2,
                eval_ts: 6_500,
                range_ms: 6_000,
                is_counter: false,
                is_rate: false,
                zero_point: None,
            },
            // The last sample further in than the average interval allows, so both ends fall back
            // to half of it.
            Window {
                sampled_from: 4_000,
                average_from: 4_000,
                last: 4_500,
                samples_minus_one: 1,
                eval_ts: 6_500,
                range_ms: 6_000,
                is_counter: true,
                is_rate: true,
                zero_point: None,
            },
            // A single sample holds no average interval to measure.
            Window {
                sampled_from: 5_000,
                average_from: 5_000,
                last: 5_000,
                samples_minus_one: 0,
                eval_ts: 6_500,
                range_ms: 6_000,
                is_counter: true,
                is_rate: false,
                zero_point: None,
            },
        ];
        for window in windows {
            for zero_point in [None, Some(0.0), Some(0.25), Some(1.5)] {
                let window = Window {
                    zero_point,
                    ..window
                };
                assert_eq!(
                    window.as_float().to_bits(),
                    window.as_histogram().to_bits(),
                    "{window:?}"
                );
            }
        }
    }

    /// A synthetic zero puts the sampled anchor before the average anchor, and no float window
    /// describes that: a float window starting at the zero would move its average interval there
    /// too. The two-anchor numbers are pinned instead, at the geometry
    /// `synthetic_zero_still_extrapolates_to_the_window_end` and the Prometheus goldens carry, so a
    /// swapped anchor cannot pass by agreeing with itself.
    #[test]
    fn a_synthetic_zero_moves_only_the_sampled_anchor() {
        // Three real samples from 2000ms to 5000ms behind a zero at 1000ms: sampled 4000ms,
        // average (5000-2000)/2 = 1500ms, and 1500ms from the last sample to the range end.
        // (4000 + 0 + 1500) / 4000 = 1.375; taking the average from the sampled interval instead
        // divides by three and answers 1.25.
        let window = Window {
            sampled_from: 1_000,
            average_from: 2_000,
            last: 5_000,
            samples_minus_one: 2,
            eval_ts: 6_500,
            range_ms: 6_000,
            is_counter: true,
            is_rate: false,
            zero_point: Some(0.0),
        };
        assert_eq!(window.as_histogram().to_bits(), 1.375f64.to_bits());
        assert_eq!(
            Window {
                is_rate: true,
                ..window
            }
            .as_histogram()
            .to_bits(),
            (1.375f64 / 6.0).to_bits()
        );
        // The float route on the same numbers starts its sampled interval at the first sample,
        // which is a different window: 1.5 rather than 1.375.
        assert_eq!(window.as_float().to_bits(), 1.5f64.to_bits());

        // One real sample behind the zero leaves no average interval, so neither end is
        // extrapolated and the factor is the identity.
        let single = Window {
            sampled_from: 1_000,
            average_from: 2_000,
            last: 2_000,
            samples_minus_one: 0,
            ..window
        };
        assert_eq!(single.as_histogram().to_bits(), 1.0f64.to_bits());
        assert_eq!(
            Window {
                is_rate: true,
                ..single
            }
            .as_histogram()
            .to_bits(),
            (1.0f64 / 6.0).to_bits()
        );
    }
}
