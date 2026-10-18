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

//! Range functions over windows that hold native histogram samples, following the histogram
//! branches of Prometheus' range functions. Windows of floats alone never get here.

use std::borrow::Cow;

use datafusion::error::Result;
use datatypes::series::histogram::CounterResetHint;

use crate::functions::{EdgeKind, count_edges, instant_delta};
use crate::grid_series::GridSample;
use crate::histogram_eval::HistogramAggregate;
use crate::histogram_eval::float_histogram::FloatHistogram;
use crate::histogram_eval::sum_or_avg;
use crate::range_eval::function::{extrapolation_factor, seconds};
use crate::range_eval::{FloatWindow, RangeFunction, WindowKernel};

/// Histogram samples of one window at their shifted timestamps, in time order.
type Histograms = [(i64, FloatHistogram)];

/// The window of one field at `eval_ts`, holding `histograms` (not empty) and `floats` when it
/// has any float sample.
pub(super) fn evaluate(
    function: RangeFunction,
    floats: Option<FloatWindow<'_>>,
    histograms: &Histograms,
    eval_ts: i64,
    range_ms: i64,
    offset_ms: i64,
    kernel: &mut WindowKernel,
) -> Result<Option<GridSample>> {
    use RangeFunction::*;
    let float = |value: f64| Some(GridSample::Float(value));
    let (last_ts, last) = &histograms[histograms.len() - 1];
    let (first_ts, first) = &histograms[0];
    Ok(match function {
        // Prometheus reads only the floats of these.
        MinOverTime
        | MaxOverTime
        | StddevOverTime
        | StdvarOverTime
        | QuantileOverTime(_)
        | Deriv
        | PredictLinear(_)
        | DoubleExponentialSmoothing { .. }
        | MadOverTime
        | TsOfMinOverTime
        | TsOfMaxOverTime => floats.and_then(kernel).map(GridSample::Float),
        CountOverTime => {
            float((floats.map_or(0, |window| window.values.len()) + histograms.len()) as f64)
        }
        PresentOverTime => float(1.0),
        LastOverTime => match floats {
            Some(window) if last_ts < window.timestamps.last().expect("not empty") => {
                float(window.values[window.values.len() - 1])
            }
            _ => Some(GridSample::Histogram(last.clone())),
        },
        FirstOverTime => match floats {
            Some(window) if window.timestamps[0] < *first_ts => float(window.values[0]),
            _ => Some(GridSample::Histogram(first.clone())),
        },
        TsOfFirstOverTime => {
            let ts = floats.map_or(*first_ts, |window| window.timestamps[0].min(*first_ts));
            float(seconds(ts - offset_ms))
        }
        // Go takes the max with 0 for a kind of sample the window does not have.
        TsOfLastOverTime => {
            let histogram_ts = last_ts - offset_ms;
            let ts = match floats {
                Some(window) => {
                    (window.timestamps.last().expect("not empty") - offset_ms).max(histogram_ts)
                }
                None => histogram_ts.max(0),
            };
            float(seconds(ts))
        }
        Resets => float(edges(floats, histograms, EdgeKind::Resets)? as f64),
        Changes => float(edges(floats, histograms, EdgeKind::Changes)? as f64),
        SumOverTime | AvgOverTime if floats.is_none() => {
            let op = if function == SumOverTime {
                HistogramAggregate::Sum
            } else {
                HistogramAggregate::Avg
            };
            sum_or_avg(op, histograms.iter().map(|(_, h)| h.clone()))?.map(GridSample::Histogram)
        }
        Rate | Increase | Delta if floats.is_none() && histograms.len() > 1 => {
            let is_counter = function != Delta;
            let Some(mut result) = histogram_rate(histograms, is_counter)? else {
                return Ok(None);
            };
            let zero_point = (is_counter && result.count > 0.0 && first.count >= 0.0)
                .then(|| first.count / result.count);
            result.mul(extrapolation_factor(
                (*first_ts, *last_ts),
                histograms.len() - 1,
                eval_ts,
                range_ms,
                is_counter,
                function == Rate,
                zero_point,
            ));
            Some(GridSample::Histogram(result))
        }
        Irate => last_two::<true>(floats, histograms)?,
        Idelta => last_two::<false>(floats, histograms)?,
        // Mixed windows, and rates over fewer than two histograms.
        SumOverTime | AvgOverTime | Rate | Increase | Delta => None,
    })
}

/// One sample of a window, in the order Prometheus merges floats and histograms.
#[derive(Clone, Copy)]
enum Sample<'a> {
    Float(f64),
    Histogram(&'a FloatHistogram),
}

/// The samples of both kinds in time order.
fn merged<'a>(
    floats: Option<FloatWindow<'a>>,
    histograms: &'a Histograms,
) -> impl Iterator<Item = (i64, Sample<'a>)> {
    let mut floats = floats
        .into_iter()
        .flat_map(|window| window.timestamps.iter().zip(window.values))
        .map(|(&ts, &value)| (ts, Sample::Float(value)))
        .peekable();
    let mut histograms = histograms
        .iter()
        .map(|(ts, h)| (*ts, Sample::Histogram(h)))
        .peekable();
    std::iter::from_fn(move || match (floats.peek(), histograms.peek()) {
        (Some((float_ts, _)), Some((histogram_ts, _))) if float_ts < histogram_ts => floats.next(),
        (_, Some(_)) => histograms.next(),
        (Some(_), None) => floats.next(),
        (None, None) => None,
    })
}

/// Prometheus `funcResets` and `funcChanges` over floats and histograms: a change of kind
/// counts as one edge.
fn edges(floats: Option<FloatWindow<'_>>, histograms: &Histograms, kind: EdgeKind) -> Result<u64> {
    let mut count = 0;
    let mut previous: Option<Sample<'_>> = None;
    for (_, current) in merged(floats, histograms) {
        if let Some(previous) = previous {
            count += match (previous, current) {
                (Sample::Float(a), Sample::Float(b)) => count_edges(&[a, b], kind),
                (Sample::Histogram(a), Sample::Histogram(b)) => u64::from(match kind {
                    EdgeKind::Resets => b.detect_reset(a)?,
                    EdgeKind::Changes => !b.equals(a),
                }),
                _ => 1,
            };
        }
        previous = Some(current);
    }
    Ok(count)
}

/// Prometheus `instantValue` over the last two samples of either kind; a float and a histogram
/// give nothing.
fn last_two<const IS_RATE: bool>(
    floats: Option<FloatWindow<'_>>,
    histograms: &Histograms,
) -> Result<Option<GridSample>> {
    let floats = floats.map(|window| {
        let from = window.values.len().saturating_sub(2);
        FloatWindow {
            timestamps: &window.timestamps[from..],
            values: &window.values[from..],
            ..window
        }
    });
    let histograms = &histograms[histograms.len().saturating_sub(2)..];
    let tail: Vec<_> = merged(floats, histograms).collect();
    let [.., (prev_ts, prev), (last_ts, last)] = tail[..] else {
        return Ok(None);
    };
    Ok(match (prev, last) {
        (Sample::Float(prev), Sample::Float(last)) => {
            instant_delta::<IS_RATE>((prev_ts, prev), (last_ts, last)).map(GridSample::Float)
        }
        (Sample::Histogram(prev), Sample::Histogram(last)) => {
            let interval = last_ts - prev_ts;
            let mut result = last.clone();
            if !IS_RATE || !last.detect_reset(prev)? {
                if result.uses_custom_buckets() != prev.uses_custom_buckets() {
                    return Ok(None);
                }
                result.sub(prev)?;
            }
            result.reset_hint = CounterResetHint::Gauge;
            result.compact();
            if IS_RATE {
                result.div(interval as f64 / 1000.0);
            }
            Some(GridSample::Histogram(result))
        }
        _ => None,
    })
}

/// Prometheus `histogramRate`: the increase from the first to the last histogram, adding back
/// what counter resets dropped. `None` when the histograms mix exponential and custom buckets.
fn histogram_rate(points: &Histograms, is_counter: bool) -> Result<Option<FloatHistogram>> {
    let last = &points[points.len() - 1].1;
    let mut prev = Cow::Borrowed(&points[0].1);
    let mut uses_custom_buckets = prev.uses_custom_buckets();
    // A reset between the first two samples makes the first one irrelevant, layout included.
    if is_counter {
        let second = &points[1].1;
        if second.detect_reset(&prev)? {
            prev = Cow::Owned(second.empty_like());
            uses_custom_buckets = second.uses_custom_buckets();
        }
    }
    if last.uses_custom_buckets() != uses_custom_buckets {
        return Ok(None);
    }
    let mut min_schema = last.schema.min(prev.schema);
    if is_counter {
        for (_, current) in &points[1..points.len() - 1] {
            min_schema = min_schema.min(current.schema);
            if current.uses_custom_buckets() != uses_custom_buckets {
                return Ok(None);
            }
        }
    }
    let mut result = last.copy_to_schema(min_schema)?;
    result.sub(&prev)?;
    if is_counter {
        for (_, current) in &points[1..] {
            if current.detect_reset(&prev)? {
                result.add(&prev)?;
            }
            prev = Cow::Borrowed(current);
        }
    }
    result.reset_hint = CounterResetHint::Gauge;
    result.compact();
    Ok(Some(result))
}
