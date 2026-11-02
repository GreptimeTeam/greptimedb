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

use common_query::promql_annotations::PromqlAnnotationCollector;
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
#[allow(clippy::too_many_arguments)]
pub(super) fn evaluate(
    function: RangeFunction,
    floats: Option<FloatWindow<'_>>,
    histograms: &Histograms,
    eval_ts: i64,
    range_ms: i64,
    offset_ms: i64,
    kernel: &mut WindowKernel,
    collector: Option<&PromqlAnnotationCollector>,
) -> Result<Option<GridSample>> {
    use RangeFunction::*;
    if floats.is_some()
        && !matches!(
            function,
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
                | TsOfMaxOverTime
                | CountOverTime
                | PresentOverTime
                | LastOverTime
                | FirstOverTime
                | TsOfFirstOverTime
                | TsOfLastOverTime
                | Resets
                | Changes
                | Irate
                | Idelta
        )
    {
        record_warning(
            collector,
            format!(
                "{}: encountered a mix of float and native histogram samples",
                function_name(function)
            ),
        );
    }
    // The functions that read only the floats of a mixed window ignore its histograms, which is
    // what the existing planner tells the client with an info. The set is the one that path lists
    // as `FloatOnly`; the experimental functions this crate also has carry no message there, so
    // they stay silent rather than inventing one.
    if floats.is_some()
        && matches!(
            function,
            MinOverTime
                | MaxOverTime
                | StddevOverTime
                | StdvarOverTime
                | QuantileOverTime(_)
                | Deriv
                | PredictLinear(_)
                | DoubleExponentialSmoothing { .. }
        )
    {
        record_info(
            collector,
            format!(
                "{}: ignored native histogram samples",
                function_name(function)
            ),
        );
    }
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
            _ => Some(GridSample::Histogram(Box::new(last.clone()))),
        },
        FirstOverTime => match floats {
            Some(window) if window.timestamps[0] < *first_ts => float(window.values[0]),
            _ => Some(GridSample::Histogram(Box::new(first.clone()))),
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
            // The hints are read from the window's own histograms, so nothing is cloned for the
            // check; the fold below clones one sample at a time as it consumes them.
            if has_contradictory_reset_hints(histograms.iter().map(|(_, h)| h)) {
                record_warning(
                    collector,
                    format!(
                        "{}: native histogram counter reset hints contradict",
                        function_name(function)
                    ),
                );
            }
            // The fold reports both answers itself: a `None` is the mixed-bucket drop, and the
            // flag is the reconciliation its own adds already observed. Re-walking the pairs to
            // learn either would double the histogram work of every window.
            let (result, custom_reconciled) =
                sum_or_avg(op, histograms.iter().map(|(_, h)| Ok(h.clone())))?;
            if result.is_none() {
                record_warning(
                    collector,
                    format!(
                        "{}: dropped native histogram range with incompatible schemas",
                        function_name(function)
                    ),
                );
            }
            if custom_reconciled {
                record_info(
                    collector,
                    format!(
                        "{}: reconciled native histograms with different custom buckets",
                        function_name(function)
                    ),
                );
            }
            result.map(|h| GridSample::Histogram(Box::new(h)))
        }
        Rate | Increase if floats.is_none() && histograms.len() == 1 => {
            let (first_ts, first) = &histograms[0];
            let range_start = eval_ts - range_ms;
            if first.start_timestamp.is_some_and(|start| {
                start != 0
                    && start.checked_add(offset_ms).is_some_and(|shifted_start| {
                        range_start < shifted_start && shifted_start < *first_ts
                    })
            }) {
                record_histogram_flavor_warning(collector, function, histograms);
                let mut result = first.clone();
                let factor = if function == Rate {
                    1.0 / seconds(range_ms)
                } else {
                    1.0
                };
                result.mul(factor);
                result.reset_hint = CounterResetHint::Gauge;
                Some(GridSample::Histogram(Box::new(result)))
            } else {
                None
            }
        }
        Rate | Increase | Delta if floats.is_none() && histograms.len() > 1 => {
            let is_counter = function != Delta;
            record_histogram_flavor_warning(collector, function, histograms);
            record_histogram_pair_annotations(collector, function, histograms);
            let range_start = eval_ts - range_ms;
            let synthetic_zero = is_counter
                .then_some(first.start_timestamp)
                .flatten()
                .and_then(|start| start.checked_add(offset_ms).map(|shifted| (start, shifted)))
                .filter(|(start, shifted)| {
                    *start != 0 && range_start < *shifted && *shifted < *first_ts
                })
                .map(|(_, shifted)| shifted);
            let Some(mut result) = histogram_rate(histograms, is_counter, offset_ms)? else {
                record_warning(
                    collector,
                    format!(
                        "{}: dropped native histogram range with incompatible schemas",
                        function_name(function)
                    ),
                );
                return Ok(None);
            };
            let effective_first_ts = synthetic_zero.unwrap_or(*first_ts);
            let sample_count_minus_one =
                histograms.len() - 1 + usize::from(synthetic_zero.is_some());
            if *last_ts <= effective_first_ts {
                return Ok(None);
            }
            let zero_point = (is_counter && synthetic_zero.is_none() && result.count > 0.0)
                .then(|| first.count / result.count);
            let factor = if let Some(synthetic_zero_start) = synthetic_zero {
                // The sampled interval runs from the synthetic zero while the average interval is
                // measured between the real samples, as `native_extrapolated_rate` does: they are
                // two different spans, and taking one for the other shifts the average by a whole
                // sample interval.
                let sampled_interval = (*last_ts - synthetic_zero_start) as f64;
                let average_interval =
                    (*last_ts - *first_ts) as f64 / (histograms.len() - 1) as f64;
                let duration_to_start = 0.0;
                let duration_to_end = (eval_ts - *last_ts) as f64;
                let threshold = average_interval * 1.1;
                let extrapolated = sampled_interval
                    + if duration_to_start < threshold {
                        duration_to_start
                    } else {
                        average_interval / 2.0
                    }
                    + if duration_to_end < threshold {
                        duration_to_end
                    } else {
                        average_interval / 2.0
                    };
                let factor = extrapolated / sampled_interval;
                if function == Rate {
                    factor / (range_ms as f64 / 1000.0)
                } else {
                    factor
                }
            } else {
                extrapolation_factor(
                    (effective_first_ts, *last_ts),
                    sample_count_minus_one,
                    eval_ts,
                    range_ms,
                    is_counter,
                    function == Rate,
                    zero_point,
                )
            };
            if synthetic_zero.is_some() && result.add(first).is_err() {
                record_warning(
                    collector,
                    format!(
                        "{}: dropped native histogram range with incompatible schemas",
                        function_name(function)
                    ),
                );
                return Ok(None);
            }
            result.mul(factor);
            Some(GridSample::Histogram(Box::new(result)))
        }
        Irate | Idelta => {
            // The annotations read the window before `last_two` consumes it.
            let had_floats = floats.is_some();
            record_mixed_last_two(collector, function, floats.as_ref(), histograms);
            let tail_histograms = &histograms[histograms.len().saturating_sub(2)..];
            if tail_histograms.len() == 2 {
                record_histogram_flavor_warning(collector, function, tail_histograms);
                if tail_histograms[1].0 > tail_histograms[0].0 {
                    record_histogram_pair_annotations(collector, function, tail_histograms);
                }
            }
            let result = if function == Irate {
                last_two::<true>(floats, histograms, offset_ms)?
            } else {
                last_two::<false>(floats, histograms, offset_ms)?
            };
            if result.is_none() && !had_floats && tail_histograms.len() == 2 {
                record_warning(
                    collector,
                    format!(
                        "{}: dropped native histogram range with incompatible schemas",
                        function_name(function)
                    ),
                );
            }
            result
        }
        // Mixed windows, and rates over fewer than two histograms. The mixed warning was already
        // recorded above for every function that reaches here, so this branch only answers.
        SumOverTime | AvgOverTime | Rate | Increase | Delta => None,
    })
}

fn function_name(function: RangeFunction) -> &'static str {
    use RangeFunction::*;
    match function {
        Rate => "rate",
        Increase => "increase",
        Delta => "delta",
        Irate => "irate",
        Idelta => "idelta",
        AvgOverTime => "avg_over_time",
        MinOverTime => "min_over_time",
        MaxOverTime => "max_over_time",
        SumOverTime => "sum_over_time",
        CountOverTime => "count_over_time",
        LastOverTime => "last_over_time",
        PresentOverTime => "present_over_time",
        StddevOverTime => "stddev_over_time",
        StdvarOverTime => "stdvar_over_time",
        QuantileOverTime(_) => "quantile_over_time",
        Resets => "resets",
        Changes => "changes",
        Deriv => "deriv",
        PredictLinear(_) => "predict_linear",
        DoubleExponentialSmoothing { .. } => "double_exponential_smoothing",
        FirstOverTime => "first_over_time",
        MadOverTime => "mad_over_time",
        TsOfFirstOverTime => "ts_of_first_over_time",
        TsOfLastOverTime => "ts_of_last_over_time",
        TsOfMinOverTime => "ts_of_min_over_time",
        TsOfMaxOverTime => "ts_of_max_over_time",
    }
}

fn record_warning(collector: Option<&PromqlAnnotationCollector>, message: String) {
    if let Some(collector) = collector {
        collector.record_warning(message);
    }
}
fn record_info(collector: Option<&PromqlAnnotationCollector>, message: String) {
    if let Some(collector) = collector {
        collector.record_info(message);
    }
}
fn has_contradictory_reset_hints<'a>(
    samples: impl IntoIterator<Item = &'a FloatHistogram>,
) -> bool {
    let mut saw_counter = false;
    let mut saw_not_counter = false;
    for histogram in samples {
        saw_counter |= histogram.reset_hint == CounterResetHint::CounterReset;
        saw_not_counter |= histogram.reset_hint == CounterResetHint::NotCounterReset;
    }
    saw_counter && saw_not_counter
}

/// The info a `rate`/`increase`/`delta` window owes when adjacent histograms carry different
/// custom bucket bounds and the fold has to reconcile them.
///
/// The check compares bounds; it does not add the pair. The fold that follows does that work, and
/// repeating it here would double the histogram arithmetic of every window. This is the same
/// per-pair check the existing planner's counter UDF makes.
fn record_histogram_flavor_warning(
    collector: Option<&PromqlAnnotationCollector>,
    function: RangeFunction,
    histograms: &Histograms,
) {
    let is_counter = matches!(
        function,
        RangeFunction::Rate | RangeFunction::Increase | RangeFunction::Irate
    );
    let wrong_flavor = if is_counter {
        histograms
            .iter()
            .any(|(_, histogram)| histogram.reset_hint == CounterResetHint::Gauge)
    } else {
        histograms
            .first()
            .is_some_and(|(_, histogram)| histogram.reset_hint != CounterResetHint::Gauge)
            || histograms
                .last()
                .is_some_and(|(_, histogram)| histogram.reset_hint != CounterResetHint::Gauge)
    };
    if wrong_flavor {
        let expected = if is_counter { "counter" } else { "gauge" };
        record_warning(
            collector,
            format!(
                "{}: native histogram input should be a {expected} histogram",
                function_name(function)
            ),
        );
    }
}

fn record_histogram_pair_annotations(
    collector: Option<&PromqlAnnotationCollector>,
    function: RangeFunction,
    histograms: &Histograms,
) {
    let reconciled = histograms.windows(2).any(|pair| {
        pair[0].1.uses_custom_buckets()
            && pair[1].1.uses_custom_buckets()
            && pair[0].1.custom_values != pair[1].1.custom_values
    });
    if reconciled {
        record_info(
            collector,
            format!(
                "{}: reconciled native histograms with different custom buckets",
                function_name(function)
            ),
        );
    }
}

fn record_mixed_last_two(
    collector: Option<&PromqlAnnotationCollector>,
    function: RangeFunction,
    floats: Option<&FloatWindow<'_>>,
    histograms: &Histograms,
) {
    // Nothing is recorded without a collector, and the walk below exists only to decide whether
    // something would be.
    if collector.is_none() {
        return;
    }
    let Some(floats) = floats else {
        return;
    };
    let mut float_index = floats.values.len();
    let mut histogram_index = histograms.len();
    let mut last = None;
    let mut previous = None;
    for _ in 0..2 {
        let sample = match (float_index.checked_sub(1), histogram_index.checked_sub(1)) {
            (Some(fi), Some(hi)) if floats.timestamps[fi] >= histograms[hi].0 => {
                float_index = fi;
                Some(Sample::Float(floats.values[fi]))
            }
            (Some(_), Some(hi)) => {
                histogram_index = hi;
                Some(Sample::Histogram(&histograms[hi].1))
            }
            (Some(fi), None) => {
                float_index = fi;
                Some(Sample::Float(floats.values[fi]))
            }
            (None, Some(hi)) => {
                histogram_index = hi;
                Some(Sample::Histogram(&histograms[hi].1))
            }
            (None, None) => None,
        };
        let Some(sample) = sample else { break };
        previous = last;
        last = Some(sample);
    }
    if matches!(
        (previous, last),
        (Some(Sample::Float(_)), Some(Sample::Histogram(_)))
            | (Some(Sample::Histogram(_)), Some(Sample::Float(_)))
    ) {
        record_warning(
            collector,
            format!(
                "{}: encountered a mix of float and native histogram samples in the last two points",
                function_name(function)
            ),
        );
    }
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
    offset_ms: i64,
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
            // `irate` drops the counter's accumulations on a reset the way the existing planner's
            // `idelta_value` does, start timestamp included; `idelta` is a plain difference and
            // reads no reset at all.
            if !IS_RATE || !last.detect_counter_reset_shifted(prev, prev_ts, last_ts, offset_ms) {
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
            Some(GridSample::Histogram(Box::new(result)))
        }
        _ => None,
    })
}

/// Prometheus `histogramRate`: the increase from the first to the last histogram, adding back
/// what counter resets dropped. `None` when the histograms mix exponential and custom buckets.
fn histogram_rate(
    points: &Histograms,
    is_counter: bool,
    offset_ms: i64,
) -> Result<Option<FloatHistogram>> {
    let last = &points[points.len() - 1].1;
    let mut prev = Cow::Borrowed(&points[0].1);
    let mut uses_custom_buckets = prev.uses_custom_buckets();
    // A reset between the first two samples makes the first one irrelevant, layout included.
    // The start timestamps witness a reset the counts alone do not, which is what a restarted
    // counter looks like when it is already above the previous value.
    if is_counter {
        let (second_ts, second) = &points[1];
        if second.detect_counter_reset_shifted(&prev, points[0].0, *second_ts, offset_ms) {
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
        for (index, (current_ts, current)) in points.iter().enumerate().skip(1) {
            if current.detect_counter_reset_shifted(
                &prev,
                points[index - 1].0,
                *current_ts,
                offset_ms,
            ) {
                result.add(&prev)?;
            }
            prev = Cow::Borrowed(current);
        }
    }
    result.reset_hint = CounterResetHint::Gauge;
    result.compact();
    Ok(Some(result))
}
