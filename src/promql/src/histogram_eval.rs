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

//! Histogram functions over evaluated series: `histogram_quantile` and `histogram_fraction` over
//! classic and native histograms (Prometheus `funcHistogramQuantile`, `funcHistogramFraction`
//! and `resetHistograms`), and `histogram_count`, `histogram_sum`, `histogram_avg`,
//! `histogram_stddev` and `histogram_stdvar` over native histograms.

mod classic;
mod native;
#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datatypes::series::MetricName;
use datatypes::series::histogram::HistogramView;
use datatypes::series::samples::{DenseSamplesRef, SampleRef, Series, SeriesSchema};
use datatypes::series::time::TimeAxis;

use crate::grid_series::{GridSeriesBuilder, LabelSet, label_set, merge_same_labelset};

const BUCKET_LABEL: &str = "le";
/// Prometheus `schema.IsMetadataLabel`: the labels removed with the metric name.
const METADATA_LABELS: [&str; 3] = ["__name__", "__type__", "__unit__"];

/// `histogram_quantile` or `histogram_fraction` with its constant arguments.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum HistogramFunction {
    /// `histogram_quantile(q, v)`.
    Quantile(f64),
    /// `histogram_fraction(lower, upper, v)`.
    Fraction { lower: f64, upper: f64 },
}

/// Evaluates `function` over the classic and native histograms in `series`.
///
/// Float series with the same labels other than `le` form a classic histogram; a float series
/// whose `le` does not parse is ignored. Native histogram samples are evaluated one by one. At a
/// point where a native histogram and classic buckets with the same labels (`le` aside) are both
/// present, neither is evaluated. Each field is evaluated on its own. Results drop `le` and
/// `__name__`, and series that then share a label set are merged as [`merge_same_labelset`]
/// does.
pub fn evaluate_histograms(
    function: HistogramFunction,
    schema: &Arc<SeriesSchema>,
    series: &[Series],
) -> Result<Vec<Series>> {
    let mut groups: HashMap<LabelSet, Group<'_>> = HashMap::new();
    for series in series {
        let range = grid_range(series)?;
        if let Some(upper_bound) = series
            .metric
            .get(BUCKET_LABEL)
            .and_then(classic::parse_bound)
        {
            let mut metric = series.metric.clone();
            metric.remove(BUCKET_LABEL);
            groups
                .entry(label_set(&metric))
                .or_insert_with(|| Group::new(metric))
                .classic
                .push((upper_bound, series, range));
        }
        if has_histograms(series) {
            let metric = series.metric.clone();
            groups
                .entry(label_set(&metric))
                .or_insert_with(|| Group::new(metric))
                .native = Some((series, range));
        }
    }

    let mut output = Vec::with_capacity(groups.len());
    let mut builder = GridSeriesBuilder::try_new(schema.clone())?;
    let mut classic_buckets = Vec::new();
    let mut native_buckets = Vec::new();
    let mut values = vec![None; schema.fields.len()];
    for mut group in groups.into_values() {
        // Pre-sorting by bound keeps the buckets of every point sorted. Like Go's comparison,
        // +0 and -0 are equal and keep input order, and coalescing keeps the first bound.
        group
            .classic
            .sort_by(|a, b| match (a.0.is_nan(), b.0.is_nan()) {
                (false, false) => a.0.partial_cmp(&b.0).expect("neither is NaN"),
                (nan_a, nan_b) => nan_a.cmp(&nan_b),
            });
        let ranges = group
            .classic
            .iter()
            .map(|member| member.2)
            .chain(group.native.map(|native| native.1));
        let start = ranges.clone().map(|range| range.0).min().unwrap_or(0);
        let end = ranges.map(|range| range.1).max().unwrap_or(0);
        for point in start..end {
            for (field, value) in values.iter_mut().enumerate() {
                classic_buckets.clear();
                for &(upper_bound, series, range) in &group.classic {
                    if let Some(SampleRef::Float(count)) = sample(series, range, field, point) {
                        classic_buckets.push(classic::Bucket { upper_bound, count });
                    }
                }
                let native = group.native.and_then(|(series, range)| {
                    match sample(series, range, field, point) {
                        Some(SampleRef::Histogram(h)) => Some(h),
                        _ => None,
                    }
                });
                // A native histogram and classic buckets of one label set conflict.
                *value = match (native, classic_buckets.is_empty()) {
                    (Some(_), false) | (None, true) => None,
                    (Some(h), true) => Some(match function {
                        HistogramFunction::Quantile(q) => {
                            native::quantile(q, &h, &mut native_buckets)?
                        }
                        HistogramFunction::Fraction { lower, upper } => {
                            native::fraction(lower, upper, &h, &mut native_buckets)?
                        }
                    }),
                    (None, false) => Some(match function {
                        HistogramFunction::Quantile(q) => {
                            classic::bucket_quantile(q, &mut classic_buckets)
                        }
                        HistogramFunction::Fraction { lower, upper } => {
                            classic::bucket_fraction(lower, upper, &mut classic_buckets)
                        }
                    }),
                };
            }
            builder.push(point, &values)?;
        }
        group.metric.remove_all(&METADATA_LABELS);
        output.extend(builder.finish(group.metric)?);
    }
    merge_same_labelset(schema, output)
}

/// The series of one label set (`le` aside): classic buckets, and the series of exactly that
/// label set when it holds native histograms.
struct Group<'a> {
    metric: MetricName,
    classic: Vec<(f64, &'a Series, (usize, usize))>,
    native: Option<(&'a Series, (usize, usize))>,
}

impl Group<'_> {
    fn new(metric: MetricName) -> Self {
        Self {
            metric,
            classic: Vec::new(),
            native: None,
        }
    }
}

/// The absolute grid range `[start, end)` of `series`.
fn grid_range(series: &Series) -> Result<(usize, usize)> {
    let TimeAxis::Grid(range) = &series.time else {
        return Err(DataFusionError::Execution(
            "histogram functions require evaluated series".into(),
        ));
    };
    Ok((range.start(), range.start() + range.len()))
}

fn sample(
    series: &Series,
    (start, end): (usize, usize),
    field: usize,
    point: usize,
) -> Option<SampleRef<'_>> {
    (start..end)
        .contains(&point)
        .then(|| series.fields.as_slice()[field].sample(point - start))
}

fn has_histograms(series: &Series) -> bool {
    series.fields.iter().any(|samples| {
        samples
            .segments()
            .iter()
            .any(|segment| !matches!(segment.dense(), Some(DenseSamplesRef::Float(_))))
    })
}

/// A function of one native histogram.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum NativeHistogramFunction {
    Count,
    Sum,
    Avg,
    StdDev,
    StdVar,
}

/// Applies `function` to the native histogram samples of `series`; float samples are dropped and
/// so is `__name__`. The result is `None` when the series has no histogram sample.
///
/// Series that share a label set once their names are dropped must then go through
/// [`merge_same_labelset`].
pub fn map_native_histogram(
    function: NativeHistogramFunction,
    schema: &Arc<SeriesSchema>,
    series: &Series,
) -> Result<Option<Series>> {
    let range = grid_range(series)?;
    let mut builder = GridSeriesBuilder::try_new(schema.clone())?;
    let mut buckets = Vec::new();
    let mut values = vec![None; schema.fields.len()];
    for point in range.0..range.1 {
        for (field, value) in values.iter_mut().enumerate() {
            *value = match sample(series, range, field, point) {
                Some(SampleRef::Histogram(h)) => Some(function.apply(&h, &mut buckets)?),
                _ => None,
            };
        }
        builder.push(point, &values)?;
    }
    let mut metric = series.metric.clone();
    // Prometheus `simpleHistogramFunc` drops only `__name__`, unlike `histogram_quantile`.
    metric.remove("__name__");
    builder.finish(metric)
}

impl NativeHistogramFunction {
    fn apply(self, h: &HistogramView<'_>, buckets: &mut Vec<native::NativeBucket>) -> Result<f64> {
        Ok(match self {
            Self::Count => h.count,
            Self::Sum => h.sum,
            Self::Avg => h.sum / h.count,
            Self::StdDev => native::variance(h, buckets)?.sqrt(),
            Self::StdVar => native::variance(h, buckets)?,
        })
    }
}
