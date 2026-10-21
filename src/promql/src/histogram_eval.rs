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

mod aggregate;
mod bounds;
mod classic;
// `sub`, `copy_to_schema`, `detect_reset` and `equals` are for the native histogram range
// functions and binary operators, which build on other branches.
#[cfg_attr(not(test), allow(dead_code))]
pub(crate) mod float_histogram;
mod native;
#[cfg(test)]
mod tests;

use std::collections::HashMap;
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datatypes::series::MetricName;
use datatypes::series::histogram::HistogramView;
use datatypes::series::samples::{DenseSamplesRef, RowCursor, SampleRef, Series, SeriesSchema};
use datatypes::series::time::{TimeAxis, union_positions};

pub(crate) use aggregate::sum_or_avg;
pub use aggregate::{HistogramAggregate, aggregate_histograms};

use crate::go_format::format_open_metrics_float;
use crate::go_sort;
use crate::grid_series::{
    GridSeriesBuilder, LabelSet, METADATA_LABELS, external, label_set, merge_same_labelset,
};

const BUCKET_LABEL: &str = "le";

/// `histogram_quantile` or `histogram_fraction` with its constant arguments.
#[derive(Debug, Clone, Copy, PartialEq)]
pub enum HistogramFunction {
    /// `histogram_quantile(q, v)`.
    Quantile(f64),
    /// `histogram_fraction(lower, upper, v)`.
    Fraction { lower: f64, upper: f64 },
}

/// `histogram_quantiles(v, label, q...)`: [`evaluate_histograms`] with `Quantile(q)` for each
/// `q`, each result labelled with `label` set to `q` as Prometheus' `FormatOpenMetricsFloat`
/// writes it. Results that then share a label set merge as [`merge_same_labelset`] does, so a
/// repeated `q`, or a `label` that overwrites the label telling two histograms apart, fails at a
/// point both hold.
pub fn histogram_quantiles(
    label: &str,
    quantiles: &[f64],
    schema: &Arc<SeriesSchema>,
    series: &[Series],
) -> Result<Vec<Series>> {
    let mut output = Vec::new();
    for &quantile in quantiles {
        let value: Arc<str> = Arc::from(format_open_metrics_float(quantile));
        for mut result in
            evaluate_histograms(HistogramFunction::Quantile(quantile), schema, series)?
        {
            result.metric.set(label, value.clone()).map_err(external)?;
            output.push(result);
        }
    }
    merge_same_labelset(schema, output)
}

/// Evaluates `function` over the classic and native histograms in `series`.
///
/// Float series with the same labels other than `le` form a classic histogram; a float series
/// whose `le` does not parse is ignored. Native histogram samples are evaluated one by one. At a
/// point where a native histogram and classic buckets with the same labels (`le` aside) are both
/// present, neither is evaluated. Each field is evaluated on its own. Classic results drop
/// `le`; all results drop `__name__`, `__type__` and `__unit__`. Series that then share a label
/// set are merged as [`merge_same_labelset`] does.
pub fn evaluate_histograms(
    function: HistogramFunction,
    schema: &Arc<SeriesSchema>,
    series: &[Series],
) -> Result<Vec<Series>> {
    let mut groups: HashMap<LabelSet, Group<'_>> = HashMap::new();
    for series in series {
        grid_axis(series)?;
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
                .push((upper_bound, series));
        }
        if has_histograms(series) {
            let metric = series.metric.clone();
            let group = groups
                .entry(label_set(&metric))
                .or_insert_with(|| Group::new(metric));
            // A label set carries at most one native histogram series: a second one would
            // overwrite the first and drop it from the result.
            debug_assert!(
                group.native.is_none(),
                "one label set carries at most one native histogram series; a second one for \
                 the same label set means the input handed one label set over as several \
                 series, which a gap in the axis is there to avoid"
            );
            group.native = Some(series);
        }
    }

    let mut output = Vec::with_capacity(groups.len());
    let mut builder = GridSeriesBuilder::try_new(schema.clone())?;
    let mut classic_buckets = Vec::new();
    let mut native_buckets = Vec::new();
    let mut values = vec![None; schema.fields.len()];
    for mut group in groups.into_values() {
        // One builder serves every group, so each result series starts explicitly.
        builder.begin_series();
        // The positions any member carries, merged once: the holes no member holds are never
        // visited, so a group of sparse series costs its rows and not its envelope.
        let axes = group
            .classic
            .iter()
            .map(|member| &member.1.time)
            .chain(group.native.map(|native| &native.time));
        // One forward cursor per member and field. The merged positions only increase, so each
        // member's rows do too: reading them costs one pass over the field, not a chunk scan per
        // row. The classic members come first, then the native one.
        let members = group
            .classic
            .iter()
            .map(|&(_, series)| series)
            .chain(group.native);
        let mut cursors: Vec<Vec<RowCursor<'_>>> = members
            .map(|series| series.fields.iter().map(|field| field.cursor()).collect())
            .collect();
        let classic_members = group.classic.len();
        for point in union_positions(axes) {
            for (field, value) in values.iter_mut().enumerate() {
                classic_buckets.clear();
                for (member, &(upper_bound, series)) in group.classic.iter().enumerate() {
                    let count = series.time.row_of(point).and_then(|row| {
                        match cursors[member][field]
                            .at(row)
                            .expect("a row never goes back")
                        {
                            SampleRef::Float(count) => Some(count),
                            _ => None,
                        }
                    });
                    if let Some(count) = count {
                        classic_buckets.push(classic::Bucket { upper_bound, count });
                    }
                }
                // Go sorts the buckets present at each point by `<`: +0 and -0 tie and NaN
                // ties with everything, so which of them coalescing keeps and where a NaN
                // bound lands follow its sort algorithm.
                go_sort::sort_by(&mut classic_buckets, |a, b| a.upper_bound < b.upper_bound);
                let native = group.native.and_then(|series| {
                    let row = series.time.row_of(point)?;
                    match cursors[classic_members][field]
                        .at(row)
                        .expect("a row never goes back")
                    {
                        SampleRef::Histogram(h) => Some(h),
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
    classic: Vec<(f64, &'a Series)>,
    native: Option<&'a Series>,
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

/// Checks that `series` is addressed on a grid, which is what a position lookup needs.
fn grid_axis(series: &Series) -> Result<&TimeAxis> {
    if series.time.grid().is_none() {
        return Err(DataFusionError::Execution(
            "histogram functions require evaluated series".into(),
        ));
    }
    Ok(&series.time)
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
/// so is `__name__`. `None` when the series has no histogram sample.
///
/// Histogram samples may leave holes, but the holes only skip positions: the result is one series
/// over the runs of consecutive positions that hold a value.
///
/// Series that share a label set once their names are dropped must then go through
/// [`merge_same_labelset`].
pub fn map_native_histogram(
    function: NativeHistogramFunction,
    schema: &Arc<SeriesSchema>,
    series: &Series,
) -> Result<Option<Series>> {
    grid_axis(series)?;
    let mut builder = GridSeriesBuilder::try_new(schema.clone())?;
    let mut buckets = Vec::new();
    let mut values = vec![None; schema.fields.len()];
    // One cursor per field, driven by the positions the axis carries: a series of several runs
    // costs its rows, and the positions it skips are never visited.
    let mut cursors = series
        .fields
        .iter()
        .map(|field| field.cursor())
        .collect::<Vec<_>>();
    for (point, row) in series.time.positions() {
        for (field, value) in values.iter_mut().enumerate() {
            *value = match cursors[field]
                .at(row)
                .expect("the axis and the fields are aligned")
            {
                SampleRef::Histogram(h) => Some(function.apply(&h, &mut buckets)?),
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
