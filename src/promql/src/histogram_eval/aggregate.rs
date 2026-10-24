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

//! `sum` and `avg` of native histograms within one aggregation group, following the histogram
//! paths of Prometheus `aggregation`.

use std::sync::Arc;

use datafusion::arrow::array::UInt32Array;
use datafusion::error::{DataFusionError, Result};
use datatypes::series::SeriesLabels;
use datatypes::series::histogram::HistogramBatchBuilder;
use datatypes::series::samples::{
    RowCursor, SampleRef, Samples, Series, SeriesFields, SeriesSchema,
};
use datatypes::series::time::{GridSpan, TimeAxis, union_positions};

use crate::grid_series::external;
use crate::histogram_eval::float_histogram::FloatHistogram;
use crate::histogram_eval::grid_axis;

/// A native histogram aggregation.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum HistogramAggregate {
    Sum,
    Avg,
}

/// Aggregates the native histograms of one group at every point, adding them in the order of
/// `group`. A point yields no histogram when the group also has a float sample there, or when
/// its histograms mix exponential and custom buckets. Float-only points are left to the float
/// aggregation. The result carries `metric`.
pub fn aggregate_histograms(
    op: HistogramAggregate,
    schema: &Arc<SeriesSchema>,
    group: &[Series],
    metric: SeriesLabels,
) -> Result<Option<Series>> {
    let grid = schema.eval_grid.clone().ok_or_else(|| {
        DataFusionError::Execution("histogram aggregation requires an evaluation grid".into())
    })?;
    for series in group {
        grid_axis(series)?;
    }

    let fields = schema.fields.len();
    // Only the positions some member carries can hold a result, and the union visits each of
    // them once: a hole between two members is never scanned, so nothing densifies here.
    let mut builders: Vec<HistogramBatchBuilder> =
        (0..fields).map(|_| HistogramBatchBuilder::new()).collect();
    let mut indices: Vec<Vec<Option<u32>>> = vec![Vec::new(); fields];
    // The maximal runs of positions that carried a result; one row is written per run row, so a
    // result over few scattered positions costs few rows.
    let mut spans: Vec<GridSpan> = Vec::new();
    let mut row: Vec<Option<FloatHistogram>> = (0..fields).map(|_| None).collect();
    // One forward cursor per member and field: the merged positions only increase, so a member's
    // rows do too, and reading them costs one pass over the field instead of a chunk scan per row.
    // A member that skips a position is asked for nothing, so its cursor stays where it was.
    let mut cursors: Vec<Vec<RowCursor<'_>>> = group
        .iter()
        .map(|series| series.fields.iter().map(|field| field.cursor()).collect())
        .collect();
    let mut rows: Vec<Option<usize>> = vec![None; group.len()];
    for point in union_positions(group.iter().map(|series| &series.time)) {
        for (member, series) in group.iter().enumerate() {
            rows[member] = series.time.row_of(point);
        }
        for (field, value) in row.iter_mut().enumerate() {
            let samples = group.iter().enumerate().filter_map(|(member, _)| {
                rows[member].map(|row| {
                    cursors[member][field]
                        .at(row)
                        .expect("a row never goes back")
                })
            });
            *value = aggregate_point(op, samples)?;
        }
        if row.iter().all(Option::is_none) {
            continue;
        }
        // A gap in the results ends a run, and a row always belongs to the run it opens.
        match spans.last_mut() {
            Some(span) if span.start() + span.len() == point => {
                *span = GridSpan::new(span.start(), span.len() + 1);
            }
            _ => spans.push(GridSpan::new(point, 1)),
        }
        for ((value, builder), indices) in row.iter().zip(&mut builders).zip(&mut indices) {
            indices.push(match value {
                Some(h) => Some(h.push_to(builder)?),
                None => None,
            });
        }
    }
    if spans.is_empty() {
        return Ok(None);
    }
    let mut samples = Vec::with_capacity(fields);
    for (builder, indices) in builders.into_iter().zip(indices) {
        let batch = builder.finish().map_err(external)?;
        samples.push(
            Samples::try_from_histogram(UInt32Array::from(indices), batch).map_err(external)?,
        );
    }
    // The result is addressed by the runs that carry a histogram, not by the span between the
    // first and the last one: the nulls padding them stay inside a run, never between two.
    let time = TimeAxis::from_grid_spans(grid, spans).map_err(external)?;
    debug_assert_eq!(time.len(), samples[0].len());
    Series::try_new(
        schema,
        metric,
        time,
        SeriesFields::try_from_vec(samples).map_err(external)?,
    )
    .map(Some)
    .map_err(external)
}

/// One point of one field.
fn aggregate_point<'a>(
    op: HistogramAggregate,
    samples: impl Iterator<Item = SampleRef<'a>>,
) -> Result<Option<FloatHistogram>> {
    let mut has_float = false;
    let mut histograms = samples.filter_map(|sample| match sample {
        SampleRef::Missing => None,
        SampleRef::Float(_) => {
            has_float = true;
            None
        }
        SampleRef::Histogram(view) => Some(FloatHistogram::try_from_view(&view)),
    });
    let value = sum_or_avg(op, histograms.by_ref())?;
    // A fold stopped by mixed bucket kinds still reports conversion errors of later samples.
    for histogram in histograms {
        histogram?;
    }
    let Some(mut value) = value.filter(|_| !has_float) else {
        return Ok(None);
    };
    value.compact();
    Ok(Some(value))
}

/// The Kahan sum or the mean of `histograms` in order, without compaction; `None` when there is
/// none or they mix exponential and custom buckets. Prometheus `sum_over_time` and
/// `avg_over_time` run the same steps as the aggregations. Histograms are consumed one at a
/// time, so the first error ends the fold. The result is derived, so it carries no start
/// timestamp.
pub(crate) fn sum_or_avg(
    op: HistogramAggregate,
    histograms: impl IntoIterator<Item = Result<FloatHistogram>>,
) -> Result<Option<FloatHistogram>> {
    let mut value: Option<FloatHistogram> = None;
    let mut compensation: Option<FloatHistogram> = None;
    let mut mean: Option<FloatHistogram> = None;
    let mut count = 0.0;
    for h in histograms {
        let h = h?;
        count += 1.0;
        let Some(current) = value.as_mut() else {
            value = Some(h);
            continue;
        };
        // Go drops the result with a warning for this addition error only; others propagate.
        if current.uses_custom_buckets() != h.uses_custom_buckets() {
            return Ok(None);
        }
        match op {
            HistogramAggregate::Sum => {
                let (c, _) = current.kahan_add(&h, compensation.take())?;
                compensation = Some(c);
            }
            HistogramAggregate::Avg => {
                if mean.is_none() {
                    let mut sum = current.clone();
                    let (c, _) = sum.kahan_add(&h, compensation.clone())?;
                    if !sum.has_overflow() {
                        *current = sum;
                        compensation = Some(c);
                        continue;
                    }
                    let mut started = current.clone();
                    started.div(count - 1.0);
                    if let Some(c) = compensation.as_mut() {
                        c.div(count - 1.0);
                    }
                    mean = Some(started);
                }
                // Go switches to an incremental mean once the plain sum overflows.
                let q = (count - 1.0) / count;
                if let Some(c) = compensation.as_mut() {
                    c.mul(q);
                }
                let mut to_add = h;
                to_add.div(count);
                let mean = mean.as_mut().expect("set above");
                mean.mul(q);
                let (c, _) = mean.kahan_add(&to_add, compensation.take())?;
                compensation = Some(c);
            }
        }
    }
    let Some(mut value) = value else {
        return Ok(None);
    };
    match op {
        HistogramAggregate::Sum => {
            if let Some(c) = compensation {
                value.add(&c)?;
            }
        }
        HistogramAggregate::Avg => match mean {
            Some(mut mean) => {
                if let Some(c) = compensation {
                    mean.add(&c)?;
                }
                value = mean;
            }
            None => {
                value.div(count);
                if let Some(mut c) = compensation {
                    c.div(count);
                    value.add(&c)?;
                }
            }
        },
    }
    // The result was derived from its inputs, so it carries no start timestamp of its own, like
    // every other operation that builds one. Clearing it here covers the fold that added nothing:
    // the first histogram of a window becomes the result as it arrived, and the adds above are
    // what drop the field otherwise, which would make the payload depend on the sample count.
    // The bucket layout is left as the fold built it -- compaction belongs to the caller.
    value.start_timestamp = None;
    Ok(Some(value))
}
