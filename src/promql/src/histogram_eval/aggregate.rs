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
use datatypes::series::MetricName;
use datatypes::series::histogram::HistogramBatchBuilder;
use datatypes::series::samples::{SampleRef, Samples, Series, SeriesFields, SeriesSchema};
use datatypes::series::time::{GridRange, TimeAxis};

use crate::grid_series::external;
use crate::histogram_eval::float_histogram::FloatHistogram;
use crate::histogram_eval::grid_range;

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
    metric: MetricName,
) -> Result<Option<Series>> {
    let grid = schema.eval_grid.clone().ok_or_else(|| {
        DataFusionError::Execution("histogram aggregation requires an evaluation grid".into())
    })?;
    let ranges = group.iter().map(grid_range).collect::<Result<Vec<_>>>()?;
    let start = ranges.iter().map(|range| range.0).min().unwrap_or(0);
    let end = ranges.iter().map(|range| range.1).max().unwrap_or(0);

    let fields = schema.fields.len();
    // Written from the first point with a result, so a sparse group allocates no full span.
    let mut builders: Vec<HistogramBatchBuilder> =
        (0..fields).map(|_| HistogramBatchBuilder::new()).collect();
    let mut indices: Vec<Vec<Option<u32>>> = vec![Vec::new(); fields];
    let mut first = None;
    let mut row: Vec<Option<FloatHistogram>> = (0..fields).map(|_| None).collect();
    for point in start..end {
        for (field, value) in row.iter_mut().enumerate() {
            let samples = group
                .iter()
                .zip(&ranges)
                .filter(|(_, (from, to))| (*from..*to).contains(&point))
                .map(|(series, &(from, _))| series.fields.as_slice()[field].sample(point - from));
            *value = aggregate_point(op, samples)?;
        }
        if row.iter().all(Option::is_none) {
            continue;
        }
        let first = *first.get_or_insert(point);
        for ((value, builder), indices) in row.iter().zip(&mut builders).zip(&mut indices) {
            indices.resize(point - first, None);
            indices.push(match value {
                Some(h) => Some(h.push_to(builder)?),
                None => None,
            });
        }
    }
    let Some(first) = first else {
        return Ok(None);
    };
    let len = indices[0].len();
    let mut samples = Vec::with_capacity(fields);
    for (builder, indices) in builders.into_iter().zip(indices) {
        let batch = builder.finish().map_err(external)?;
        samples.push(
            Samples::try_from_histogram(UInt32Array::from(indices), batch).map_err(external)?,
        );
    }
    let range = GridRange::try_new(grid, first, len).map_err(external)?;
    Series::try_new(
        schema,
        metric,
        TimeAxis::Grid(range),
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
    let (mut histograms, mut has_float) = (Vec::new(), false);
    for sample in samples {
        match sample {
            SampleRef::Missing => {}
            SampleRef::Float(_) => has_float = true,
            SampleRef::Histogram(view) => histograms.push(FloatHistogram::try_from_view(&view)?),
        }
    }
    let Some(mut value) = sum_or_avg(op, histograms)? else {
        return Ok(None);
    };
    if has_float {
        return Ok(None);
    }
    value.compact();
    Ok(Some(value))
}

/// The Kahan sum or the mean of `histograms` in order, without compaction; `None` when there is
/// none or they mix exponential and custom buckets. Prometheus `sum_over_time` and
/// `avg_over_time` run the same steps as the aggregations.
pub(crate) fn sum_or_avg(
    op: HistogramAggregate,
    histograms: impl IntoIterator<Item = FloatHistogram>,
) -> Result<Option<FloatHistogram>> {
    let mut value: Option<FloatHistogram> = None;
    let mut compensation: Option<FloatHistogram> = None;
    let mut mean: Option<FloatHistogram> = None;
    let mut count = 0.0;
    for h in histograms {
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
    Ok(Some(value))
}
