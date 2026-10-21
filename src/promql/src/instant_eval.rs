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

use std::ops::RangeInclusive;
use std::sync::Arc;

use common_query::prometheus::is_prometheus_stale_nan;
use datafusion::error::{DataFusionError, Result};
use datatypes::series::MetricName;
use datatypes::series::raw::RawTimestamps;
use datatypes::series::samples::{SampleRef, Series, SeriesFields, SeriesSchema};

use crate::grid_series::GridSeriesBuilder;
use crate::map::drop_metric_name;

#[cfg(test)]
mod tests;

/// Selects source rows for an aligned PromQL instant-evaluation range.
///
/// Returns original logical row indices and evaluation timestamps from a complete source axis
/// of strictly increasing milliseconds. Any provided field's stale marker suppresses selection;
/// missing samples are not stale and remain selectable.
///
/// The two vectors it returns hold one `u64` source row index and one `i64` evaluation timestamp
/// per selected point, so what it allocates is 16 bytes per point of the clipped grid rather than
/// anything proportional to the samples of the axis. It reserves none of that: every caller grows
/// a memory reservation for those two vectors before it calls, which is what bounds an instant
/// selection now that the kernel carries no point limit of its own.
pub fn select_instants(
    time: &RawTimestamps,
    fields: &SeriesFields,
    range: RangeInclusive<i64>,
    step_ms: i64,
    lookback_ms: i64,
    offset_ms: i64,
) -> Result<(Vec<u64>, Vec<i64>)> {
    let start = *range.start();
    let end = *range.end();
    if step_ms <= 0 {
        return Err(DataFusionError::Execution(
            "InstantManipulate interval must be positive".into(),
        ));
    }
    if start > end {
        return Err(DataFusionError::Execution(
            "InstantManipulate start must not exceed end".into(),
        ));
    }
    if lookback_ms < 0 {
        return Err(DataFusionError::Execution(
            "InstantManipulate lookback must be non-negative".into(),
        ));
    }
    let timestamps = time.as_buffer();
    if timestamps.windows(2).any(|pair| pair[0] >= pair[1]) {
        return Err(DataFusionError::Execution(
            "raw timestamps must be strictly increasing".into(),
        ));
    }
    if fields
        .as_slice()
        .iter()
        .any(|field| field.len() != timestamps.len())
    {
        return Err(DataFusionError::Execution(
            "series fields and time axis have unequal lengths".into(),
        ));
    }
    if timestamps.is_empty() {
        return Ok((Vec::new(), Vec::new()));
    }

    // Match the native-tick selector's bounds calculation in wide arithmetic before scanning.
    let first = timestamps[0] as i128 + offset_ms as i128;
    let last = timestamps[timestamps.len() - 1] as i128 + offset_ms as i128;
    let useful_last = if lookback_ms == 0 {
        last
    } else {
        last + lookback_ms as i128 - 1
    };
    let query_start = start as i128;
    let query_end = end as i128;
    let step = step_ms as i128;
    let max_start = first.max(query_start);
    let min_end = useful_last.min(query_end);
    let (aligned_start, aligned_end) = if max_start > min_end {
        (1_i128, 0_i128)
    } else {
        (
            query_start + (max_start - query_start) / step * step,
            query_end - (query_end - min_end) / step * step,
        )
    };
    let estimated_points = if aligned_end >= aligned_start {
        (aligned_end - aligned_start) / step + 1
    } else {
        0
    };
    let mut indices = Vec::with_capacity(estimated_points as usize);
    let mut evaluation_timestamps = Vec::with_capacity(estimated_points as usize);
    let mut sample_iters: Vec<_> = fields.iter().map(|field| field.iter()).collect();
    let mut current_stale = false;
    let mut cursor = 0usize;
    for expected in (aligned_start..=aligned_end).step_by(step_ms as usize) {
        let source_expected = expected - offset_ms as i128;
        while cursor < timestamps.len() && timestamps[cursor] as i128 <= source_expected {
            current_stale = false;
            for samples in &mut sample_iters {
                let sample = samples.next().ok_or_else(|| {
                    DataFusionError::Execution(
                        "series sample iterator ended before the time axis".into(),
                    )
                })?;
                let stale = match sample {
                    SampleRef::Float(value) => is_prometheus_stale_nan(value),
                    SampleRef::Histogram(histogram) => is_prometheus_stale_nan(histogram.sum),
                    SampleRef::Missing => false,
                };
                current_stale |= stale;
            }
            cursor += 1;
        }
        let Some(candidate) = cursor.checked_sub(1) else {
            continue;
        };
        let candidate_ts = timestamps[candidate] as i128 + offset_ms as i128;
        let lower = expected - lookback_ms as i128;
        if (candidate_ts == expected || candidate_ts > lower)
            && candidate_ts <= expected
            && !current_stale
        {
            indices.push(candidate as u64);
            evaluation_timestamps.push(expected as i64);
        }
    }
    Ok((indices, evaluation_timestamps))
}

/// `timestamp(v)` over an instant selector: at each point the selection holds, the original time of
/// the selected sample in seconds, not the time it was looked up at, so an offset or `@` moves
/// which sample is selected but not the time reported. The metric name, `__type__` and `__unit__`
/// are dropped.
///
/// `indices` and `evaluation_timestamps` are what [`select_instants`] returns for `time` and
/// `fields`; `schema` holds the evaluation grid the timestamps lie on and one field per field of
/// `fields`. A field without a sample in the selected row has no result there.
pub fn timestamp_of_selection(
    schema: &Arc<SeriesSchema>,
    metric: &MetricName,
    time: &RawTimestamps,
    fields: &SeriesFields,
    indices: &[u64],
    evaluation_timestamps: &[i64],
) -> Result<Option<Series>> {
    let selection = Selection::try_new(schema, time, fields, indices, evaluation_timestamps)?;
    selection.project(
        drop_metric_name(metric)?,
        |_, time| Ok(time as f64 / 1000.0),
    )
}

/// The series `info()` reads its data labels from: at each point the selection holds, the original
/// time of the selected sample in milliseconds, as Prometheus' `evalSeries` records it for `info`.
/// The labels are kept. Info series hold float samples, so a selected native histogram fails.
///
/// Takes the same selection as [`timestamp_of_selection`].
pub fn info_sample_times(
    schema: &Arc<SeriesSchema>,
    metric: &MetricName,
    time: &RawTimestamps,
    fields: &SeriesFields,
    indices: &[u64],
    evaluation_timestamps: &[i64],
) -> Result<Option<Series>> {
    let selection = Selection::try_new(schema, time, fields, indices, evaluation_timestamps)?;
    selection.project(metric.clone(), |sample, time| match sample {
        SampleRef::Histogram(_) => Err(DataFusionError::Execution(format!(
            "this should be an info metric, with float samples: {metric:?}"
        ))),
        _ => Ok(time as f64),
    })
}

/// `timestamp(v)` over an evaluated `v`, such as `timestamp(abs(x))`: the evaluation time in
/// seconds at each point a field of `series` holds a value. The metric name, `__type__` and
/// `__unit__` are dropped.
pub fn timestamp_of_evaluated(
    schema: &Arc<SeriesSchema>,
    series: &Series,
) -> Result<Option<Series>> {
    let grid = schema.eval_grid.as_ref().ok_or_else(|| {
        DataFusionError::Execution("timestamp requires an evaluation grid".into())
    })?;
    if series.time.grid().map(|grid| grid.spec()) != Some(grid.spec())
        || series.fields.len() != schema.fields.len()
    {
        return Err(DataFusionError::Execution(
            "timestamp input does not match its schema".into(),
        ));
    }
    let timestamps = grid.timestamps();
    let mut builder = GridSeriesBuilder::try_new(schema.clone())?;
    let mut cursors: Vec<_> = series.fields.iter().map(|field| field.cursor()).collect();
    let mut row = Vec::with_capacity(cursors.len());
    for (point, index) in series.time.positions() {
        let seconds = timestamps[point] as f64 / 1000.0;
        row.clear();
        for cursor in &mut cursors {
            let sample = cursor
                .at(index)
                .expect("the axis and the fields are aligned");
            row.push((!matches!(sample, SampleRef::Missing)).then_some(seconds));
        }
        builder.push(point, &row)?;
    }
    builder.finish(drop_metric_name(&series.metric)?)
}

/// A checked instant selection: the grid position and source row of each selected point.
struct Selection<'a> {
    schema: &'a Arc<SeriesSchema>,
    time: &'a RawTimestamps,
    fields: &'a SeriesFields,
    /// `(grid position, source row)` per selected point, positions increasing.
    points: Vec<(usize, usize)>,
}

impl<'a> Selection<'a> {
    fn try_new(
        schema: &'a Arc<SeriesSchema>,
        time: &'a RawTimestamps,
        fields: &'a SeriesFields,
        indices: &[u64],
        evaluation_timestamps: &[i64],
    ) -> Result<Self> {
        let grid = schema.eval_grid.as_ref().ok_or_else(|| {
            DataFusionError::Execution("an instant selection requires an evaluation grid".into())
        })?;
        if indices.len() != evaluation_timestamps.len() || fields.len() != schema.fields.len() {
            return Err(DataFusionError::Execution(
                "instant selection does not match its series or schema".into(),
            ));
        }
        let spec = grid.spec();
        let mut points = Vec::with_capacity(indices.len());
        for (&row, &timestamp) in indices.iter().zip(evaluation_timestamps) {
            let offset = timestamp as i128 - spec.start_ms as i128;
            let position = offset / spec.step_ms as i128;
            let row = usize::try_from(row).ok().filter(|row| *row < time.len());
            match row {
                Some(row)
                    if offset % spec.step_ms as i128 == 0
                        && (0..spec.len as i128).contains(&position)
                        && points.last().is_none_or(|&(last, last_row)| {
                            (last as i128) < position && last_row <= row
                        }) =>
                {
                    points.push((position as usize, row));
                }
                _ => {
                    return Err(DataFusionError::Execution(
                        "instant selection points off the grid or out of the series".into(),
                    ));
                }
            }
        }
        Ok(Self {
            schema,
            time,
            fields,
            points,
        })
    }

    /// One series over the selected points, each field holding `value(sample, original time)`
    /// where its selected sample is present.
    fn project(
        &self,
        metric: MetricName,
        value: impl Fn(SampleRef<'_>, i64) -> Result<f64>,
    ) -> Result<Option<Series>> {
        let mut builder = GridSeriesBuilder::try_new(self.schema.clone())?;
        let mut cursors: Vec<_> = self.fields.iter().map(|field| field.cursor()).collect();
        // Consecutive points often select the same row, which a cursor reads only once.
        let mut row: Vec<Option<f64>> = Vec::with_capacity(self.fields.len());
        let mut row_index = None;
        for &(position, index) in &self.points {
            if row_index != Some(index) {
                let time = self
                    .time
                    .timestamp(index)
                    .expect("checked against the axis");
                row.clear();
                for cursor in &mut cursors {
                    let sample = cursor
                        .at(index)
                        .expect("selected rows only increase and lie in the fields");
                    row.push(match sample {
                        SampleRef::Missing => None,
                        sample => Some(value(sample, time)?),
                    });
                }
                row_index = Some(index);
            }
            builder.push(position, &row)?;
        }
        builder.finish(metric)
    }
}
