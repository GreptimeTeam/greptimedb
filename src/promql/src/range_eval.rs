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

//! Range-vector window evaluation of raw series onto an evaluation grid.

use std::sync::Arc;

use common_query::prometheus::is_prometheus_stale_nan;
use datafusion::arrow::buffer::Buffer;
use datafusion::error::{DataFusionError, Result};
use datatypes::series::MetricName;
use datatypes::series::raw::RawSeriesChunk;
use datatypes::series::samples::{SampleRef, Series, SeriesSchema};
use datatypes::series::time::EvalGrid;

use crate::grid_series::GridSeriesBuilder;

mod function;
#[cfg(test)]
mod tests;

pub use function::RangeFunction;

/// The samples a range selector reads at evaluation time `T`:
/// source timestamps in `(T - offset_ms - range_ms, T - offset_ms]`.
#[derive(Clone, Copy, Debug)]
pub struct RangeWindow {
    /// Positive window length in milliseconds.
    pub range_ms: i64,
    /// Selector offset in milliseconds; positive reads earlier samples.
    pub offset_ms: i64,
}

/// The valid samples of one field inside one window, in source order.
pub struct FloatWindow<'a> {
    /// Sample timestamps shifted by the offset onto the evaluation timeline; non-empty.
    pub timestamps: &'a [i64],
    pub values: &'a [f64],
    /// Evaluation timestamp `T`; `timestamps` lie in `(T - range_ms, T]`.
    pub eval_ts: i64,
    pub range_ms: i64,
}

/// Computes one window result; `None` means the window has no result.
pub type WindowKernel = Box<dyn FnMut(FloatWindow<'_>) -> Option<f64> + Send>;

/// Evaluates range windows over raw float series, one series at a time.
///
/// Each series is pushed as its chunks in order and completes on the chunk marked `is_last`.
/// Fields are evaluated independently over their own valid samples; a row holding a stale
/// marker in any field is dropped, as the ordinary range selector does. Only the samples later
/// windows can still use are retained.
pub struct RangeEvaluator {
    grid: Arc<EvalGrid>,
    window: RangeWindow,
    kernel: WindowKernel,
    output_schema: Arc<SeriesSchema>,
    output: GridSeriesBuilder,
    series: Option<SeriesState>,
    fields: Vec<FieldSamples>,
    row: Vec<Option<f64>>,
}

struct SeriesState {
    primary_key: Buffer,
    metric: MetricName,
    last_ts: i64,
    /// Next grid index to evaluate.
    cursor: usize,
}

#[derive(Default)]
struct FieldSamples {
    timestamps: Vec<i64>,
    values: Vec<f64>,
    /// Samples before `head` are behind every remaining window.
    head: usize,
}

impl RangeEvaluator {
    /// Creates an evaluator for raw series of `input` onto `grid`.
    pub fn try_new(
        input: &SeriesSchema,
        grid: Arc<EvalGrid>,
        window: RangeWindow,
        kernel: WindowKernel,
    ) -> Result<Self> {
        if input.eval_grid.is_some() {
            return Err(DataFusionError::Plan(
                "range evaluation input must be a raw series schema".into(),
            ));
        }
        if window.range_ms <= 0 {
            return Err(DataFusionError::Plan(format!(
                "range must be positive, got {} ms",
                window.range_ms
            )));
        }
        let output_schema = Arc::new(SeriesSchema {
            fields: input.fields.clone(),
            eval_grid: Some(grid.clone()),
        });
        Ok(Self {
            grid,
            window,
            kernel,
            output: GridSeriesBuilder::try_new(output_schema.clone())?,
            output_schema,
            series: None,
            fields: input
                .fields
                .iter()
                .map(|_| FieldSamples::default())
                .collect(),
            row: Vec::with_capacity(input.fields.len()),
        })
    }

    /// Schema of the evaluated series: the input fields on the evaluation grid.
    pub fn output_schema(&self) -> &Arc<SeriesSchema> {
        &self.output_schema
    }

    /// Consumes the next chunk of the current series, or starts a new series.
    ///
    /// Returns the evaluated series on its `is_last` chunk, or `None` there when no grid point
    /// has a result.
    /// The evaluator must not be reused after an error.
    pub fn push(&mut self, chunk: &RawSeriesChunk) -> Result<Option<Series>> {
        self.check_chunk(chunk)?;
        self.ingest(chunk)?;
        let last_ts = chunk.time.as_buffer()[chunk.time.len() - 1];
        if !chunk.is_last {
            // Later chunks only hold samples after `last_ts`.
            self.evaluate(Some(
                i128::from(last_ts) + i128::from(self.window.offset_ms),
            ))?;
            return Ok(None);
        }
        self.evaluate(None)?;
        let state = self.series.take().expect("checked by check_chunk");
        for field in &mut self.fields {
            field.timestamps.clear();
            field.values.clear();
            field.head = 0;
        }
        self.output.finish(state.metric)
    }

    /// Checks that the input did not end inside a series.
    pub fn finish(self) -> Result<()> {
        if self.series.is_some() {
            return Err(DataFusionError::Execution(
                "raw series input ended before the last chunk of a series".into(),
            ));
        }
        Ok(())
    }

    fn check_chunk(&mut self, chunk: &RawSeriesChunk) -> Result<()> {
        let time = chunk.time.as_buffer();
        if time.is_empty()
            || chunk.fields.len() != self.fields.len()
            || chunk.fields.iter().any(|field| field.len() != time.len())
        {
            return Err(DataFusionError::Execution(format!(
                "raw series chunk with {} timestamps and {} fields does not match {} schema fields",
                time.len(),
                chunk.fields.len(),
                self.fields.len()
            )));
        }
        match &mut self.series {
            None => {
                let metric = chunk.metric.clone().ok_or_else(|| {
                    DataFusionError::Execution(
                        "range evaluation requires materialized series labels".into(),
                    )
                })?;
                self.series = Some(SeriesState {
                    primary_key: chunk.primary_key.clone(),
                    metric,
                    last_ts: time[time.len() - 1],
                    cursor: 0,
                });
            }
            Some(state) => {
                if state.primary_key.as_slice() != chunk.primary_key.as_slice()
                    || chunk.metric.as_ref() != Some(&state.metric)
                {
                    return Err(DataFusionError::Execution(
                        "raw series identity changed before its last chunk".into(),
                    ));
                }
                if time[0] <= state.last_ts {
                    return Err(DataFusionError::Execution(format!(
                        "raw series timestamps are not increasing across chunks: {} after {}",
                        time[0], state.last_ts
                    )));
                }
                state.last_ts = time[time.len() - 1];
            }
        }
        Ok(())
    }

    fn ingest(&mut self, chunk: &RawSeriesChunk) -> Result<()> {
        let mut samples: Vec<_> = chunk.fields.iter().map(|field| field.iter()).collect();
        for &timestamp in chunk.time.as_buffer().iter() {
            self.row.clear();
            let mut stale = false;
            for field in &mut samples {
                match field.next().expect("field length checked by check_chunk") {
                    SampleRef::Missing => self.row.push(None),
                    SampleRef::Float(value) => {
                        stale |= is_prometheus_stale_nan(value);
                        self.row.push(Some(value));
                    }
                    SampleRef::Histogram(_) => {
                        return Err(DataFusionError::NotImplemented(
                            "range evaluation of histogram samples".into(),
                        ));
                    }
                }
            }
            if stale {
                continue;
            }
            // Like RangeManipulate, any kept sample whose shifted time leaves i64 fails the series.
            let shifted = timestamp
                .checked_add(self.window.offset_ms)
                .ok_or_else(|| {
                    DataFusionError::Execution(format!(
                        "sample timestamp {timestamp} overflows with offset {} ms",
                        self.window.offset_ms
                    ))
                })?;
            for (field, value) in self.fields.iter_mut().zip(&self.row) {
                if let Some(value) = value {
                    field.timestamps.push(shifted);
                    field.values.push(*value);
                }
            }
        }
        Ok(())
    }

    /// Evaluates grid points up to `ready` on the evaluation timeline, or all remaining points
    /// when `ready` is `None`.
    fn evaluate(&mut self, ready: Option<i128>) -> Result<()> {
        let state = self.series.as_mut().expect("checked by check_chunk");
        let spec = self.grid.spec();
        let grid = self.grid.timestamps();
        let range = i128::from(self.window.range_ms);
        while state.cursor < grid.len() {
            let eval_ts = grid[state.cursor];
            if ready.is_some_and(|ready| i128::from(eval_ts) > ready) {
                break;
            }
            let lower = i128::from(eval_ts) - range;
            let mut earliest = None;
            for field in &mut self.fields {
                field.head += field.timestamps[field.head..]
                    .partition_point(|&timestamp| i128::from(timestamp) <= lower);
                if let Some(&timestamp) = field.timestamps.get(field.head) {
                    earliest = Some(earliest.map_or(timestamp, |min: i64| min.min(timestamp)));
                }
            }
            // No retained sample: either the series is done or the next one is not here yet.
            let Some(earliest) = earliest else {
                break;
            };
            if earliest > eval_ts {
                // Skip the points whose windows end before the earliest sample.
                let step = i128::from(spec.step_ms);
                let steps = (i128::from(earliest) - i128::from(spec.start_ms) + step - 1) / step;
                state.cursor = usize::try_from(steps).map_or(grid.len(), |i| i.min(grid.len()));
                continue;
            }

            self.row.clear();
            for field in &self.fields {
                let timestamps = &field.timestamps[field.head..];
                let len = timestamps.partition_point(|&timestamp| timestamp <= eval_ts);
                self.row.push(if len == 0 {
                    None
                } else {
                    (self.kernel)(FloatWindow {
                        timestamps: &timestamps[..len],
                        values: &field.values[field.head..field.head + len],
                        eval_ts,
                        range_ms: self.window.range_ms,
                    })
                });
            }
            self.output.push(state.cursor, &self.row)?;
            state.cursor += 1;
        }
        for field in &mut self.fields {
            field.timestamps.drain(..field.head);
            field.values.drain(..field.head);
            field.head = 0;
        }
        Ok(())
    }
}
