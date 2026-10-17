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
use datatypes::series::samples::{SampleRef, Series, SeriesFields, SeriesSchema};
use datatypes::series::time::{EvalGrid, GridSpec, TimeAxis};

use crate::grid_series::GridSeriesBuilder;

mod function;
#[cfg(test)]
mod tests;

pub use function::RangeFunction;

/// The samples a range selector reads at evaluation time `T`: source timestamps in
/// `(E - range_ms, E]`, where `E` is `T - offset_ms`, or `anchor_ms - offset_ms` with `@`.
#[derive(Clone, Copy, Debug)]
pub struct RangeWindow {
    /// Positive window length in milliseconds.
    pub range_ms: i64,
    /// Selector offset in milliseconds; positive reads earlier samples.
    pub offset_ms: i64,
    /// The resolved `@` timestamp in milliseconds, with `start()` and `end()` already replaced
    /// by the statement's start and end.
    pub anchor_ms: Option<i64>,
}

/// The valid samples of one field inside one window, in source order.
pub(crate) struct FloatWindow<'a> {
    /// Sample timestamps shifted by the offset onto the evaluation timeline; non-empty.
    pub timestamps: &'a [i64],
    pub values: &'a [f64],
    /// Evaluation timestamp `T`. `timestamps` lie in `(T - range_ms, T]`, except for an
    /// `@`-anchored `predict_linear`, which evaluates the first point's window at every `T`.
    pub eval_ts: i64,
    pub range_ms: i64,
}

/// Computes one window result; `None` means the window has no result.
pub(crate) type WindowKernel = Box<dyn FnMut(FloatWindow<'_>) -> Option<f64> + Send>;

/// Evaluates range windows over raw float series, one series at a time.
///
/// Each series is pushed as its chunks in order and completes on the chunk marked `is_last`.
/// Fields are evaluated independently over their own valid samples; a row holding a stale
/// marker in any field is dropped, as the ordinary range selector does. Only the samples later
/// windows can still use are retained.
pub struct RangeEvaluator {
    grid: Arc<EvalGrid>,
    /// The window with `offset_ms` replaced by the offset samples are shifted by.
    window: RangeWindow,
    replay: Replay,
    kernel: WindowKernel,
    output_schema: Arc<SeriesSchema>,
    output: GridSeriesBuilder,
    /// The grid of evaluated input series, such as the inner result of a subquery; `None` for
    /// raw input.
    input_grid: Option<GridSpec>,
    series: Option<SeriesState>,
    fields: Vec<FieldSamples>,
    row: Vec<Option<f64>>,
}

struct SeriesState {
    /// `None` for evaluated input, which arrives as one complete series.
    primary_key: Option<Buffer>,
    metric: MetricName,
    last_ts: i64,
    /// Next grid index to evaluate.
    cursor: usize,
}

/// How the windows of an `@`-anchored selector map to the grid.
///
/// Like Prometheus, an anchored selector is read once, as the window of the first grid point
/// with the offset rewritten to `start - (anchor - offset)`.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum Replay {
    /// Not anchored: every grid point reads its own window.
    PerPoint,
    /// The first point's result is reported at every point (step-invariant functions).
    Once,
    /// The first point's window is evaluated at every point (`predict_linear`).
    EveryPoint,
}

#[derive(Default)]
struct FieldSamples {
    timestamps: Vec<i64>,
    values: Vec<f64>,
    /// Samples before `head` are behind every remaining window.
    head: usize,
}

impl RangeEvaluator {
    /// Creates an evaluator of `function` for raw series of `input` onto `grid`.
    pub fn try_new(
        input: &SeriesSchema,
        grid: Arc<EvalGrid>,
        window: RangeWindow,
        function: RangeFunction,
    ) -> Result<Self> {
        if let RangeFunction::DoubleExponentialSmoothing { smoothing, trend } = function {
            for (name, factor) in [("smoothing", smoothing), ("trend", trend)] {
                // Prometheus rejects factors outside (0, 1); NaN passes its check.
                if factor <= 0.0 || factor >= 1.0 {
                    return Err(DataFusionError::Plan(format!(
                        "invalid {name} factor. Expected: 0 < factor < 1, got: {factor}"
                    )));
                }
            }
        }
        let fold_offset = fold_offset(grid.spec(), window)?;
        let step_invariant = !matches!(function, RangeFunction::PredictLinear(_));
        if !step_invariant {
            let spec = grid.spec();
            let last =
                i128::from(spec.start_ms) + i128::from(spec.step_ms) * (i128::from(spec.len) - 1);
            for eval_ts in [i128::from(spec.start_ms), last] {
                if i64::try_from(eval_ts + i128::from(fold_offset)).is_err() {
                    return Err(DataFusionError::Plan(format!(
                        "predict_linear evaluation time {eval_ts} overflows with offset {fold_offset} ms"
                    )));
                }
            }
        }
        Self::with_kernel(
            input,
            grid,
            window,
            function.kernel(fold_offset),
            step_invariant,
        )
    }

    /// `step_invariant` tells whether an `@`-anchored result holds at every grid point.
    pub(crate) fn with_kernel(
        input: &SeriesSchema,
        grid: Arc<EvalGrid>,
        mut window: RangeWindow,
        kernel: WindowKernel,
        step_invariant: bool,
    ) -> Result<Self> {
        if window.range_ms <= 0 {
            return Err(DataFusionError::Plan(format!(
                "range must be positive, got {} ms",
                window.range_ms
            )));
        }
        let replay = match (window.anchor_ms, step_invariant) {
            (None, _) => Replay::PerPoint,
            (Some(_), true) => Replay::Once,
            (Some(_), false) => Replay::EveryPoint,
        };
        window.offset_ms = fold_offset(grid.spec(), window)?;
        let output_schema = Arc::new(SeriesSchema {
            fields: input.fields.clone(),
            eval_grid: Some(grid.clone()),
        });
        Ok(Self {
            grid,
            window,
            replay,
            kernel,
            output: GridSeriesBuilder::try_new(output_schema.clone())?,
            input_grid: input.eval_grid.as_ref().map(|grid| grid.spec()),
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
    ///
    /// The evaluator must not be reused after an error.
    pub fn push(&mut self, chunk: &RawSeriesChunk) -> Result<Option<Series>> {
        if self.input_grid.is_some() {
            return Err(DataFusionError::Execution(
                "raw series chunk pushed to an evaluator of evaluated series".into(),
            ));
        }
        self.check_chunk(chunk)?;
        self.ingest(chunk.time.as_buffer(), &chunk.fields, true)?;
        let last_ts = chunk.time.as_buffer()[chunk.time.len() - 1];
        if !chunk.is_last {
            // Later chunks only hold samples after `last_ts`.
            self.evaluate(Some(
                i128::from(last_ts) + i128::from(self.window.offset_ms),
            ))?;
            return Ok(None);
        }
        self.finish_series()
    }

    /// Evaluates one complete evaluated series, such as the inner result of a subquery: the
    /// value at each grid point is a sample at that time. Stale markers are kept as values,
    /// since the input is computed.
    pub fn push_series(&mut self, series: &Series) -> Result<Option<Series>> {
        let TimeAxis::Grid(range) = &series.time else {
            return Err(DataFusionError::Execution(
                "evaluated range input must be a grid series".into(),
            ));
        };
        if self.input_grid != Some(range.grid().spec())
            || series.fields.len() != self.fields.len()
            || series.fields.iter().any(|field| field.len() != range.len())
        {
            return Err(DataFusionError::Execution(
                "evaluated series does not match the input schema".into(),
            ));
        }
        if self.series.is_some() {
            return Err(DataFusionError::Execution(
                "evaluated series pushed inside a raw series".into(),
            ));
        }
        self.series = Some(SeriesState {
            primary_key: None,
            metric: series.metric.clone(),
            last_ts: 0,
            cursor: 0,
        });
        self.ingest(&range.timestamps(), &series.fields, false)?;
        self.finish_series()
    }

    fn finish_series(&mut self) -> Result<Option<Series>> {
        self.evaluate(None)?;
        let state = self.series.take().expect("set by the caller");
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
        let primary_key = Some(chunk.primary_key.as_slice());
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
                    primary_key: Some(chunk.primary_key.clone()),
                    metric,
                    last_ts: time[time.len() - 1],
                    cursor: 0,
                });
            }
            Some(state) => {
                if state.primary_key.as_ref().map(|key| key.as_slice()) != primary_key
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

    /// Retains the samples of `fields` at `timestamps`; with `drop_stale`, a row holding a stale
    /// marker in any field is dropped.
    fn ingest(
        &mut self,
        timestamps: &[i64],
        fields: &SeriesFields,
        drop_stale: bool,
    ) -> Result<()> {
        let state = self.series.as_ref().expect("set by the caller");
        if state.cursor >= self.points_read() {
            // Every window that reads samples has been evaluated.
            return Ok(());
        }
        let mut samples: Vec<_> = fields.iter().map(|field| field.iter()).collect();
        for &timestamp in timestamps {
            self.row.clear();
            let mut stale = false;
            for field in &mut samples {
                match field.next().expect("field length checked by the caller") {
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
            if stale && drop_stale {
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
        let points_read = self.points_read();
        let state = self.series.as_mut().expect("checked by check_chunk");
        let spec = self.grid.spec();
        let grid = self.grid.timestamps();
        let range = i128::from(self.window.range_ms);
        while state.cursor < points_read {
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

            match self.replay {
                Replay::PerPoint => {
                    evaluate_row(
                        &self.fields,
                        &mut self.kernel,
                        self.window.range_ms,
                        eval_ts,
                        eval_ts,
                        &mut self.row,
                    );
                    self.output.push(state.cursor, &self.row)?;
                }
                Replay::Once => {
                    evaluate_row(
                        &self.fields,
                        &mut self.kernel,
                        self.window.range_ms,
                        eval_ts,
                        eval_ts,
                        &mut self.row,
                    );
                    for index in 0..grid.len() {
                        self.output.push(index, &self.row)?;
                    }
                }
                Replay::EveryPoint => {
                    for (index, &point) in grid.iter().enumerate() {
                        evaluate_row(
                            &self.fields,
                            &mut self.kernel,
                            self.window.range_ms,
                            eval_ts,
                            point,
                            &mut self.row,
                        );
                        self.output.push(index, &self.row)?;
                    }
                }
            }
            state.cursor += 1;
        }
        for field in &mut self.fields {
            field.timestamps.drain(..field.head);
            field.values.drain(..field.head);
            field.head = 0;
        }
        Ok(())
    }

    /// The number of leading grid points whose windows are read; an anchored selector reads
    /// only the first.
    fn points_read(&self) -> usize {
        match self.replay {
            Replay::PerPoint => self.grid.timestamps().len(),
            Replay::Once | Replay::EveryPoint => 1,
        }
    }
}

/// Fills `row` with the kernel results over each field's window ending at `window_end`,
/// evaluated at `eval_ts`.
fn evaluate_row(
    fields: &[FieldSamples],
    kernel: &mut WindowKernel,
    range_ms: i64,
    window_end: i64,
    eval_ts: i64,
    row: &mut Vec<Option<f64>>,
) {
    row.clear();
    for field in fields {
        let timestamps = &field.timestamps[field.head..];
        let len = timestamps.partition_point(|&timestamp| timestamp <= window_end);
        row.push(if len == 0 {
            None
        } else {
            kernel(FloatWindow {
                timestamps: &timestamps[..len],
                values: &field.values[field.head..field.head + len],
                eval_ts,
                range_ms,
            })
        });
    }
}

/// The offset samples are shifted by: the selector offset, or for `@` the offset that places
/// the anchored window at the first grid point.
fn fold_offset(grid: GridSpec, window: RangeWindow) -> Result<i64> {
    let Some(anchor) = window.anchor_ms else {
        return Ok(window.offset_ms);
    };
    anchor
        .checked_sub(window.offset_ms)
        .and_then(|end| grid.start_ms.checked_sub(end))
        .ok_or_else(|| {
            DataFusionError::Plan(format!(
                "@ {anchor} ms with offset {} ms is out of range",
                window.offset_ms
            ))
        })
}

/// The inner grid of a subquery read by `window` from the outer grid `outer`: the multiples of
/// `step_ms` after `start - F - range` up to the last window end, where `F` is the fold offset
/// of `window`; with `@`, the only window end is `start - F`. `None` when no multiple falls in
/// the windows.
pub fn subquery_grid(
    outer: GridSpec,
    window: RangeWindow,
    step_ms: i64,
) -> Result<Option<GridSpec>> {
    if step_ms <= 0 {
        return Err(DataFusionError::Plan(format!(
            "subquery step must be positive, got {step_ms} ms"
        )));
    }
    let fold_offset = i128::from(fold_offset(outer, window)?);
    let step = i128::from(step_ms);
    let start = i128::from(outer.start_ms);
    let last_eval = match window.anchor_ms {
        Some(_) => start,
        None => start + i128::from(outer.step_ms) * (i128::from(outer.len) - 1),
    };
    let first = ((start - fold_offset - i128::from(window.range_ms)).div_euclid(step) + 1) * step;
    let last = last_eval - fold_offset;
    if first > last {
        return Ok(None);
    }
    let len = (last - first) / step + 1;
    let first = i64::try_from(first).map_err(|_| {
        DataFusionError::Plan(format!("subquery grid start {first} ms is out of range"))
    })?;
    GridSpec::try_new(first, step_ms, len as u64)
        .map(Some)
        .map_err(|error| DataFusionError::External(Box::new(error)))
}
