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

use std::ops::Range;
use std::sync::Arc;

use common_query::prometheus::is_prometheus_stale_nan;
use datafusion::arrow::array::{Array, Float64Array};
use datafusion::arrow::buffer::Buffer;
use datafusion::error::{DataFusionError, Result};
use datatypes::series::MetricName;
use datatypes::series::histogram::CounterResetHint;
use datatypes::series::raw::RawSeriesChunk;
use datatypes::series::samples::{
    DenseSamplesRef, SampleRef, Samples, Series, SeriesFields, SeriesSchema,
};
use datatypes::series::time::{EvalGrid, GridRange, GridSpec, TimeAxis};

use crate::evaluated::EvaluatedChunk;
use crate::grid_series::{GridSample, GridSeriesBuilder, METADATA_LABELS, external};
use crate::histogram_eval::float_histogram::FloatHistogram;

mod function;
mod histogram;
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
    /// The counter resets this window contains, ascending by sample. Empty for every function
    /// that does not correct counters and for every window of a series that never reset.
    pub counter_resets: &'a [CounterReset],
    /// Evaluation timestamp `T`. `timestamps` lie in `(T - range_ms, T]`, except for an
    /// `@`-anchored `predict_linear`, which evaluates the first point's window at every `T`.
    pub eval_ts: i64,
    pub range_ms: i64,
}

/// Computes one window result; `None` means the window has no result.
pub(crate) type WindowKernel = Box<dyn FnMut(FloatWindow<'_>) -> Option<f64> + Send>;

/// The windows of one field at consecutive grid points, as ranges into the field's retained
/// samples.
pub(crate) struct FloatWindows<'a> {
    /// The retained samples, in time order.
    pub timestamps: &'a [i64],
    pub values: &'a [f64],
    /// Counter resets over the retained samples, ascending by position.
    pub resets: &'a [CounterReset],
    /// Absolute position of `timestamps[0]`: a reset position is `base` plus a sample index.
    pub base: u64,
    /// One range per window, ascending and left-open at the start.
    pub windows: &'a [Range<usize>],
    /// The evaluation timestamp of each window.
    pub eval_ts: &'a [i64],
    pub range_ms: i64,
}

impl FloatWindows<'_> {
    /// The resets inside window `index`.
    ///
    /// `windows` are absolute indices into `timestamps`, so a window whose start has already
    /// been released still maps to the right reset.
    pub(crate) fn resets_of(&self, index: usize) -> &[CounterReset] {
        let span = &self.windows[index];
        resets_between(
            self.resets,
            self.base + span.start as u64,
            self.base + span.end as u64,
        )
    }
}

/// Computes a batch of one field's windows at once, appending one result per window to `out`.
pub(crate) type BatchKernel = Box<dyn FnMut(&FloatWindows<'_>, &mut Vec<Option<f64>>) + Send>;

/// Grid points accumulated before a batch kernel call. Sized to keep the per-field span and
/// column buffers in cache while staying large enough to amortise the call.
const EVALUATION_BATCH: usize = 512;

/// Evaluates range windows over raw series, one series at a time, handing out its results as they
/// become final.
///
/// Each series is pushed as its chunks in order. A push evaluates the grid points whose windows
/// the pushed samples complete, and hands them out as an [`EvaluatedChunk`], so a consumer can
/// stream a long series instead of waiting for its last chunk. The series closes on a chunk marked
/// `is_last`, which is handed out even when it holds no position.
///
/// Fields are evaluated independently over their own valid samples; a row holding a stale
/// marker in any field is dropped, as the ordinary range selector does. Only the samples later
/// windows can still use are retained. Native histogram samples need a [`RangeFunction`]; an
/// evaluator built from a bare kernel rejects them.
pub struct RangeEvaluator {
    grid: Arc<EvalGrid>,
    /// The window with `offset_ms` replaced by the offset samples are shifted by.
    window: RangeWindow,
    replay: Replay,
    kernel: WindowKernel,
    function: Option<RangeFunction>,
    output_schema: Arc<SeriesSchema>,
    output: GridSeriesBuilder,
    /// The grid of evaluated input series, such as the inner result of a subquery; `None` for
    /// raw input.
    input_grid: Option<GridSpec>,
    series: Option<SeriesState>,
    fields: Vec<FieldSamples>,
    row: Vec<Option<GridSample>>,
    drops_metric_name: bool,
    /// Raised by the first non-empty window, where Prometheus validates function arguments.
    window_error: Option<String>,
    /// Chunks appended by the bulk path, so a test can prove that path is reached rather than
    /// only that the output is right, which the per-sample path would also produce.
    #[cfg(test)]
    bulk_ingests: usize,
    /// The batch form of `function`, when it has one and the input allows it. Present only for
    /// `Replay::PerPoint`, whose windows advance with the grid; an anchored selector replays one
    /// window and gains nothing from batching.
    batch: Option<BatchKernel>,
    /// Windows accumulated for the next batch call.
    pending: PendingBatch,
    /// Batch kernel calls made so far. Test-only: both dispatch shapes produce the same output,
    /// so a test comparing only output could not tell which one ran.
    #[cfg(test)]
    batch_calls: usize,
}

/// Windows accumulated for the next batch call: one list per field.
struct PendingBatch {
    /// Grid index of each accumulated point, ascending.
    ///
    /// Not assumed contiguous: `evaluate` skips the grid points whose window ends before the
    /// series has a sample, so a batch can span such a gap and must write each point to its own
    /// row.
    indices: Vec<usize>,
    /// Evaluation timestamp of each accumulated point.
    eval_ts: Vec<i64>,
    /// The retained-sample range of each point's window, one list per field.
    spans: Vec<Vec<Range<usize>>>,
    /// One output column per field, written by the batch kernel.
    columns: Vec<Vec<Option<f64>>>,
}

impl PendingBatch {
    fn with_fields(fields: usize) -> Self {
        Self {
            indices: Vec::new(),
            eval_ts: Vec::new(),
            spans: vec![Vec::new(); fields],
            columns: vec![Vec::new(); fields],
        }
    }

    fn len(&self) -> usize {
        self.eval_ts.len()
    }

    fn is_empty(&self) -> bool {
        self.eval_ts.is_empty()
    }

    /// Drops the accumulated points, keeping the allocations.
    fn clear_points(&mut self) {
        self.indices.clear();
        self.eval_ts.clear();
        for spans in &mut self.spans {
            spans.clear();
        }
    }
}

/// Records one point for the next batch call: its evaluation timestamp and, per field, the range
/// of retained samples its window covers.
///
/// The ranges are relative to the retained samples, so samples dropped before the batch flushes
/// would shift them; [`flush_batch`] therefore runs before any `advance`.
fn accumulate(pending: &mut PendingBatch, index: usize, eval_ts: i64, fields: &[FieldSamples]) {
    debug_assert_eq!(pending.spans.len(), fields.len());
    pending.indices.push(index);
    pending.eval_ts.push(eval_ts);
    for (spans, field) in pending.spans.iter_mut().zip(fields) {
        let end = field.head
            + field.timestamps[field.head..].partition_point(|&timestamp| timestamp <= eval_ts);
        spans.push(field.head..end);
    }
}

/// Runs the batch kernel over the accumulated points and appends its columns.
fn flush_batch(
    kernel: &mut BatchKernel,
    pending: &mut PendingBatch,
    fields: &[FieldSamples],
    range_ms: i64,
    row: &mut Vec<Option<GridSample>>,
    output: &mut GridSeriesBuilder,
    #[cfg(test)] batch_calls: &mut usize,
) -> Result<()> {
    if pending.is_empty() {
        return Ok(());
    }
    // Counted here rather than at the call site, so the number a test reads is by construction
    // the number of kernel calls that happened.
    #[cfg(test)]
    {
        *batch_calls += 1;
    }
    for (index, field) in fields.iter().enumerate() {
        // `pending.spans` index the retained arrays absolutely, so no adjustment is needed for
        // samples `advance` has released: those sit before every span start.
        let batch = FloatWindows {
            timestamps: &field.timestamps,
            values: &field.values,
            resets: &field.resets,
            base: field.base,
            windows: &pending.spans[index],
            eval_ts: &pending.eval_ts,
            range_ms,
        };
        kernel(&batch, &mut pending.columns[index]);
    }
    for (offset, &grid_index) in pending.indices.iter().enumerate() {
        row.clear();
        for column in &pending.columns {
            row.push(column[offset].map(GridSample::Float));
        }
        output.push_samples(grid_index, row)?;
    }
    pending.clear_points();
    Ok(())
}

struct SeriesState {
    /// `None` for evaluated input, which arrives as one complete series.
    primary_key: Option<Buffer>,
    metric: MetricName,
    last_ts: i64,
    /// Next grid index to evaluate.
    cursor: usize,
    /// Grid index after the last position the evaluator has decided, whether it holds a value or
    /// not. This is what a consumer needs in order to advance, so it is reported as the chunk
    /// span even where no value was produced.
    completed: usize,
    /// Grid index after the last position handed out to the consumer.
    emitted_end: usize,
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
    /// Native histogram samples, kept apart from the floats as Prometheus does.
    histograms: Vec<(i64, FloatHistogram)>,
    histogram_head: usize,
    /// Sequence number of `values[0]`. Absolute and monotonic across the series this evaluator
    /// evaluates, so a reset keeps its position when `head` samples are dropped instead of every
    /// index being renumbered. An empty field holds the number of samples already dropped, which
    /// keeps positions increasing when the next series starts.
    base: u64,
    /// Resets between consecutive retained samples, ascending by position: exactly the ones a
    /// window over these samples can still contain. Maintained only for the counter-correcting
    /// functions, and released as the samples they reach back to are dropped.
    resets: Vec<CounterReset>,
    /// Whether `resets` is maintained for this field.
    index_resets: bool,
}

/// One counter reset: a sample that dropped below the sample before it.
#[derive(Clone, Copy, Debug)]
pub(crate) struct CounterReset {
    /// Position of the later sample, as an absolute sequence number. Its predecessor is
    /// `position - 1`, so a window holding the samples `[start, end)` contains this reset
    /// exactly when `start < position < end`.
    pub position: u64,
    /// The value the reset adds back: the sample at `position - 1`, which is inside the window
    /// whenever the reset is.
    pub previous_value: f64,
}

/// The resets inside the window over the absolute positions `[start, end)`.
///
/// Left-open: a reset at `start` would add back a sample behind the window.
fn resets_between(resets: &[CounterReset], start: u64, end: u64) -> &[CounterReset] {
    let first = resets.partition_point(|reset| reset.position <= start);
    let count = resets[first..].partition_point(|reset| reset.position < end);
    &resets[first..first + count]
}

impl FieldSamples {
    fn new(index_resets: bool) -> Self {
        Self {
            index_resets,
            ..Self::default()
        }
    }

    /// Appends one valid sample, recording a counter reset when it dropped below the sample
    /// before it.
    ///
    /// "Before it" means the previous entry of `values`, which leaves native histogram samples
    /// out. A drop across an intervening histogram is therefore recorded as a reset of two
    /// samples that are not neighbours. That is unobservable: a window holding both floats also
    /// holds the histogram between them, so it is a mixed window, whose counter functions report
    /// nothing; and a window starting after the earlier float excludes a reset on its first
    /// sample. Should the mixed-window rules ever start reading the float resets, this has to
    /// compare timestamps instead.
    fn push(&mut self, timestamp: i64, value: f64) {
        if self.index_resets
            && let Some(&previous_value) = self.values.last()
            && value < previous_value
        {
            self.resets.push(CounterReset {
                position: self.base + self.values.len() as u64,
                previous_value,
            });
        }
        self.timestamps.push(timestamp);
        self.values.push(value);
    }

    /// Appends samples that are all present, in bulk.
    ///
    /// Same result as calling [`push`](Self::push) once per sample, including the reset scan,
    /// which stays per-sample for a field that maintains the index; the two appends become two
    /// `memcpy`s.
    fn push_bulk(&mut self, timestamps: &[i64], values: &[f64]) {
        debug_assert_eq!(timestamps.len(), values.len());
        if self.index_resets {
            let start = self.values.len();
            let mut previous = self.values.last().copied();
            for (offset, &value) in values.iter().enumerate() {
                if let Some(previous_value) = previous
                    && value < previous_value
                {
                    self.resets.push(CounterReset {
                        position: self.base + (start + offset) as u64,
                        previous_value,
                    });
                }
                previous = Some(value);
            }
        }
        self.timestamps.extend_from_slice(timestamps);
        self.values.extend_from_slice(values);
    }

    /// Drops the samples before `head` and the resets whose predecessor is one of them.
    fn advance(&mut self) {
        let head = std::mem::take(&mut self.head);
        if head == 0 {
            return;
        }
        self.base += head as u64;
        self.timestamps.drain(..head);
        self.values.drain(..head);
        // A reset at `base` would add back the last dropped sample, so it goes with it.
        let dropped = self
            .resets
            .partition_point(|reset| reset.position <= self.base);
        self.resets.drain(..dropped);
    }

    /// The resets inside the window holding `len` samples from `head`.
    fn resets_in(&self, len: usize) -> &[CounterReset] {
        let start = self.base + self.head as u64;
        resets_between(&self.resets, start, start + len as u64)
    }
}

impl RangeEvaluator {
    /// Creates an evaluator of `function` for the series of `input` onto `grid`.
    pub fn try_new(
        input: &SeriesSchema,
        grid: Arc<EvalGrid>,
        window: RangeWindow,
        function: RangeFunction,
    ) -> Result<Self> {
        let mut window_error = None;
        if let RangeFunction::DoubleExponentialSmoothing { smoothing, trend } = function {
            // Prometheus rejects factors outside (0, 1); NaN passes its check.
            window_error = [("smoothing", smoothing), ("trend", trend)]
                .into_iter()
                .find(|(_, factor)| *factor <= 0.0 || *factor >= 1.0)
                .map(|(name, factor)| {
                    format!("invalid {name} factor. Expected: 0 < factor < 1, got: {factor}")
                });
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
        let mut evaluator = Self::with_kernel(
            input,
            grid,
            window,
            function.kernel(fold_offset),
            step_invariant,
            function.indexes_counter_resets(),
        )?;
        evaluator.drops_metric_name = function.drops_metric_name();
        evaluator.window_error = window_error;
        evaluator.function = Some(function);
        // An anchored selector replays one window at every point, so its windows do not advance
        // with the grid and a batch would only repeat the same call.
        if matches!(evaluator.replay, Replay::PerPoint) {
            evaluator.batch = function.batch_kernel();
        }
        Ok(evaluator)
    }

    /// `step_invariant` tells whether an `@`-anchored result holds at every grid point.
    /// `index_resets` tells whether the kernel corrects counters, which is the only case where
    /// the per-field reset index is worth maintaining.
    pub(crate) fn with_kernel(
        input: &SeriesSchema,
        grid: Arc<EvalGrid>,
        mut window: RangeWindow,
        kernel: WindowKernel,
        step_invariant: bool,
        index_resets: bool,
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
            function: None,
            output: GridSeriesBuilder::try_new(output_schema.clone())?,
            input_grid: input.eval_grid.as_ref().map(|grid| grid.spec()),
            output_schema,
            series: None,
            fields: input
                .fields
                .iter()
                .map(|_| FieldSamples::new(index_resets))
                .collect(),
            row: Vec::with_capacity(input.fields.len()),
            drops_metric_name: false,
            window_error: None,
            #[cfg(test)]
            bulk_ingests: 0,
            batch: None,
            pending: PendingBatch::with_fields(input.fields.len()),
            #[cfg(test)]
            batch_calls: 0,
        })
    }

    /// Chunks appended without the per-sample loop. Test-only: both paths produce the same
    /// output by construction, so a test that only compared output could not tell whether the
    /// bulk path ran at all.
    #[cfg(test)]
    pub(crate) fn bulk_ingest_count(&self) -> usize {
        self.bulk_ingests
    }

    /// Batch kernel calls made so far. Test-only: both dispatch shapes produce the same output,
    /// so a test comparing only output could not tell which one ran.
    #[cfg(test)]
    pub(crate) fn batch_call_count(&self) -> usize {
        self.batch_calls
    }

    /// Schema of the evaluated series: the input fields on the evaluation grid.
    pub fn output_schema(&self) -> &Arc<SeriesSchema> {
        &self.output_schema
    }

    /// Consumes the next chunk of the current series, or starts a new series.
    ///
    /// Returns the grid points this chunk decided, as one chunk whose range is absolute on the
    /// evaluation grid; `None` when it decided no new point. A decided point whose window has no
    /// result is a missing value inside the range, as it is in a whole series; the range is what
    /// tells a consumer that the point is settled rather than still waiting for input.
    ///
    /// The last chunk of a series is always returned, empty when it decided nothing new, because
    /// that chunk is what closes the series for the consumer.
    ///
    /// The evaluator must not be reused after an error.
    pub fn push(&mut self, chunk: &RawSeriesChunk) -> Result<Option<EvaluatedChunk>> {
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
            return self.emit(false);
        }
        self.close_series()
    }

    /// Evaluates one complete evaluated series, such as the inner result of a subquery: the
    /// value at each grid point is a sample at that time. Stale markers are kept as values,
    /// since the input is computed.
    ///
    /// The input arrives complete, so its result is one closing chunk.
    pub fn push_series(&mut self, series: &Series) -> Result<Option<EvaluatedChunk>> {
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
            completed: 0,
            emitted_end: 0,
        });
        self.ingest(&range.timestamps(), &series.fields, false)?;
        self.close_series()
    }

    /// Evaluates the rest of the series of the last input chunk and closes it.
    fn close_series(&mut self) -> Result<Option<EvaluatedChunk>> {
        self.evaluate(None)?;
        let chunk = self.emit(true)?;
        self.series = None;
        Ok(chunk)
    }

    /// Hands out the positions decided since the last chunk, if any.
    ///
    /// The span covers everything decided, so a position decided to hold no value travels with
    /// the chunk that decided it: a consumer needs that progress to advance, and to tell a
    /// finished position from one still waiting for input. `is_last` closes the series, and then a
    /// chunk is always handed out, empty when there is nothing left to report.
    fn emit(&mut self, is_last: bool) -> Result<Option<EvaluatedChunk>> {
        let Some(state) = self.series.as_ref() else {
            return Ok(None);
        };
        let (start, end) = (state.emitted_end, state.completed);
        // `evaluate` opens the builder's span at the first not-yet-handed-out position even when
        // this push completed nothing, so finishing here would produce a zero-length chunk that
        // claims progress the push did not make. Only the last chunk is handed out regardless.
        if end == start && !is_last {
            return Ok(None);
        }
        let mut metric = state.metric.clone();
        if self.drops_metric_name {
            metric.remove_all(&METADATA_LABELS);
        }
        if end > start {
            // Every decided position joins the span, including the ones without a value.
            self.output.cover_from(start)?;
            self.output.cover_through(end)?;
        }
        let Some(series) = self.output.finish(metric)? else {
            debug_assert_eq!(end, start, "a decided position widens the span");
            return if is_last {
                self.empty_chunk(start)
            } else {
                Ok(None)
            };
        };
        self.series
            .as_mut()
            .expect("checked at the entry of emit")
            .emitted_end = end;
        EvaluatedChunk::try_new(series, is_last).map(Some)
    }

    /// A closing chunk without values, addressed after the last position handed out.
    fn empty_chunk(&self, at: usize) -> Result<Option<EvaluatedChunk>> {
        let state = self.series.as_ref().expect("checked by the caller");
        let mut metric = state.metric.clone();
        if self.drops_metric_name {
            metric.remove_all(&METADATA_LABELS);
        }
        let empty = Samples::from_float(Float64Array::from(Vec::<f64>::new()));
        let fields = SeriesFields::try_from_vec(vec![empty; self.output_schema.fields.len()])
            .map_err(external)?;
        let time = GridRange::try_new(self.grid.clone(), at, 0).map_err(external)?;
        let series = Series::try_new(&self.output_schema, metric, TimeAxis::Grid(time), fields)
            .map_err(external)?;
        EvaluatedChunk::try_new(series, true).map(Some)
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
                    completed: 0,
                    emitted_end: 0,
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

    /// Appends the chunk in bulk when every field is an entirely present, unshifted float column.
    ///
    /// Returns `false` when any condition of the per-sample path applies, so the caller can run
    /// it. A chunk carrying a stale marker takes that path too: the marker drops the whole row,
    /// and reproducing that here would cost the same scan the loop already makes.
    ///
    /// A field mixing floats and native histograms never qualifies: `dense()` reports
    /// [`DenseSamplesRef::Mixed`] for it, keeping its windows on the histogram path where the
    /// mixed-window rules decide the result.
    fn ingest_in_bulk(
        &mut self,
        timestamps: &[i64],
        fields: &SeriesFields,
        drop_stale: bool,
    ) -> bool {
        // Lengths are checked by `check_chunk` for raw input and by `push_series` for evaluated
        // input, so only the shape of the columns is decided here.
        if self.window.offset_ms != 0 {
            return false;
        }
        let mut columns = Vec::with_capacity(fields.len());
        for field in fields.iter() {
            let Some(DenseSamplesRef::Float(values)) = field.dense() else {
                return false;
            };
            if values.null_count() != 0 {
                return false;
            }
            columns.push(values);
        }
        // A kept sample's shifted time must fit `i64`; with no offset it is the source timestamp,
        // which `RawTimestamps` already validated as increasing and which the frame widened into
        // this array without narrowing, so there is nothing left to check here.
        if drop_stale
            && columns.iter().any(|values| {
                values
                    .values()
                    .iter()
                    .any(|value| is_prometheus_stale_nan(*value))
            })
        {
            return false;
        }
        for (field, values) in self.fields.iter_mut().zip(columns) {
            field.push_bulk(timestamps, values.values());
        }
        #[cfg(test)]
        {
            self.bulk_ingests += 1;
        }
        true
    }

    /// Retains the samples of `fields` at `timestamps`. From `raw` input a row holding a stale
    /// marker in any field is dropped; evaluated input keeps it as a value.
    fn ingest(&mut self, timestamps: &[i64], fields: &SeriesFields, raw: bool) -> Result<()> {
        if self.ingest_in_bulk(timestamps, fields, raw) {
            return Ok(());
        }
        let mut samples: Vec<_> = fields.iter().map(|field| field.iter()).collect();
        let mut row = Vec::with_capacity(samples.len());
        for &timestamp in timestamps {
            row.clear();
            let mut stale = false;
            for field in &mut samples {
                let sample = field.next().expect("field length checked by the caller");
                match &sample {
                    SampleRef::Missing => {}
                    SampleRef::Float(value) => stale |= is_prometheus_stale_nan(*value),
                    SampleRef::Histogram(view) => {
                        if self.function.is_none() {
                            return Err(DataFusionError::NotImplemented(
                                "range evaluation of histogram samples".into(),
                            ));
                        }
                        stale |= is_prometheus_stale_nan(view.sum);
                    }
                }
                row.push(sample);
            }
            if stale && raw {
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
            for (field, sample) in self.fields.iter_mut().zip(&row) {
                match sample {
                    SampleRef::Missing => {}
                    SampleRef::Float(value) => {
                        field.push(shifted, *value);
                    }
                    SampleRef::Histogram(view) => {
                        let mut h = FloatHistogram::try_from_view(view)?;
                        // Like Prometheus' `evalSubquery`: an explicit counter reset hint
                        // describes the sample's raw neighbours, not its evaluated ones.
                        if !raw && h.reset_hint != CounterResetHint::Gauge {
                            h.reset_hint = CounterResetHint::Unknown;
                        }
                        field.histograms.push((shifted, h));
                    }
                }
            }
        }
        Ok(())
    }

    /// Evaluates grid points up to `ready` on the evaluation timeline, or all remaining points
    /// when `ready` is `None`.
    fn evaluate(&mut self, ready: Option<i128>) -> Result<()> {
        // Positions handed out so far stay behind the builder's span, so a chunk never repeats or
        // reorders them.
        let emitted = self
            .series
            .as_ref()
            .expect("checked by check_chunk")
            .emitted_end;
        self.output.cover_from(emitted)?;
        let points_read = self.points_read();
        let state = self.series.as_mut().expect("checked by check_chunk");
        let spec = self.grid.spec();
        let grid = self.grid.timestamps();
        let range = i128::from(self.window.range_ms);
        let mut empty_tail = false;
        while state.cursor < points_read {
            let eval_ts = grid[state.cursor];
            let lower = i128::from(eval_ts) - range;
            let mut earliest = None;
            // Later windows start no earlier, so samples behind this one go even when the point
            // itself is not ready yet.
            for field in &mut self.fields {
                field.head += field.timestamps[field.head..]
                    .partition_point(|&timestamp| i128::from(timestamp) <= lower);
                field.histogram_head += field.histograms[field.histogram_head..]
                    .partition_point(|(timestamp, _)| i128::from(*timestamp) <= lower);
                let first_histogram = field.histograms.get(field.histogram_head);
                for &timestamp in field
                    .timestamps
                    .get(field.head)
                    .into_iter()
                    .chain(first_histogram.map(|(timestamp, _)| timestamp))
                {
                    earliest = Some(earliest.map_or(timestamp, |min: i64| min.min(timestamp)));
                }
            }
            if ready.is_some_and(|ready| i128::from(eval_ts) > ready) {
                break;
            }
            // No retained sample: every point from here on is decided too. A sample that still
            // has to arrive lies beyond the window of every one of them, so none can gain a value.
            let Some(earliest) = earliest else {
                empty_tail = true;
                break;
            };
            if earliest > eval_ts {
                // Skip the points whose windows end before the earliest sample.
                let step = i128::from(spec.step_ms);
                let steps = (i128::from(earliest) - i128::from(spec.start_ms) + step - 1) / step;
                state.cursor = usize::try_from(steps).map_or(grid.len(), |i| i.min(grid.len()));
                continue;
            }
            if let Some(error) = &self.window_error {
                return Err(DataFusionError::Execution(error.clone()));
            }

            let mut evaluator = RowEvaluator {
                fields: &self.fields,
                kernel: &mut self.kernel,
                function: self.function,
                range_ms: self.window.range_ms,
                offset_ms: self.window.offset_ms,
            };
            match self.replay {
                Replay::PerPoint => {
                    // A window holding a native histogram is decided by the mixed-window rules,
                    // which the batch kernel -- reading floats alone -- cannot reproduce. Flush
                    // what is pending and decide this point on the per-window path.
                    //
                    // Histograms are retained in time order, so the first one still ahead of the
                    // window head decides: a later one cannot be inside this window if this one
                    // is not. The check runs only when a batch kernel is set, so a function
                    // without one never pays for it.
                    let histogram_window = self.batch.is_some()
                        && self.fields.iter().any(|field| {
                            field.histograms.get(field.histogram_head).is_some_and(
                                |(timestamp, _)| i128::from(*timestamp) <= i128::from(eval_ts),
                            )
                        });
                    if let Some(kernel) = self.batch.as_mut()
                        && !histogram_window
                    {
                        accumulate(&mut self.pending, state.cursor, eval_ts, &self.fields);
                        if self.pending.len() >= EVALUATION_BATCH {
                            flush_batch(
                                kernel,
                                &mut self.pending,
                                &self.fields,
                                self.window.range_ms,
                                &mut self.row,
                                &mut self.output,
                                #[cfg(test)]
                                &mut self.batch_calls,
                            )?;
                        }
                        state.cursor += 1;
                        continue;
                    }
                    if let Some(kernel) = self.batch.as_mut() {
                        flush_batch(
                            kernel,
                            &mut self.pending,
                            &self.fields,
                            self.window.range_ms,
                            &mut self.row,
                            &mut self.output,
                            #[cfg(test)]
                            &mut self.batch_calls,
                        )?;
                    }
                    evaluator.evaluate(eval_ts, eval_ts, &mut self.row)?;
                    self.output.push_samples(state.cursor, &self.row)?;
                }
                Replay::Once => {
                    evaluator.evaluate(eval_ts, eval_ts, &mut self.row)?;
                    for index in 0..grid.len() {
                        self.output.push_samples(index, &self.row)?;
                    }
                }
                Replay::EveryPoint => {
                    for (index, &point) in grid.iter().enumerate() {
                        evaluator.evaluate(eval_ts, point, &mut self.row)?;
                        self.output.push_samples(index, &self.row)?;
                    }
                }
            }
            state.cursor += 1;
        }
        // The accumulated spans index the retained arrays, which `advance` below rewrites, so the
        // batch has to flush first.
        if let Some(kernel) = self.batch.as_mut() {
            flush_batch(
                kernel,
                &mut self.pending,
                &self.fields,
                self.window.range_ms,
                &mut self.row,
                &mut self.output,
                #[cfg(test)]
                &mut self.batch_calls,
            )?;
        }
        // How far the series is decided. A point is decided once it has been evaluated, once its
        // window can no longer gain a sample, or -- at the end of the series or of what the input
        // has reached -- once no retained sample reaches it. The last case matters: the loop stops
        // at the first such point instead of walking the rest of the grid, so the points behind it
        // still have to be reported as decided, or a consumer waits for input that decided them.
        let completed = if empty_tail {
            match self.replay {
                Replay::PerPoint => {
                    let mut decided = state.cursor;
                    if let Some(ready) = ready {
                        while decided < points_read && i128::from(grid[decided]) <= ready {
                            decided += 1;
                        }
                    } else {
                        decided = points_read;
                    }
                    // Later rounds start evaluating from here, and none of these points can gain
                    // a value, so the cursor may skip them.
                    state.cursor = decided;
                    decided
                }
                // An anchored selector broadcasts its one window over the whole grid, so deciding
                // that window decides every point. Its window ends at the grid's start rather than
                // at each point, so it is decided once the input has ended or has passed that end.
                Replay::Once | Replay::EveryPoint => match ready {
                    None => grid.len(),
                    Some(ready) if ready >= i128::from(spec.start_ms) => grid.len(),
                    Some(_) => state.completed,
                },
            }
        } else if matches!(self.replay, Replay::PerPoint) {
            state.cursor
        } else if state.cursor > 0 {
            grid.len()
        } else {
            0
        };
        state.completed = state.completed.max(completed);
        for field in &mut self.fields {
            // After the last window, samples are still ingested so that the overflow and
            // histogram checks do not depend on how the series is chunked, but none are kept.
            // An anchored selector can skip past the one point it reads.
            if state.cursor >= points_read {
                field.head = field.timestamps.len();
                field.histogram_head = field.histograms.len();
            }
            field.advance();
            field.histograms.drain(..field.histogram_head);
            field.histogram_head = 0;
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

/// Computes the result of every field's window at one grid point.
struct RowEvaluator<'a> {
    fields: &'a [FieldSamples],
    kernel: &'a mut WindowKernel,
    function: Option<RangeFunction>,
    range_ms: i64,
    offset_ms: i64,
}

impl RowEvaluator<'_> {
    /// Fills `row` with the result over each field's window ending at `window_end`, evaluated
    /// at `eval_ts`.
    fn evaluate(
        &mut self,
        window_end: i64,
        eval_ts: i64,
        row: &mut Vec<Option<GridSample>>,
    ) -> Result<()> {
        row.clear();
        for field in self.fields {
            let timestamps = &field.timestamps[field.head..];
            let len = timestamps.partition_point(|&timestamp| timestamp <= window_end);
            let floats = (len > 0).then(|| FloatWindow {
                timestamps: &timestamps[..len],
                values: &field.values[field.head..field.head + len],
                counter_resets: field.resets_in(len),
                eval_ts,
                range_ms: self.range_ms,
            });
            let histograms = &field.histograms[field.histogram_head..];
            let histograms = &histograms
                [..histograms.partition_point(|(timestamp, _)| *timestamp <= window_end)];
            row.push(match self.function {
                Some(function) if !histograms.is_empty() => histogram::evaluate(
                    function,
                    floats,
                    histograms,
                    eval_ts,
                    self.range_ms,
                    self.offset_ms,
                    &mut *self.kernel,
                )?,
                _ => floats.and_then(|window| (self.kernel)(window).map(GridSample::Float)),
            });
        }
        Ok(())
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
    let len = u64::try_from((last - first) / step + 1)
        .map_err(|_| DataFusionError::Plan("subquery grid has too many points".into()))?;
    let first = i64::try_from(first).map_err(|_| {
        DataFusionError::Plan(format!("subquery grid start {first} ms is out of range"))
    })?;
    GridSpec::try_new(first, step_ms, len)
        .map(Some)
        .map_err(|error| DataFusionError::External(Box::new(error)))
}
