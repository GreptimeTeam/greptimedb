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
use datatypes::series::SeriesLabels;
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
pub(crate) use function::quantile;

/// The samples a range selector reads at evaluation time `T`: source timestamps in
/// `(E - range_ms, E]`, where `E` is `T - offset_ms`, or `anchor_ms - offset_ms` with `@`. A
/// modifier widens that: `anchored` also reads the lookback before `E - range_ms`, `smoothed` the
/// lookback on both sides, and a scan feeding the window has to read that much more.
#[derive(Clone, Copy, Debug)]
pub struct RangeWindow {
    /// Positive window length in milliseconds.
    pub range_ms: i64,
    /// Selector offset in milliseconds; positive reads earlier samples.
    pub offset_ms: i64,
    /// The resolved `@` timestamp in milliseconds, with `start()` and `end()` already replaced
    /// by the statement's start and end.
    pub anchor_ms: Option<i64>,
    /// The experimental `anchored` or `smoothed` selector modifier, which also reads samples
    /// around the window.
    pub modifier: Option<RangeModifier>,
}

/// The experimental range selector modifiers of Prometheus, which read samples beyond the
/// window to pin its boundary values. Not to be confused with an `@` anchor, which fixes the
/// window's end.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum RangeModifier {
    /// `anchored`: the window also reads the last sample at or before its start, up to the
    /// statement's lookback before it, and takes it as the value at the start.
    Anchored { lookback_ms: i64 },
    /// `smoothed`: the window also reads samples up to the lookback before its start and after its
    /// end, and interpolates the values at both boundaries.
    Smoothed { lookback_ms: i64 },
}

impl RangeModifier {
    /// How far before the window start the window also reads.
    pub(crate) fn before_ms(self) -> i64 {
        match self {
            Self::Anchored { lookback_ms } | Self::Smoothed { lookback_ms } => lookback_ms,
        }
    }

    /// How far after the window end the window also reads.
    pub(crate) fn after_ms(self) -> i64 {
        match self {
            Self::Anchored { .. } => 0,
            Self::Smoothed { lookback_ms } => lookback_ms,
        }
    }

    pub(crate) fn name(self) -> &'static str {
        match self {
            Self::Anchored { .. } => "anchored",
            Self::Smoothed { .. } => "smoothed",
        }
    }
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
    /// `@`-anchored `predict_linear`, which evaluates the first point's window at every `T`, and
    /// for a range modifier, which widens the window by the lookback as [`RangeWindow`] says.
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
    /// One range per window, ascending. Each is a half-open index range `[start, end)`; the time
    /// window it was derived from is left-open, `(T - range_ms, T]`.
    pub windows: &'a [Range<usize>],
    /// The evaluation timestamp of each window.
    pub eval_ts: &'a [i64],
    pub range_ms: i64,
}

impl FloatWindows<'_> {
    /// The resets inside window `index`.
    ///
    /// `windows` index the retained arrays as they stand in this batch, not absolutely: each
    /// span is relative to `timestamps` and `values`, which is why `base` plus a span start is
    /// the absolute position of the sample the span starts at.
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
/// the pushed samples complete, and hands them out as one [`EvaluatedChunk`], so a consumer can
/// stream a long series instead of waiting for its last chunk. One chunk may hold several runs of
/// positions, separated by stretches holding no value, and the axis names them. The series closes
/// on a chunk marked `is_last`, which is handed out even when it holds no position.
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
    ///
    /// A span is relative to the retained arrays at the time it was recorded, not an absolute
    /// index, so [`FieldSamples::advance`] invalidates it: that is why `evaluate` flushes the
    /// batch before advancing.
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

    /// The bytes this batch's own vectors hold, by capacity.
    fn bytes(&self) -> usize {
        let spans: usize = self.spans.iter().map(|spans| spans.capacity()).sum();
        let columns: usize = self.columns.iter().map(|column| column.capacity()).sum();
        self.indices.capacity() * std::mem::size_of::<usize>()
            + self.eval_ts.capacity() * std::mem::size_of::<i64>()
            + spans * std::mem::size_of::<Range<usize>>()
            + columns * std::mem::size_of::<Option<f64>>()
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
    metric: SeriesLabels,
    /// Largest source timestamp retained so far, or `None` while the series holds no sample: a
    /// closing chunk of a series that never produced a value carries no position to read.
    last_ts: Option<i64>,
    /// Next grid index to evaluate.
    cursor: usize,
    /// Whether the input, or the evaluated series, has ended. The state stays until the buffered
    /// rows have been handed out: the closing chunk still needs its metric.
    closing: bool,
    /// Grid index after the last position handed out to the consumer.
    emitted_end: usize,
}

impl SeriesState {
    /// The bytes this series' identity holds, which the rows handed out do not cover.
    fn identity_bytes(&self) -> usize {
        // A scan slices the primary key out of a decoded buffer, so the bytes the key shows are not
        // the bytes it keeps: a key of a few bytes pins the allocation it was sliced out of.
        // Charging the slice length instead would show a handful of bytes for an allocation the
        // size of a decoded batch. One evaluator holds one series at a time, and an evaluated input
        // holds no key at all, so there is no second slice of the same allocation to dedupe.
        let primary_key = self
            .primary_key
            .as_ref()
            .map_or(0, |key| key.capacity().max(key.len()));
        let metric: usize = self.metric.name().map_or(0, |name| name.len())
            + self
                .metric
                .tags()
                .iter()
                .map(|tag| tag.name.len() + tag.value.len())
                .sum::<usize>();
        std::mem::size_of::<SeriesState>() + primary_key + metric
    }
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
    /// Histograms before `histogram_head` are behind every remaining window; their payloads are
    /// released by [`advance_histograms`](Self::advance_histograms).
    histogram_head: usize,
    /// Estimated payload of the histograms this field still holds: the bucket lists and other
    /// buffers the samples own, which no `size_of` of a slot shows.
    ///
    /// Measured for a histogram when it is admitted and released with it, so a retained sample is
    /// not walked again on every push. A histogram is never modified once admitted, so the payload
    /// its release subtracts is the one its admission added. Samples already behind every window
    /// keep their payload in the figure until the drain releases them.
    histogram_bytes: usize,
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
    /// The bytes this field's buffers hold, by capacity: the samples, the history behind `head`
    /// included, plus the estimated payload of the histograms still held.
    fn bytes(&self) -> usize {
        self.timestamps.capacity() * std::mem::size_of::<i64>()
            + self.values.capacity() * std::mem::size_of::<f64>()
            + self.histograms.capacity() * std::mem::size_of::<(i64, FloatHistogram)>()
            + self.resets.capacity() * std::mem::size_of::<CounterReset>()
            + self.histogram_bytes
    }

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

    /// Drops the histograms before `histogram_head`, releasing the payload they held.
    ///
    /// The dropped samples are measured as they go, which costs one measurement per dropped
    /// histogram in total: the retained ones are never scanned again.
    fn advance_histograms(&mut self) {
        let head = std::mem::take(&mut self.histogram_head);
        if head == 0 {
            return;
        }
        let dropped: usize = self.histograms[..head]
            .iter()
            .map(|(_, histogram)| histogram.heap_bytes())
            .sum();
        self.histogram_bytes = self.histogram_bytes.saturating_sub(dropped);
        self.histograms.drain(..head);
    }

    /// The resets inside the window holding `len` samples from `head`.
    fn resets_in(&self, len: usize) -> &[CounterReset] {
        let start = self.base + self.head as u64;
        resets_between(&self.resets, start, start + len as u64)
    }
}

/// The bytes the histograms a row holds box, beyond the slots they sit in.
///
/// A slot is an `Option<GridSample>`: a pointer and a tag. A result that is a native histogram
/// points at a [`FloatHistogram`] on the heap, which owns its bucket lists in turn, and both stay
/// alive between pushes. Only slots the last evaluated point left in place are counted: a push
/// clears the row.
fn boxed_bytes(row: &[Option<GridSample>]) -> usize {
    row.iter()
        .flatten()
        .map(|sample| match sample {
            GridSample::Histogram(histogram) => {
                std::mem::size_of::<FloatHistogram>() + histogram.heap_bytes()
            }
            GridSample::Float(_) => 0,
        })
        .sum()
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
        if let Some(modifier) = window.modifier {
            check_modifier(input, modifier, function)?;
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
        // A modified window corrects counters over the samples it holds rather than through the
        // reset index, and has no batch form.
        let (kernel, index_resets) = match window.modifier {
            Some(modifier) => (function.modified_kernel(modifier), false),
            None => (
                function.kernel(fold_offset),
                function.indexes_counter_resets(),
            ),
        };
        let mut evaluator =
            Self::with_kernel(input, grid, window, kernel, step_invariant, index_resets)?;
        evaluator.drops_metric_name = function.drops_metric_name();
        evaluator.window_error = window_error;
        evaluator.function = Some(function);
        // An anchored selector replays one window at every point, so its windows do not advance
        // with the grid and a batch would only repeat the same call.
        if matches!(evaluator.replay, Replay::PerPoint) && window.modifier.is_none() {
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

    /// An estimate of the bytes this evaluator keeps between pushes.
    ///
    /// Counted by capacity, so it is what the storage holds rather than what it uses: the retained
    /// samples per field (timestamps, values, the reset index and the native histograms with the
    /// payload each of them owns), the row a push fills with the histograms it boxes, the identity
    /// of the open series and the pending batch's own vectors. Left out are the kernel's scratch
    /// buffers, this struct's `Vec` headers and the grid, which every chunk addressed on it shares.
    ///
    /// The figure is an estimate, not a ledger of every allocation: a histogram's payload is
    /// measured once when it is admitted and released with it, so neither the retained samples nor
    /// the transient allocations of a push are walked again.
    ///
    /// A push is the only caller that also holds the incoming chunk, and it charges that itself for
    /// as long as the call runs. The figure must not be per-chunk either: charging every chunk
    /// consumed would make the charge grow with the chunk count, so a fixed window over a long
    /// series would read as an allocation failure even though the window holds no more samples.
    pub(crate) fn retained_bytes(&self) -> usize {
        let fields: usize = self.fields.iter().map(FieldSamples::bytes).sum();
        let row = self.row.capacity() * std::mem::size_of::<Option<GridSample>>();
        fields
            + row
            + boxed_bytes(&self.row)
            + self.series.as_ref().map_or(0, SeriesState::identity_bytes)
            + self.pending.bytes()
    }

    /// An upper bound of the rows the next push hands out, for input whose last sample is at
    /// `last_ts` (`None` when it holds none) and which closes its series when `last`.
    ///
    /// A push writes at most one row per point it decides, from the series' cursor -- the first
    /// point for a series not started yet. Unless the series closes, it decides no point past the
    /// time the input has reached; and a point holds a row only while its window still reaches the
    /// latest sample, so the points after that one's window are decided empty. A replayed `@`
    /// result fills every grid point from the one window it evaluates first.
    pub(crate) fn output_rows_bound(&self, last_ts: Option<i64>, last: bool) -> usize {
        let spec = self.grid.spec();
        let len = usize::try_from(spec.len).unwrap_or(usize::MAX);
        let state = self.series.as_ref();
        let Some(latest) = last_ts.or_else(|| state.and_then(|state| state.last_ts)) else {
            return 0;
        };
        let cursor = state.map_or(0, |state| state.cursor);
        if !matches!(self.replay, Replay::PerPoint) {
            return if cursor == 0 { len } else { 0 };
        }
        // The grid points whose evaluation time is before `bound`.
        let (start, step) = (i128::from(spec.start_ms), i128::from(spec.step_ms));
        let points_before = |bound: i128| {
            let offset = bound - start;
            if offset <= 0 {
                return 0;
            }
            usize::try_from((offset + step - 1) / step).map_or(len, |points| points.min(len))
        };
        let shifted = i128::from(latest) + i128::from(self.window.offset_ms);
        let before = self
            .window
            .modifier
            .map_or(0, |modifier| i128::from(modifier.before_ms()));
        let mut end = points_before(shifted + i128::from(self.window.range_ms) + before);
        if !last && last_ts.is_some() {
            end = end.min(points_before(shifted + 1));
        }
        end.saturating_sub(cursor)
    }

    /// Whether this evaluator can answer with a native histogram.
    pub(crate) fn yields_histograms(&self) -> bool {
        self.function.is_some_and(RangeFunction::yields_histograms)
    }

    /// The heap bytes of the widest native histogram this evaluator retains, `None` when it
    /// retains none.
    pub(crate) fn widest_retained_histogram(&self) -> Option<usize> {
        self.fields
            .iter()
            .flat_map(|field| &field.histograms)
            .map(|(_, histogram)| histogram.heap_bytes())
            .max()
    }

    /// Schema of the evaluated series: the input fields on the evaluation grid.
    pub fn output_schema(&self) -> &Arc<SeriesSchema> {
        &self.output_schema
    }

    /// Consumes the next chunk of the current series, or starts a new series.
    ///
    /// Returns every position this chunk decided, in one chunk addressed absolutely on the
    /// evaluation grid; `None` when it decided no position holding a value. A decided position
    /// without a result is a row of no chunk, so the positions between two runs of one chunk hold
    /// no value and cost nothing. One push hands out at most one chunk.
    ///
    /// A series always closes exactly once: on the chunk holding its last rows, or on an empty
    /// chunk after it when nothing is left. The evaluator must not be reused after an error.
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
            return self.emit();
        }
        self.close_series()
    }

    /// Evaluates one run of an evaluated series, such as the inner result of a subquery: the
    /// value at each grid point is a sample at that time. Stale markers are kept as values,
    /// since the input is computed.
    ///
    /// `last` says whether this delivery closes its series. A stream hands over the same logical
    /// series more than once -- a prefix now, its closing chunk later -- and the windows of a later
    /// delivery read the samples of the earlier ones, so the state a series retains lives on across
    /// them: only the `last` call ends it. Handing the deliveries over without saying which is last
    /// would close the series early and lose exactly the samples a window across the boundary
    /// needs.
    ///
    /// One delivery may hold several runs of positions, separated by stretches holding no value;
    /// they all reach the caller in one chunk, addressed by the axis. A delivery that holds no
    /// position at all is a legal close when it is `last`, and an error otherwise.
    pub fn push_series(&mut self, series: &Series, last: bool) -> Result<Option<EvaluatedChunk>> {
        let time = &series.time;
        let Some(grid) = time.grid() else {
            return Err(DataFusionError::Execution(
                "evaluated range input must be a grid series".into(),
            ));
        };
        if self.input_grid != Some(grid.spec())
            || series.fields.len() != self.fields.len()
            || series.fields.iter().any(|field| field.len() != time.len())
        {
            return Err(DataFusionError::Execution(
                "evaluated series does not match the input schema".into(),
            ));
        }
        // The input may skip positions: it is ingested by the positions it carries, and the points
        // it skips hold no sample, which is exactly what a hole means.
        let timestamps = time.timestamps().map_err(external)?;
        let last_ts = timestamps.last().copied();
        if !last && last_ts.is_none() {
            // An empty delivery inside a series decides nothing: it is indistinguishable from a
            // stall, so only the closing `is_last` one is a legal close.
            return Err(DataFusionError::Execution(
                "evaluated range input holds no position before its last run".into(),
            ));
        }
        match &mut self.series {
            None => {
                self.output.begin_series();
                self.series = Some(SeriesState {
                    primary_key: None,
                    metric: series.metric.clone(),
                    last_ts,
                    cursor: 0,
                    closing: false,
                    emitted_end: 0,
                });
            }
            Some(state) => {
                // A run continues its series; the earlier runs' samples stay retained for it.
                if state.primary_key.is_some() {
                    return Err(DataFusionError::Execution(
                        "evaluated series pushed inside a raw series".into(),
                    ));
                }
                if state.closing {
                    return Err(DataFusionError::Execution(
                        "evaluated series pushed after it closed".into(),
                    ));
                }
                if state.metric != series.metric {
                    return Err(DataFusionError::Execution(
                        "evaluated series changed identity before its last run".into(),
                    ));
                }
                if let Some(last_ts) = last_ts {
                    if let Some(previous) = state.last_ts
                        && timestamps[0] <= previous
                    {
                        return Err(DataFusionError::Execution(format!(
                            "evaluated series runs are not increasing: {} after {previous}",
                            timestamps[0]
                        )));
                    }
                    state.last_ts = Some(last_ts);
                }
            }
        }
        if let Some(last_ts) = last_ts {
            self.ingest(&timestamps, &series.fields, false)?;
            if last {
                return self.close_series();
            }
            // Later runs only hold samples after `last_ts`, so every point up to it is decided.
            // The samples are shifted onto the evaluation timeline by the selector offset, so the
            // ready point is on that timeline too.
            self.evaluate(Some(
                i128::from(last_ts) + i128::from(self.window.offset_ms),
            ))?;
            return self.emit();
        }
        // A closing chunk holding no position: there is nothing to ingest, but the samples the
        // earlier deliveries left retained still decide the windows that follow them, so the close
        // finishes them rather than ending the series on the spot.
        debug_assert!(last, "an empty chunk that is not last is rejected above");
        self.close_series()
    }

    /// Evaluates the rest of the series of the last input chunk and closes it.
    ///
    /// The rows still buffered are handed out here as one chunk, and the chunk that sees nothing
    /// behind it carries `is_last`.
    fn close_series(&mut self) -> Result<Option<EvaluatedChunk>> {
        self.evaluate(None)?;
        self.series
            .as_mut()
            .expect("every path that closes a series started one")
            .closing = true;
        self.emit()
    }

    /// Hands out the rows buffered so far as one chunk, and closes the series when nothing is left.
    ///
    /// The chunk holds every row decided since the last hand-out, in one go and in one [`Series`]:
    /// several runs of positions may sit inside it, separated by stretches holding no value, and
    /// the axis names them. A position decided to hold none costs no row, so it lies between the
    /// runs rather than inside one. `is_last` closes the series, and a series always closes exactly
    /// once: on the chunk that carries its last rows, or on an empty chunk after it when no row is
    /// left.
    fn emit(&mut self) -> Result<Option<EvaluatedChunk>> {
        let Some(state) = self.series.as_ref() else {
            return Ok(None);
        };
        let closing = state.closing;
        let emitted_end = state.emitted_end;
        let mut metric = state.metric.clone();
        if self.drops_metric_name {
            metric.remove_all(&METADATA_LABELS);
        }
        let Some(series) = self.output.take_buffered(metric)? else {
            return if closing {
                self.empty_chunk(emitted_end)
            } else {
                Ok(None)
            };
        };
        // The builder addresses every position it hands out, so the chunk's bounds come from the
        // axis itself: `first_position` is where it starts, and `last_position` where it ends.
        let start = series
            .time
            .first_position()
            .expect("the builder addresses every row on the grid");
        let end = series.time.last_position().map_or(start, |last| last + 1);
        debug_assert!(start >= emitted_end, "a chunk never goes back");
        if closing {
            // The buffer was drained into this chunk, so nothing of the series is left behind it.
            self.series = None;
            EvaluatedChunk::try_new(series, true).map(Some)
        } else {
            self.series
                .as_mut()
                .expect("checked at the entry of emit")
                .emitted_end = end;
            EvaluatedChunk::try_new(series, false).map(Some)
        }
    }

    /// A closing chunk without values, addressed after the last position handed out.
    fn empty_chunk(&mut self, at: usize) -> Result<Option<EvaluatedChunk>> {
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
        self.series = None;
        EvaluatedChunk::try_new(series, true).map(Some)
    }

    /// Checks that the input did not end inside a series, with rows nobody took.
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
                self.output.begin_series();
                self.series = Some(SeriesState {
                    primary_key: Some(chunk.primary_key.clone()),
                    metric,
                    last_ts: Some(time[time.len() - 1]),
                    cursor: 0,
                    closing: false,
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
                if let Some(last_ts) = state.last_ts
                    && time[0] <= last_ts
                {
                    return Err(DataFusionError::Execution(format!(
                        "raw series timestamps are not increasing across chunks: {} after {last_ts}",
                        time[0]
                    )));
                }
                state.last_ts = Some(time[time.len() - 1]);
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
            // Refused here rather than when a point is evaluated: a smoothed point can be decided
            // before the rest of its window arrives, so whether a later histogram is seen would
            // depend on how the input is chunked.
            if let Some(modifier) = self.window.modifier
                && row
                    .iter()
                    .any(|sample| matches!(sample, SampleRef::Histogram(_)))
                && self.decided_window_reads(shifted)
            {
                return Err(DataFusionError::NotImplemented(format!(
                    "the {} range modifier over native histogram samples",
                    modifier.name()
                )));
            }
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
                        field.histogram_bytes =
                            field.histogram_bytes.saturating_add(h.heap_bytes());
                        field.histograms.push((shifted, h));
                    }
                }
            }
        }
        Ok(())
    }

    /// Whether a sample at `timestamp` on the evaluation timeline lies in the window of a point
    /// this evaluator decides: a window ending at `E` reads `(E - range - before, E + after]`, and
    /// only the first point's window is read when the result is replayed.
    fn decided_window_reads(&self, timestamp: i64) -> bool {
        let (before, after) = self.window.modifier.map_or((0, 0), |modifier| {
            (
                i128::from(modifier.before_ms()),
                i128::from(modifier.after_ms()),
            )
        });
        let spec = self.grid.spec();
        let ends = match self.replay {
            Replay::PerPoint => i128::from(spec.len),
            Replay::Once | Replay::EveryPoint => 1,
        };
        let (start, step) = (i128::from(spec.start_ms), i128::from(spec.step_ms));
        // The window ends that read the sample are those in `[timestamp - after, timestamp +
        // range + before)`; the first grid point at or after the lower bound decides.
        let lowest = i128::from(timestamp) - after;
        let highest = i128::from(timestamp) + i128::from(self.window.range_ms) + before;
        let first = if lowest <= start {
            0
        } else {
            (lowest - start + step - 1) / step
        };
        first < ends && start + first * step < highest
    }

    /// Evaluates grid points up to `ready` on the evaluation timeline, or all remaining points
    /// when `ready` is `None`.
    fn evaluate(&mut self, ready: Option<i128>) -> Result<()> {
        let points_read = self.points_read();
        let state = self.series.as_mut().expect("checked by check_chunk");
        let spec = self.grid.spec();
        let grid = self.grid.timestamps().map_err(external)?;
        // A modifier also reads samples up to the lookback ahead of the window start, so they are
        // kept that much longer.
        let before = self
            .window
            .modifier
            .map_or(0, |modifier| i128::from(modifier.before_ms()));
        // `smoothed` interpolates the window end with the first sample at or after it, which can
        // arrive up to the lookback later. A point is decided once every field holds a sample at
        // or after it, or once the lookback past it has arrived. The input's last row does not say
        // that alone: a stale row is dropped, and a field can hold no value in it.
        let ready = match (ready, self.window.modifier) {
            (Some(ready), Some(RangeModifier::Smoothed { lookback_ms })) => {
                let held = self
                    .fields
                    .iter()
                    .map(|field| {
                        field
                            .timestamps
                            .last()
                            .map_or(i128::MIN, |&t| i128::from(t))
                    })
                    .min()
                    .unwrap_or(i128::MIN);
                Some(held.max(ready - i128::from(lookback_ms)))
            }
            (ready, _) => ready,
        };
        let range = i128::from(self.window.range_ms) + before;
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
                // Skip the points whose windows end before the earliest sample. A smoothed window
                // reads past its end, but without a sample at or before its end it has no result,
                // as Prometheus returns nothing when the first sample is after the end.
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
                modifier: self.window.modifier,
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
        // A point is decided once it has been evaluated, once its window can no longer gain a
        // sample, or -- at the end of the series or of what the input has reached -- once no
        // retained sample reaches it. The last case is what keeps a long empty stretch cheap: the
        // loop stops at the first such point instead of walking the rest of the grid, and none of
        // the points behind it can gain a value, so the cursor skips them.
        if empty_tail && matches!(self.replay, Replay::PerPoint) {
            let mut decided = state.cursor;
            if let Some(ready) = ready {
                while decided < points_read && i128::from(grid[decided]) <= ready {
                    decided += 1;
                }
            } else {
                decided = points_read;
            }
            state.cursor = decided;
        }
        debug_assert!(
            self.pending.is_empty(),
            "`evaluate` flushes the batch before `advance` rewrites the retained arrays a \
             pending span indexes"
        );
        for field in &mut self.fields {
            // After the last window, samples are still ingested so that the overflow and
            // histogram checks do not depend on how the series is chunked, but none are kept.
            // An anchored selector can skip past the one point it reads.
            if state.cursor >= points_read {
                field.head = field.timestamps.len();
                field.histogram_head = field.histograms.len();
            }
            field.advance();
            field.advance_histograms();
        }
        Ok(())
    }

    /// The number of leading grid points whose windows are read; an anchored selector reads
    /// only the first.
    fn points_read(&self) -> usize {
        match self.replay {
            Replay::PerPoint => self.grid.spec().len as usize,
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
    modifier: Option<RangeModifier>,
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
        // A smoothed window also reads the samples after its end.
        let window_end = window_end + self.modifier.map_or(0, RangeModifier::after_ms);
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

/// Rejects a modifier on a function Prometheus does not allow it with, as Prometheus rejects the
/// query (`AnchoredSafeFunctions`, `SmoothedSafeFunctions`), and on evaluated input, which reads
/// no raw samples around its windows.
fn check_modifier(
    input: &SeriesSchema,
    modifier: RangeModifier,
    function: RangeFunction,
) -> Result<()> {
    if input.eval_grid.is_some() {
        return Err(DataFusionError::NotImplemented(format!(
            "the {} range modifier over evaluated series",
            modifier.name()
        )));
    }
    let allowed = match modifier {
        RangeModifier::Anchored { .. } => matches!(
            function,
            RangeFunction::Rate
                | RangeFunction::Increase
                | RangeFunction::Delta
                | RangeFunction::Resets
                | RangeFunction::Changes
        ),
        RangeModifier::Smoothed { .. } => matches!(
            function,
            RangeFunction::Rate | RangeFunction::Increase | RangeFunction::Delta
        ),
    };
    if !allowed {
        let list = match modifier {
            RangeModifier::Anchored { .. } => "changes, delta, increase, rate, resets",
            RangeModifier::Smoothed { .. } => "delta, increase, rate",
        };
        return Err(DataFusionError::Plan(format!(
            "{} modifier can only be used with: {list} - not with {function:?}",
            modifier.name()
        )));
    }
    if modifier.before_ms() < 0 {
        return Err(DataFusionError::Plan(format!(
            "lookback must not be negative, got {} ms",
            modifier.before_ms()
        )));
    }
    Ok(())
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
