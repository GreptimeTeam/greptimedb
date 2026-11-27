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

//! Running a bound tree, node by node.
//!
//! Each execution owns a recursive runtime tree; the compiled [`NativeTree`] stays immutable, so
//! one tree builds independent executions. A caller registers the tree's per-node metric handles
//! into its own metrics set once, before the execution starts.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;

use common_query::promql_annotations::PromqlAnnotationCollector;
use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datatypes::arrow::array::{Array, ArrowPrimitiveType, Float64Array, PrimitiveArray};
use datatypes::arrow::buffer::{Buffer, ScalarBuffer};
use datatypes::series::histogram::HistogramView;
use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
use datatypes::series::samples::{
    DenseSamplesRef, FieldSpec, RowCursor, SampleRef, Samples, Series, SeriesFields, SeriesSchema,
};
use datatypes::series::time::{EvalGrid, GridRange, GridSpan, GridSpec, TimeAxis};
use datatypes::series::{SeriesLabels, SeriesOrdering};

use crate::aggregate;
use crate::grid_series::{
    GridSample, GridSeriesBuilder, fields_bytes, merge_same_labelset, owned, series_bytes,
    table_bytes,
};
use crate::histogram_eval::float_histogram::FloatHistogram;
use crate::histogram_eval::{
    HistogramFunction, HistogramParameters, evaluate_histograms_with_annotations,
    map_native_histogram,
};
use crate::info::{InfoSelector, info_with_reservation};
use crate::instant_eval::{
    info_sample_times, instant_candidate_span, select_instant_at, select_instants,
    timestamp_of_evaluated, timestamp_of_selection,
};
use crate::map::{
    ArithmeticOp, Comparison, ComparisonOp, ParameterizedMath, ScalarOperand, ScalarSeriesOperand,
    drop_metric_name, is_float_series, map_arithmetic_literal, map_arithmetic_series,
    map_compare_literal, map_compare_series, map_math, map_math_with_parameters,
};
use crate::map_time::{self, DateFunction};
use crate::native::aggregate::{AggregateOutput, AggregateRuntime};
use crate::native::batch::EvaluatedChunk;
use crate::native::metrics::NodeMetrics;
use crate::native::plan::{
    BoundInstant, BoundScalar, BoundSetOp, BoundVectorOp, Grouping, InstantValue, MapOp,
    NativeExpr, NativeNode, NativeTree, NativeType, ParameterizedOp, unifies_its_own_contributions,
};
use crate::native::state::{StateChunk, state_bytes};
use crate::range_eval::RangeEvaluator;
use crate::select::{self, SortOrder};
use crate::vector_match::{
    BinaryKind, BinaryMatching, Cardinality, DuplicateEvidence, FillValues, MatchingLabels,
    binary_vectors_with_reservation, set_vectors_with_reservation, validate_binary_operand,
};

#[cfg(test)]
mod tests;

/// A chunk on an internal edge.
enum NativeChunk {
    /// Raw source samples, with their own timestamps.
    Raw(RawSeriesChunk),
    /// Evaluated values, addressed on their absolute grid positions.
    Evaluated(EvaluatedChunk),
    /// Partial aggregate state, addressed on its group's absolute grid positions.
    AggregateState(StateChunk),
}

impl NativeChunk {
    /// The positions this chunk carries: decided grid positions, or raw timestamps.
    fn positions(&self) -> usize {
        match self {
            Self::Raw(chunk) => chunk.time.len(),
            Self::Evaluated(chunk) => chunk.series.time.len(),
            Self::AggregateState(StateChunk::Series { series, .. }) => {
                series.iter().map(|s| s.time.len()).sum()
            }
            Self::AggregateState(StateChunk::Row(row)) => row.time.len(),
        }
    }

    /// The chunk kind, for error messages.
    fn describe(&self) -> &'static str {
        match self {
            Self::Raw(_) => "raw samples",
            Self::Evaluated(_) => "an evaluated chunk",
            Self::AggregateState(_) => "aggregate state",
        }
    }

    /// Whether this chunk closes its source series (not its label set, which two source series
    /// can share).
    fn is_last(&self) -> bool {
        match self {
            Self::Raw(chunk) => chunk.is_last,
            Self::Evaluated(chunk) => chunk.is_last,
            // The producer is done with these groups; a `Final` still closes a group only when
            // its whole input ends.
            Self::AggregateState(_) => true,
        }
    }

    /// An estimate of the bytes this chunk holds; shared buffers and tables are counted in full
    /// for every chunk.
    fn bytes(&self) -> usize {
        match self {
            Self::Raw(chunk) => raw_bytes(chunk),
            Self::Evaluated(chunk) => evaluated_bytes(chunk),
            Self::AggregateState(chunk) => state_bytes(chunk),
        }
    }
}

/// The bytes a raw chunk holds: its identity, its timestamps and the Arrow buffers behind its
/// fields.
pub(crate) fn raw_bytes(chunk: &RawSeriesChunk) -> usize {
    std::mem::size_of::<RawSeriesChunk>()
        .saturating_add(buffer_bytes(&chunk.primary_key))
        .saturating_add(chunk.metric.as_ref().map_or(0, metric_bytes))
        .saturating_add(buffer_bytes(chunk.time.as_buffer().inner()))
        .saturating_add(fields_bytes(&chunk.fields))
}

/// The bytes an evaluated chunk holds: its struct and the Arrow buffers behind its fields, but not
/// the shared grid.
pub(super) fn evaluated_bytes(chunk: &EvaluatedChunk) -> usize {
    std::mem::size_of::<EvaluatedChunk>().saturating_add(fields_bytes(&chunk.series.fields))
}

/// [`series_bytes`] without the sample buffers and without the `Series` struct: its axis, segment
/// descriptors and labels. For a retainer that bills buffers through [`unbilled_sample_buffers`]
/// and counts the struct in its own container.
pub(crate) fn chunk_bytes_beside_samples(series: &Series) -> usize {
    series_bytes(series)
        .saturating_sub(fields_bytes(&series.fields))
        .saturating_sub(std::mem::size_of::<Series>())
}

/// The bytes one chunk retained by a binary operand holds: [`chunk_bytes_beside_samples`] plus the
/// `Series` struct plus the rows its sample slices cover, so a series split into many slices of one
/// buffer is not billed that buffer per chunk. Histogram tables are excluded; the operand bills
/// each once through [`unbilled_tables`].
pub(crate) fn operand_chunk_bytes(series: &Series) -> usize {
    chunk_bytes_beside_samples(series)
        .saturating_add(std::mem::size_of::<Series>())
        .saturating_add(sample_slice_bytes(&series.fields))
}

/// The dense segments of every field of one chunk. A merged field has several, and asking it for a
/// single dense view answers `None`.
pub(crate) fn field_segments(fields: &SeriesFields) -> impl Iterator<Item = &Samples> + '_ {
    fields.iter().flat_map(|field| field.segments())
}

/// The bytes the sample slices of one series cover, by rows: floats per value, histograms per
/// index.
fn sample_slice_bytes(fields: &SeriesFields) -> usize {
    field_segments(fields)
        .map(|segment| match segment.dense() {
            Some(DenseSamplesRef::Float(values)) => {
                values.len().saturating_mul(std::mem::size_of::<f64>())
            }
            Some(DenseSamplesRef::Histogram(histograms)) => histograms
                .indices()
                .len()
                .saturating_mul(std::mem::size_of::<u32>()),
            Some(DenseSamplesRef::Mixed(mixed)) => mixed
                .floats()
                .len()
                .saturating_mul(std::mem::size_of::<f64>())
                .saturating_add(
                    mixed
                        .histograms()
                        .indices()
                        .len()
                        .saturating_mul(std::mem::size_of::<u32>()),
                ),
            // `segments` hands out dense segments only, so a nested chunk never reaches here.
            None => 0,
        })
        .fold(0usize, usize::saturating_add)
}

/// The Arrow allocations (values and validity bitmaps) `series`' samples live in that neither
/// `billed` nor this chunk has already listed, with their bytes: full capacity plus one identity
/// slot each. A retained slice keeps its whole allocation resident, so it is billed in full, once.
///
/// The identity is [`Buffer::data_ptr`], shared by every slice of one allocation; it stays unique
/// while remembered because the retained chunk keeps the allocation alive. The caller adds the
/// returned identities to `billed` after retaining the chunk.
pub(crate) fn unbilled_sample_buffers(
    series: &Series,
    billed: &HashSet<usize>,
) -> (Vec<usize>, usize) {
    let mut fresh: Vec<usize> = Vec::new();
    let mut bytes = 0usize;
    for field in series.fields.iter() {
        for segment in field.segments() {
            match segment.dense() {
                Some(DenseSamplesRef::Float(values)) => {
                    unbilled_array_buffers(values, &mut fresh, billed, &mut bytes)
                }
                Some(DenseSamplesRef::Histogram(histograms)) => {
                    unbilled_array_buffers(histograms.indices(), &mut fresh, billed, &mut bytes)
                }
                Some(DenseSamplesRef::Mixed(mixed)) => {
                    unbilled_array_buffers(mixed.floats(), &mut fresh, billed, &mut bytes);
                    unbilled_array_buffers(
                        mixed.histograms().indices(),
                        &mut fresh,
                        billed,
                        &mut bytes,
                    );
                }
                // `segments` hands out dense segments only, so a nested chunk never reaches here.
                None => {}
            }
        }
    }
    (fresh, bytes)
}

/// [`unbilled_buffer`] for a sample array's values and validity bitmap.
fn unbilled_array_buffers<T: ArrowPrimitiveType>(
    values: &PrimitiveArray<T>,
    fresh: &mut Vec<usize>,
    billed: &HashSet<usize>,
    bytes: &mut usize,
) {
    unbilled_buffer(values.values().inner(), fresh, billed, bytes);
    if let Some(nulls) = values.nulls() {
        unbilled_buffer(nulls.buffer(), fresh, billed, bytes);
    }
}

/// Adds the allocation `buffer` is a slice of to `fresh` and `bytes`, once.
fn unbilled_buffer(
    buffer: &Buffer,
    fresh: &mut Vec<usize>,
    billed: &HashSet<usize>,
    bytes: &mut usize,
) {
    let held = buffer_bytes(buffer);
    if held == 0 {
        return;
    }
    let identity = buffer.data_ptr().as_ptr() as usize;
    if billed.contains(&identity) || fresh.contains(&identity) {
        return;
    }
    fresh.push(identity);
    *bytes = bytes
        .saturating_add(held)
        .saturating_add(std::mem::size_of::<usize>());
}

/// The histogram tables `series` references that neither `billed` nor this chunk has already
/// listed, with their [`table_bytes`] plus one identity slot each. The identity is the `Arc`'s
/// address, unique while the retained chunk keeps the table alive. The caller adds the identities
/// to `billed` after retaining the chunk.
pub(crate) fn unbilled_tables(series: &Series, billed: &HashSet<usize>) -> (Vec<usize>, usize) {
    let mut fresh: Vec<usize> = Vec::new();
    let mut bytes = 0usize;
    for field in series.fields.iter() {
        for segment in field.segments() {
            let table = match segment.dense() {
                Some(DenseSamplesRef::Histogram(histograms)) => histograms.batch(),
                Some(DenseSamplesRef::Mixed(mixed)) => mixed.histograms().batch(),
                _ => continue,
            };
            let identity = Arc::as_ptr(table) as usize;
            if billed.contains(&identity) || fresh.contains(&identity) {
                continue;
            }
            fresh.push(identity);
            bytes = bytes
                .saturating_add(table_bytes(table))
                .saturating_add(std::mem::size_of::<usize>());
        }
    }
    (fresh, bytes)
}

/// A rough workspace estimate for merging fragments that hold `rows` rows between them. Values
/// and histogram tables stay shared with the fragments and are not counted.
pub(crate) fn merge_workspace_bytes(rows: usize, fragments: usize, fields: usize) -> usize {
    let sorted_rows = rows.saturating_mul(
        std::mem::size_of::<(usize, usize, usize)>() + std::mem::size_of::<usize>(),
    );
    let readers = fragments.saturating_mul(
        fields.saturating_mul(std::mem::size_of::<Samples>() + 4 * std::mem::size_of::<usize>()),
    );
    // Run and field descriptors for rows alternating between fragments or gaps.
    let merged_field_per_row =
        2 * std::mem::size_of::<Samples>() + 3 * std::mem::size_of::<usize>();
    let merged_fields = rows
        .saturating_mul(fields)
        .saturating_mul(merged_field_per_row);
    let output = fragments.saturating_mul(
        std::mem::size_of::<Series>() + fields.saturating_mul(std::mem::size_of::<Samples>()),
    );
    sorted_rows
        .saturating_add(readers)
        .saturating_add(merged_fields)
        .saturating_add(output)
}

/// The bytes of the whole allocation `buffer` is a slice of: the capacity is what a slice keeps
/// alive.
fn buffer_bytes(buffer: &Buffer) -> usize {
    buffer.capacity().max(buffer.len())
}

/// Which of an execution's inputs a caller's chunk belongs to: the leaf's position counted left to
/// right. Meaningful only within one tree.
#[derive(Clone, Copy, Debug, PartialEq, Eq, Hash)]
pub struct InputId(usize);

impl InputId {
    /// The input at `index`, counted left to right.
    pub fn new(index: usize) -> Self {
        Self(index)
    }

    /// The position this id names.
    pub fn index(self) -> usize {
        self.0
    }
}

/// The series whose first chunk this buffer received and whose closing chunk it has not seen.
struct OpenSeries {
    metric: SeriesLabels,
    last_position: Option<usize>,
}

/// The input a [`RuntimeOp::Window`] or a [`RuntimeOp::Merge`] collects until its input ends, then
/// merges into one series per label set ([`crate::grid_series::merge_same_labelset`]).
///
/// The per-series contract is checked here because the merge would sort a stream that broke it into
/// a plausible answer.
struct MergeInput {
    schema: Arc<SeriesSchema>,
    /// In arrival order.
    fragments: Vec<Series>,
    /// An estimate of the fragments plus the merge workspace.
    bytes: usize,
    /// The series the next chunk must continue, if the last chunk did not close it.
    open: Option<OpenSeries>,
}

impl MergeInput {
    fn new(schema: Arc<SeriesSchema>) -> Self {
        Self {
            schema,
            fragments: Vec::new(),
            bytes: 0,
            open: None,
        }
    }

    /// Checks one chunk against the series contract and tracks the open series: it must match the
    /// input grid and schema, continue the open series' labels, and move strictly forward. A chunk
    /// with `is_last` closes the series whether or not it holds rows.
    fn check(&mut self, series: &Series, is_last: bool) -> Result<()> {
        let on_grid = match (&self.schema.eval_grid, series.time.grid()) {
            (Some(input), Some(grid)) => input.spec() == grid.spec(),
            _ => false,
        };
        if !on_grid
            || series.fields.len() != self.schema.fields.len()
            || series
                .fields
                .iter()
                .any(|field| field.len() != series.time.len())
        {
            return Err(DataFusionError::Execution(
                "merge input does not match its input schema".to_string(),
            ));
        }
        let (first, last) = (series.time.first_position(), series.time.last_position());
        if !is_last && first.is_none() {
            return Err(DataFusionError::Execution(
                "merge input has no position before the last run of its series".to_string(),
            ));
        }
        if let Some(open) = &self.open
            && open.metric != series.metric
        {
            return Err(DataFusionError::Execution(
                "merge input changed identity before its last run".to_string(),
            ));
        }
        if let Some(previous) = self.open.as_ref().and_then(|open| open.last_position)
            && let Some(first) = first
            && first <= previous
        {
            return Err(DataFusionError::Execution(format!(
                "merge input runs are not increasing: {first} after {previous}"
            )));
        }
        self.open = if is_last {
            None
        } else {
            Some(OpenSeries {
                metric: series.metric.clone(),
                last_position: last,
            })
        };
        Ok(())
    }

    /// The workspace merging the fragments needs; see [`merge_workspace_bytes`].
    fn workspace_bytes(&self) -> usize {
        let rows: usize = self
            .fragments
            .iter()
            .map(|fragment| fragment.time.len())
            .sum();
        merge_workspace_bytes(rows, self.fragments.len(), self.schema.fields.len())
    }
}

/// A node's metric handles and its memory reservation in the query's pool.
pub(super) struct NodeBuffer {
    metrics: Arc<NodeMetrics>,
    reservation: MemoryReservation,
    /// The bytes reserved for this node's grid timestamps, kept across every resize;
    /// `None` until [`NodeBuffer::reserve_grid`] reserves them.
    grid: Option<usize>,
}

impl NodeBuffer {
    /// Registers a reservation with `pool`, labelled `name` (e.g. `native.window at root.left`) in
    /// errors and pool reports.
    pub(super) fn new(
        name: impl Into<String>,
        metrics: Arc<NodeMetrics>,
        pool: &Arc<dyn MemoryPool>,
    ) -> Self {
        let consumer = MemoryConsumer::new(name.into());
        Self {
            metrics,
            reservation: consumer.register(pool),
            grid: None,
        }
    }

    /// Reserves `grid`'s timestamps before this node first reads them, once per node. An
    /// [`EvalGrid`] materializes them lazily and keeps them for the plan's lifetime, so the node
    /// that triggers materialization keeps the reservation; a grid already materialized was charged
    /// by whoever did it. The `grid` record also short-circuits: a collecting window reserves on
    /// every chunk long before it materializes the grid at flush.
    pub(super) fn reserve_grid(&mut self, grid: &EvalGrid) -> Result<()> {
        if self.grid.is_some() || grid.is_materialized() {
            return Ok(());
        }
        let len = usize::try_from(grid.spec().len).map_err(|_| {
            DataFusionError::Execution("grid length does not fit memory size".to_string())
        })?;
        let bytes = len.checked_mul(std::mem::size_of::<i64>()).ok_or_else(|| {
            DataFusionError::Execution("grid timestamp size overflow".to_string())
        })?;
        self.grow(bytes)?;
        self.grid = Some(bytes);
        Ok(())
    }

    /// Reserves `bytes` beside what this node already holds.
    pub(super) fn grow(&mut self, bytes: usize) -> Result<()> {
        self.reservation.try_grow(bytes)?;
        self.record();
        Ok(())
    }

    /// Sets the reservation to `bytes` plus the grid timestamps reserved by [`Self::reserve_grid`].
    pub(super) fn resize(&mut self, bytes: usize) -> Result<()> {
        self.reservation
            .try_resize(bytes.saturating_add(self.grid.unwrap_or(0)))?;
        self.record();
        Ok(())
    }

    /// Gives `bytes` of what this node holds back to the pool.
    fn shrink(&mut self, bytes: usize) -> Result<()> {
        self.reservation.try_shrink(bytes)?;
        self.record();
        Ok(())
    }

    /// Splits `bytes` off into a separate reservation; the pool total is unchanged.
    fn transfer(&mut self, bytes: usize) -> Result<MemoryReservation> {
        // Only an accounting drift gets here; `split` would panic.
        if bytes > self.reservation.size() {
            return Err(DataFusionError::Internal(format!(
                "a queued chunk holds {bytes} bytes, more than the {} its node reserves",
                self.reservation.size()
            )));
        }
        let reservation = self.reservation.split(bytes);
        self.record();
        Ok(reservation)
    }

    /// Reports what this node now reserves.
    fn record(&self) {
        self.metrics.record_reserved_bytes(self.reservation.size());
    }
}

impl Drop for NodeBuffer {
    fn drop(&mut self) {
        self.reservation.free();
        self.record();
    }
}

/// The bytes a metric name holds: its name and each tag's name and value, one row per label.
fn metric_bytes(metric: &SeriesLabels) -> usize {
    metric.name().map_or(0, |name| name.len())
        + std::mem::size_of_val(metric.tags())
        + metric
            .tags()
            .iter()
            .map(|tag| tag.name.len() + tag.value.len())
            .sum::<usize>()
}

/// What a map node applies.
struct MapRuntime {
    ops: Arc<[MapOp]>,
    /// The output schema, which the input is checked against.
    schema: Arc<SeriesSchema>,
}

/// An instant selection. [`select_instants`] takes a complete source axis, so this buffers each
/// series until its last chunk and emits one chunk for it.
struct InstantRuntime {
    selection: BoundInstant,
    /// The input's fields on the selection's output grid.
    schema: Arc<SeriesSchema>,
    output: GridSeriesBuilder,
    /// The chunks of the open series; empty while none is open.
    chunks: Vec<RawSeriesChunk>,
    /// The bytes `chunks` hold.
    held: usize,
}

impl InstantRuntime {
    fn try_new(input: &SeriesSchema, op: &BoundInstant) -> Result<Self> {
        let schema = Arc::new(SeriesSchema {
            fields: Arc::clone(&input.fields),
            eval_grid: Some(Arc::clone(&op.output_grid)),
        });
        Ok(Self {
            selection: op.clone(),
            output: GridSeriesBuilder::try_new(Arc::clone(&schema))?,
            schema,
            chunks: Vec::new(),
            held: 0,
        })
    }

    fn buffered_bytes(&self) -> usize {
        self.held
    }

    /// An upper bound on what gathering the series `closing` ends allocates beside the buffered
    /// chunks: the concatenated axis and segment descriptors, the [`select_instants`] output, and
    /// the builder's rows doubled for `Vec` growth. A histogram field is charged its largest row
    /// per selected point, since every point can select the same row.
    fn gather_bytes(&self, closing: &RawSeriesChunk) -> usize {
        let points = self.selected_points(closing);
        let chunks = self.chunks.len().saturating_add(1);
        let rows = self
            .chunks
            .iter()
            .chain(std::iter::once(closing))
            .map(|chunk| chunk.time.len())
            .fold(0usize, usize::saturating_add);
        let fields = self.schema.fields.len();
        let descriptor = std::mem::size_of::<Samples>();
        let segments = self
            .chunks
            .iter()
            .chain(std::iter::once(closing))
            .flat_map(|chunk| chunk.fields.as_slice())
            .map(|field| field.segments().len())
            .fold(0usize, usize::saturating_add);
        let axis = rows.saturating_mul(std::mem::size_of::<i64>());
        let layout = chunks
            .saturating_mul(fields)
            .saturating_mul(descriptor)
            .saturating_add(segments.saturating_mul(3).saturating_mul(descriptor))
            .saturating_add(fields.saturating_mul(std::mem::size_of::<Vec<Samples>>()));
        let selection =
            points.saturating_mul(std::mem::size_of::<u64>() + std::mem::size_of::<i64>());
        let mut rows_of_points = points.saturating_mul(
            std::mem::size_of::<usize>() + fields.saturating_mul(std::mem::size_of::<f64>() + 1),
        );
        for largest in self.largest_histograms(closing).iter().flatten() {
            rows_of_points = rows_of_points.saturating_add(
                points.saturating_mul(std::mem::size_of::<Option<u32>>().saturating_add(*largest)),
            );
        }
        let row = fields.saturating_mul(
            std::mem::size_of::<Option<GridSample>>() + std::mem::size_of::<RowCursor<'_>>(),
        );
        axis.saturating_add(layout)
            .saturating_add(selection)
            .saturating_add(row)
            .saturating_add(rows_of_points.saturating_mul(2))
    }

    /// The candidate grid points [`select_instants`] can decide for the buffered series, clipped to
    /// the series' span unless an `@` modifier pins the selection.
    fn selected_points(&self, closing: &RawSeriesChunk) -> usize {
        let spec = self.selection.output_grid.spec();
        if self.selection.at_ms.is_some() {
            return usize::try_from(spec.len).unwrap_or(usize::MAX);
        }
        // The selection refuses these itself, before it can decide anything.
        if spec.step_ms <= 0 || self.selection.lookback_ms < 0 {
            return 0;
        }
        let query_end = grid_end_ms(spec);
        if query_end < spec.start_ms {
            return 0;
        }
        let series = || self.chunks.iter().chain(std::iter::once(closing));
        let endpoints = series()
            .find_map(|chunk| chunk.time.as_buffer().first().copied())
            .zip(
                series()
                    .rev()
                    .find_map(|chunk| chunk.time.as_buffer().last().copied()),
            );
        let span = instant_candidate_span(
            endpoints,
            spec.start_ms..=query_end,
            spec.step_ms,
            self.selection.lookback_ms,
            self.selection.offset_ms,
        );
        let points = usize::try_from(span.count).unwrap_or(usize::MAX);
        if self.selection.lookback_ms == 0 {
            // Each point then selects a sample exactly at it, so no more points than samples.
            let rows = series()
                .map(|chunk| chunk.time.len())
                .fold(0usize, usize::saturating_add);
            return points.min(rows);
        }
        points
    }

    /// The largest histogram row of each field, `None` for a float field.
    fn largest_histograms(&self, closing: &RawSeriesChunk) -> Vec<Option<usize>> {
        let mut largest = vec![None; self.schema.fields.len()];
        for chunk in self.chunks.iter().chain(std::iter::once(closing)) {
            for (slot, field) in largest.iter_mut().zip(chunk.fields.as_slice()) {
                if let Some(bytes) = histogram_row_bytes(field) {
                    *slot = Some(slot.unwrap_or(0).max(bytes));
                }
            }
        }
        largest
    }

    /// Buffers one chunk of the open series and gathers it on its last chunk; `None` while the
    /// series is open.
    fn push(&mut self, chunk: RawSeriesChunk) -> Result<Option<EvaluatedChunk>> {
        if let Some(open) = self.chunks.first()
            && (open.primary_key.as_slice() != chunk.primary_key.as_slice()
                || open.metric != chunk.metric)
        {
            return Err(series_changed());
        }
        let closing = chunk.is_last;
        self.held = self.held.saturating_add(raw_bytes(&chunk));
        self.chunks.push(chunk);
        if !closing {
            return Ok(None);
        }
        self.gather().map(Some)
    }

    /// Selects the instants of the buffered series and frees the buffer.
    fn gather(&mut self) -> Result<EvaluatedChunk> {
        let chunk = self.select()?;
        self.chunks.clear();
        self.chunks.shrink_to_fit();
        self.held = 0;
        Ok(chunk)
    }

    /// The last sample at or before each grid point of the buffered series, as one chunk.
    fn select(&mut self) -> Result<EvaluatedChunk> {
        let Some(first) = self.chunks.first() else {
            return Err(DataFusionError::Execution(
                "the instant selection holds no series to select".to_string(),
            ));
        };
        let metric = first.metric.clone().ok_or_else(no_labels)?;
        let time = buffered_time(&self.chunks)?;
        let fields = buffered_fields(&self.chunks, self.schema.fields.len())?;
        let grid = Arc::clone(&self.selection.output_grid);
        let spec = grid.spec();
        if let Some(at_ms) = self.selection.at_ms {
            let timestamp_window = if self.selection.value == InstantValue::Timestamp
                && self.selection.lookback_ms >= 0
            {
                let anchor = at_ms as i128;
                let shifted = anchor - self.selection.offset_ms as i128;
                let lower = anchor.max(shifted) - self.selection.lookback_ms as i128;
                let upper = anchor.min(shifted);
                if lower > upper || (lower == upper && self.selection.lookback_ms != 0) {
                    None
                } else {
                    let effective_lookback = i64::try_from(upper - lower).ok();
                    effective_lookback.and_then(|lookback| {
                        i64::try_from(upper).ok().map(|upper| (upper, lookback))
                    })
                }
            } else {
                Some((at_ms, self.selection.lookback_ms))
            };
            let Some((selection_at, selection_lookback)) = timestamp_window else {
                let metric = drop_metric_name(&metric)?;
                return no_row_chunk(&self.schema, metric);
            };
            let Some(index) = select_instant_at(
                &time,
                &fields,
                selection_at,
                selection_lookback,
                if self.selection.value == InstantValue::Timestamp {
                    0
                } else {
                    self.selection.offset_ms
                },
            )?
            else {
                return no_row_chunk(&self.schema, self.output_metric(&metric)?);
            };
            let grid_len = usize::try_from(spec.len).unwrap_or(usize::MAX);
            if self.selection.value != InstantValue::Sample {
                let indices = vec![index; grid_len];
                let timestamps = grid
                    .timestamps()
                    .map_err(crate::grid_series::external)?
                    .to_vec();
                return self.project(&metric, &time, &fields, &indices, &timestamps);
            }
            self.output.begin_series();
            let mut cursors: Vec<RowCursor<'_>> = fields.iter().map(Samples::cursor).collect();
            let mut row = Vec::with_capacity(fields.len());
            for cursor in &mut cursors {
                row.push(owned(
                    cursor
                        .at(index as usize)
                        .ok_or_else(|| missing_row(index as usize))?,
                )?);
            }
            for position in 0..grid_len {
                self.output.push_samples(position, &row)?;
            }
            return match self.output.finish(metric.clone())? {
                Some(series) => EvaluatedChunk::try_new(series, true),
                None => no_row_chunk(&self.schema, metric),
            };
        }
        let (indices, timestamps) = select_instants(
            &time,
            &fields,
            spec.start_ms..=grid_end_ms(spec),
            spec.step_ms,
            self.selection.lookback_ms,
            self.selection.offset_ms,
        )?;
        if self.selection.value != InstantValue::Sample {
            return self.project(&metric, &time, &fields, &indices, &timestamps);
        }
        self.output.begin_series();
        let mut cursors: Vec<RowCursor<'_>> = fields.iter().map(Samples::cursor).collect();
        // `GridSample` is not `Clone`, so the row is filled slot by slot rather than from one
        // value.
        let mut row: Vec<Option<GridSample>> = (0..fields.len()).map(|_| None).collect();
        let mut read: Option<usize> = None;
        for (index, timestamp) in indices.iter().zip(&timestamps) {
            let index = *index as usize;
            let position = grid_position(&grid, *timestamp)?;
            // Consecutive grid points can select the same source row.
            if read != Some(index) {
                for (slot, cursor) in row.iter_mut().zip(&mut cursors) {
                    let sample = cursor.at(index).ok_or_else(|| missing_row(index))?;
                    *slot = owned(sample)?;
                }
                read = Some(index);
            }
            // The builder drops a position where every field is empty.
            self.output.push_samples(position, &row)?;
        }
        match self.output.finish(metric.clone())? {
            Some(series) => EvaluatedChunk::try_new(series, true),
            None => no_row_chunk(&self.schema, metric),
        }
    }

    /// The labels this selection reports a series under: `timestamp(x)` drops the metric name.
    fn output_metric(&self, metric: &SeriesLabels) -> Result<SeriesLabels> {
        match self.selection.value {
            InstantValue::Timestamp => drop_metric_name(metric),
            InstantValue::Sample | InstantValue::InfoTimestamp => Ok(metric.clone()),
        }
    }

    /// The selected sample times, for `timestamp()` and `info` selections.
    fn project(
        &self,
        metric: &SeriesLabels,
        time: &RawTimestamps,
        fields: &SeriesFields,
        indices: &[u64],
        timestamps: &[i64],
    ) -> Result<EvaluatedChunk> {
        let projected = match self.selection.value {
            InstantValue::Timestamp => {
                timestamp_of_selection(&self.schema, metric, time, fields, indices, timestamps)?
            }
            InstantValue::InfoTimestamp => {
                info_sample_times(&self.schema, metric, time, fields, indices, timestamps)?
            }
            InstantValue::Sample => {
                return Err(DataFusionError::Internal(
                    "an instant selection of samples has no sample times to project".to_string(),
                ));
            }
        };
        match projected {
            Some(series) => EvaluatedChunk::try_new(series, true),
            None => no_row_chunk(&self.schema, self.output_metric(metric)?),
        }
    }
}

/// An input a node reads whole before it computes (a binary operand, or the input of a vector
/// operation). Chunks are assembled into one series per logical series once the input ends.
#[derive(Default)]
struct RetainedInput {
    /// The open series and the position after its last row. A label set may close and start again
    /// (as two logical series); starting another series while one is open is an error.
    open: Option<(SeriesLabels, usize)>,
    /// In arrival order.
    series: Vec<Series>,
    /// The exclusive chunk index at which each logical series closed.
    ends: Vec<usize>,
    /// Addresses of histogram tables already billed; see [`unbilled_tables`].
    tables: HashSet<usize>,
    /// The bytes `series`, `ends` and the billed tables hold.
    held: usize,
}

impl RetainedInput {
    /// The bytes `chunk` would add; the same figure [`Self::enqueue`] adds, so it can be reserved
    /// first.
    fn pending(&self, chunk: &EvaluatedChunk) -> usize {
        let end = if chunk.is_last {
            std::mem::size_of::<usize>()
        } else {
            0
        };
        let (_, tables) = unbilled_tables(&chunk.series, &self.tables);
        tables
            .saturating_add(operand_chunk_bytes(&chunk.series))
            .saturating_add(end)
    }

    /// Retains one chunk, refusing one that starts another series while one is open or goes back
    /// inside the open one.
    fn retain(&mut self, chunk: EvaluatedChunk, reader: Reader) -> Result<()> {
        let (first, last) = (
            chunk.series.time.first_position(),
            chunk.series.time.last_position(),
        );
        let metric = chunk.series.metric.clone();
        let end = self.open.as_ref().map_or(0, |(_, end)| *end);
        if let Some((open, _)) = &self.open
            && *open != metric
        {
            let (owner, input) = (reader.owner(), reader.input());
            return Err(DataFusionError::Execution(format!(
                "{owner} reads whole series, and {input} handed over {metric:?} before {open:?} \
                 ended"
            )));
        }
        if let Some(first) = first
            && first < end
        {
            let (owner, input) = (reader.owner(), reader.input());
            return Err(DataFusionError::Execution(format!(
                "{owner} was handed chunks of {metric:?} by {input} that do not continue each \
                 other: one starts at position {first}, after the series already reached {end}"
            )));
        }
        self.open = (!chunk.is_last).then(|| (metric, last.map_or(end, |last| last + 1)));
        self.enqueue(chunk);
        Ok(())
    }

    /// Refuses an input that ended with a series still open.
    fn ensure_closed(&self, reader: Reader) -> Result<()> {
        match &self.open {
            Some((metric, _)) => Err(DataFusionError::Execution(format!(
                "the input of {} ended before the last chunk of a series: {} handed over \
                 {metric:?} without the chunk that closes it",
                reader.owner(),
                reader.input()
            ))),
            None => Ok(()),
        }
    }

    /// Retains one chunk and adds the [`Self::pending`] figure to `held`.
    fn enqueue(&mut self, chunk: EvaluatedChunk) {
        let closing = chunk.is_last;
        let (tables, bytes) = unbilled_tables(&chunk.series, &self.tables);
        self.held = self
            .held
            .saturating_add(if closing {
                std::mem::size_of::<usize>()
            } else {
                0
            })
            .saturating_add(operand_chunk_bytes(&chunk.series))
            .saturating_add(bytes);
        self.tables.extend(tables);
        self.series.push(chunk.series);
        if closing {
            self.ends.push(self.series.len());
        }
    }

    /// The chunk ranges of the logical series this operand closed, in arrival order.
    fn boundaries(&self) -> impl Iterator<Item = std::ops::Range<usize>> + '_ {
        self.ends.iter().scan(0, |start, &end| {
            let range = *start..end;
            *start = end;
            Some(range)
        })
    }

    /// The metadata assembling this operand's chunks into logical series allocates; samples are
    /// shared, not copied. Per series: the entry and field list; per chunk and field: one
    /// descriptor; per segment: three descriptors (flatten copy, `Arc<[Samples]>` slot, vector
    /// doubling); per run: the collected span plus [`TimeAxis::from_grid_spans`]'s copies.
    fn assembly_bytes(&self, schema: &SeriesSchema) -> Option<usize> {
        let samples = std::mem::size_of::<Samples>();
        let grids = std::mem::size_of::<GridSpan>();
        let width = schema.fields.len();
        let mut bytes = 0usize;
        for boundary in self.boundaries() {
            let chunks = &self.series[boundary];
            let mut runs = 0usize;
            let mut segments = 0usize;
            for chunk in chunks {
                runs = runs.checked_add(chunk.time.runs().count())?;
                for field in chunk.fields.iter() {
                    segments = segments.checked_add(field.segments().len())?;
                }
            }
            // `+ width`: a field with no segment still builds one empty `Samples`.
            let descriptors = chunks
                .len()
                .checked_mul(width)?
                .checked_add(segments.checked_mul(3)?.checked_add(width)?)?;
            let axis = runs.checked_mul(
                grids
                    .checked_mul(2)?
                    .checked_add(std::mem::size_of::<usize>())?,
            )?;
            bytes = bytes
                .checked_add(std::mem::size_of::<Series>())?
                .checked_add(width.checked_mul(samples)?)?
                .checked_add(descriptors.checked_mul(samples)?)?
                .checked_add(axis)?;
        }
        Some(bytes)
    }

    /// The logical series this operand closed, each as one series on `schema`'s grid. Fields are
    /// chained segment descriptors sharing the retained buffers; reserve [`Self::assembly_bytes`]
    /// first.
    fn assemble(&self, schema: &SeriesSchema, reader: Reader) -> Result<Vec<Series>> {
        let grid = schema.eval_grid.clone().ok_or_else(|| {
            let (owner, input) = (reader.owner(), reader.input());
            DataFusionError::Execution(format!(
                "{owner} assembles {input} on an evaluation grid, and its schema names none"
            ))
        })?;
        let mut assembled = Vec::with_capacity(self.ends.len());
        for boundary in self.boundaries() {
            let chunks = &self.series[boundary];
            let metric = chunks
                .first()
                .map(|chunk| chunk.metric.clone())
                .ok_or_else(|| {
                    let (owner, input) = (reader.owner(), reader.input());
                    DataFusionError::Execution(format!(
                        "{owner} closed a logical series of {input} without a chunk to assemble"
                    ))
                })?;
            let runs = chunks
                .iter()
                .map(|chunk| chunk.time.runs().count())
                .fold(0usize, usize::saturating_add);
            let mut spans = Vec::with_capacity(runs);
            for chunk in chunks {
                for run in chunk.time.runs() {
                    spans.push(GridSpan::new(run.position, run.len));
                }
            }
            let mut fields = Vec::with_capacity(schema.fields.len());
            for index in 0..schema.fields.len() {
                let mut segments = Vec::with_capacity(chunks.len());
                for chunk in chunks {
                    let field = chunk.fields.get(index).ok_or_else(|| {
                        let (owner, input) = (reader.owner(), reader.input());
                        DataFusionError::Execution(format!(
                            "{owner} cannot assemble {input}: a chunk of it carries {} fields, not \
                             {}",
                            chunk.fields.len(),
                            schema.fields.len()
                        ))
                    })?;
                    segments.push(field.clone());
                }
                fields.push(Samples::try_from_chunks(segments).map_err(series_error)?);
            }
            let fields = SeriesFields::try_from_vec(fields).map_err(series_error)?;
            let time = TimeAxis::from_grid_spans(grid.clone(), spans).map_err(series_error)?;
            assembled.push(Series::try_new(schema, metric, time, fields).map_err(series_error)?);
        }
        Ok(assembled)
    }

    /// Drops the retained chunks, giving back what they hold.
    fn release(&mut self) {
        self.open = None;
        self.series.clear();
        self.series.shrink_to_fit();
        self.ends.clear();
        self.ends.shrink_to_fit();
        self.tables.clear();
        self.tables.shrink_to_fit();
        self.held = 0;
    }
}

/// A binary, set or `info` node: retains both operands until they end, then assembles their logical
/// series and runs the kernel once.
struct BinaryRuntime {
    node_index: usize,
    /// Evidence shared with the other output partitions of this fused-series execution.
    evidence: Option<Arc<DuplicateEvidence>>,
    /// The query's collector for dropped histogram sample pairs, when requested.
    annotations: Option<PromqlAnnotationCollector>,
    kernel: PairKernel,
    /// Left then right; each chunk is checked against its operand's schema.
    schemas: [Arc<SeriesSchema>; 2],
    operands: [RetainedInput; 2],
    /// Whether the kernel has run; it runs once, since it drops the operands.
    computed: bool,
}

/// The kernel a [`BinaryRuntime`] runs over its two assembled operands.
enum PairKernel {
    /// An arithmetic or comparison operation between matched series.
    Binary(Arc<BinaryMatching>),
    /// `and`, `or` or `unless`, which keeps samples of either side as they are.
    Set(BoundSetOp),
    /// `info`, whose left operand is the base and right operand the info series' sample times.
    Info(InfoSelector),
}

impl PairKernel {
    /// The node name used in errors.
    fn node(&self) -> &'static str {
        match self {
            Self::Binary(_) => "the binary node",
            Self::Set(_) => "the set node",
            Self::Info(_) => "the info node",
        }
    }
}

fn side_name(index: usize) -> &'static str {
    match index {
        0 => "left",
        _ => "right",
    }
}

/// The owner of a [`RetainedInput`], named in its errors.
#[derive(Clone, Copy)]
enum Reader {
    Operand { side: usize, node: &'static str },
    Vector,
}

impl Reader {
    fn owner(self) -> String {
        match self {
            Self::Operand { node, .. } => node.to_string(),
            Self::Vector => "the vector operation".to_string(),
        }
    }

    fn input(self) -> String {
        match self {
            Self::Operand { side, .. } => format!("its {} operand", side_name(side)),
            Self::Vector => "its input".to_string(),
        }
    }
}

impl BinaryRuntime {
    /// This node's evidence slot for a field.
    fn slot(&self, field: usize) -> (usize, usize) {
        (self.node_index, field)
    }

    fn new(
        kernel: PairKernel,
        schemas: [Arc<SeriesSchema>; 2],
        node_index: usize,
        evidence: Option<Arc<DuplicateEvidence>>,
    ) -> Self {
        Self {
            node_index,
            evidence,
            annotations: None,
            kernel,
            schemas,
            operands: [RetainedInput::default(), RetainedInput::default()],
            computed: false,
        }
    }

    fn reader(&self, side: usize) -> Reader {
        Reader::Operand {
            side,
            node: self.kernel.node(),
        }
    }

    fn retained_bytes(&self) -> usize {
        self.operands
            .iter()
            .map(|operand| operand.held)
            .fold(0usize, usize::saturating_add)
    }

    fn assembly_bytes(&self) -> Option<usize> {
        let [left, right] = &self.operands;
        let [left_schema, right_schema] = &self.schemas;
        left.assembly_bytes(left_schema)?
            .checked_add(right.assembly_bytes(right_schema)?)
    }

    fn assemble(&self) -> Result<[Vec<Series>; 2]> {
        let [left, right] = &self.operands;
        let [left_schema, right_schema] = &self.schemas;
        Ok([
            left.assemble(left_schema, self.reader(0))?,
            right.assemble(right_schema, self.reader(1))?,
        ])
    }

    /// The bytes the chunk of `side` would add to that operand; see [`RetainedInput::pending`].
    fn pending(&self, side: usize, chunk: &EvaluatedChunk) -> Result<usize> {
        self.operands
            .get(side)
            .map(|operand| operand.pending(chunk))
            .ok_or_else(invalid_binary_operand)
    }

    /// Refuses an operand that ended with a series still open.
    fn closed(&self) -> Result<()> {
        for (side, operand) in self.operands.iter().enumerate() {
            operand.ensure_closed(self.reader(side))?;
        }
        Ok(())
    }

    /// Retains one chunk of the operand at `side`, after the kernel's own operand check. The bound
    /// tree is no proof of what a chunk carries: `Series` fields are public, so a struct-literal
    /// chunk can carry another field count or fewer samples than its axis places rows.
    fn retain(&mut self, side: usize, chunk: EvaluatedChunk) -> Result<()> {
        let Some(schema) = self.schemas.get(side) else {
            return Err(invalid_binary_operand());
        };
        validate_binary_operand(schema.as_ref(), std::slice::from_ref(&chunk.series)).map_err(
            |error| {
                DataFusionError::Execution(format!(
                    "{} rejected its {} operand: {error}",
                    self.kernel.node(),
                    side_name(side)
                ))
            },
        )?;
        let reader = self.reader(side);
        let Some(operand) = self.operands.get_mut(side) else {
            return Err(invalid_binary_operand());
        };
        operand.retain(chunk, reader)
    }

    /// Runs the kernel over both assembled operands, or returns `None` if it already ran.
    ///
    /// Chunks are assembled into logical series first; otherwise every chunk would be a separate
    /// match candidate. The reservation covers retained chunks, assembly and the kernel's reported
    /// workspace, then the output until the caller queues it.
    fn compute(&mut self, buffer: &mut NodeBuffer) -> Result<Option<Vec<Series>>> {
        if self.computed {
            return Ok(None);
        }
        self.closed()?;
        let [left_schema, right_schema] = &self.schemas;
        let retained = self.retained_bytes();
        let node = self.kernel.node();
        let assembly = self
            .assembly_bytes()
            .ok_or_else(|| reservation_overflow(node))?;
        buffer.grow(assembly)?;
        let assembled = match self.assemble() {
            Ok(assembled) => assembled,
            Err(error) => {
                buffer.resize(retained)?;
                return Err(error);
            }
        };
        let [left, right] = &assembled;
        let metrics = Arc::clone(&buffer.metrics);
        let output = {
            let _timer = metrics.timer();
            let mut reserve = |workspace: usize| {
                let bytes = retained
                    .checked_add(assembly)
                    .and_then(|held| held.checked_add(workspace))
                    .ok_or_else(|| reservation_overflow(node))?;
                buffer.resize(bytes)
            };
            match &self.kernel {
                PairKernel::Binary(matching) => {
                    let evidence = self.evidence.as_deref();
                    binary_vectors_with_reservation(
                        matching,
                        left_schema,
                        left,
                        right_schema,
                        right,
                        evidence.map(|evidence| crate::vector_match::NodeEvidence {
                            node: self.slot(0).0,
                            evidence,
                        }),
                        self.annotations.as_ref(),
                        &mut reserve,
                    )
                }
                PairKernel::Set(set) => set_vectors_with_reservation(
                    set.op,
                    &set.labels,
                    left_schema,
                    left,
                    right_schema,
                    right,
                    &mut reserve,
                ),
                PairKernel::Info(selector) => {
                    info_with_reservation(left_schema, left, right, selector, &mut reserve)
                }
            }
        };
        let output = output?;
        drop(assembled);
        for operand in &mut self.operands {
            operand.release();
        }
        self.computed = true;
        Ok(Some(output))
    }
}

/// A vector operation (`topk`, `limitk`, ...): its answer at a point depends on every series there,
/// so it retains the whole input and runs the kernel once.
struct VectorRuntime {
    op: BoundVectorOp,
    /// Shared by input and output.
    schema: Arc<SeriesSchema>,
    input: RetainedInput,
    /// Whether pieces of one label set are merged before the kernel; `limitk` needs it when its
    /// input can split a label set.
    merge_input: bool,
    computed: bool,
    annotations: Option<PromqlAnnotationCollector>,
    /// The per-point k of a parameterized topk/bottomk, in grid position order.
    parameter: Option<Vec<Option<f64>>>,
    /// The bytes charged for the parameter values.
    parameter_bytes: usize,
}

impl VectorRuntime {
    /// Validates the operation's grouping against `schema`.
    fn try_new(op: BoundVectorOp, schema: Arc<SeriesSchema>) -> Result<Self> {
        // These operations group by labels only; a `by` naming a value field would wrongly merge
        // groups.
        let bound_grouping = match &op {
            BoundVectorOp::TopK { group, .. }
            | BoundVectorOp::LimitK { group, .. }
            | BoundVectorOp::CountValues { group, .. } => Some(group),
            _ => None,
        };
        if let Some(group) = bound_grouping {
            let names: Vec<&str> = match group {
                Grouping::By(names) | Grouping::Without(names) => {
                    names.iter().map(AsRef::as_ref).collect()
                }
            };
            let grouping = match group {
                Grouping::By(_) => aggregate::Grouping::By(&names),
                Grouping::Without(_) => aggregate::Grouping::Without(&names),
            };
            match &op {
                BoundVectorOp::CountValues { label, .. } => {
                    aggregate::validate_count_values(&schema, label, grouping)?
                }
                _ => aggregate::validate_vector_grouping(&schema, grouping)?,
            }
        }
        Ok(Self {
            op,
            schema,
            input: RetainedInput::default(),
            merge_input: false,
            computed: false,
            annotations: None,
            parameter: None,
            parameter_bytes: 0,
        })
    }

    /// Runs the kernel over the whole input, or returns `None` if it already ran. The kernels do
    /// not report allocations, so their workspace is estimated by [`vector_workspace_bytes`].
    fn compute(&mut self, buffer: &mut NodeBuffer) -> Result<Option<Vec<Series>>> {
        self.compute_with_histogram_parameters(buffer, &[])
    }

    fn compute_with_histogram_parameters(
        &mut self,
        buffer: &mut NodeBuffer,
        parameters: &[&[Option<f64>]],
    ) -> Result<Option<Vec<Series>>> {
        if self.computed {
            return Ok(None);
        }
        self.input.ensure_closed(Reader::Vector)?;
        let retained = self.input.held.saturating_add(self.parameter_bytes);
        let assembly = self
            .input
            .assembly_bytes(&self.schema)
            .ok_or_else(vector_overflow)?;
        buffer.grow(assembly)?;
        let assembled = match self.input.assemble(&self.schema, Reader::Vector) {
            Ok(assembled) => assembled,
            Err(error) => {
                buffer.resize(retained)?;
                return Err(error);
            }
        };
        // `limitk` keeps series in input order, as Prometheus does; pieces of one label set are one
        // series there, so they are merged first.
        let assembled = if self.merge_input {
            let rows = assembled.iter().map(|series| series.time.len()).sum();
            buffer.grow(merge_workspace_bytes(
                rows,
                assembled.len(),
                self.schema.fields.len(),
            ))?;
            merge_same_labelset(&self.schema, assembled)?
        } else {
            assembled
        };
        if !parameters.is_empty() && !matches!(self.op, BoundVectorOp::Histogram(_)) {
            return Err(DataFusionError::Internal(
                "histogram parameters supplied to non-histogram op".into(),
            ));
        }
        let assembled = if !parameters.is_empty() {
            let rows = assembled.iter().map(|series| series.time.len()).sum();
            buffer.grow(merge_workspace_bytes(
                rows,
                assembled.len(),
                self.schema.fields.len(),
            ))?;
            merge_same_labelset(&self.schema, assembled)?
        } else {
            assembled
        };
        if !parameters.is_empty() {
            let grid = self
                .schema
                .eval_grid
                .as_ref()
                .ok_or_else(|| DataFusionError::Internal("histogram grid missing".into()))?;
            if parameters
                .iter()
                .any(|values| values.len() != grid.spec().len as usize)
            {
                return Err(DataFusionError::Internal(
                    "histogram parameter length differs from its grid".into(),
                ));
            }
        }
        if self.parameter.is_some() && !matches!(self.op, BoundVectorOp::TopK { .. }) {
            return Err(DataFusionError::Internal(
                "only topk accepts a per-point k".into(),
            ));
        }
        if let Some(values) = self.parameter.as_ref() {
            let grid = self
                .schema
                .eval_grid
                .as_ref()
                .ok_or_else(|| DataFusionError::Internal("topk grid missing".into()))?;
            if values.len() != grid.spec().len as usize {
                return Err(DataFusionError::Internal(
                    "topk parameter length differs from its grid".into(),
                ));
            }
        }
        buffer.grow(vector_workspace_bytes(&self.op, &self.schema, &assembled))?;
        let metrics = Arc::clone(&buffer.metrics);
        let output = {
            let _timer = metrics.timer();
            if parameters.is_empty() {
                vector_series_with_k(
                    &self.op,
                    &self.schema,
                    &assembled,
                    self.parameter.as_deref(),
                    self.annotations.as_ref(),
                )?
            } else {
                let function = match self.op {
                    BoundVectorOp::Histogram(function) => function,
                    _ => {
                        return Err(DataFusionError::Internal(
                            "parameterized histogram op mismatch".into(),
                        ));
                    }
                };
                let histogram_parameters = match function {
                    HistogramFunction::Quantile(_) => HistogramParameters {
                        quantile: Some(parameters[0]),
                        lower: None,
                        upper: None,
                    },
                    HistogramFunction::Fraction { .. } => HistogramParameters {
                        quantile: None,
                        lower: Some(parameters[0]),
                        upper: Some(parameters[1]),
                    },
                };
                evaluate_histograms_with_annotations(
                    function,
                    &self.schema,
                    &assembled,
                    Some(histogram_parameters),
                    self.annotations.as_ref(),
                )?
            }
        };
        self.input.release();
        self.computed = true;
        Ok(Some(output))
    }
}

/// The error for a vector operation whose reservation does not fit an address space.
fn vector_overflow() -> DataFusionError {
    DataFusionError::Execution(
        "the input of the vector operation does not fit one address space: the bytes it would \
         reserve overflow"
            .to_string(),
    )
}

fn vector_series_with_k(
    op: &BoundVectorOp,
    schema: &Arc<SeriesSchema>,
    series: &[Series],
    values: Option<&[Option<f64>]>,
    collector: Option<&PromqlAnnotationCollector>,
) -> Result<Vec<Series>> {
    let grouping = |group: &Grouping| match group {
        Grouping::By(labels) => select::Grouping::By(labels.to_vec()),
        Grouping::Without(labels) => select::Grouping::Without(labels.to_vec()),
    };
    match op {
        BoundVectorOp::TopK { k, top, group } => {
            if let Some(values) = values {
                let grid = schema.eval_grid.as_ref().ok_or_else(|| {
                    DataFusionError::Internal("topk output has no evaluation grid".into())
                })?;
                if values.len() != grid.spec().len as usize {
                    return Err(DataFusionError::Internal(
                        "topk parameter length differs from its grid".into(),
                    ));
                }
                select::topk_at(
                    // No sample selects nothing, as Prometheus does for an empty parameter; a NaN
                    // sample is still an error (`select::validate_topk_parameters`).
                    |point| match values[point] {
                        Some(k) => select::k_param(k, series.len()),
                        None => Ok(None),
                    },
                    *top,
                    &grouping(group),
                    schema,
                    series,
                )
            } else {
                select::topk(*k, *top, &grouping(group), schema, series)
            }
        }
        BoundVectorOp::LimitK { k, group } => select::limitk(*k, &grouping(group), schema, series),
        BoundVectorOp::LimitRatio { ratio } => select::limit_ratio(*ratio, schema, series),
        BoundVectorOp::Sort(order) => select::sort_instant(order, schema, series.to_vec()),
        // The kernel takes one native histogram series per label set, so pieces of one label set
        // are merged first.
        BoundVectorOp::Histogram(function) => evaluate_histograms_with_annotations(
            *function,
            schema,
            &merge_same_labelset(schema, series.to_vec())?,
            None,
            collector,
        ),
        BoundVectorOp::CountValues { label, group } => {
            let (by, labels) = match group {
                Grouping::By(labels) => (true, labels),
                Grouping::Without(labels) => (false, labels),
            };
            let labels: Vec<&str> = labels.iter().map(|label| &**label).collect();
            let grouping = if by {
                aggregate::Grouping::By(&labels)
            } else {
                aggregate::Grouping::Without(&labels)
            };
            aggregate::count_values(schema, series, label, grouping)
        }
        BoundVectorOp::Absent { labels } => Ok(select::absent(schema, series, labels.clone())?
            .into_iter()
            .collect()),
    }
}

/// A rough estimate of what the vector operation `op` allocates over `series`, beside the input it
/// reads: ranking candidates and copied histograms, sort keys and float rebuilds, `absent`'s
/// per-point flags, `count_values`' per-sample groups, and the histogram functions' merge and
/// per-row buckets.
fn vector_workspace_bytes(op: &BoundVectorOp, schema: &SeriesSchema, series: &[Series]) -> usize {
    let fields = schema.fields.len();
    match op {
        BoundVectorOp::Sort(order) => {
            let rebuilds = matches!(order, SortOrder::Asc | SortOrder::Desc);
            return series.iter().fold(0usize, |total, item| {
                let floats = if rebuilds
                    && item
                        .fields
                        .iter()
                        .any(|field| histogram_row_bytes(field).is_some())
                {
                    item.time.len().saturating_mul(std::mem::size_of::<f64>())
                } else {
                    0
                };
                total
                    .saturating_add(std::mem::size_of::<(f64, Series)>())
                    .saturating_add(floats)
            });
        }
        BoundVectorOp::Absent { .. } => {
            let points = schema.eval_grid.as_ref().map_or(0, |grid| {
                usize::try_from(grid.spec().len).unwrap_or(usize::MAX)
            });
            return points.saturating_mul(fields.saturating_mul(1 + std::mem::size_of::<f64>()));
        }
        BoundVectorOp::Histogram(_) => {
            let rows: usize = series.iter().map(|item| item.time.len()).sum();
            let label = std::mem::size_of::<(Arc<str>, Arc<str>)>();
            let row = std::mem::size_of::<(f64, f64)>() + fields * std::mem::size_of::<f64>();
            let per_series = std::mem::size_of::<(f64, &Series)>()
                + fields * std::mem::size_of::<RowCursor<'_>>();
            return series.iter().fold(
                merge_workspace_bytes(rows, series.len(), fields),
                |total, item| {
                    total
                        .saturating_add(per_series)
                        .saturating_add((item.metric.tags().len() + 1).saturating_mul(label))
                        .saturating_add(item.time.len().saturating_mul(row))
                },
            );
        }
        BoundVectorOp::CountValues { .. } => {
            let label = std::mem::size_of::<(Arc<str>, Arc<str>)>();
            // A 17-digit value with sign, point and exponent. Large magnitudes formatted in full
            // can be longer.
            let value = 24;
            let group = std::mem::size_of::<Vec<f64>>()
                + fields * std::mem::size_of::<f64>()
                + std::mem::size_of::<Series>()
                + fields * std::mem::size_of::<Samples>();
            return series.iter().fold(0usize, |total, item| {
                let labels = 2 * (item.metric.tags().len() + 2) * label + value;
                let samples = item.time.len().saturating_mul(fields);
                total.saturating_add(samples.saturating_mul(group + labels))
            });
        }
        BoundVectorOp::TopK { .. }
        | BoundVectorOp::LimitK { .. }
        | BoundVectorOp::LimitRatio { .. } => {}
    }
    let row = std::mem::size_of::<(usize, usize)>()
        + std::mem::size_of::<(usize, usize, f64)>()
        + fields * std::mem::size_of::<f64>();
    let per_series = 2 * std::mem::size_of::<usize>()
        + std::mem::size_of::<Vec<(usize, usize)>>()
        + std::mem::size_of::<RowCursor<'_>>()
        + std::mem::size_of::<Series>()
        + fields * std::mem::size_of::<Samples>();
    let label = std::mem::size_of::<(Arc<str>, Arc<str>)>();
    series.iter().fold(0usize, |total, item| {
        let rows = item.time.len();
        let labels = item.metric.tags().len() + 1;
        let histograms = item
            .fields
            .iter()
            .filter_map(histogram_row_bytes)
            .fold(0usize, |bytes, widest| {
                bytes.saturating_add(widest.saturating_mul(rows))
            });
        total
            .saturating_add(rows.saturating_mul(row))
            .saturating_add(per_series)
            .saturating_add(labels.saturating_mul(label))
            .saturating_add(histograms)
    })
}

/// The operation a scalar broadcast applies, with the side the constant sits on already resolved.
#[derive(Clone, Copy, Debug)]
enum BroadcastOp {
    Arithmetic(ArithmeticOp),
    Compare(Comparison),
}

/// The scalar side of a broadcast.
enum BroadcastScalar {
    Literal(ScalarOperand),
    /// A per-point scalar, read whole before any vector chunk is answered.
    Dynamic {
        /// Whether the scalar is the left operand.
        left: bool,
        value: Option<Series>,
    },
}

/// A binary node with one scalar operand. Unlike [`BinaryRuntime`] it applies to each vector chunk
/// as it arrives and retains nothing.
struct BroadcastRuntime {
    scalar: BroadcastScalar,
    /// Whether the vector is the left operand.
    vector_left: bool,
    op: BroadcastOp,
    /// The vector side's schema; the kernel checks each chunk against it.
    sample_schema: Arc<SeriesSchema>,
    /// The bytes charged for a [`BroadcastScalar::Dynamic`] value.
    scalar_bytes: usize,
    annotations: Option<PromqlAnnotationCollector>,
}

/// A map with scalar parameters (`round`, `clamp`, `max_of`, ...), holding each parameter once it
/// arrives.
struct ParameterizedMapRuntime {
    op: ParameterizedOp,
    schema: Arc<SeriesSchema>,
    /// A constant input (as in `max_of(0, time())`), which its node does not hand over as a chunk,
    /// and whether its one chunk has been built.
    input_literal: Option<f64>,
    input_produced: bool,
    /// Constant parameters, which their nodes do not hand over as chunks.
    literals: Vec<Option<f64>>,
    /// Each parameter's unlabelled series, in argument order.
    values: Vec<Option<Series>>,
    /// The bytes charged for `values` and for the constant input's chunk.
    parameter_bytes: usize,
}

impl ParameterizedMapRuntime {
    fn ready(&self) -> bool {
        self.values.iter().all(Option::is_some)
    }

    /// Applies the function to one chunk with each parameter read at each point.
    fn apply(&self, chunk: &EvaluatedChunk) -> Result<Option<Series>> {
        let value = |index: usize| {
            self.values
                .get(index)
                .and_then(Option::as_ref)
                .ok_or_else(|| {
                    DataFusionError::Execution(
                        "the map was applied before its parameters arrived".to_string(),
                    )
                })
        };
        let function = match self.op {
            ParameterizedOp::Round => ParameterizedMath::Round {
                to_nearest: value(0)?,
            },
            ParameterizedOp::Clamp => ParameterizedMath::Clamp {
                min: value(0)?,
                max: value(1)?,
            },
            ParameterizedOp::ClampMin => ParameterizedMath::ClampMin { min: value(0)? },
            ParameterizedOp::ClampMax => ParameterizedMath::ClampMax { max: value(0)? },
            ParameterizedOp::MaxOf => ParameterizedMath::MaxOf { other: value(0)? },
            ParameterizedOp::MinOf => ParameterizedMath::MinOf { other: value(0)? },
        };
        map_math_with_parameters(&self.schema, &chunk.series, function)
    }
}

impl BroadcastRuntime {
    /// Whether the scalar varies per point and has not arrived yet.
    fn awaits_scalar(&self) -> bool {
        matches!(self.scalar, BroadcastScalar::Dynamic { value: None, .. })
    }

    /// Whether the operation drops the metric name (arithmetic and `bool` comparisons). Two input
    /// series can then share one label set; a [`NativeExpr::Merge`] above handles that.
    fn drops_labels(&self) -> bool {
        match self.op {
            BroadcastOp::Arithmetic(_) | BroadcastOp::Compare(Comparison::Bool(_)) => true,
            BroadcastOp::Compare(Comparison::Filter(_)) => false,
        }
    }

    /// Reports histogram samples dropped by an operation with no histogram form, as Prometheus
    /// does instead of failing.
    fn record_dropped_histograms(&self, dropped_histogram: bool, op: impl std::fmt::Display) {
        if dropped_histogram && let Some(collector) = &self.annotations {
            collector.record_info(format!(
                "{op}: dropped native histogram samples because this binary operation is not supported for native histograms"
            ));
        }
    }

    /// The label set `metric` becomes under this operation.
    fn output_metric(&self, metric: &SeriesLabels) -> Result<SeriesLabels> {
        if self.drops_labels() {
            drop_metric_name(metric)
        } else {
            Ok(metric.clone())
        }
    }

    /// Applies the operation to one vector chunk. `None` means every position was emptied and the
    /// chunk does not close its series; a closing chunk always yields a series, possibly with no
    /// rows, so the close reaches the consumer.
    fn apply(&self, chunk: &EvaluatedChunk) -> Result<Option<Series>> {
        let output = match &self.scalar {
            BroadcastScalar::Literal(operand) => match self.op {
                BroadcastOp::Arithmetic(op) => {
                    let (output, dropped_histogram) =
                        map_arithmetic_literal(&self.sample_schema, &chunk.series, op, *operand)?;
                    self.record_dropped_histograms(dropped_histogram, op);
                    Some(output)
                }
                BroadcastOp::Compare(comparison) => {
                    let (output, dropped_histogram) = map_compare_literal(
                        &self.sample_schema,
                        &chunk.series,
                        comparison,
                        *operand,
                    )?;
                    self.record_dropped_histograms(dropped_histogram, comparison);
                    output
                }
            },
            BroadcastScalar::Dynamic { left, value } => {
                let value = value.as_ref().ok_or_else(|| {
                    DataFusionError::Execution(
                        "the binary node broadcast a scalar that has not arrived".to_string(),
                    )
                })?;
                let operand = if *left {
                    ScalarSeriesOperand::Left(value)
                } else {
                    ScalarSeriesOperand::Right(value)
                };
                match self.op {
                    BroadcastOp::Arithmetic(op) => {
                        if !is_float_series(&chunk.series) {
                            return Err(DataFusionError::NotImplemented(
                                "dynamic scalar arithmetic does not support histogram or mixed samples"
                                    .into(),
                            ));
                        }
                        map_arithmetic_series(&self.sample_schema, &chunk.series, op, operand)?
                    }
                    BroadcastOp::Compare(comparison) => {
                        let (output, dropped_histogram) = map_compare_series(
                            &self.sample_schema,
                            &chunk.series,
                            comparison,
                            operand,
                        )?;
                        self.record_dropped_histograms(dropped_histogram, comparison);
                        output
                    }
                }
            }
        };
        match output {
            Some(output) => Ok(Some(output)),
            None if chunk.is_last => {
                let metric = self.output_metric(&chunk.series.metric)?;
                no_row_series(&self.sample_schema, &chunk.series, metric).map(Some)
            }
            None => Ok(None),
        }
    }
}

/// The buffered chunks as the one source axis they make up.
fn buffered_time(chunks: &[RawSeriesChunk]) -> Result<RawTimestamps> {
    let len = chunks.iter().map(|chunk| chunk.time.len()).sum();
    let mut timestamps: Vec<i64> = Vec::with_capacity(len);
    for chunk in chunks {
        timestamps.extend_from_slice(chunk.time.as_buffer());
    }
    RawTimestamps::try_new(ScalarBuffer::from(timestamps)).map_err(series_error)
}

/// The buffered chunks' fields, each chaining its chunks' samples without copying.
fn buffered_fields(chunks: &[RawSeriesChunk], fields: usize) -> Result<SeriesFields> {
    let mut per_field: Vec<Vec<Samples>> = vec![Vec::with_capacity(chunks.len()); fields];
    for chunk in chunks {
        for (field, slot) in chunk.fields.iter().zip(&mut per_field) {
            slot.push(field.clone());
        }
    }
    let fields = per_field
        .into_iter()
        .map(|chunks| Samples::try_from_chunks(chunks).map_err(series_error))
        .collect::<Result<Vec<_>>>()?;
    SeriesFields::try_from_vec(fields).map_err(series_error)
}

/// The bytes of the field's largest histogram row in the builder's table, `None` for a float
/// field.
fn histogram_row_bytes(field: &Samples) -> Option<usize> {
    let holds_histograms = field.segments().iter().any(|segment| {
        matches!(
            segment.dense(),
            Some(DenseSamplesRef::Histogram(_) | DenseSamplesRef::Mixed(_))
        )
    });
    if !holds_histograms {
        return None;
    }
    Some(
        field
            .iter()
            .filter_map(|sample| match sample {
                SampleRef::Histogram(view) => Some(histogram_bytes(&view)),
                _ => None,
            })
            .fold(0, usize::max),
    )
}

/// The bytes the histogram table holds for one row of `view`: its scalar columns, the offset each
/// bucket list pays for it, and the values those lists hold.
fn histogram_bytes(view: &HistogramView<'_>) -> usize {
    let custom = view
        .custom_values
        .len()
        .saturating_mul(std::mem::size_of::<f64>());
    let sides = [view.positive, view.negative]
        .iter()
        .map(|side| {
            side.span_offsets
                .len()
                .saturating_mul(2)
                .saturating_mul(std::mem::size_of::<i32>())
                .saturating_add(
                    side.buckets
                        .len()
                        .saturating_mul(std::mem::size_of::<f64>()),
                )
        })
        .fold(0usize, usize::saturating_add);
    HISTOGRAM_ROW_HEADER_BYTES
        .saturating_add(custom)
        .saturating_add(sides)
}

/// The bytes a histogram table row takes before its bucket lists: the schema, zero threshold, sum,
/// count, zero count, reset hint and start timestamp, the narrow columns counted at the width of
/// the widest, and one offset per list column -- custom bounds, two span columns and one bucket
/// column per side.
const HISTOGRAM_ROW_HEADER_BYTES: usize =
    7 * std::mem::size_of::<i64>() + 7 * std::mem::size_of::<u32>();

/// The last (inclusive) timestamp of `spec`. [`GridSpec::try_new`] guarantees it fits `i64`.
fn grid_end_ms(spec: GridSpec) -> i64 {
    (i128::from(spec.start_ms) + i128::from(spec.step_ms) * (i128::from(spec.len) - 1)) as i64
}

/// The grid position of `timestamp`. A timestamp off the grid is an internal disagreement, so it
/// is an error rather than skipped.
fn grid_position(grid: &EvalGrid, timestamp: i64) -> Result<usize> {
    let spec = grid.spec();
    // Fast `i64` path per point; overflow or an off-grid timestamp falls through to `i128`.
    if spec.step_ms > 0
        && let Some(offset) = timestamp.checked_sub(spec.start_ms)
        && offset >= 0
    {
        let (position, remainder) = (offset / spec.step_ms, offset % spec.step_ms);
        if remainder == 0 && (position as u64) < spec.len {
            return Ok(position as usize);
        }
    }
    let offset = i128::from(timestamp) - i128::from(spec.start_ms);
    let step = i128::from(spec.step_ms);
    let position = offset.div_euclid(step);
    if offset.rem_euclid(step) != 0 || position < 0 || position >= i128::from(spec.len) {
        return Err(DataFusionError::Execution(format!(
            "the instant selection reported the timestamp {timestamp}ms, which is not a point of \
             its grid {spec:?}"
        )));
    }
    Ok(position as usize)
}

/// Checks that an evaluated series lies on the grid its consumer is bound to.
fn ensure_grid(grid: Option<&EvalGrid>, time: &TimeAxis) -> Result<()> {
    if grid.is_none_or(|grid| {
        time.grid()
            .is_none_or(|actual| actual.spec() != grid.spec())
    }) {
        return Err(DataFusionError::Execution(
            "vector() input does not match its evaluation grid".to_string(),
        ));
    }
    Ok(())
}

/// The layout of a scalar's value carrier: one field on `grid`.
fn scalar_schema(grid: Arc<EvalGrid>) -> SeriesSchema {
    SeriesSchema {
        fields: Arc::from([FieldSpec {
            name: "value".into(),
        }]),
        eval_grid: Some(grid),
    }
}

/// The bytes one value per point of `grid` takes in a chunk of its own, reserved before it exists.
fn grid_chunk_bytes(grid: &EvalGrid) -> Result<usize> {
    let len = usize::try_from(grid.spec().len).map_err(|_| {
        DataFusionError::Execution("scalar grid length does not fit memory size".into())
    })?;
    std::mem::size_of::<EvaluatedChunk>()
        .checked_add(std::mem::size_of::<Float64Array>())
        .and_then(|bytes| {
            len.checked_mul(std::mem::size_of::<f64>())
                .and_then(|values| bytes.checked_add(values))
        })
        .ok_or_else(|| DataFusionError::Execution("scalar allocation size overflow".into()))
}

/// An upper bound of what a chain of map operations allocates over `series`. Each operation's
/// output lives until the next one is written, so the bound is the largest sum of two consecutive
/// workspaces. Sizing every operation from `series` relies on each one writing floats only, at no
/// more rows than it read.
fn map_workspace_bytes(ops: &[MapOp], series: &Series) -> Result<usize> {
    let (mut previous, mut bound) = (0usize, 0usize);
    let mut first = true;
    for op in ops {
        let workspace = match op {
            MapOp::Label(_) => continue,
            MapOp::Math(_) if !first || is_float_series(series) => mapped_workspace_bytes(series)?,
            _ if first => built_series_workspace_bytes(series)?,
            _ => built_workspace_bytes(series.time.len(), series.time.len(), series.fields.len())?,
        };
        let live = previous
            .checked_add(workspace)
            .ok_or_else(|| workspace_overflow(series.time.len(), series.fields.len()))?;
        bound = bound.max(live);
        previous = workspace;
        first = false;
    }
    Ok(bound)
}

/// An upper bound of what a kernel that maps each dense segment of `series` into a new array of
/// equal length allocates. A histogram row is charged at the field's largest payload, plus a buffer
/// header and 64-byte alignment per segment.
fn mapped_workspace_bytes(series: &Series) -> Result<usize> {
    let segments = series
        .fields
        .iter()
        .map(|field| field.segments().len())
        .fold(0usize, usize::saturating_add);
    let bound = || {
        let mut bytes = assembly_bytes(series.fields.len(), segments, &series.metric)?;
        for field in series.fields.iter() {
            let row = histogram_row_bytes(field).map_or(std::mem::size_of::<f64>(), |histogram| {
                histogram.max(std::mem::size_of::<f64>())
            });
            for segment in field.segments() {
                bytes = segment
                    .len()
                    .checked_mul(row)?
                    .checked_add(64 + BUFFER_HEADER_BYTES)?
                    .checked_add(bytes)?;
            }
        }
        Some(bytes)
    };
    bound().ok_or_else(|| workspace_overflow(series.time.len(), series.fields.len()))
}

/// An upper bound of what a kernel that pushes a subset of the rows of `series` into a
/// [`GridSeriesBuilder`] allocates: [`built_workspace_bytes`] over the series' rows and runs, and
/// the result's assembly ([`assembly_bytes`]), one segment per field.
fn built_series_workspace_bytes(series: &Series) -> Result<usize> {
    let fields = series.fields.len();
    let rows = built_workspace_bytes(series.time.len(), series.time.runs().count(), fields)?;
    assembly_bytes(fields, fields, &series.metric)
        .and_then(|assembly| assembly.checked_add(rows))
        .ok_or_else(|| workspace_overflow(series.time.len(), fields))
}

/// The bytes a result of `fields` fields over `segments` segments is assembled from, beside its
/// values: the descriptor lists (a growing list holds at most `2 * n + 4` entries) and the label
/// set rebuilt from `metric`.
fn assembly_bytes(fields: usize, segments: usize, metric: &SeriesLabels) -> Option<usize> {
    let descriptors = segments
        .checked_mul(6)?
        .checked_add(fields.checked_mul(16)?)?
        .checked_add(8)?
        .checked_mul(std::mem::size_of::<Samples>())?;
    descriptors
        .checked_add(std::mem::size_of_val(metric.tags()))?
        .checked_add(BUFFER_HEADER_BYTES + std::mem::size_of::<Series>())
}

/// An allowance for the shared header an Arrow buffer or a label list is allocated behind.
const BUFFER_HEADER_BYTES: usize = 64;

/// An upper bound of the bytes a [`GridSeriesBuilder`] allocates for a result of `fields` float
/// fields whose positions are a subset of `rows` positions in `runs` runs.
///
/// Keeping `kept` rows in `split` runs costs `2 * kept + 4` entries per growing column plus a span
/// per run. Dropping a position splits at most one run, so `split <= min(kept, runs + rows - kept)`
/// and the maximum is at one end of that range. Pass `runs = rows` when the result's positions are
/// not a subset of the input's.
fn built_workspace_bytes(rows: usize, runs: usize, fields: usize) -> Result<usize> {
    let cost = |kept: usize, split: usize| {
        let capacity = kept.checked_mul(2)?.checked_add(4)?;
        let indices = capacity.checked_mul(std::mem::size_of::<usize>())?;
        let validity = kept.div_ceil(8).checked_mul(2)?.checked_add(64)?;
        let field = capacity
            .checked_mul(std::mem::size_of::<f64>())?
            .checked_add(validity)?;
        let spans = split
            .checked_mul(2 * std::mem::size_of::<GridSpan>() + 2 * std::mem::size_of::<usize>())?;
        fields
            .checked_mul(field)?
            .checked_add(indices)?
            .checked_add(spans)
    };
    let runs = runs.min(rows);
    let fragmented = rows.checked_add(runs).map(|total| total.div_ceil(2));
    fragmented
        .and_then(|fragmented| Some(cost(rows, runs)?.max(cost(fragmented, fragmented)?)))
        .ok_or_else(|| workspace_overflow(rows, fields))
}

fn workspace_overflow(rows: usize, fields: usize) -> DataFusionError {
    DataFusionError::Execution(format!(
        "the workspace for {rows} rows of {fields} fields does not fit one address space"
    ))
}

/// An upper bound of what `window`'s next push allocates for its output rows, given the input's
/// last sample time `last_ts` and whether it closes the series.
///
/// Rows come from [`RangeEvaluator::output_rows_bound`], each possibly its own run. A
/// histogram-valued function adds an index and a doubled table row per field and row, sized by the
/// widest histogram retained or incoming; a result combining disjoint layouts can exceed that and
/// is not bounded.
fn window_workspace_bytes(
    window: &RangeEvaluator,
    last_ts: Option<i64>,
    last: bool,
    fields: &SeriesFields,
    metric: &SeriesLabels,
) -> Result<usize> {
    let rows = window.output_rows_bound(last_ts, last);
    let width = window.output_schema().fields.len();
    let floats = assembly_bytes(width, width, metric)
        .ok_or_else(|| workspace_overflow(rows, width))?
        .checked_add(built_workspace_bytes(rows, rows, width)?)
        .ok_or_else(|| workspace_overflow(rows, width))?;
    if !window.yields_histograms() {
        return Ok(floats);
    }
    let retained = window
        .widest_retained_histogram()
        .map(|heap| heap.saturating_add(HISTOGRAM_ROW_HEADER_BYTES));
    let incoming = fields.iter().filter_map(histogram_row_bytes).max();
    let Some(widest) = retained.max(incoming) else {
        return Ok(floats);
    };
    let histograms = || {
        let row = widest
            .checked_mul(2)?
            .checked_add(2 * std::mem::size_of::<Option<u32>>() + std::mem::size_of::<u32>() + 1)?;
        let in_flight = widest.checked_add(std::mem::size_of::<FloatHistogram>())?;
        rows.checked_mul(row)?
            .checked_add(in_flight)?
            .checked_mul(width)?
            .checked_add(floats)
    };
    histograms().ok_or_else(|| {
        DataFusionError::Execution(format!(
            "the histogram workspace for {rows} rows of {width} fields does not fit one address \
             space"
        ))
    })
}

/// The time of the last sample a chunk carries, on its own timeline.
fn last_sample_ms(chunk: &NativeChunk) -> Result<Option<i64>> {
    match chunk {
        NativeChunk::Raw(chunk) => Ok(chunk.time.as_buffer().last().copied()),
        NativeChunk::Evaluated(chunk) => Ok(last_series_ms(&chunk.series)),
        NativeChunk::AggregateState(_) => Err(DataFusionError::Execution(
            "an aggregate state chunk reached a node that reads values or samples".to_string(),
        )),
    }
}

/// The time of the last row `series` carries.
fn last_series_ms(series: &Series) -> Option<i64> {
    let row = series.time.len().checked_sub(1)?;
    series.time.timestamp(row)
}

/// One unlabelled constant value per statement grid point, closed on its only chunk.
fn scalar_chunk(grid: Arc<EvalGrid>, value: f64) -> Result<EvaluatedChunk> {
    scalar_chunk_on(&scalar_schema(Arc::clone(&grid)), grid, value)
}

/// [`scalar_chunk`] under `schema`, which names the one field.
fn scalar_chunk_on(
    schema: &SeriesSchema,
    grid: Arc<EvalGrid>,
    value: f64,
) -> Result<EvaluatedChunk> {
    let len = grid.spec().len as usize;
    let time = GridRange::try_new(grid, 0, len).map_err(series_error)?;
    let values = Float64Array::from(vec![value; len]);
    let fields = SeriesFields::one(Samples::from_float(values));
    let metric = SeriesLabels::try_new(None, Vec::new()).map_err(series_error)?;
    let series =
        Series::try_new(schema, metric, TimeAxis::Grid(time), fields).map_err(series_error)?;
    EvaluatedChunk::try_new(series, true)
}

/// A closing chunk carrying `metric` and no rows.
fn no_row_chunk(schema: &SeriesSchema, metric: SeriesLabels) -> Result<EvaluatedChunk> {
    let grid = schema.eval_grid.clone().ok_or_else(|| {
        DataFusionError::Execution(
            "an instant selection produces an evaluated chunk, which is addressed on a grid"
                .to_string(),
        )
    })?;
    let empty = Samples::from_float(Float64Array::from(Vec::<f64>::new()));
    let fields =
        SeriesFields::try_from_vec(vec![empty; schema.fields.len()]).map_err(series_error)?;
    let time = GridRange::try_new(grid, 0, 0).map_err(series_error)?;
    let series =
        Series::try_new(schema, metric, TimeAxis::Grid(time), fields).map_err(series_error)?;
    EvaluatedChunk::try_new(series, true)
}

/// A runtime operator and its directly owned children and metrics.
struct RuntimeNode {
    metrics: Arc<NodeMetrics>,
    op: RuntimeOp,
}

// Each payload precedes its NodeState, so it drops before that node's reservation.
enum RuntimeOp {
    Input(InputId),
    Scalar,
    ScalarOf {
        input: Box<RuntimeNode>,
        input_schema: Arc<SeriesSchema>,
        grid: Arc<EvalGrid>,
        retained: RetainedInput,
        produced: bool,
        state: NodeState,
    },
    /// The evaluation time or a date function of it, produced once as one closed chunk.
    EvalTime {
        grid: Arc<EvalGrid>,
        date: Option<DateFunction>,
        produced: bool,
        state: NodeState,
    },
    /// A scalar handed on as a vector. A constant input has no chunk of its own, so its value is
    /// produced here, once.
    VectorOf {
        input: Box<RuntimeNode>,
        literal: Option<f64>,
        produced: bool,
        schema: Arc<SeriesSchema>,
        state: NodeState,
    },
    Instant {
        input: Box<RuntimeNode>,
        instant: InstantRuntime,
        state: NodeState,
    },
    Merge {
        input: Box<RuntimeNode>,
        merge: Option<MergeInput>,
        state: NodeState,
    },
    Window {
        input: Box<RuntimeNode>,
        // `None` once `finish_windows` has consumed the evaluator.
        window: Option<RangeEvaluator>,
        merge: Option<MergeInput>,
        state: NodeState,
    },
    Map {
        input: Box<RuntimeNode>,
        map: MapRuntime,
        state: NodeState,
    },
    ParameterizedMap {
        input: Box<RuntimeNode>,
        parameters: Vec<RuntimeNode>,
        map: ParameterizedMapRuntime,
        state: NodeState,
    },
    Timestamp {
        input: Box<RuntimeNode>,
        schema: Arc<SeriesSchema>,
        state: NodeState,
    },
    Binary {
        left: Box<RuntimeNode>,
        right: Box<RuntimeNode>,
        binary: BinaryRuntime,
        state: NodeState,
    },
    Broadcast {
        left: Box<RuntimeNode>,
        right: Box<RuntimeNode>,
        broadcast: BroadcastRuntime,
        state: NodeState,
    },
    Aggregate {
        input: Box<RuntimeNode>,
        aggregate: AggregateRuntime,
        state: NodeState,
    },
    Vector {
        input: Box<RuntimeNode>,
        vector: VectorRuntime,
        state: NodeState,
    },
    ParameterizedTopK {
        input: Box<RuntimeNode>,
        // Taken and dropped after its scalar values are installed.
        parameter: Option<Box<RuntimeNode>>,
        vector: VectorRuntime,
        state: NodeState,
    },
    ParameterizedAggregate {
        input: Box<RuntimeNode>,
        // Taken and dropped once the fold holds the series it answers with.
        parameter: Option<Box<RuntimeNode>>,
        aggregate: AggregateRuntime,
        state: NodeState,
    },
    ParameterizedHistogram {
        input: Box<RuntimeNode>,
        // Each parameter is taken and dropped after its scalar values are installed.
        parameters: Vec<Option<RuntimeNode>>,
        literals: Vec<Option<f64>>,
        vector: VectorRuntime,
        values: Vec<Option<Vec<Option<f64>>>>,
        parameter_bytes: Vec<usize>,
        state: NodeState,
    },
}

/// State shared by the stateful operators. The queue precedes the buffer so it drops before its
/// reservation.
struct NodeState {
    queued: VecDeque<(NativeChunk, usize)>,
    queued_tables: usize,
    buffer: NodeBuffer,
}

/// The kind of chunk an input leaf is bound to, and so the only kind it accepts.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum LeafKind {
    Raw,
    Evaluated,
    AggregateState,
}

impl LeafKind {
    fn of(output: &NativeType) -> Result<Self> {
        match output {
            NativeType::Samples(schema) if schema.eval_grid.is_some() => Ok(Self::Evaluated),
            NativeType::Samples(_) => Ok(Self::Raw),
            NativeType::AggregateState(_) => Ok(Self::AggregateState),
            NativeType::Scalar(_) => Err(DataFusionError::Execution(
                "an input leaf cannot be bound to a scalar".to_string(),
            )),
        }
    }

    fn describe(self) -> &'static str {
        match self {
            Self::Raw => "raw samples",
            Self::Evaluated => "an evaluated series",
            Self::AggregateState => "aggregate state",
        }
    }

    fn accepts(self, chunk: &NativeChunk) -> bool {
        matches!(
            (self, chunk),
            (Self::Raw, NativeChunk::Raw(_))
                | (Self::Evaluated, NativeChunk::Evaluated(_))
                | (Self::AggregateState, NativeChunk::AggregateState(_))
        )
    }
}

/// The caller-owned state of one input leaf.
struct InputSlot {
    source_index: usize,
    kind: LeafKind,
    /// Whether the series whose chunk this leaf most recently handed over is still open.
    open_source_series: bool,
    /// Whether the series whose chunk the caller most recently queued here is still open. Tracked
    /// separately from handovers because a node waiting on a parameter can leave a chunk queued at
    /// the end of a run.
    open_queued_series: bool,
    state: NodeState,
}

impl InputSlot {
    /// Whether an input the caller ended here left a series open, handed over or still queued.
    fn ends_open(&self) -> bool {
        self.open_source_series || self.open_queued_series
    }
}

/// State for one execution of a bound tree. The live tree and input slots are dropped together
/// when the executor finishes or fails; the public state remains available for inspection.
pub struct NativeExecutionState {
    live: Option<LiveExecution>,
}

struct LiveExecution {
    root: RuntimeNode,
    inputs: Vec<InputSlot>,
    /// A constant root waiting for its only pull; operand scalars remain chunkless.
    scalar_root: Option<(Arc<EvalGrid>, f64, NodeState)>,
}

impl NodeState {
    /// Adds the node name to `error`: an `Execution` message gets it as a prefix and stays
    /// `Execution`; any other variant is wrapped in `Context`.
    fn context(&self, error: DataFusionError) -> DataFusionError {
        let name = self.buffer.reservation.consumer().name();
        match error {
            DataFusionError::Execution(message) => {
                DataFusionError::Execution(format!("{name}: {message}"))
            }
            other => DataFusionError::Context(name.to_string(), Box::new(other)),
        }
    }

    pub(super) fn new(
        name: impl Into<String>,
        metrics: Arc<NodeMetrics>,
        pool: &Arc<dyn MemoryPool>,
    ) -> Self {
        let name = name.into();
        Self {
            queued: VecDeque::new(),
            queued_tables: 0,
            buffer: NodeBuffer::new(name, metrics, pool),
        }
    }
}

/// Remaining stack below which [`RuntimeNode::build`] and [`RuntimeNode::pull`] grow it. Both
/// recurse once per tree level with large debug-build frames (about 140 KiB and 80 KiB, measured on
/// a 100-term chain), and the tree has no depth limit, so an 8 MiB worker stack can overflow.
const NATIVE_STACK_RED_ZONE: usize = 512 * 1024;

/// Stack added per growth; holds several levels.
const NATIVE_STACK_GROWTH: usize = 4 * 1024 * 1024;

impl RuntimeNode {
    /// Builds the runtime node for `node` and, recursively, for its inputs, growing the stack as it
    /// descends.
    fn build(
        node: &NativeNode,
        pool: &Arc<dyn MemoryPool>,
        inputs: &mut Vec<InputSlot>,
        binary_before: &mut usize,
        location: String,
        duplicate_evidence: Option<Arc<DuplicateEvidence>>,
        sources: &SourceFacts,
    ) -> Result<Self> {
        stacker::maybe_grow(NATIVE_STACK_RED_ZONE, NATIVE_STACK_GROWTH, || {
            Self::build_within_stack(
                node,
                pool,
                inputs,
                binary_before,
                location,
                duplicate_evidence,
                sources,
            )
        })
    }

    fn build_within_stack(
        node: &NativeNode,
        pool: &Arc<dyn MemoryPool>,
        inputs: &mut Vec<InputSlot>,
        binary_before: &mut usize,
        location: String,
        duplicate_evidence: Option<Arc<DuplicateEvidence>>,
        sources: &SourceFacts,
    ) -> Result<Self> {
        let metrics = Arc::new(NodeMetrics::default());
        let op = match &node.expr {
            NativeExpr::Input(bound) => {
                if bound.ordering == SeriesOrdering::Interleaved {
                    return Err(DataFusionError::NotImplemented(format!(
                        "the native runtime does not accept {:?} input ordering",
                        bound.ordering
                    )));
                }
                let id = InputId(inputs.len());
                inputs.push(InputSlot {
                    source_index: bound.source_index,
                    kind: LeafKind::of(&node.output)?,
                    open_source_series: false,
                    open_queued_series: false,
                    state: NodeState::new(
                        format!(
                            "native.input at {location} (source_index={})",
                            bound.source_index
                        ),
                        Arc::clone(&metrics),
                        pool,
                    ),
                });
                RuntimeOp::Input(id)
            }
            NativeExpr::Scalar(_) => RuntimeOp::Scalar,
            NativeExpr::ScalarOf { input } => {
                let input_schema = input.output.samples().cloned().ok_or_else(|| {
                    DataFusionError::Execution("scalar() consumes non-sample input".to_string())
                })?;
                if input_schema.fields.len() != 1 {
                    return Err(DataFusionError::NotImplemented(
                        "the native runtime does not execute multi-field scalar()".into(),
                    ));
                }
                let grid = input_schema.eval_grid.clone().ok_or_else(|| {
                    DataFusionError::Execution("scalar() consumes raw samples".to_string())
                })?;
                let child = Self::build(
                    input,
                    pool,
                    inputs,
                    binary_before,
                    format!("{location}.input"),
                    duplicate_evidence.clone(),
                    sources,
                )?;
                RuntimeOp::ScalarOf {
                    input: Box::new(child),
                    input_schema,
                    grid,
                    retained: RetainedInput::default(),
                    produced: false,
                    state: NodeState::new(
                        format!("native.scalar_of at {location}"),
                        Arc::clone(&metrics),
                        pool,
                    ),
                }
            }
            NativeExpr::EvalTime { date } => {
                let NativeType::Scalar(grid) = &node.output else {
                    return Err(DataFusionError::Execution(
                        "the evaluation time does not produce a scalar".to_string(),
                    ));
                };
                RuntimeOp::EvalTime {
                    grid: Arc::clone(grid),
                    date: *date,
                    produced: false,
                    state: NodeState::new(
                        format!("native.time at {location}"),
                        Arc::clone(&metrics),
                        pool,
                    ),
                }
            }
            NativeExpr::VectorOf { input } => {
                let schema = node.output.samples().cloned().ok_or_else(|| {
                    DataFusionError::Execution("vector() does not produce samples".to_string())
                })?;
                let literal = match &input.expr {
                    NativeExpr::Scalar(BoundScalar { value }) => Some(*value),
                    _ => None,
                };
                let child = Self::build(
                    input,
                    pool,
                    inputs,
                    binary_before,
                    format!("{location}.input"),
                    duplicate_evidence.clone(),
                    sources,
                )?;
                RuntimeOp::VectorOf {
                    input: Box::new(child),
                    literal,
                    produced: false,
                    schema,
                    state: NodeState::new(
                        format!("native.vector_of at {location}"),
                        Arc::clone(&metrics),
                        pool,
                    ),
                }
            }
            NativeExpr::Instant { input, op } => {
                let child = Self::build(
                    input,
                    pool,
                    inputs,
                    binary_before,
                    format!("{location}.input"),
                    duplicate_evidence.clone(),
                    sources,
                )?;
                let input_schema = input.output.samples().cloned().ok_or_else(|| {
                    DataFusionError::Execution(
                        "instant selection consumes non-sample input".to_string(),
                    )
                })?;
                RuntimeOp::Instant {
                    input: Box::new(child),
                    instant: InstantRuntime::try_new(&input_schema, op)?,
                    state: {
                        NodeState::new(
                            format!("native.instant at {location}"),
                            Arc::clone(&metrics),
                            pool,
                        )
                    },
                }
            }
            NativeExpr::Merge { input } => {
                let child = Self::build(
                    input,
                    pool,
                    inputs,
                    binary_before,
                    format!("{location}.input"),
                    duplicate_evidence.clone(),
                    sources,
                )?;
                let schema = node.output.samples().cloned().ok_or_else(|| {
                    DataFusionError::Execution("merge does not produce samples".to_string())
                })?;
                let state = NodeState::new(
                    format!("native.merge at {location}"),
                    Arc::clone(&metrics),
                    pool,
                );
                RuntimeOp::Merge {
                    input: Box::new(child),
                    merge: Some(MergeInput::new(schema)),
                    state,
                }
            }
            NativeExpr::Window { input, op } => {
                let child = Self::build(
                    input,
                    pool,
                    inputs,
                    binary_before,
                    format!("{location}.input"),
                    duplicate_evidence.clone(),
                    sources,
                )?;
                let input_schema = input.output.samples().cloned().ok_or_else(|| {
                    DataFusionError::Execution("window consumes non-sample input".to_string())
                })?;
                // Collect and merge fragments only when the input cannot prove its series keep
                // distinct label sets: the same check the canonical pass uses to place merges.
                let unification =
                    unifies_its_own_contributions(input, &sources.tags, &sources.metrics);
                let merge = matches!(unification, Some(false))
                    .then(|| MergeInput::new(Arc::clone(&input_schema)));
                let state = NodeState::new(
                    format!("native.window at {location}"),
                    Arc::clone(&metrics),
                    pool,
                );
                RuntimeOp::Window {
                    input: Box::new(child),
                    window: Some(RangeEvaluator::try_new(
                        &input_schema,
                        Arc::clone(&op.output_grid),
                        op.window,
                        op.function,
                    )?),
                    merge,
                    state,
                }
            }
            NativeExpr::Map { input, op } => {
                let child = Self::build(
                    input,
                    pool,
                    inputs,
                    binary_before,
                    format!("{location}.input"),
                    duplicate_evidence.clone(),
                    sources,
                )?;
                let schema = node.output.samples().cloned().ok_or_else(|| {
                    DataFusionError::Execution("map does not produce samples".to_string())
                })?;
                let state = NodeState::new(
                    format!("native.map at {location}"),
                    Arc::clone(&metrics),
                    pool,
                );
                RuntimeOp::Map {
                    input: Box::new(child),
                    map: MapRuntime {
                        ops: Arc::clone(&op.ops),
                        schema,
                    },
                    state,
                }
            }
            NativeExpr::ParameterizedMap {
                input,
                op,
                parameters,
            } => {
                let child = Self::build(
                    input,
                    pool,
                    inputs,
                    binary_before,
                    format!("{location}.input"),
                    duplicate_evidence.clone(),
                    sources,
                )?;
                let schema = match &node.output {
                    NativeType::Samples(schema) => Arc::clone(schema),
                    NativeType::Scalar(grid) => Arc::new(scalar_schema(Arc::clone(grid))),
                    NativeType::AggregateState(_) => {
                        return Err(DataFusionError::Execution(
                            "map does not produce samples or a scalar, but aggregate state"
                                .to_string(),
                        ));
                    }
                };
                let literals = parameters
                    .iter()
                    .map(|parameter| match &parameter.expr {
                        NativeExpr::Scalar(BoundScalar { value }) => Some(*value),
                        _ => None,
                    })
                    .collect();
                let input_literal = match &input.expr {
                    NativeExpr::Scalar(BoundScalar { value }) => Some(*value),
                    _ => None,
                };
                let nodes = parameters
                    .iter()
                    .enumerate()
                    .map(|(index, parameter)| {
                        Self::build(
                            parameter,
                            pool,
                            inputs,
                            binary_before,
                            format!("{location}.parameter{index}"),
                            duplicate_evidence.clone(),
                            sources,
                        )
                    })
                    .collect::<Result<Vec<_>>>()?;
                let state = NodeState::new(
                    format!("native.map at {location}"),
                    Arc::clone(&metrics),
                    pool,
                );
                RuntimeOp::ParameterizedMap {
                    input: Box::new(child),
                    parameters: nodes,
                    map: ParameterizedMapRuntime {
                        op: *op,
                        schema,
                        input_literal,
                        input_produced: false,
                        literals,
                        values: vec![None; parameters.len()],
                        parameter_bytes: 0,
                    },
                    state,
                }
            }
            NativeExpr::Timestamp { input } => {
                let child = Self::build(
                    input,
                    pool,
                    inputs,
                    binary_before,
                    format!("{location}.input"),
                    duplicate_evidence.clone(),
                    sources,
                )?;
                let schema = node.output.samples().cloned().ok_or_else(|| {
                    DataFusionError::Execution("timestamp does not produce samples".to_string())
                })?;
                let state = NodeState::new(
                    format!("native.timestamp at {location}"),
                    Arc::clone(&metrics),
                    pool,
                );
                RuntimeOp::Timestamp {
                    input: Box::new(child),
                    schema,
                    state,
                }
            }
            NativeExpr::Binary {
                left,
                right,
                matching,
            } => {
                let left_schema = left.output.samples();
                let right_schema = right.output.samples();
                let (binary, broadcast) = match (left_schema, right_schema) {
                    (Some(left_schema), Some(right_schema))
                        if left_schema.eval_grid.is_some() && right_schema.eval_grid.is_some() =>
                    {
                        let node_index = *binary_before;
                        *binary_before += 1;
                        (
                            Some(BinaryRuntime::new(
                                PairKernel::Binary(Arc::new(matching.clone())),
                                [Arc::clone(left_schema), Arc::clone(right_schema)],
                                node_index,
                                duplicate_evidence.clone(),
                            )),
                            None,
                        )
                    }
                    _ => (None, Some(broadcast_runtime(left, right, matching)?)),
                };
                let left_node = Self::build(
                    left,
                    pool,
                    inputs,
                    binary_before,
                    format!("{location}.left"),
                    duplicate_evidence.clone(),
                    sources,
                )?;
                let right_node = Self::build(
                    right,
                    pool,
                    inputs,
                    binary_before,
                    format!("{location}.right"),
                    duplicate_evidence.clone(),
                    sources,
                )?;
                match (binary, broadcast) {
                    (Some(binary), None) => RuntimeOp::Binary {
                        left: Box::new(left_node),
                        right: Box::new(right_node),
                        binary,
                        state: NodeState::new(
                            format!("native.binary at {location}"),
                            Arc::clone(&metrics),
                            pool,
                        ),
                    },
                    (None, Some(broadcast)) => {
                        let state = NodeState::new(
                            format!("native.binary at {location}"),
                            Arc::clone(&metrics),
                            pool,
                        );
                        RuntimeOp::Broadcast {
                            left: Box::new(left_node),
                            right: Box::new(right_node),
                            broadcast,
                            state,
                        }
                    }
                    _ => {
                        return Err(DataFusionError::Internal(
                            "binary runtime build selected conflicting operation modes".to_string(),
                        ));
                    }
                }
            }
            NativeExpr::Set { left, right, op } => {
                let (Some(left_schema), Some(right_schema)) =
                    (left.output.samples(), right.output.samples())
                else {
                    return Err(DataFusionError::Execution(
                        "a set operation consumes two vectors".to_string(),
                    ));
                };
                let node_index = *binary_before;
                *binary_before += 1;
                let set = BinaryRuntime::new(
                    PairKernel::Set(op.clone()),
                    [Arc::clone(left_schema), Arc::clone(right_schema)],
                    node_index,
                    duplicate_evidence.clone(),
                );
                let left_node = Self::build(
                    left,
                    pool,
                    inputs,
                    binary_before,
                    format!("{location}.left"),
                    duplicate_evidence.clone(),
                    sources,
                )?;
                let right_node = Self::build(
                    right,
                    pool,
                    inputs,
                    binary_before,
                    format!("{location}.right"),
                    duplicate_evidence.clone(),
                    sources,
                )?;
                RuntimeOp::Binary {
                    left: Box::new(left_node),
                    right: Box::new(right_node),
                    binary: set,
                    state: NodeState::new(
                        format!("native.set at {location}"),
                        Arc::clone(&metrics),
                        pool,
                    ),
                }
            }
            NativeExpr::Info {
                base,
                info,
                selector,
            } => {
                let (Some(base_schema), Some(info_schema)) =
                    (base.output.samples(), info.output.samples())
                else {
                    return Err(DataFusionError::Execution(
                        "info consumes two vectors".to_string(),
                    ));
                };
                let node_index = *binary_before;
                *binary_before += 1;
                let runtime = BinaryRuntime::new(
                    PairKernel::Info(selector.clone()),
                    [Arc::clone(base_schema), Arc::clone(info_schema)],
                    node_index,
                    duplicate_evidence.clone(),
                );
                let base_node = Self::build(
                    base,
                    pool,
                    inputs,
                    binary_before,
                    format!("{location}.base"),
                    duplicate_evidence.clone(),
                    sources,
                )?;
                let info_node = Self::build(
                    info,
                    pool,
                    inputs,
                    binary_before,
                    format!("{location}.info"),
                    duplicate_evidence.clone(),
                    sources,
                )?;
                RuntimeOp::Binary {
                    left: Box::new(base_node),
                    right: Box::new(info_node),
                    binary: runtime,
                    state: NodeState::new(
                        format!("native.info at {location}"),
                        Arc::clone(&metrics),
                        pool,
                    ),
                }
            }
            NativeExpr::Aggregate { input, op } => {
                let schema = node
                    .output
                    .samples()
                    .or_else(|| input.output.samples())
                    .cloned()
                    .ok_or_else(|| {
                        DataFusionError::Execution("aggregate does not produce samples".to_string())
                    })?;
                let mut state = NodeState::new(
                    format!("native.aggregate at {location}"),
                    Arc::clone(&metrics),
                    pool,
                );
                let aggregate = AggregateRuntime::new(op.clone(), schema, &mut state.buffer)?;
                let child = Self::build(
                    input,
                    pool,
                    inputs,
                    binary_before,
                    format!("{location}.input"),
                    duplicate_evidence.clone(),
                    sources,
                )?;
                RuntimeOp::Aggregate {
                    input: Box::new(child),
                    aggregate,
                    state,
                }
            }
            NativeExpr::ParameterizedAggregate {
                input,
                op,
                parameter,
            } => {
                let schema = node
                    .output
                    .samples()
                    .or_else(|| input.output.samples())
                    .cloned()
                    .ok_or_else(|| {
                        DataFusionError::Execution(
                            "a parameterized aggregate does not produce samples".into(),
                        )
                    })?;
                let mut state = NodeState::new(
                    format!("native.aggregate at {location}"),
                    Arc::clone(&metrics),
                    pool,
                );
                let aggregate =
                    AggregateRuntime::new_parameterized(op.clone(), schema, &mut state.buffer)?;
                let child = Self::build(
                    input,
                    pool,
                    inputs,
                    binary_before,
                    format!("{location}.input"),
                    duplicate_evidence.clone(),
                    sources,
                )?;
                // Input before parameter: the order the selector walk binds the two sources in.
                let parameter = Self::build(
                    parameter,
                    pool,
                    inputs,
                    binary_before,
                    format!("{location}.parameter"),
                    duplicate_evidence.clone(),
                    sources,
                )?;
                RuntimeOp::ParameterizedAggregate {
                    input: Box::new(child),
                    parameter: Some(Box::new(parameter)),
                    aggregate,
                    state,
                }
            }
            NativeExpr::ParameterizedTopK {
                input,
                op,
                parameter,
            } => {
                let schema = node.output.samples().cloned().ok_or_else(|| {
                    DataFusionError::Execution("parameterized topk does not produce samples".into())
                })?;
                let one_field = schema.fields.len() == 1;
                if !one_field {
                    return Err(DataFusionError::NotImplemented(
                        "the native runtime does not execute multi-field vector operations".into(),
                    ));
                }
                let vector = VectorRuntime::try_new(op.clone(), schema)?;
                let child = Self::build(
                    input,
                    pool,
                    inputs,
                    binary_before,
                    format!("{location}.input"),
                    duplicate_evidence.clone(),
                    sources,
                )?;
                let parameter = Self::build(
                    parameter,
                    pool,
                    inputs,
                    binary_before,
                    format!("{location}.parameter"),
                    duplicate_evidence.clone(),
                    sources,
                )?;
                let state = NodeState::new(
                    format!("native.vector at {location}"),
                    Arc::clone(&metrics),
                    pool,
                );
                RuntimeOp::ParameterizedTopK {
                    input: Box::new(child),
                    parameter: Some(Box::new(parameter)),
                    vector,
                    state,
                }
            }
            NativeExpr::ParameterizedHistogram {
                input,
                op,
                parameters,
            } => {
                if !matches!(op, BoundVectorOp::Histogram(_)) {
                    return Err(DataFusionError::Internal(
                        "parameterized histogram has non-histogram op".into(),
                    ));
                }
                if parameters.len()
                    != match op {
                        BoundVectorOp::Histogram(HistogramFunction::Quantile(_)) => 1,
                        BoundVectorOp::Histogram(HistogramFunction::Fraction { .. }) => 2,
                        _ => 0,
                    }
                {
                    return Err(DataFusionError::Internal(
                        "parameterized histogram has the wrong arity".into(),
                    ));
                }
                let literals = parameters
                    .iter()
                    .map(|parameter| match &parameter.expr {
                        NativeExpr::Scalar(BoundScalar { value }) => Some(*value),
                        _ => None,
                    })
                    .collect();
                let schema = node.output.samples().cloned().ok_or_else(|| {
                    DataFusionError::Execution(
                        "parameterized histogram does not produce samples".into(),
                    )
                })?;
                let vector = VectorRuntime::try_new(op.clone(), schema)?;
                let child = Self::build(
                    input,
                    pool,
                    inputs,
                    binary_before,
                    format!("{location}.input"),
                    duplicate_evidence.clone(),
                    sources,
                )?;
                let mut built = Vec::with_capacity(parameters.len());
                for (index, parameter) in parameters.iter().enumerate() {
                    let runtime = Self::build(
                        parameter,
                        pool,
                        inputs,
                        binary_before,
                        format!("{location}.parameter{index}"),
                        duplicate_evidence.clone(),
                        sources,
                    )?;
                    built.push(Some(runtime));
                }
                RuntimeOp::ParameterizedHistogram {
                    input: Box::new(child),
                    parameter_bytes: vec![0; built.len()],
                    values: vec![None; built.len()],
                    parameters: built,
                    literals,
                    vector,
                    state: NodeState::new(
                        format!("native.vector at {location}"),
                        Arc::clone(&metrics),
                        pool,
                    ),
                }
            }
            NativeExpr::Vector { input, op } => {
                let schema = node.output.samples().cloned().ok_or_else(|| {
                    DataFusionError::Execution(
                        "vector operation does not produce samples".to_string(),
                    )
                })?;
                let one_field = matches!(
                    op,
                    BoundVectorOp::TopK { .. }
                        | BoundVectorOp::LimitK { .. }
                        | BoundVectorOp::Sort(SortOrder::Asc | SortOrder::Desc)
                );
                if one_field && schema.fields.len() != 1 {
                    return Err(DataFusionError::NotImplemented(
                        "the native runtime does not execute multi-field vector operations"
                            .to_string(),
                    ));
                }
                let mut vector = VectorRuntime::try_new(op.clone(), schema)?;
                let child = Self::build(
                    input,
                    pool,
                    inputs,
                    binary_before,
                    format!("{location}.input"),
                    duplicate_evidence.clone(),
                    sources,
                )?;
                let limitk = matches!(op, BoundVectorOp::LimitK { .. });
                if limitk
                    && matches!(
                        unifies_its_own_contributions(input, &sources.tags, &sources.metrics),
                        Some(false)
                    )
                {
                    vector.merge_input = true;
                }
                RuntimeOp::Vector {
                    input: Box::new(child),
                    vector,
                    state: NodeState::new(
                        format!("native.vector at {location}"),
                        Arc::clone(&metrics),
                        pool,
                    ),
                }
            }
        };
        Ok(Self { metrics, op })
    }

    fn register_metrics(&self, name: &str, set: &ExecutionPlanMetricsSet, partition: usize) {
        self.metrics.register(name, set, partition);
        match &self.op {
            RuntimeOp::Input(_) | RuntimeOp::Scalar | RuntimeOp::EvalTime { .. } => {}
            RuntimeOp::ScalarOf { input, .. } => {
                input.register_metrics(&format!("{name}.input"), set, partition)
            }
            RuntimeOp::Instant { input, .. }
            | RuntimeOp::Window { input, .. }
            | RuntimeOp::Merge { input, .. }
            | RuntimeOp::Map { input, .. }
            | RuntimeOp::Timestamp { input, .. }
            | RuntimeOp::Aggregate { input, .. }
            | RuntimeOp::Vector { input, .. }
            | RuntimeOp::VectorOf { input, .. } => {
                input.register_metrics(&format!("{name}.input"), set, partition)
            }
            RuntimeOp::Binary { left, right, .. } | RuntimeOp::Broadcast { left, right, .. } => {
                left.register_metrics(&format!("{name}.left"), set, partition);
                right.register_metrics(&format!("{name}.right"), set, partition);
            }
            RuntimeOp::ParameterizedTopK {
                input, parameter, ..
            }
            | RuntimeOp::ParameterizedAggregate {
                input, parameter, ..
            } => {
                input.register_metrics(&format!("{name}.input"), set, partition);
                if let Some(parameter) = parameter {
                    parameter.register_metrics(&format!("{name}.parameter"), set, partition);
                }
            }
            RuntimeOp::ParameterizedHistogram {
                input, parameters, ..
            } => {
                input.register_metrics(&format!("{name}.input"), set, partition);
                for (i, parameter) in parameters.iter().enumerate() {
                    if let Some(parameter) = parameter {
                        parameter.register_metrics(&format!("{name}.parameter{i}"), set, partition);
                    }
                }
            }
            RuntimeOp::ParameterizedMap {
                input, parameters, ..
            } => {
                input.register_metrics(&format!("{name}.input"), set, partition);
                for (index, parameter) in parameters.iter().enumerate() {
                    parameter.register_metrics(&format!("{name}.parameter{index}"), set, partition);
                }
            }
        }
    }

    fn reserved_bytes(&self) -> usize {
        match &self.op {
            RuntimeOp::Input(_) | RuntimeOp::Scalar => 0,
            RuntimeOp::EvalTime { state, .. } => state.buffer.reservation.size(),
            RuntimeOp::ScalarOf { input, state, .. } => state
                .buffer
                .reservation
                .size()
                .saturating_add(input.reserved_bytes()),
            RuntimeOp::Instant { input, state, .. }
            | RuntimeOp::Window { input, state, .. }
            | RuntimeOp::Merge { input, state, .. }
            | RuntimeOp::Map { input, state, .. }
            | RuntimeOp::Timestamp { input, state, .. }
            | RuntimeOp::Aggregate { input, state, .. }
            | RuntimeOp::Vector { input, state, .. }
            | RuntimeOp::VectorOf { input, state, .. } => state
                .buffer
                .reservation
                .size()
                .saturating_add(input.reserved_bytes()),
            RuntimeOp::Binary {
                left, right, state, ..
            }
            | RuntimeOp::Broadcast {
                left, right, state, ..
            } => state
                .buffer
                .reservation
                .size()
                .saturating_add(left.reserved_bytes())
                .saturating_add(right.reserved_bytes()),
            RuntimeOp::ParameterizedHistogram {
                input,
                parameters,
                state,
                ..
            } => parameters.iter().fold(
                state
                    .buffer
                    .reservation
                    .size()
                    .saturating_add(input.reserved_bytes()),
                |n, p| n.saturating_add(p.as_ref().map_or(0, RuntimeNode::reserved_bytes)),
            ),
            RuntimeOp::ParameterizedTopK {
                input,
                parameter,
                state,
                ..
            }
            | RuntimeOp::ParameterizedAggregate {
                input,
                parameter,
                state,
                ..
            } => state
                .buffer
                .reservation
                .size()
                .saturating_add(input.reserved_bytes())
                .saturating_add(parameter.as_ref().map_or(0, |p| p.reserved_bytes())),
            RuntimeOp::ParameterizedMap {
                input,
                parameters,
                state,
                ..
            } => parameters.iter().fold(
                state
                    .buffer
                    .reservation
                    .size()
                    .saturating_add(input.reserved_bytes()),
                |total, parameter| total.saturating_add(parameter.reserved_bytes()),
            ),
        }
    }
}

impl RuntimeNode {
    fn set_annotations(&mut self, collector: PromqlAnnotationCollector) {
        match &mut self.op {
            RuntimeOp::Window { input, window, .. } => {
                if let Some(window) = window {
                    window.set_annotations(Some(collector.clone()));
                }
                input.set_annotations(collector);
            }
            RuntimeOp::Map { input, .. } => input.set_annotations(collector),
            RuntimeOp::ParameterizedMap {
                input, parameters, ..
            } => {
                input.set_annotations(collector.clone());
                for parameter in parameters {
                    parameter.set_annotations(collector.clone());
                }
            }
            RuntimeOp::VectorOf { input, .. }
            | RuntimeOp::Instant { input, .. }
            | RuntimeOp::Merge { input, .. } => input.set_annotations(collector),
            RuntimeOp::Binary {
                left,
                right,
                binary,
                ..
            } => {
                binary.annotations = Some(collector.clone());
                left.set_annotations(collector.clone());
                right.set_annotations(collector);
            }
            RuntimeOp::Broadcast {
                left,
                right,
                broadcast,
                ..
            } => {
                broadcast.annotations = Some(collector.clone());
                left.set_annotations(collector.clone());
                right.set_annotations(collector);
            }
            RuntimeOp::Aggregate {
                input, aggregate, ..
            } => {
                aggregate.set_annotations(collector.clone());
                input.set_annotations(collector);
            }
            RuntimeOp::ParameterizedAggregate {
                input,
                parameter,
                aggregate,
                ..
            } => {
                aggregate.set_annotations(collector.clone());
                input.set_annotations(collector.clone());
                if let Some(parameter) = parameter {
                    parameter.set_annotations(collector);
                }
            }
            RuntimeOp::Timestamp { input, .. } => input.set_annotations(collector),
            RuntimeOp::Vector { input, vector, .. } => {
                vector.annotations = Some(collector.clone());
                input.set_annotations(collector);
            }
            RuntimeOp::ScalarOf { input, .. } => input.set_annotations(collector),
            RuntimeOp::ParameterizedTopK {
                input,
                parameter,
                vector,
                ..
            } => {
                vector.annotations = Some(collector.clone());
                input.set_annotations(collector.clone());
                if let Some(parameter) = parameter {
                    parameter.set_annotations(collector);
                }
            }
            RuntimeOp::ParameterizedHistogram {
                input,
                parameters,
                vector,
                ..
            } => {
                vector.annotations = Some(collector.clone());
                input.set_annotations(collector.clone());
                for parameter in parameters.iter_mut().flatten() {
                    parameter.set_annotations(collector.clone());
                }
            }
            RuntimeOp::Input(_) | RuntimeOp::Scalar | RuntimeOp::EvalTime { .. } => {}
        }
    }

    /// Pulls this node's next chunk, pulling its children as needed. `inputs_finished` says no
    /// more source chunks will arrive, so a node that needs its whole input can tell "nothing more"
    /// from "nothing yet". Grows the stack as it recurses ([`NATIVE_STACK_RED_ZONE`]).
    fn pull(
        &mut self,
        inputs: &mut [InputSlot],
        inputs_finished: bool,
    ) -> Result<Option<NativeChunk>> {
        stacker::maybe_grow(NATIVE_STACK_RED_ZONE, NATIVE_STACK_GROWTH, || {
            self.pull_within_stack(inputs, inputs_finished)
        })
    }

    fn pull_within_stack(
        &mut self,
        inputs: &mut [InputSlot],
        inputs_finished: bool,
    ) -> Result<Option<NativeChunk>> {
        match &mut self.op {
            RuntimeOp::Input(id) => {
                let input = *id;
                let count = inputs.len();
                let slot = inputs
                    .get_mut(input.index())
                    .ok_or_else(|| no_input(count, input))?;
                slot.pop_source()
            }
            // A constant has no chunk: consumers read the value off their own binding.
            RuntimeOp::Scalar => Ok(None),
            RuntimeOp::ScalarOf {
                input,
                input_schema,
                grid,
                retained,
                produced,
                state,
            } => {
                while let Some(chunk) = input.pull(inputs, inputs_finished)? {
                    let NativeChunk::Evaluated(chunk) = chunk else {
                        return Err(DataFusionError::Execution(
                            "scalar() consumes evaluated chunks, not raw samples".into(),
                        ));
                    };
                    validate_binary_operand(input_schema, std::slice::from_ref(&chunk.series))
                        .map_err(|error| {
                            DataFusionError::Execution(format!(
                                "scalar() rejected its input: {error}"
                            ))
                        })?;
                    let pending = retained.pending(&chunk);
                    state.buffer.grow(pending).map_err(|e| state.context(e))?;
                    retained
                        .retain(chunk, Reader::Vector)
                        .map_err(|e| state.context(e))?;
                    state
                        .buffer
                        .resize(retained.held)
                        .map_err(|e| state.context(e))?;
                }
                if !inputs_finished || !input.settled(inputs) {
                    return Ok(None);
                }
                if !*produced {
                    let len = grid.spec().len as usize;
                    let bytes = grid_chunk_bytes(grid).map_err(|e| state.context(e))?;
                    // The kernel's per-point sample counts.
                    let counts_bytes = len.saturating_mul(std::mem::size_of::<u8>());
                    let assembly = retained
                        .assembly_bytes(input_schema)
                        .ok_or_else(vector_overflow)?;
                    state
                        .buffer
                        .grow(assembly.saturating_add(bytes).saturating_add(counts_bytes))
                        .map_err(|e| state.context(e))?;
                    let assembled = retained
                        .assemble(input_schema, Reader::Vector)
                        .map_err(|e| state.context(e))?;
                    retained.release();
                    // As Prometheus' `funcScalar`: NaN where a point does not hold exactly one
                    // float sample; histogram samples are ignored.
                    let answer =
                        select::scalar(input_schema, &assembled).map_err(|e| state.context(e))?;
                    drop(assembled);
                    state.buffer.resize(bytes).map_err(|e| state.context(e))?;
                    let schema = scalar_schema(Arc::clone(grid));
                    let series =
                        Series::try_new(&schema, answer.metric, answer.time, answer.fields)
                            .map_err(series_error)?;
                    state.queued.push_back((
                        NativeChunk::Evaluated(EvaluatedChunk::try_new(series, true)?),
                        bytes,
                    ));
                    *produced = true;
                }
                state.pop_queued(evaluated_values)
            }
            RuntimeOp::EvalTime {
                grid,
                date,
                produced,
                state,
            } => {
                if !std::mem::replace(produced, true) {
                    // `time()` and the date functions materialize the grid's timestamps.
                    state
                        .buffer
                        .reserve_grid(grid)
                        .map_err(|error| state.context(error))?;
                    let schema = scalar_schema(Arc::clone(grid));
                    state
                        .produce(grid, || match date {
                            None => map_time::time(&schema),
                            Some(date) => map_time::date_of_eval_time(*date, &schema),
                        })
                        .map_err(|error| state.context(error))?;
                }
                state.pop_queued(evaluated_values)
            }
            RuntimeOp::VectorOf {
                input,
                literal,
                produced,
                schema,
                state,
            } => {
                if let Some(value) = literal {
                    if !std::mem::replace(produced, true) {
                        let grid = schema.eval_grid.clone().ok_or_else(|| {
                            DataFusionError::Execution(
                                "vector() produces values without a grid".to_string(),
                            )
                        })?;
                        state
                            .produce(&grid, || {
                                Ok(scalar_chunk_on(schema, Arc::clone(&grid), *value)?.series)
                            })
                            .map_err(|error| state.context(error))?;
                    }
                    return state.pop_queued(evaluated_values);
                }
                // A scalar is already one unlabelled series; only the schema changes.
                let Some(chunk) = input.pull(inputs, inputs_finished)? else {
                    return Ok(None);
                };
                let NativeChunk::Evaluated(EvaluatedChunk { series, is_last }) = chunk else {
                    return Err(DataFusionError::Execution(
                        "vector() consumes an evaluated scalar, not raw samples".to_string(),
                    ));
                };
                ensure_grid(schema.eval_grid.as_deref(), &series.time)?;
                let series = Series::try_new(schema, series.metric, series.time, series.fields)
                    .map_err(series_error)?;
                Ok(Some(NativeChunk::Evaluated(EvaluatedChunk::try_new(
                    series, is_last,
                )?)))
            }
            RuntimeOp::Instant {
                input,
                instant,
                state,
            } => {
                let selection = instant;
                // Only a series' closing chunk yields output, so keep pulling until one does.
                loop {
                    let Some(chunk) = input.pull(inputs, inputs_finished)? else {
                        return Ok(None);
                    };
                    if let Some(output) = state
                        .instant(selection, chunk)
                        .map_err(|error| state.context(error))?
                    {
                        return Ok(Some(NativeChunk::Evaluated(output)));
                    }
                }
            }
            RuntimeOp::Merge {
                input,
                merge,
                state,
            } => {
                if let Some(chunk) = state.take_output()? {
                    return Ok(Some(chunk));
                }
                while let Some(chunk) = input.pull(inputs, inputs_finished)? {
                    let NativeChunk::Evaluated(chunk) = chunk else {
                        return Err(DataFusionError::Execution(
                            "merge consumes evaluated samples, not raw samples".into(),
                        ));
                    };
                    state.collect_merge(
                        merge.as_mut().ok_or_else(|| {
                            DataFusionError::Execution("merge buffer was released".into())
                        })?,
                        chunk,
                    )?;
                }
                Ok(None)
            }
            RuntimeOp::Window {
                input,
                window,
                merge,
                state,
            } => {
                // Chunks queued by a flush of collected input go first.
                if let Some(chunk) = state.take_output().map_err(|error| state.context(error))? {
                    return Ok(Some(chunk));
                }
                let Some(evaluator) = window.as_mut() else {
                    return Err(no_window());
                };
                // A chunk that decides no point yields nothing; keep pulling.
                loop {
                    let Some(chunk) = input.pull(inputs, inputs_finished)? else {
                        return Ok(None);
                    };
                    if let Some(output) = state
                        .window(evaluator, merge, chunk)
                        .map_err(|error| state.context(error))?
                    {
                        return Ok(Some(NativeChunk::Evaluated(output)));
                    }
                }
            }
            RuntimeOp::Map { input, map, state } => {
                let operation = map;
                // A chunk the operation empties entirely yields nothing; keep pulling.
                loop {
                    let Some(chunk) = input.pull(inputs, inputs_finished)? else {
                        return Ok(None);
                    };
                    let NativeChunk::Evaluated(chunk) = chunk else {
                        return Err(DataFusionError::Execution(
                            "the map node consumes an evaluated chunk, not raw samples".to_string(),
                        ));
                    };
                    if let Some(output) = state
                        .map(operation, chunk)
                        .map_err(|error| state.context(error))?
                    {
                        return Ok(Some(NativeChunk::Evaluated(output)));
                    }
                }
            }
            RuntimeOp::ParameterizedMap {
                input,
                parameters,
                map,
                state,
            } => {
                let operation = map;
                // Read every parameter whole first; a constant one is built from its binding.
                for (index, parameter) in parameters.iter_mut().enumerate() {
                    let slot = &mut operation.values[index];
                    if slot.is_some() {
                        continue;
                    }
                    if let Some(value) = operation.literals[index] {
                        let grid = operation.schema.eval_grid.clone().ok_or_else(|| {
                            DataFusionError::Execution(
                                "a map produces values without a grid".to_string(),
                            )
                        })?;
                        state
                            .hold_constant(&mut operation.parameter_bytes, slot, grid, value)
                            .map_err(|error| state.context(error))?;
                        continue;
                    }
                    while let Some(chunk) = parameter.pull(inputs, inputs_finished)? {
                        state
                            .hold_scalar(&mut operation.parameter_bytes, slot, chunk)
                            .map_err(|error| state.context(error))?;
                    }
                }
                if !operation.ready() {
                    // A parameter like `scalar(selector)` answers only after its input ends.
                    if !inputs_finished
                        || !parameters.iter().all(|parameter| parameter.settled(inputs))
                    {
                        return Ok(None);
                    }
                    return Err(DataFusionError::Execution(
                        "a map parameter did not hand over its value".to_string(),
                    ));
                }
                // A constant input has no chunk of its own; build it on the map's grid.
                if let Some(value) = operation.input_literal
                    && !std::mem::replace(&mut operation.input_produced, true)
                {
                    let grid = operation.schema.eval_grid.clone().ok_or_else(|| {
                        DataFusionError::Execution(
                            "a map produces values without a grid".to_string(),
                        )
                    })?;
                    let mut slot = None;
                    state
                        .hold_constant(&mut operation.parameter_bytes, &mut slot, grid, value)
                        .map_err(|error| state.context(error))?;
                    let series = slot.expect("the constant input was just built");
                    let chunk = EvaluatedChunk::try_new(series, true)
                        .map_err(|error| state.context(error))?;
                    return state
                        .parameterized_map(operation, chunk)
                        .map_err(|error| state.context(error))
                        .map(|output| output.map(NativeChunk::Evaluated));
                }
                loop {
                    let chunk = match input.pull(inputs, inputs_finished)? {
                        Some(NativeChunk::Evaluated(chunk)) => chunk,
                        Some(NativeChunk::Raw(_)) => {
                            return Err(DataFusionError::Execution(
                                "the map node consumes an evaluated chunk, not raw samples"
                                    .to_string(),
                            ));
                        }
                        Some(NativeChunk::AggregateState(_)) => {
                            return Err(DataFusionError::Execution(
                                "an aggregate state chunk reached a node that reads values or samples"
                                    .to_string(),
                            ));
                        }
                        None => return Ok(None),
                    };
                    if let Some(output) = state
                        .parameterized_map(operation, chunk)
                        .map_err(|error| state.context(error))?
                    {
                        return Ok(Some(NativeChunk::Evaluated(output)));
                    }
                }
            }
            RuntimeOp::Timestamp {
                input,
                schema,
                state,
            } => loop {
                let Some(chunk) = input.pull(inputs, inputs_finished)? else {
                    return Ok(None);
                };
                let NativeChunk::Evaluated(chunk) = chunk else {
                    return Err(DataFusionError::Execution(
                        "timestamp consumes evaluated samples, not raw samples".into(),
                    ));
                };
                if let Some(output) = state
                    .timestamp(schema, chunk)
                    .map_err(|error| state.context(error))?
                {
                    return Ok(Some(NativeChunk::Evaluated(output)));
                }
            },
            RuntimeOp::Binary {
                left,
                right,
                binary,
                state,
            } => {
                let operation = binary;
                // Retain everything both operands hand over; nothing is decided until both are
                // whole.
                while let Some(chunk) = left.pull(inputs, inputs_finished)? {
                    state
                        .retain_binary(operation, 0, chunk)
                        .map_err(|error| state.context(error))?;
                }
                while let Some(chunk) = right.pull(inputs, inputs_finished)? {
                    state
                        .retain_binary(operation, 1, chunk)
                        .map_err(|error| state.context(error))?;
                }
                // An empty pull is not the end: a collecting window below may not have flushed.
                if !inputs_finished || !left.settled(inputs) || !right.settled(inputs) {
                    return Ok(None);
                }
                state
                    .compute_binary(operation)
                    .map_err(|error| state.context(error))?;
                state.pop_queued(evaluated_values)
            }
            RuntimeOp::Broadcast {
                left,
                right,
                broadcast,
                state,
            } => {
                let operation = broadcast;
                // A dynamic scalar is read whole first; then each vector chunk is mapped as it
                // arrives.
                let (vector, scalar) = if operation.vector_left {
                    (&mut **left, &mut **right)
                } else {
                    (&mut **right, &mut **left)
                };
                if let BroadcastScalar::Dynamic { value, .. } = &mut operation.scalar
                    && value.is_none()
                {
                    while let Some(chunk) = scalar.pull(inputs, inputs_finished)? {
                        state
                            .hold_scalar(&mut operation.scalar_bytes, value, chunk)
                            .map_err(|error| state.context(error))?;
                    }
                }
                if operation.awaits_scalar() {
                    if !inputs_finished || !scalar.settled(inputs) {
                        return Ok(None);
                    }
                    return Err(DataFusionError::Execution(
                        "a broadcast scalar did not hand over its value".to_string(),
                    ));
                }
                // A chunk the operation empties entirely yields nothing; keep pulling.
                loop {
                    let Some(chunk) = vector.pull(inputs, inputs_finished)? else {
                        return Ok(None);
                    };
                    if let Some(output) = state
                        .broadcast(operation, chunk)
                        .map_err(|error| state.context(error))?
                    {
                        return Ok(Some(NativeChunk::Evaluated(output)));
                    }
                }
            }
            RuntimeOp::Aggregate {
                input,
                aggregate,
                state,
            } => {
                let fold = aggregate;
                // Nothing is answered before the input ends.
                while let Some(chunk) = input.pull(inputs, inputs_finished)? {
                    state
                        .retain_aggregate(fold, chunk)
                        .map_err(|error| state.context(error))?;
                }
                if !inputs_finished || !input.settled(inputs) {
                    return Ok(None);
                }
                state
                    .compute_aggregate(fold)
                    .map_err(|error| state.context(error))?;
                state.pop_queued(evaluated_values)
            }
            RuntimeOp::ParameterizedAggregate {
                input,
                parameter,
                aggregate,
                state,
            } => {
                let fold = aggregate;
                // φ is read whole before the fold can answer.
                if let Some(parameter_node) = parameter.as_mut() {
                    let mut slot = None;
                    let mut held = 0usize;
                    while let Some(chunk) = parameter_node.pull(inputs, inputs_finished)? {
                        state
                            .hold_scalar(&mut held, &mut slot, chunk)
                            .map_err(|error| state.context(error))?;
                    }
                    let series = {
                        // While φ is pending, drain the input into the fold anyway: the source
                        // queue bills each queued slice for its whole allocation, while the fold
                        // bills a shared allocation once.
                        if !parameter_node.settled(inputs) {
                            while let Some(chunk) = input.pull(inputs, inputs_finished)? {
                                state
                                    .retain_aggregate(fold, chunk)
                                    .map_err(|error| state.context(error))?;
                            }
                            return Ok(None);
                        }
                        slot.ok_or_else(|| {
                            DataFusionError::Execution(
                                "the quantile parameter produced no scalar".into(),
                            )
                        })?
                    };
                    fold.hold_parameter(series, &mut state.buffer)
                        .map_err(|error| state.context(error))?;
                    // Run the evaluators' truncation check now: `unclosed` cannot reach this
                    // subtree once it is dropped.
                    parameter_node.finish_windows()?;
                    *parameter = None;
                }
                while let Some(chunk) = input.pull(inputs, inputs_finished)? {
                    state
                        .retain_aggregate(fold, chunk)
                        .map_err(|error| state.context(error))?;
                }
                if !inputs_finished || !input.settled(inputs) {
                    return Ok(None);
                }
                state
                    .compute_aggregate(fold)
                    .map_err(|error| state.context(error))?;
                state.pop_queued(evaluated_values)
            }
            RuntimeOp::ParameterizedTopK {
                input,
                parameter,
                vector,
                state,
            } => {
                let operation = vector;
                if operation.parameter.is_none() {
                    let mut slot = None;
                    let parameter_node = parameter.as_mut().ok_or_else(|| {
                        DataFusionError::Internal("topk parameter already consumed".into())
                    })?;
                    while let Some(chunk) = parameter_node.pull(inputs, inputs_finished)? {
                        state
                            .hold_scalar(&mut operation.parameter_bytes, &mut slot, chunk)
                            .map_err(|error| state.context(error))?;
                    }
                    let series = {
                        if !parameter_node.settled(inputs) {
                            return Ok(None);
                        }
                        slot.ok_or_else(|| {
                            DataFusionError::Execution("topk parameter produced no scalar".into())
                        })?
                    };
                    let grid = operation
                        .schema
                        .eval_grid
                        .as_ref()
                        .ok_or_else(|| DataFusionError::Internal("topk grid missing".into()))?;
                    let len = grid.spec().len as usize;
                    if series.time.len() != len {
                        return Err(DataFusionError::Execution(
                            "topk parameter does not cover its grid".into(),
                        ));
                    }
                    let bytes = len.saturating_mul(std::mem::size_of::<Option<f64>>());
                    state
                        .buffer
                        .grow(bytes)
                        .map_err(|error| state.context(error))?;
                    let mut values = Vec::with_capacity(len);
                    {
                        let mut cursor = series.fields.as_slice()[0].cursor();
                        for row in 0..len {
                            values.push(match cursor.at(row) {
                                Some(SampleRef::Float(value)) => Some(value),
                                _ => None,
                            });
                        }
                    }
                    select::validate_topk_parameters(&values)
                        .map_err(|error| state.context(error))?;
                    operation.parameter = Some(values);
                    drop(series);
                    operation.parameter_bytes = bytes;
                    state
                        .settle_vector(operation)
                        .map_err(|error| state.context(error))?;
                    // Run the evaluators' truncation check now: `unclosed` cannot reach this
                    // subtree once it is dropped.
                    parameter_node.finish_windows()?;
                    *parameter = None;
                }
                while let Some(chunk) = input.pull(inputs, inputs_finished)? {
                    state
                        .retain_vector(operation, chunk)
                        .map_err(|error| state.context(error))?;
                }
                if !inputs_finished || !input.settled(inputs) {
                    return Ok(None);
                }
                state
                    .compute_vector(operation)
                    .map_err(|error| state.context(error))?;
                state.pop_queued(evaluated_values)
            }
            RuntimeOp::ParameterizedHistogram {
                input,
                parameters,
                literals,
                vector,
                values,
                parameter_bytes,
                state,
            } => {
                let operation = vector;
                for index in 0..parameters.len() {
                    if values[index].is_none() {
                        let mut slot = None;
                        if let Some(value) = literals[index] {
                            let grid = operation.schema.eval_grid.clone().ok_or_else(|| {
                                DataFusionError::Internal("histogram grid missing".into())
                            })?;
                            state.hold_constant(
                                &mut parameter_bytes[index],
                                &mut slot,
                                grid,
                                value,
                            )?;
                        } else {
                            let parameter = parameters[index].as_mut().ok_or_else(|| {
                                DataFusionError::Internal(
                                    "histogram parameter already consumed".into(),
                                )
                            })?;
                            while let Some(chunk) = parameter.pull(inputs, inputs_finished)? {
                                state.hold_scalar(&mut parameter_bytes[index], &mut slot, chunk)?;
                            }
                            if !parameter.settled(inputs) {
                                return Ok(None);
                            }
                        }
                        let series = slot.ok_or_else(|| {
                            DataFusionError::Execution(
                                "histogram parameter produced no scalar".into(),
                            )
                        })?;
                        let len = operation
                            .schema
                            .eval_grid
                            .as_ref()
                            .ok_or_else(|| {
                                DataFusionError::Internal("histogram grid missing".into())
                            })?
                            .spec()
                            .len as usize;
                        if series.time.len() != len {
                            return Err(DataFusionError::Execution(
                                "histogram parameter does not cover its grid".into(),
                            ));
                        }
                        let bytes = len.saturating_mul(std::mem::size_of::<Option<f64>>());
                        state.buffer.grow(bytes)?;
                        let collected = {
                            let mut cursor = series.fields.as_slice()[0].cursor();
                            (0..len)
                                .map(|row| match cursor.at(row) {
                                    Some(SampleRef::Float(v)) => Some(v),
                                    _ => None,
                                })
                                .collect::<Vec<Option<f64>>>()
                        };
                        values[index] = Some(collected);
                        parameter_bytes[index] = bytes;
                        drop(series);
                        operation.parameter_bytes = parameter_bytes
                            .iter()
                            .copied()
                            .fold(0usize, usize::saturating_add);
                        state.settle_vector(operation)?;
                        if let Some(parameter) = &mut parameters[index] {
                            parameter.finish_windows()?;
                        }
                        parameters[index] = None;
                    }
                }
                while let Some(chunk) = input.pull(inputs, inputs_finished)? {
                    state.retain_vector(operation, chunk)?;
                }
                if !inputs_finished || !input.settled(inputs) {
                    return Ok(None);
                }
                let collected = values
                    .iter()
                    .map(|v| {
                        v.as_deref().ok_or_else(|| {
                            DataFusionError::Internal("histogram parameters not collected".into())
                        })
                    })
                    .collect::<Result<Vec<_>>>()?;
                state.compute_histogram(operation, &collected)?;
                state.pop_queued(evaluated_values)
            }
            RuntimeOp::Vector {
                input,
                vector,
                state,
            } => {
                let operation = vector;
                while let Some(chunk) = input.pull(inputs, inputs_finished)? {
                    state
                        .retain_vector(operation, chunk)
                        .map_err(|error| state.context(error))?;
                }
                if !inputs_finished || !input.settled(inputs) {
                    return Ok(None);
                }
                state
                    .compute_vector(operation)
                    .map_err(|error| state.context(error))?;
                state.pop_queued(evaluated_values)
            }
        }
    }

    /// Whether this node has produced everything it ever will. An empty pull does not imply it: a
    /// collecting window yields nothing until flushed, and a binary node nothing until both
    /// operands are done.
    fn settled(&self, inputs: &[InputSlot]) -> bool {
        match &self.op {
            RuntimeOp::Input(id) => inputs
                .get(id.index())
                .is_some_and(|slot| slot.state.queued.is_empty()),
            RuntimeOp::Scalar => true,
            RuntimeOp::EvalTime {
                produced, state, ..
            } => *produced && state.queued.is_empty(),
            RuntimeOp::ScalarOf {
                input,
                produced,
                retained,
                state,
                ..
            } => {
                *produced && state.queued.is_empty() && retained.held == 0 && input.settled(inputs)
            }
            RuntimeOp::ParameterizedMap {
                input,
                parameters,
                state,
                ..
            } => {
                state.queued.is_empty()
                    && input.settled(inputs)
                    && parameters.iter().all(|parameter| parameter.settled(inputs))
            }
            RuntimeOp::ParameterizedHistogram {
                input,
                vector,
                state,
                ..
            } => {
                // `computed` implies the released parameters were consumed.
                state.queued.is_empty() && vector.computed && input.settled(inputs)
            }
            RuntimeOp::ParameterizedTopK {
                input,
                vector,
                state,
                ..
            } => {
                // `computed` implies the released parameter was consumed.
                state.queued.is_empty() && vector.computed && input.settled(inputs)
            }
            RuntimeOp::VectorOf {
                input,
                literal,
                produced,
                state,
                ..
            } => {
                state.queued.is_empty() && (*produced || literal.is_none()) && input.settled(inputs)
            }
            RuntimeOp::Instant { input, state, .. }
            | RuntimeOp::Map { input, state, .. }
            | RuntimeOp::Timestamp { input, state, .. } => {
                state.queued.is_empty() && input.settled(inputs)
            }
            // A collecting window still holding its merge input owes output.
            RuntimeOp::Window {
                input,
                merge,
                state,
                ..
            } => state.queued.is_empty() && merge.is_none() && input.settled(inputs),
            RuntimeOp::Merge {
                input,
                merge,
                state,
            } => state.queued.is_empty() && merge.is_none() && input.settled(inputs),
            // A settled child is not enough: the fold must also have run.
            RuntimeOp::Aggregate {
                input,
                aggregate,
                state,
            } => state.queued.is_empty() && aggregate.computed && input.settled(inputs),
            // `computed` implies the released parameter was consumed.
            RuntimeOp::ParameterizedAggregate {
                input,
                aggregate,
                state,
                ..
            } => state.queued.is_empty() && aggregate.computed && input.settled(inputs),
            RuntimeOp::Vector {
                input,
                vector,
                state,
            } => state.queued.is_empty() && vector.computed && input.settled(inputs),
            // A vector match is settled once its kernel ran and both operands are: what one side
            // answers at a point depends on the other side.
            RuntimeOp::Binary {
                left,
                right,
                binary,
                state,
            } => {
                state.queued.is_empty()
                    && binary.computed
                    && left.settled(inputs)
                    && right.settled(inputs)
            }
            RuntimeOp::Broadcast {
                left, right, state, ..
            } => state.queued.is_empty() && left.settled(inputs) && right.settled(inputs),
        }
    }

    /// Flushes every collecting window and merge whose input has settled, deepest first, so an
    /// outer one never merges input still held below it. Returns whether anything was flushed.
    fn flush_ready_merges(&mut self, inputs: &[InputSlot]) -> Result<bool> {
        match &mut self.op {
            RuntimeOp::Input(_) | RuntimeOp::Scalar | RuntimeOp::EvalTime { .. } => Ok(false),
            RuntimeOp::ScalarOf { input, .. } => input.flush_ready_merges(inputs),
            RuntimeOp::ParameterizedMap {
                input, parameters, ..
            } => {
                let mut flushed = input.flush_ready_merges(inputs)?;
                for parameter in parameters {
                    flushed |= parameter.flush_ready_merges(inputs)?;
                }
                Ok(flushed)
            }
            RuntimeOp::Instant { input, .. }
            | RuntimeOp::Map { input, .. }
            | RuntimeOp::Timestamp { input, .. }
            | RuntimeOp::Aggregate { input, .. }
            | RuntimeOp::Vector { input, .. }
            | RuntimeOp::VectorOf { input, .. } => input.flush_ready_merges(inputs),
            RuntimeOp::ParameterizedHistogram {
                input, parameters, ..
            } => {
                let mut flushed = input.flush_ready_merges(inputs)?;
                for p in parameters.iter_mut().flatten() {
                    flushed |= p.flush_ready_merges(inputs)?;
                }
                Ok(flushed)
            }
            RuntimeOp::ParameterizedTopK {
                input, parameter, ..
            }
            | RuntimeOp::ParameterizedAggregate {
                input, parameter, ..
            } => {
                let mut flushed = input.flush_ready_merges(inputs)?;
                if let Some(parameter) = parameter {
                    flushed |= parameter.flush_ready_merges(inputs)?;
                }
                Ok(flushed)
            }
            RuntimeOp::Binary { left, right, .. } | RuntimeOp::Broadcast { left, right, .. } => {
                let left_flushed = left.flush_ready_merges(inputs)?;
                let right_flushed = right.flush_ready_merges(inputs)?;
                Ok(left_flushed || right_flushed)
            }
            RuntimeOp::Merge {
                input,
                merge,
                state,
            } => {
                let mut flushed = input.flush_ready_merges(inputs)?;
                if merge.is_some() && input.settled(inputs) {
                    state.flush_canonical_merge(merge)?;
                    flushed = true;
                }
                Ok(flushed)
            }
            RuntimeOp::Window {
                input,
                window,
                merge,
                state,
            } => {
                let mut flushed = input.flush_ready_merges(inputs)?;
                if merge.is_some() && input.settled(inputs) {
                    let Some(evaluator) = window.as_mut() else {
                        return Err(no_window());
                    };
                    state
                        .flush_merge(evaluator, merge)
                        .map_err(|error| state.context(error))?;
                    flushed = true;
                }
                Ok(flushed)
            }
        }
    }

    /// Whether any window still holds collected input that has not been merged.
    fn any_merge_pending(&self) -> bool {
        match &self.op {
            RuntimeOp::Input(_) | RuntimeOp::Scalar | RuntimeOp::EvalTime { .. } => false,
            RuntimeOp::ScalarOf { input, .. } => input.any_merge_pending(),
            RuntimeOp::ParameterizedMap {
                input, parameters, ..
            } => input.any_merge_pending() || parameters.iter().any(RuntimeNode::any_merge_pending),
            RuntimeOp::Instant { input, .. }
            | RuntimeOp::Map { input, .. }
            | RuntimeOp::Timestamp { input, .. }
            | RuntimeOp::Aggregate { input, .. }
            | RuntimeOp::Vector { input, .. }
            | RuntimeOp::VectorOf { input, .. } => input.any_merge_pending(),
            RuntimeOp::ParameterizedHistogram {
                input, parameters, ..
            } => {
                input.any_merge_pending()
                    || parameters
                        .iter()
                        .flatten()
                        .any(RuntimeNode::any_merge_pending)
            }
            RuntimeOp::ParameterizedTopK {
                input, parameter, ..
            }
            | RuntimeOp::ParameterizedAggregate {
                input, parameter, ..
            } => {
                input.any_merge_pending()
                    || parameter.as_ref().is_some_and(|p| p.any_merge_pending())
            }
            RuntimeOp::Binary { left, right, .. } | RuntimeOp::Broadcast { left, right, .. } => {
                left.any_merge_pending() || right.any_merge_pending()
            }
            RuntimeOp::Window { input, merge, .. } => merge.is_some() || input.any_merge_pending(),
            RuntimeOp::Merge { input, merge, .. } => merge.is_some() || input.any_merge_pending(),
        }
    }

    /// Refuses input that ended with a series still open, in every node that retains whole series;
    /// otherwise half a series would be read as a complete one.
    fn unclosed(&self) -> Result<()> {
        match &self.op {
            RuntimeOp::Input(_) | RuntimeOp::Scalar | RuntimeOp::EvalTime { .. } => Ok(()),
            RuntimeOp::ScalarOf {
                input,
                retained,
                state,
                ..
            } => {
                retained
                    .ensure_closed(Reader::Vector)
                    .map_err(|error| state.context(error))?;
                input.unclosed()
            }
            RuntimeOp::ParameterizedMap {
                input, parameters, ..
            } => {
                input.unclosed()?;
                parameters.iter().try_for_each(RuntimeNode::unclosed)
            }
            RuntimeOp::Instant { input, .. }
            | RuntimeOp::Map { input, .. }
            | RuntimeOp::Timestamp { input, .. }
            | RuntimeOp::Window { input, .. }
            | RuntimeOp::Aggregate { input, .. }
            | RuntimeOp::VectorOf { input, .. }
            | RuntimeOp::Merge { input, .. } => input.unclosed(),
            RuntimeOp::ParameterizedTopK {
                input,
                parameter,
                vector,
                state,
            } => {
                vector
                    .input
                    .ensure_closed(Reader::Vector)
                    .map_err(|e| state.context(e))?;
                input.unclosed()?;
                if let Some(parameter) = parameter {
                    parameter.unclosed()?;
                }
                Ok(())
            }
            // Only the children are checked: the fold's retained input does not track open series.
            RuntimeOp::ParameterizedAggregate {
                input, parameter, ..
            } => {
                input.unclosed()?;
                if let Some(parameter) = parameter {
                    parameter.unclosed()?;
                }
                Ok(())
            }
            RuntimeOp::ParameterizedHistogram {
                input,
                parameters,
                vector,
                state,
                ..
            } => {
                vector
                    .input
                    .ensure_closed(Reader::Vector)
                    .map_err(|e| state.context(e))?;
                input.unclosed()?;
                parameters
                    .iter()
                    .flatten()
                    .try_for_each(RuntimeNode::unclosed)
            }
            RuntimeOp::Binary {
                left,
                right,
                binary,
                state,
            } => {
                binary.closed().map_err(|error| state.context(error))?;
                left.unclosed()?;
                right.unclosed()
            }
            RuntimeOp::Broadcast { left, right, .. } => {
                left.unclosed()?;
                right.unclosed()
            }
            RuntimeOp::Vector {
                input,
                vector,
                state,
            } => {
                vector
                    .input
                    .ensure_closed(Reader::Vector)
                    .map_err(|error| state.context(error))?;
                input.unclosed()
            }
        }
    }

    /// Finishes every window evaluator still present in this subtree. All are finished even after
    /// one fails, so none keeps its bytes; the first error is returned.
    fn finish_windows(&mut self) -> Result<()> {
        match &mut self.op {
            RuntimeOp::Input(_) | RuntimeOp::Scalar | RuntimeOp::EvalTime { .. } => Ok(()),
            RuntimeOp::ScalarOf { input, .. } => input.finish_windows(),
            RuntimeOp::ParameterizedMap {
                input, parameters, ..
            } => {
                let first = input.finish_windows();
                parameters
                    .iter_mut()
                    .map(RuntimeNode::finish_windows)
                    .fold(first, Result::and)
            }
            RuntimeOp::Instant { input, .. }
            | RuntimeOp::Map { input, .. }
            | RuntimeOp::Timestamp { input, .. }
            | RuntimeOp::Aggregate { input, .. }
            | RuntimeOp::Vector { input, .. }
            | RuntimeOp::VectorOf { input, .. }
            | RuntimeOp::Merge { input, .. } => input.finish_windows(),
            RuntimeOp::ParameterizedTopK {
                input, parameter, ..
            }
            | RuntimeOp::ParameterizedAggregate {
                input, parameter, ..
            } => {
                let first = input.finish_windows();
                let parameter = parameter.as_mut().map_or(Ok(()), |p| p.finish_windows());
                first.and(parameter)
            }
            RuntimeOp::ParameterizedHistogram {
                input, parameters, ..
            } => {
                let first = input.finish_windows();
                parameters
                    .iter_mut()
                    .flatten()
                    .map(RuntimeNode::finish_windows)
                    .fold(first, Result::and)
            }
            RuntimeOp::Binary { left, right, .. } | RuntimeOp::Broadcast { left, right, .. } => {
                let first = left.finish_windows();
                let second = right.finish_windows();
                first.and(second)
            }
            RuntimeOp::Window {
                input,
                window,
                state,
                ..
            } => {
                let child = input.finish_windows();
                let own = window
                    .take()
                    .map_or(Ok(()), |evaluator| {
                        state.buffer.reserve_grid(evaluator.grid())?;
                        RangeEvaluator::finish(evaluator)
                    })
                    .map_err(|error| state.context(error));
                child.and(own)
            }
        }
    }
}

/// Input label sets and metric names used to decide when series must be merged.
#[derive(Clone, Debug, Default)]
pub struct SourceFacts {
    /// The complete label set of each input, indexed by leaf `source_index`.
    tags: Vec<Vec<String>>,
    /// The one metric each input is bound to, indexed the same way; `None` when it may have
    /// several.
    metrics: Vec<Option<Arc<str>>>,
}

impl SourceFacts {
    /// No facts; every decision stays conservative.
    pub fn none() -> Self {
        Self::default()
    }

    /// The same tags and metric for each of `sources` inputs, for a statement reading one table.
    /// With `metric = None` nothing can be proven.
    pub fn uniform(tags: Vec<String>, metric: Option<Arc<str>>, sources: usize) -> Self {
        Self {
            tags: vec![tags; sources],
            metrics: vec![metric; sources],
        }
    }
}

impl NativeExecutionState {
    pub fn try_new(
        tree: &NativeTree,
        pool: Arc<dyn MemoryPool>,
        duplicate_evidence: Option<Arc<DuplicateEvidence>>,
    ) -> Result<Self> {
        Self::try_new_with_sources(tree, pool, duplicate_evidence, &SourceFacts::none())
    }

    /// Builds the execution over `tree`. `sources` must not prove more than the canonical pass
    /// assumed when it placed its merges; [`SourceFacts::none`] is always safe.
    pub fn try_new_with_sources(
        tree: &NativeTree,
        pool: Arc<dyn MemoryPool>,
        duplicate_evidence: Option<Arc<DuplicateEvidence>>,
        sources: &SourceFacts,
    ) -> Result<Self> {
        let mut inputs = Vec::new();
        let mut binary_before = 0usize;
        let root = RuntimeNode::build(
            tree.root(),
            &pool,
            &mut inputs,
            &mut binary_before,
            "root".to_string(),
            duplicate_evidence.clone(),
            sources,
        )?;
        let scalar_root = match (&tree.root().expr, &tree.root().output) {
            (NativeExpr::Scalar(BoundScalar { value }), NativeType::Scalar(grid)) => Some((
                Arc::clone(grid),
                *value,
                NodeState::new(
                    "native.scalar at root".to_string(),
                    Arc::clone(&root.metrics),
                    &pool,
                ),
            )),
            _ => None,
        };
        match &tree.root().output {
            NativeType::Scalar(_) => {}
            NativeType::Samples(schema) if schema.eval_grid.is_some() => {}
            NativeType::Samples(_) => {
                return Err(DataFusionError::Execution(
                    "native runtime root produces raw samples".to_string(),
                ));
            }
            // A `Partial` aggregate root; read through `NativeExecutor::next_state`.
            NativeType::AggregateState(_) => {}
        }
        Ok(Self {
            live: Some(LiveExecution {
                root,
                inputs,
                scalar_root,
            }),
        })
    }

    pub fn set_annotations(&mut self, collector: Option<PromqlAnnotationCollector>) {
        if let (Some(live), Some(collector)) = (&mut self.live, collector) {
            live.root.set_annotations(collector);
        }
    }

    /// Registers every node's metric handles with `set`. Call once, before the execution starts.
    pub(crate) fn register_metrics(&self, set: &ExecutionPlanMetricsSet, partition: usize) {
        if let Some(live) = &self.live {
            live.root.register_metrics("root", set, partition);
        }
    }

    /// Returns the runtime input slots bound to `source_index`.
    pub fn input_ids(&self, source_index: usize) -> impl Iterator<Item = InputId> + '_ {
        self.live
            .iter()
            .flat_map(|live| live.inputs.iter())
            .enumerate()
            .filter(move |(_, slot)| slot.source_index == source_index)
            .map(|(index, _)| InputId::new(index))
    }

    pub fn reserved_bytes(&self) -> usize {
        self.live.as_ref().map_or(0, |live| {
            live.inputs
                .iter()
                .map(|slot| slot.state.buffer.reservation.size())
                .fold(0usize, usize::saturating_add)
                .saturating_add(live.root.reserved_bytes())
                .saturating_add(
                    live.scalar_root
                        .as_ref()
                        .map_or(0, |(_, _, state)| state.buffer.reservation.size()),
                )
        })
    }
}

impl LiveExecution {
    /// Pulls the root's next chunk; a constant root is produced here on its first pull.
    fn pull(&mut self, inputs_finished: bool) -> Result<Option<NativeChunk>> {
        if let Some((grid, value, mut state)) = self.scalar_root.take() {
            let bytes = grid_chunk_bytes(&grid).map_err(|error| state.context(error))?;
            state
                .buffer
                .grow(bytes)
                .map_err(|error| state.context(error))?;
            let chunk = scalar_chunk(grid, value).map_err(|error| state.context(error))?;
            state.buffer.metrics.record_completed_series(1);
            state
                .queued
                .push_back((NativeChunk::Evaluated(chunk), bytes));
            return state.pop_queued(evaluated_values);
        }
        self.root.pull(&mut self.inputs, inputs_finished)
    }

    /// Refuses an input left with an open series. Input slots are checked first, so the error names
    /// the input rather than a node that retained it.
    fn unclosed(&self) -> Result<()> {
        if let Some(index) = self.inputs.iter().position(InputSlot::ends_open) {
            let slot = &self.inputs[index];
            return Err(slot.state.context(truncated_input(InputId::new(index))));
        }
        self.root.unclosed()
    }

    fn flush_ready_merges(&mut self) -> Result<bool> {
        self.root.flush_ready_merges(&self.inputs)
    }

    fn any_merge_pending(&self) -> bool {
        self.root.any_merge_pending()
    }

    /// Finishes every window before dropping the live tree and its reservations.
    fn close(mut self) -> Result<()> {
        self.root.finish_windows()
    }
}

impl NodeState {
    /// Queues a source chunk, reserving its [`NativeChunk::bytes`] first so a refused chunk leaves
    /// nothing behind.
    fn enqueue(&mut self, chunk: NativeChunk) -> Result<()> {
        let positions = chunk.positions();
        let bytes = chunk.bytes();
        self.buffer.grow(bytes)?;
        self.queued.push_back((chunk, bytes));
        self.buffer.metrics.record_input(1, positions);
        Ok(())
    }

    /// Hands over the next queued chunk and releases its charge, recording output metrics; `values`
    /// counts the chunk's values (positions for a source, valid values for a result). A state
    /// chunk takes its charge with it. [`Self::take_output`] is the metric-free variant used for
    /// flushed window and merge output.
    fn pop_queued(&mut self, values: fn(&NativeChunk) -> usize) -> Result<Option<NativeChunk>> {
        let Some((mut chunk, bytes)) = self.queued.pop_front() else {
            return Ok(None);
        };
        // The consumer keeps a state chunk across polls, so its charge moves with it.
        let moved = match &mut chunk {
            NativeChunk::AggregateState(StateChunk::Series { reservation, .. }) => {
                debug_assert!(
                    reservation.is_none(),
                    "a handed-over state chunk was charged to a queue twice"
                );
                let transferred = self.buffer.transfer(bytes)?;
                let moved = transferred.size();
                *reservation = Some(transferred);
                moved
            }
            _ => 0,
        };
        let metrics = Arc::clone(&self.buffer.metrics);
        {
            let _timer = metrics.timer();
            self.buffer.shrink(bytes - moved)?;
            self.release_queued_tables()?;
        }
        metrics.record_output(1, values(&chunk));
        // Raw timestamps are not grid points.
        if let NativeChunk::Evaluated(chunk) = &chunk {
            metrics.record_grid_points(chunk.series.time.len());
        }
        Ok(Some(chunk))
    }

    /// Maps one evaluated chunk, keeping its closing. `None` means every position was emptied and
    /// the chunk does not close its series.
    fn map(&mut self, map: &MapRuntime, chunk: EvaluatedChunk) -> Result<Option<EvaluatedChunk>> {
        let (positions, closing) = (chunk.series.time.len(), chunk.is_last);
        let workspace = map_workspace_bytes(&map.ops, &chunk.series)?;
        self.buffer.grow(evaluated_bytes(&chunk))?;
        self.buffer.grow(workspace)?;
        let metrics = Arc::clone(&self.buffer.metrics);
        let outcome = {
            let _timer = metrics.timer();
            map_chunk(&map.schema, &chunk, &map.ops, &mut |bytes| {
                self.buffer.grow(bytes)
            })
        };
        metrics.record_input(1, positions);
        if closing {
            metrics.record_completed_series(1);
        }
        // On failure give back the chunk and workspace now, not when the stopped executor drops.
        let output = match outcome {
            Ok(output) => output,
            Err(error) => {
                self.buffer.resize(0)?;
                return Err(error);
            }
        };
        if let Some(output) = &output {
            metrics.record_grid_points(output.series.time.len());
            metrics.record_output(1, valid_values(output));
        }
        self.buffer.resize(0)?;
        Ok(output)
    }

    /// Maps one evaluated chunk with its parameters read per point. `None` means no point kept a
    /// value and the chunk does not close its series.
    fn parameterized_map(
        &mut self,
        map: &ParameterizedMapRuntime,
        chunk: EvaluatedChunk,
    ) -> Result<Option<EvaluatedChunk>> {
        let (positions, closing) = (chunk.series.time.len(), chunk.is_last);
        let workspace = built_series_workspace_bytes(&chunk.series)?;
        self.buffer.grow(evaluated_bytes(&chunk))?;
        self.buffer.grow(workspace)?;
        let metrics = Arc::clone(&self.buffer.metrics);
        let outcome = {
            let _timer = metrics.timer();
            map.apply(&chunk)
        };
        metrics.record_input(1, positions);
        if closing {
            metrics.record_completed_series(1);
        }
        let outcome = match outcome {
            Ok(outcome) => outcome,
            Err(error) => {
                self.buffer.resize(map.parameter_bytes)?;
                return Err(error);
            }
        };
        let output = match outcome {
            Some(series) => Some(EvaluatedChunk::try_new(series, closing)?),
            // A closing chunk is always handed over so the close reaches the consumer.
            None if closing => {
                let metric = drop_metric_name(&chunk.series.metric)?;
                let empty = no_row_series(&map.schema, &chunk.series, metric)?;
                Some(EvaluatedChunk::try_new(empty, true)?)
            }
            None => None,
        };
        if let Some(output) = &output {
            metrics.record_grid_points(output.series.time.len());
            metrics.record_output(1, valid_values(output));
        }
        self.buffer.resize(map.parameter_bytes)?;
        Ok(output)
    }

    /// Projects evaluation timestamps from one evaluated chunk, preserving its closing.
    fn timestamp(
        &mut self,
        schema: &Arc<SeriesSchema>,
        chunk: EvaluatedChunk,
    ) -> Result<Option<EvaluatedChunk>> {
        let positions = chunk.series.time.len();
        let closing = chunk.is_last;
        // The kernel materializes the grid's timestamps.
        if let Some(grid) = schema.eval_grid.as_deref() {
            self.buffer.reserve_grid(grid)?;
        }
        let workspace = built_series_workspace_bytes(&chunk.series)?;
        self.buffer.grow(evaluated_bytes(&chunk))?;
        self.buffer.grow(workspace)?;
        let labels = drop_metric_name(&chunk.series.metric)?;
        let metrics = Arc::clone(&self.buffer.metrics);
        let result = {
            let _timer = metrics.timer();
            timestamp_of_evaluated(schema, &chunk.series)
        };
        metrics.record_input(1, positions);
        if closing {
            metrics.record_completed_series(1);
        }
        let result = match result {
            Ok(result) => result,
            Err(error) => {
                self.buffer.resize(0)?;
                return Err(error);
            }
        };
        let output = match result {
            Some(series) => Some(EvaluatedChunk::try_new(series, closing)?),
            None if closing => Some(no_row_chunk(schema, labels)?),
            None => None,
        };
        if let Some(output) = &output {
            metrics.record_grid_points(output.series.time.len());
            metrics.record_output(1, valid_values(output));
        }
        self.buffer.resize(0)?;
        Ok(output)
    }

    /// Applies the broadcast to one vector chunk without retaining it; see
    /// [`BroadcastRuntime::apply`].
    fn broadcast(
        &mut self,
        broadcast: &BroadcastRuntime,
        chunk: NativeChunk,
    ) -> Result<Option<EvaluatedChunk>> {
        let NativeChunk::Evaluated(chunk) = chunk else {
            return Err(DataFusionError::Execution(
                "the binary node broadcasts over an evaluated vector, not raw samples".to_string(),
            ));
        };
        let (positions, closing) = (chunk.series.time.len(), chunk.is_last);
        // Literal arithmetic maps segments; everything else builds a subset of rows.
        let workspace = match (&broadcast.scalar, broadcast.op) {
            (BroadcastScalar::Literal(_), BroadcastOp::Arithmetic(_)) => {
                mapped_workspace_bytes(&chunk.series)?
            }
            _ => built_series_workspace_bytes(&chunk.series)?,
        };
        self.buffer.grow(evaluated_bytes(&chunk))?;
        self.buffer.grow(workspace)?;
        let metrics = Arc::clone(&self.buffer.metrics);
        let outcome = {
            let _timer = metrics.timer();
            broadcast.apply(&chunk)
        };
        metrics.record_input(1, positions);
        if closing {
            metrics.record_completed_series(1);
        }
        let output = match outcome {
            Ok(output) => output,
            Err(error) => {
                self.buffer.resize(broadcast.scalar_bytes)?;
                return Err(error);
            }
        };
        let output = match output {
            Some(series) => Some(EvaluatedChunk::try_new(series, closing)?),
            None => None,
        };
        if let Some(output) = &output {
            metrics.record_grid_points(output.series.time.len());
            metrics.record_output(1, valid_values(output));
        }
        self.buffer.resize(broadcast.scalar_bytes)?;
        Ok(output)
    }

    /// Keeps a dynamic scalar operand's single closed chunk in `slot`, adding its bytes to `held`.
    fn hold_scalar(
        &mut self,
        held: &mut usize,
        slot: &mut Option<Series>,
        chunk: NativeChunk,
    ) -> Result<()> {
        let NativeChunk::Evaluated(EvaluatedChunk { series, is_last }) = chunk else {
            return Err(DataFusionError::Execution(
                "a scalar operand is an evaluated series, not raw samples".to_string(),
            ));
        };
        if slot.is_some() || !is_last {
            return Err(DataFusionError::Execution(
                "a scalar operand hands over exactly one closed chunk".to_string(),
            ));
        }
        let bytes = series_bytes(&series);
        self.buffer.grow(bytes)?;
        *held = held.saturating_add(bytes);
        *slot = Some(series);
        Ok(())
    }

    /// Builds a constant's series on `grid` into `slot`, reserving it first.
    fn hold_constant(
        &mut self,
        held: &mut usize,
        slot: &mut Option<Series>,
        grid: Arc<EvalGrid>,
        value: f64,
    ) -> Result<()> {
        let bytes = grid_chunk_bytes(&grid)?;
        self.buffer.grow(bytes)?;
        *held = held.saturating_add(bytes);
        *slot = Some(scalar_chunk(grid, value)?.series);
        Ok(())
    }

    /// Builds and queues the single closed chunk of a node with no input, reserving it first.
    fn produce(&mut self, grid: &EvalGrid, series: impl FnOnce() -> Result<Series>) -> Result<()> {
        let bytes = grid_chunk_bytes(grid)?;
        self.buffer.grow(bytes)?;
        let chunk = EvaluatedChunk::try_new(series()?, true)?;
        self.buffer.metrics.record_completed_series(1);
        self.queued
            .push_back((NativeChunk::Evaluated(chunk), bytes));
        Ok(())
    }

    /// Feeds one chunk to this node's window. A collecting window only retains it until its input
    /// ends; any other window evaluates it now.
    fn window(
        &mut self,
        window: &mut RangeEvaluator,
        merge: &mut Option<MergeInput>,
        chunk: NativeChunk,
    ) -> Result<Option<EvaluatedChunk>> {
        self.buffer.reserve_grid(window.grid())?;
        if let Some(merge) = merge.as_mut() {
            let NativeChunk::Evaluated(chunk) = chunk else {
                return Err(DataFusionError::Execution(
                    "the window collects evaluated fragments, but it was handed raw samples"
                        .to_string(),
                ));
            };
            return self.collect(merge, window, chunk);
        }
        let (positions, closing) = (chunk.positions(), chunk.is_last());
        let (fields, metric) = match &chunk {
            NativeChunk::Raw(chunk) => (&chunk.fields, chunk.metric.as_ref()),
            NativeChunk::Evaluated(chunk) => (&chunk.series.fields, Some(&chunk.series.metric)),
            NativeChunk::AggregateState(_) => {
                return Err(DataFusionError::Execution(
                    "an aggregate state chunk reached a node that reads values or samples"
                        .to_string(),
                ));
            }
        };
        let workspace = match metric {
            Some(metric) => {
                window_workspace_bytes(window, last_sample_ms(&chunk)?, closing, fields, metric)?
            }
            // The evaluator refuses a raw chunk without labels before it writes a row.
            None => 0,
        };
        self.buffer.grow(chunk.bytes())?;
        self.buffer.grow(workspace)?;
        let metrics = Arc::clone(&self.buffer.metrics);
        let outcome = {
            let _timer = metrics.timer();
            match &chunk {
                NativeChunk::Raw(chunk) => window.push(chunk),
                NativeChunk::Evaluated(chunk) => window.push_series(&chunk.series, chunk.is_last),
                NativeChunk::AggregateState(_) => {
                    return Err(DataFusionError::Execution(
                        "an aggregate state chunk reached a node that reads values or samples"
                            .to_string(),
                    ));
                }
            }
        };
        metrics.record_input(1, positions);
        if closing {
            // Counted on input: a window that decided nothing still closed the series.
            metrics.record_completed_series(1);
        }
        let output = match outcome {
            Ok(output) => output,
            Err(error) => {
                self.buffer.resize(window.retained_bytes())?;
                return Err(error);
            }
        };
        if let Some(output) = &output {
            metrics.record_grid_points(output.series.time.len());
            metrics.record_output(1, valid_values(output));
        }
        // Settle on what the evaluator retains, not on a per-chunk sum.
        self.buffer.resize(window.retained_bytes())?;
        Ok(output)
    }

    /// Retains one operand chunk, reserving it first so a refused chunk leaves the operand
    /// unchanged.
    fn retain_binary(
        &mut self,
        binary: &mut BinaryRuntime,
        side: usize,
        chunk: NativeChunk,
    ) -> Result<()> {
        let NativeChunk::Evaluated(chunk) = chunk else {
            return Err(DataFusionError::Execution(format!(
                "{} consumes evaluated chunks, not raw samples",
                binary.kernel.node()
            )));
        };
        let pending = binary.pending(side, &chunk)?;
        let positions = chunk.series.time.len();
        self.buffer.grow(pending)?;
        let metrics = Arc::clone(&self.buffer.metrics);
        {
            let _timer = metrics.timer();
            binary.retain(side, chunk)?;
            let retained = binary.retained_bytes();
            self.buffer.resize(retained)?;
        }
        metrics.record_input(1, positions);
        Ok(())
    }

    /// Runs this node's binary kernel and queues the chunks it produced.
    fn compute_binary(&mut self, binary: &mut BinaryRuntime) -> Result<()> {
        let output = binary.compute(&mut self.buffer)?;
        // A kernel that already ran has moved its result into the queue, which the handover below
        // charges for: leaving the reservation alone here is what keeps a queued result covered.
        let Some(output) = output else {
            return Ok(());
        };
        self.queue_results(output, false, 0)
    }

    /// Settles a vector node's reservation on its retained input plus its parameter values.
    fn settle_vector(&mut self, vector: &VectorRuntime) -> Result<()> {
        let held = vector.input.held.saturating_add(vector.parameter_bytes);
        self.buffer.resize(held)
    }

    /// Retains one chunk of a vector node's input, reserving it first.
    fn retain_vector(&mut self, vector: &mut VectorRuntime, chunk: NativeChunk) -> Result<()> {
        let NativeChunk::Evaluated(chunk) = chunk else {
            return Err(DataFusionError::Execution(
                "the vector operation consumes evaluated chunks, not raw samples".to_string(),
            ));
        };
        validate_binary_operand(vector.schema.as_ref(), std::slice::from_ref(&chunk.series))
            .map_err(|error| {
                DataFusionError::Execution(format!(
                    "the vector operation rejected its input: {error}"
                ))
            })?;
        let pending = vector.input.pending(&chunk);
        let positions = chunk.series.time.len();
        self.buffer.grow(pending)?;
        let metrics = Arc::clone(&self.buffer.metrics);
        {
            let _timer = metrics.timer();
            vector.input.retain(chunk, Reader::Vector)?;
            self.settle_vector(vector)?;
        }
        metrics.record_input(1, positions);
        Ok(())
    }

    /// Runs the histogram kernel with its per-point parameters and queues its output.
    fn compute_histogram(
        &mut self,
        vector: &mut VectorRuntime,
        parameters: &[&[Option<f64>]],
    ) -> Result<()> {
        let output = vector.compute_with_histogram_parameters(&mut self.buffer, parameters)?;
        if let Some(output) = output {
            self.queue_results(output, false, vector.parameter_bytes)?;
        }
        Ok(())
    }

    fn compute_vector(&mut self, vector: &mut VectorRuntime) -> Result<()> {
        let shares_input = matches!(vector.op, BoundVectorOp::Sort(_));
        let output = vector.compute(&mut self.buffer)?;
        let Some(output) = output else {
            return Ok(());
        };
        self.queue_results(output, shares_input, vector.parameter_bytes)
    }

    /// Queues a whole-input kernel's output, one closed chunk per series, and settles the
    /// reservation on the queue plus `beside` (e.g. parameter values). With `shares_input` (a sort
    /// returns its input) each series is measured by [`operand_chunk_bytes`] and its tables billed
    /// once.
    fn queue_results(
        &mut self,
        output: Vec<Series>,
        shares_input: bool,
        beside: usize,
    ) -> Result<()> {
        let mut tables = HashSet::new();
        let mut closed = 0usize;
        for series in output {
            let bytes = if shares_input {
                let (fresh, table_bytes) = unbilled_tables(&series, &tables);
                tables.extend(fresh);
                self.queued_tables = self.queued_tables.saturating_add(table_bytes);
                operand_chunk_bytes(&series)
            } else {
                series_bytes(&series)
            };
            let chunk = NativeChunk::Evaluated(EvaluatedChunk::try_new(series, true)?);
            self.queued.push_back((chunk, bytes));
            closed += 1;
        }
        self.buffer.metrics.record_completed_series(closed);
        let queued = self
            .queued
            .iter()
            .fold(self.queued_tables, |total, (_, bytes)| {
                total.saturating_add(*bytes)
            });
        self.buffer.resize(queued.saturating_add(beside))
    }

    /// Folds one value or state chunk into this node's aggregate, reserving it first.
    fn retain_aggregate(
        &mut self,
        aggregate: &mut AggregateRuntime,
        chunk: NativeChunk,
    ) -> Result<()> {
        if let NativeChunk::AggregateState(chunk) = chunk {
            let pending = aggregate.pending_state(&chunk);
            let positions = match &chunk {
                StateChunk::Series { series, .. } => {
                    series.iter().map(|series| series.time.len()).sum()
                }
                StateChunk::Row(row) => row.time.len(),
            };
            self.buffer.grow(pending)?;
            let metrics = Arc::clone(&self.buffer.metrics);
            {
                let _timer = metrics.timer();
                aggregate.accumulate_state(chunk, &mut self.buffer, pending)?;
            }
            metrics.record_input(1, positions);
            return Ok(());
        }
        let NativeChunk::Evaluated(chunk) = chunk else {
            return Err(DataFusionError::Execution(
                "the aggregate consumes evaluated chunks, not raw samples".to_string(),
            ));
        };
        let pending = aggregate.pending(&chunk);
        let positions = chunk.series.time.len();
        self.buffer.grow(pending)?;
        let metrics = Arc::clone(&self.buffer.metrics);
        {
            let _timer = metrics.timer();
            aggregate.accumulate(chunk, &mut self.buffer, pending)?;
        }
        metrics.record_input(1, positions);
        Ok(())
    }

    /// Finishes this node's fold and queues one closed chunk per group, or one state chunk for a
    /// `Partial`. The queue's capacity is reserved before it grows.
    fn compute_aggregate(&mut self, aggregate: &mut AggregateRuntime) -> Result<()> {
        let metrics = Arc::clone(&self.buffer.metrics);
        let _timer = metrics.timer();
        let groups = aggregate.groups();
        let queue_capacity = self.queued.capacity();
        let queue_target = self.queued.len().saturating_add(groups);
        let queue_growth = queue_target
            .saturating_sub(queue_capacity)
            .saturating_mul(std::mem::size_of::<(NativeChunk, usize)>());
        self.buffer.grow(queue_growth)?;
        if groups > 0 {
            self.queued.try_reserve_exact(groups).map_err(|error| {
                DataFusionError::Execution(format!(
                    "cannot reserve aggregate output queue: {error}"
                ))
            })?;
        }
        let queue_bytes = queue_target
            .max(queue_capacity)
            .saturating_mul(std::mem::size_of::<(NativeChunk, usize)>());
        let output = aggregate.compute(&mut self.buffer, queue_bytes)?;
        let Some(output) = output else {
            return Ok(());
        };
        let mut closed = 0usize;
        match output {
            AggregateOutput::Samples(series) => {
                for series in series {
                    let bytes = series_bytes(&series);
                    let chunk = NativeChunk::Evaluated(EvaluatedChunk::try_new(series, true)?);
                    self.queued.push_back((chunk, bytes));
                    closed += 1;
                }
            }
            AggregateOutput::State(chunk) => {
                let StateChunk::Series { series, .. } = &chunk else {
                    return Err(DataFusionError::Internal(
                        "Partial aggregate produced row state".into(),
                    ));
                };
                closed = series.len();
                let bytes = state_bytes(&chunk);
                self.queued
                    .push_back((NativeChunk::AggregateState(chunk), bytes));
            }
        }
        self.buffer.metrics.record_completed_series(closed);
        let queued_bytes = self
            .queued
            .iter()
            .fold(0usize, |total, (_, bytes)| total.saturating_add(*bytes));
        let queue_backing = self
            .queued
            .capacity()
            .saturating_mul(std::mem::size_of::<(NativeChunk, usize)>());
        self.buffer
            .resize(queued_bytes.saturating_add(queue_backing))
    }

    /// Feeds one chunk to this node's instant selection. `None` means the series has not ended yet.
    fn instant(
        &mut self,
        instant: &mut InstantRuntime,
        chunk: NativeChunk,
    ) -> Result<Option<EvaluatedChunk>> {
        let NativeChunk::Raw(chunk) = chunk else {
            return Err(DataFusionError::Execution(
                "the instant selection consumes raw samples, not an evaluated chunk".to_string(),
            ));
        };
        let (positions, closing) = (chunk.time.len(), chunk.is_last);
        // A timestamp selection materializes the grid's timestamps.
        if instant.selection.value != InstantValue::Sample {
            self.buffer.reserve_grid(&instant.selection.output_grid)?;
        }
        self.buffer.grow(raw_bytes(&chunk))?;
        // The closing chunk triggers the gather; reserve its workspace up front.
        let scratch = if closing {
            instant.gather_bytes(&chunk)
        } else {
            0
        };
        self.buffer.grow(scratch)?;
        let metrics = Arc::clone(&self.buffer.metrics);
        let outcome = {
            let _timer = metrics.timer();
            instant.push(chunk)
        };
        metrics.record_input(1, positions);
        if closing {
            metrics.record_completed_series(1);
        }
        let output = match outcome {
            Ok(output) => output,
            Err(error) => {
                self.settle_buffered(instant)?;
                return Err(error);
            }
        };
        if let Some(output) = &output {
            metrics.record_grid_points(output.series.time.len());
            metrics.record_output(1, valid_values(output));
        }
        self.settle_buffered(instant)?;
        Ok(output)
    }

    /// Sets this node's reservation to the chunks of the series it is still gathering.
    fn settle_buffered(&mut self, instant: &InstantRuntime) -> Result<()> {
        self.buffer.resize(instant.buffered_bytes())
    }

    /// Validates and retains one fragment for a later merge.
    fn collect_merge(&mut self, merge: &mut MergeInput, chunk: EvaluatedChunk) -> Result<()> {
        let EvaluatedChunk { series, is_last } = chunk;
        merge.check(&series, is_last)?;
        let bytes = series_bytes(&series);
        self.buffer.grow(bytes)?;
        merge.bytes = merge.bytes.saturating_add(bytes);
        merge.fragments.push(series);
        self.buffer
            .metrics
            .record_input(1, merge.fragments.last().map_or(0, |s| s.time.len()));
        if is_last {
            self.buffer.metrics.record_completed_series(1);
        }
        Ok(())
    }

    /// Merges all collected fragments by label set and queues the result.
    fn flush_canonical_merge(&mut self, merge: &mut Option<MergeInput>) -> Result<()> {
        let Some(merge) = merge.take() else {
            return Ok(());
        };
        if merge.fragments.is_empty() {
            return self.buffer.shrink(merge.bytes);
        }
        let workspace = merge.workspace_bytes();
        self.buffer.grow(workspace)?;
        let held = merge.bytes.saturating_add(workspace);
        let merged = merge_same_labelset(&merge.schema, merge.fragments)?;
        for series in merged {
            let chunk = EvaluatedChunk::try_new(series, true)?;
            self.buffer
                .metrics
                .record_grid_points(chunk.series.time.len());
            self.buffer.metrics.record_output(1, valid_values(&chunk));
            let bytes = series_bytes(&chunk.series);
            self.buffer.grow(bytes)?;
            self.queued
                .push_back((NativeChunk::Evaluated(chunk), bytes));
        }
        self.buffer.shrink(held)
    }

    /// Collects one fragment of a collecting window's input; nothing is evaluated until the flush.
    fn collect(
        &mut self,
        merge: &mut MergeInput,
        window: &RangeEvaluator,
        chunk: EvaluatedChunk,
    ) -> Result<Option<EvaluatedChunk>> {
        self.collect_merge(merge, chunk)?;
        let retained = window.retained_bytes();
        self.buffer.resize(retained.saturating_add(merge.bytes))?;
        Ok(None)
    }

    /// Merges this window's collected input and evaluates each merged series as one closed series.
    /// `merge` is taken first, so a failed merge is not retried.
    fn flush_merge(
        &mut self,
        window: &mut RangeEvaluator,
        merge: &mut Option<MergeInput>,
    ) -> Result<()> {
        let Some(merge) = merge.take() else {
            return Ok(());
        };
        if merge.fragments.is_empty() {
            return self.buffer.shrink(merge.bytes);
        }
        self.buffer.reserve_grid(window.grid())?;
        let workspace = merge.workspace_bytes();
        let held = merge.bytes.saturating_add(workspace);
        let schema = Arc::clone(&merge.schema);
        self.buffer.grow(workspace)?;
        // Already-queued output stays charged through every resize below.
        let mut queued_bytes = self
            .queued
            .iter()
            .fold(self.queued_tables, |total, (_, bytes)| {
                total.saturating_add(*bytes)
            });
        // Merged series slice the fragments' segments, so the fragments' charge covers them.
        let merged = merge_same_labelset(&schema, merge.fragments)?;
        for series in merged {
            let workspace = window_workspace_bytes(
                window,
                last_series_ms(&series),
                true,
                &series.fields,
                &series.metric,
            )?;
            self.buffer.grow(workspace)?;
            let output = {
                let metrics = Arc::clone(&self.buffer.metrics);
                let _timer = metrics.timer();
                window.push_series(&series, true)?
            };
            let Some(output) = output else {
                self.buffer.shrink(workspace)?;
                continue;
            };
            self.buffer
                .metrics
                .record_grid_points(output.series.time.len());
            self.buffer.metrics.record_output(1, valid_values(&output));
            let retained = window.retained_bytes();
            self.buffer
                .resize(retained.saturating_add(held).saturating_add(queued_bytes))?;
            let bytes = evaluated_bytes(&output);
            self.enqueue_output(output)?;
            queued_bytes = queued_bytes.saturating_add(bytes);
        }
        self.buffer.shrink(held)
    }

    /// Queues one output computed while flushing, charged until [`Self::take_output`] hands it
    /// over.
    fn enqueue_output(&mut self, chunk: EvaluatedChunk) -> Result<()> {
        let bytes = evaluated_bytes(&chunk);
        self.buffer.grow(bytes)?;
        self.queued
            .push_back((NativeChunk::Evaluated(chunk), bytes));
        Ok(())
    }

    /// Hands over the next queued chunk without recording metrics (the flush already did).
    fn take_output(&mut self) -> Result<Option<NativeChunk>> {
        let Some((chunk, bytes)) = self.queued.pop_front() else {
            return Ok(None);
        };
        self.buffer.shrink(bytes)?;
        self.release_queued_tables()?;
        Ok(Some(chunk))
    }

    /// Gives back the shared tables' charge once no queued result references them.
    fn release_queued_tables(&mut self) -> Result<()> {
        if self.queued.is_empty() && self.queued_tables > 0 {
            self.buffer
                .shrink(std::mem::take(&mut self.queued_tables))?;
        }
        Ok(())
    }
}

impl InputSlot {
    /// Queues one source chunk at this input, refusing a chunk of another [`LeafKind`].
    fn enqueue(&mut self, chunk: NativeChunk) -> Result<()> {
        if !self.kind.accepts(&chunk) {
            return Err(self.state.context(DataFusionError::Execution(format!(
                "the native source is bound to {}, but {} was queued at it",
                self.kind.describe(),
                chunk.describe(),
            ))));
        }
        // Updated only after the pool admits the chunk.
        let closing = chunk.is_last();
        self.state
            .enqueue(chunk)
            .map_err(|error| self.state.context(error))?;
        self.open_queued_series = !closing;
        Ok(())
    }

    /// Hands over the next queued source chunk and updates [`Self::open_source_series`].
    fn pop_source(&mut self) -> Result<Option<NativeChunk>> {
        let Some(chunk) = self.state.pop_queued(NativeChunk::positions)? else {
            return Ok(None);
        };
        self.open_source_series = !chunk.is_last();
        if chunk.is_last() {
            self.state.buffer.metrics.record_completed_series(1);
        }
        Ok(Some(chunk))
    }
}

/// Drives one execution of a bound tree: the caller pushes source chunks, pulls output, and ends
/// the input with [`Self::end_of_input`] or [`Self::finish`]. Streaming nodes (windows, maps,
/// broadcasts) answer as input arrives; collecting windows and whole-input nodes answer only after
/// the input ends.
pub struct NativeExecutor {
    state: NativeExecutionState,
    phase: Phase,
    /// Passed down every pull; set by `next_output` once the input has ended, the tree has gone
    /// quiet, and no series is left open. Until then whole-input nodes only retain.
    inputs_finished: bool,
    /// Whether the caller ended the input. After it, an empty pull advances the ending stages in
    /// `next_output` instead of returning `None`.
    ended: bool,
}

/// Whether the execution can still produce chunks.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Phase {
    Running,
    /// The input ended and every node closed.
    Finished,
    /// Stopped after an error; evaluators must not be reused.
    Failed(String),
}

impl NativeExecutor {
    pub fn new(state: NativeExecutionState) -> Self {
        Self {
            state,
            phase: Phase::Running,
            inputs_finished: false,
            ended: false,
        }
    }

    pub fn state(&self) -> &NativeExecutionState {
        &self.state
    }

    fn live(&self) -> Result<&LiveExecution> {
        self.state.live.as_ref().ok_or_else(no_live_execution)
    }

    fn live_mut(&mut self) -> Result<&mut LiveExecution> {
        self.state.live.as_mut().ok_or_else(no_live_execution)
    }

    /// Queues one raw source chunk at `input`. An unknown id or a chunk kind the leaf is not bound
    /// to is an error and stops the execution.
    pub fn push_raw(&mut self, input: InputId, chunk: RawSeriesChunk) -> Result<()> {
        self.push_source(input, NativeChunk::Raw(chunk))
    }

    /// Queues one evaluated source chunk at the input `input` names.
    pub fn push_evaluated(&mut self, input: InputId, chunk: EvaluatedChunk) -> Result<()> {
        self.push_source(input, NativeChunk::Evaluated(chunk))
    }

    /// Queues one aggregate-state chunk at the input `input` names; only a state leaf accepts it.
    pub(crate) fn push_state(&mut self, input: InputId, chunk: StateChunk) -> Result<()> {
        self.push_source(input, NativeChunk::AggregateState(chunk))
    }

    /// The next chunk the root produces, or `None` when nothing is ready yet (or, after the input
    /// ended, when the execution has finished). A chunk's `is_last` closes one input series, not
    /// its label set; see [`crate::native::batch`].
    pub fn next_chunk(&mut self) -> Result<Option<EvaluatedChunk>> {
        match self.next_output()? {
            Some(NativeChunk::Evaluated(chunk)) => Ok(Some(chunk)),
            Some(NativeChunk::Raw(_)) => {
                let error = DataFusionError::Execution(
                    "the native runtime produces evaluated chunks, but its root produced raw \
                     samples"
                        .to_string(),
                );
                self.stop(&error);
                Err(error)
            }
            Some(NativeChunk::AggregateState(_)) => {
                let error = DataFusionError::Execution(
                    "the native runtime produces evaluated chunks, but its root produced \
                     aggregate state"
                        .to_string(),
                );
                self.stop(&error);
                Err(error)
            }
            None => Ok(None),
        }
    }

    /// Like [`Self::next_chunk`], for a tree whose root is a `Partial` aggregate.
    pub(crate) fn next_state(&mut self) -> Result<Option<StateChunk>> {
        match self.next_output()? {
            Some(NativeChunk::AggregateState(chunk)) => Ok(Some(chunk)),
            Some(other) => {
                let error = DataFusionError::Execution(format!(
                    "the native runtime produces aggregate state, but its root produced {}",
                    other.describe()
                ));
                self.stop(&error);
                Err(error)
            }
            None => Ok(None),
        }
    }

    /// The next chunk the root produces, of any kind. After the input ended, an empty pull runs the
    /// next ending stage: the open-series check, then merge flushes, then closing the tree.
    fn next_output(&mut self) -> Result<Option<NativeChunk>> {
        self.check_running()?;
        loop {
            let inputs_finished = self.inputs_finished;
            match self.live_mut()?.pull(inputs_finished) {
                Ok(Some(chunk)) => return Ok(Some(chunk)),
                Ok(None) => {}
                Err(error) => {
                    self.stop(&error);
                    return Err(error);
                }
            }
            if !self.ended {
                return Ok(None);
            }
            if !self.inputs_finished {
                // Refuse a series the caller never closed before any whole-input node computes.
                if let Err(error) = self.live()?.unclosed() {
                    self.stop(&error);
                    return Err(error);
                }
                self.inputs_finished = true;
                continue;
            }
            match self.live_mut()?.flush_ready_merges() {
                Ok(true) => continue,
                Ok(false) => {}
                Err(error) => {
                    self.stop(&error);
                    return Err(error);
                }
            }
            if self.live()?.any_merge_pending() {
                continue;
            }
            // Closing the tree frees every reservation.
            let live = self.state.live.take().ok_or_else(no_live_execution)?;
            return match live.close() {
                Ok(()) => {
                    self.phase = Phase::Finished;
                    Ok(None)
                }
                Err(error) => {
                    self.stop(&error);
                    Err(error)
                }
            };
        }
    }

    /// Ends the input; later pulls run the ending stages. Errors if called twice.
    pub fn end_of_input(&mut self) -> Result<()> {
        if self.ended {
            return Err(DataFusionError::Execution(
                "the native execution already ended its input".to_string(),
            ));
        }
        self.ended = true;
        Ok(())
    }

    /// [`Self::end_of_input`] followed by pulling every remaining chunk.
    pub fn finish(&mut self) -> Result<Vec<EvaluatedChunk>> {
        self.end_of_input()?;
        self.drain()
    }

    fn drain(&mut self) -> Result<Vec<EvaluatedChunk>> {
        let mut drained = Vec::new();
        loop {
            match self.next_chunk() {
                Ok(Some(chunk)) => drained.push(chunk),
                Ok(None) => return Ok(drained),
                Err(error) => return Err(error),
            }
        }
    }

    /// Queues a source chunk at the input leaf `input` names.
    fn push_source(&mut self, input: InputId, chunk: NativeChunk) -> Result<()> {
        self.check_running()?;
        if self.ended {
            return Err(DataFusionError::Execution(
                "the native execution already ended its input, so no chunk can be queued at it"
                    .to_string(),
            ));
        }
        let result = self.queue_source(input, chunk);
        if let Err(error) = &result {
            self.stop(error);
        }
        result
    }

    fn queue_source(&mut self, input: InputId, chunk: NativeChunk) -> Result<()> {
        let live = self.live_mut()?;
        let count = live.inputs.len();
        let slot = live
            .inputs
            .get_mut(input.index())
            .ok_or_else(|| no_input(count, input))?;
        slot.enqueue(chunk)
    }

    /// Refuses to continue an execution that finished or stopped.
    fn check_running(&self) -> Result<()> {
        match &self.phase {
            Phase::Running => Ok(()),
            Phase::Finished => Err(DataFusionError::Execution(
                "the native execution already finished".to_string(),
            )),
            Phase::Failed(reason) => Err(DataFusionError::Execution(format!(
                "the native execution stopped after an earlier error: {reason}"
            ))),
        }
    }

    /// Stops the execution for good, dropping the live tree and every reservation.
    fn stop(&mut self, error: &DataFusionError) {
        self.state.live = None;
        self.phase = Phase::Failed(error.to_string());
    }
}

/// Maps one chunk with `ops`, keeping its closing. `None` means every position was emptied and
/// the chunk does not close its series; a closing chunk is always returned, possibly with no rows.
/// `admit` reserves the bytes a label rewrite writes before it runs.
fn map_chunk(
    schema: &Arc<SeriesSchema>,
    chunk: &EvaluatedChunk,
    ops: &[MapOp],
    admit: &mut dyn FnMut(usize) -> Result<()>,
) -> Result<Option<EvaluatedChunk>> {
    let mut series = chunk.series.clone();
    for (index, op) in ops.iter().enumerate() {
        let mapped = match op {
            MapOp::Label(rewrite) => {
                admit(rewrite.written_bytes(&series.metric))?;
                rewrite.apply(&mut series.metric)?;
                continue;
            }
            MapOp::Math(math) => map_math(schema, &series, *math)?,
            MapOp::Histogram(function) => map_native_histogram(*function, schema, &series)?,
        };
        if let Some(mapped) = mapped {
            series = mapped;
            continue;
        }
        if !chunk.is_last {
            return Ok(None);
        }
        // The empty closing chunk still carries the labels the remaining ops would produce.
        let mut metric = series.metric.clone();
        for op in &ops[index..] {
            match op {
                MapOp::Label(rewrite) => {
                    admit(rewrite.written_bytes(&metric))?;
                    rewrite.apply(&mut metric)?;
                }
                MapOp::Math(_) => metric = drop_metric_name(&metric)?,
                // `simpleHistogramFunc` drops only the name, not the type and unit.
                MapOp::Histogram(_) => metric.remove("__name__"),
            }
        }
        let empty = no_row_series(schema, &series, metric)?;
        return EvaluatedChunk::try_new(empty, true).map(Some);
    }
    EvaluatedChunk::try_new(series, chunk.is_last).map(Some)
}

/// A series carrying `metric` and no rows, placed after `series`' last position.
fn no_row_series(schema: &SeriesSchema, series: &Series, metric: SeriesLabels) -> Result<Series> {
    let grid = series.time.grid().ok_or_else(|| {
        DataFusionError::Execution("a mapped chunk must be addressed on a grid".to_string())
    })?;
    let fields = (0..schema.fields.len())
        .map(|_| Samples::from_float(Float64Array::from(Vec::<f64>::new())))
        .collect();
    let fields = SeriesFields::try_from_vec(fields).map_err(series_error)?;
    let at = series.time.last_position().map_or(0, |last| last + 1);
    let time = GridRange::try_new(Arc::clone(grid), at, 0).map_err(series_error)?;
    Series::try_new(schema, metric, TimeAxis::Grid(time), fields).map_err(series_error)
}

/// The valid values a chunk holds: one per valid field sample.
fn valid_values(chunk: &EvaluatedChunk) -> usize {
    chunk
        .series
        .fields
        .iter()
        .map(|field| {
            field
                .iter()
                .filter(|sample| !matches!(sample, SampleRef::Missing))
                .count()
        })
        .sum()
}

/// The output-values metric for a chunk: valid values, or positions for raw and state chunks.
fn evaluated_values(chunk: &NativeChunk) -> usize {
    match chunk {
        NativeChunk::Evaluated(chunk) => valid_values(chunk),
        NativeChunk::Raw(chunk) => chunk.time.len(),
        NativeChunk::AggregateState(StateChunk::Series { series, .. }) => {
            series.iter().map(|series| series.time.len()).sum()
        }
        NativeChunk::AggregateState(StateChunk::Row(row)) => row.time.len(),
    }
}

/// The error for a chunk of another series arriving while one is being selected.
fn series_changed() -> DataFusionError {
    DataFusionError::Execution(
        "the instant selection combines one series into one axis, and a chunk of another series \
         arrived before the first one ended"
            .to_string(),
    )
}

/// The error for a series an instant selection has no labels to report it under.
fn no_labels() -> DataFusionError {
    DataFusionError::Execution(
        "the instant selection requires materialized series labels".to_string(),
    )
}

/// The error for a source row the selection named but the series does not hold.
fn missing_row(index: usize) -> DataFusionError {
    DataFusionError::Execution(format!(
        "the instant selection selected the source row {index}, which the series it read does not \
         hold"
    ))
}

/// The error for an input that ended without the chunk closing its series.
fn truncated_input(input: InputId) -> DataFusionError {
    DataFusionError::Execution(format!(
        "the input at routing slot {} ended before the last chunk of a series: the last chunk it \
         handed over did not say `is_last`",
        input.index()
    ))
}

/// The error for a chunk routed to an input the execution does not have.
fn no_input(inputs: usize, input: InputId) -> DataFusionError {
    DataFusionError::Execution(format!(
        "the native execution has {inputs} inputs, and no input {} was queued at",
        input.index()
    ))
}

/// A binary operation was asked to read an operand other than its two inputs.
fn invalid_binary_operand() -> DataFusionError {
    DataFusionError::Internal("invalid binary operand index".to_string())
}

fn no_live_execution() -> DataFusionError {
    DataFusionError::Internal("the running native execution has no state".to_string())
}

/// The error for a node asked to evaluate a window without its evaluator.
fn no_window() -> DataFusionError {
    DataFusionError::Execution("the node holds no window evaluator".to_string())
}

/// The error for a vector-scalar operation carrying vector-matching modifiers.
fn matched_scalar() -> DataFusionError {
    DataFusionError::NotImplemented(
        "the native runtime does not execute a scalar broadcast whose constant carries matching \
         modifiers: a constant has no labels to match, so the operation this Binary describes is \
         not the one it would compute"
            .to_string(),
    )
}

/// The error for a binary node whose operands are neither two evaluated vectors nor a vector and a
/// scalar; `operands` describes what arrived.
fn unsupported_binary(operands: &str) -> DataFusionError {
    DataFusionError::NotImplemented(format!(
        "the native runtime does not execute this Binary node: it reads {operands}, and this \
         runtime reads two evaluated vectors or one evaluated vector and one constant"
    ))
}

/// The broadcast runtime of a binary node with a scalar operand: over the vector, or for two
/// scalars over the varying one. A constant is read from its node's binding; a dynamic scalar
/// from its node's chunk.
fn broadcast_runtime(
    left: &NativeNode,
    right: &NativeNode,
    matching: &BinaryMatching,
) -> Result<BroadcastRuntime> {
    let literal = |node: &NativeNode| match &node.expr {
        NativeExpr::Scalar(BoundScalar { value }) => Some(*value),
        _ => None,
    };
    let (vector_left, sample_schema) = match (&left.output, &right.output) {
        (NativeType::Samples(vector), NativeType::Scalar(_)) if vector.eval_grid.is_some() => {
            (true, Arc::clone(vector))
        }
        (NativeType::Scalar(_), NativeType::Samples(vector)) if vector.eval_grid.is_some() => {
            (false, Arc::clone(vector))
        }
        (NativeType::Scalar(grid), NativeType::Scalar(_)) => {
            // The compiler folds two constants; an unfolded pair has no series to carry the result.
            if literal(left).is_some() && literal(right).is_some() {
                return Err(unsupported_binary(
                    "two constants, which broadcast over no vector",
                ));
            }
            (
                literal(left).is_none(),
                Arc::new(scalar_schema(Arc::clone(grid))),
            )
        }
        (_, _) => {
            return Err(unsupported_binary(
                "an operand that is neither evaluated samples nor a scalar, which no validated \
                 tree binds as a binary operand",
            ));
        }
    };
    let scalar_side = if vector_left { right } else { left };
    let scalar = match literal(scalar_side) {
        Some(value) if vector_left => BroadcastScalar::Literal(ScalarOperand::Right(value)),
        Some(value) => BroadcastScalar::Literal(ScalarOperand::Left(value)),
        None => BroadcastScalar::Dynamic {
            left: !vector_left,
            value: None,
        },
    };
    // A scalar has no labels; matching modifiers here would describe a different operation.
    if matching.labels != MatchingLabels::Ignoring(Vec::new())
        || matching.cardinality != Cardinality::OneToOne
        || matching.fill != FillValues::default()
    {
        return Err(matched_scalar());
    }
    Ok(BroadcastRuntime {
        scalar,
        vector_left,
        op: broadcast_op(matching)?,
        sample_schema,
        scalar_bytes: 0,
        annotations: None,
    })
}

/// The operation a binary node's `matching` describes; `bool` on arithmetic is refused.
fn broadcast_op(matching: &BinaryMatching) -> Result<BroadcastOp> {
    let arithmetic = |op: ArithmeticOp| {
        if matching.return_bool {
            Err(DataFusionError::Execution(
                "the binary node carries `bool` on an arithmetic operator, which has no bool form"
                    .to_string(),
            ))
        } else {
            Ok(BroadcastOp::Arithmetic(op))
        }
    };
    let comparison = |op: ComparisonOp| {
        if matching.return_bool {
            Ok(BroadcastOp::Compare(Comparison::Bool(op)))
        } else {
            Ok(BroadcastOp::Compare(Comparison::Filter(op)))
        }
    };
    match matching.op.kind() {
        BinaryKind::Arithmetic(op) => arithmetic(op),
        BinaryKind::Comparison(op) => comparison(op),
    }
}

/// The error for a reservation that overflows `usize`.
fn reservation_overflow(node: &str) -> DataFusionError {
    DataFusionError::Execution(format!(
        "the operands and the kernel's workspace of {node} do not fit one address space: the bytes \
         it would reserve overflow"
    ))
}

fn series_error(error: datatypes::error::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
