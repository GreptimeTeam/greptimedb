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
//! [`NativeExecutionState`] owns the run-time state one execution of a [`NativeTree`] needs, keyed
//! by the position [`crate::native::metrics::walk`] gives each node. The tree is what the compiler
//! produced and carries no state of its own, so a second execution builds a second state from the
//! same tree and the two never share a buffer, an evaluator or a memory reservation.
//!
//! [`NativeExecutor`] drives that state one chunk at a time. Nodes hand each other
//! [`EvaluatedChunk`]s: a source hands over raw samples, and everything above a window hands over
//! the evaluator's own chunk type. No row batch and no frame is materialised on an internal edge,
//! so a chunk a window produced is fed to the next node as it is.
//!
//! Every window owns a [`RangeEvaluator`], which is what keeps its input grid and its output grid
//! its own: `Window -> Map -> Window` evaluates the inner window on the inner grid and the outer
//! window on the outer grid, and the map in between carries the inner grid through untouched. The
//! outer window reads the inner result as the samples it is, not as something to be resampled onto
//! the outer grid first.
//!
//! An instant selection owns no evaluator, because [`select_instants`] takes a complete source
//! axis: the node buffers the chunks of the series it is selecting until the chunk that carries
//! `is_last`, selects the last sample at or before each grid point of it, and hands the series over
//! as one chunk addressed on its own grid. Nothing is decided for a series before it ends, which is
//! the price of the kernel taking one axis rather than a chunk of one.
//!
//! A vector–vector binary node is the only kind that reads two inputs: it retains the chunks of both
//! operands and runs the matching kernel once the input has ended, because the operation pairs two
//! whole vectors point by point and nothing is decided for a point until both sides are complete.
//! The chunks of one logical series are assembled into the single series the kernel matches before it
//! runs, so a series that arrived in many chunks is one candidate, not one per chunk.
//!
//! A constant scalar is a value node rather than a stream: it has no input and hands over no chunk,
//! because what it produces is the one value it was bound to and not a series. A binary node with a
//! scalar operand therefore reads one input — the vector side — and applies the operation to each
//! chunk it is handed as it arrives: the scalar is read off the node's binding rather than pulled,
//! and no chunk of the vector is retained, because one vector value pairs with one constant and
//! nothing is decided by what comes after it. That broadcast is a different mode from the vector
//! matching above and keeps neither operand: what it hands out is the chunk it transformed.
//!
//! A node kind this runtime does not execute yet is rejected when the execution is built, naming
//! the kind and its position. That is an aggregate, and a binary node whose two operands are
//! neither two evaluated vectors nor one evaluated vector and one constant. An unsupported tree
//! therefore never starts, rather than answering half a query or panicking halfway through one.
//!
//! A node that rewrites labels — a window drops the metric name and the empty-valued labels, a map
//! drops the name, and a binary node drops it for arithmetic and for a `bool` comparison — can turn
//! two distinct input series into one output label set. That pair is legal when the two hold values
//! at disjoint positions, which is exactly what
//! [`crate::grid_series::merge_same_labelset`] merges: `is_last` closes the source series whose
//! data the chunk carries, not the label set it shares with another series, so a second series may
//! fill the positions the first one left empty. The node that rewrote the labels keeps which
//! positions each output label set already occupies; a second series that puts a value where the
//! same field already holds one is refused there — before a node above reads it — while a chunk
//! that continues a set where it ended, or fills positions the set has not occupied, is handed on.
//!
//! A label set reachable by more than one series needs one more guarantee: a window reads one
//! series to its `is_last` before the next one, so the per-series state of its evaluator ends
//! before a second series onto the same set arrives. The window immediately above such a rewriting
//! node therefore collects its whole input, merges each canonical label set once with
//! [`crate::grid_series::merge_same_labelset`], and only then evaluates it: every logical series
//! reaches its evaluator once and whole, and the pieces that would have been separate streams
//! become one. Such a window reports at the end of its input instead of as its input arrives. A
//! window directly over a source, and a map at the root, are not affected and keep streaming their
//! chunks on as they produce them.
//!
//! What leaves the tree is therefore a piece stream, not a series collection: a chunk's `is_last`
//! says the source series it carries ended. [`NativeExecutor::finish`] performs the merge for every
//! affected window inside the tree. A caller that needs one series per canonical label set from the
//! root itself — a downstream window outside this runtime, or a protocol that reports one series —
//! collects the root's chunks and merges them with the same helper.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datatypes::arrow::array::{Array, Float64Array};
use datatypes::arrow::buffer::{Buffer, ScalarBuffer};
use datatypes::series::histogram::{HistogramBatch, HistogramView};
use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
use datatypes::series::samples::{
    DenseSamplesRef, RowCursor, SampleRef, Samples, Series, SeriesFields, SeriesSchema,
};
use datatypes::series::time::{EvalGrid, GridRange, GridSpan, GridSpec, TimeAxis};
use datatypes::series::{MetricName, SeriesOrdering};

use crate::grid_series::{
    GridSample, GridSeriesBuilder, LabelSet, label_set, merge_same_labelset, owned,
};
use crate::instant_eval::select_instants;
use crate::map::{
    ArithmeticOp, Comparison, ComparisonOp, MathOp, ScalarOperand, drop_metric_name,
    map_arithmetic, map_compare, map_math,
};
use crate::native::batch::EvaluatedChunk;
use crate::native::metrics::{NodeMetrics, NodePath};
use crate::native::plan::{
    BoundInstant, BoundScalar, NativeExpr, NativeNode, NativeTree, NativeType,
};
use crate::range_eval::RangeEvaluator;
use crate::vector_match::{
    BinaryMatching, BinaryOp, Cardinality, FillValues, MatchingLabels,
    binary_vectors_with_reservation, validate_binary_operand,
};

#[cfg(test)]
mod tests;

/// A chunk on an internal edge.
///
/// Only a source hands over raw samples; everything above a window hands over evaluated chunks.
/// The two cannot be confused once the tree is built, because a window's bound input grid says
/// which one its input produces.
#[derive(Clone, Debug)]
enum NativeChunk {
    /// Raw source samples, with their own timestamps.
    Raw(RawSeriesChunk),
    /// Evaluated values, addressed on their absolute grid positions.
    Evaluated(EvaluatedChunk),
}

impl NativeChunk {
    /// The positions this chunk carries: decided grid positions, or raw timestamps.
    fn positions(&self) -> usize {
        match self {
            Self::Raw(chunk) => chunk.time.len(),
            Self::Evaluated(chunk) => chunk.series.time.len(),
        }
    }

    /// Whether this chunk closes the source series whose data it carries.
    ///
    /// A chunk never closes the label set it holds: two source series can share one output label
    /// set when the positions they fill are disjoint, and each of them ends on its own chunk.
    fn is_last(&self) -> bool {
        match self {
            Self::Raw(chunk) => chunk.is_last,
            Self::Evaluated(chunk) => chunk.is_last,
        }
    }

    /// An estimate of the bytes this chunk holds.
    ///
    /// It is what the chunk alone stands for, so a buffer or a histogram table two chunks share is
    /// counted once per chunk. That over-count is the price of a figure that costs one pass over
    /// the chunk it names and keeps no state between chunks; it is bounded by the number of queued
    /// chunks, not by the data they carry together.
    fn bytes(&self) -> usize {
        match self {
            Self::Raw(chunk) => raw_bytes(chunk),
            Self::Evaluated(chunk) => evaluated_bytes(chunk),
        }
    }
}

/// The bytes a raw chunk holds: its identity, its timestamps and the Arrow buffers behind its
/// fields.
///
/// The figure is what the chunk keeps alive rather than one float per field and row: a histogram
/// field holds no floats at all, only indices into the table its samples live in.
fn raw_bytes(chunk: &RawSeriesChunk) -> usize {
    std::mem::size_of::<RawSeriesChunk>()
        .saturating_add(buffer_bytes(&chunk.primary_key))
        .saturating_add(chunk.metric.as_ref().map_or(0, metric_bytes))
        .saturating_add(buffer_bytes(chunk.time.as_buffer().inner()))
        .saturating_add(fields_bytes(&chunk.fields))
}

/// The bytes an evaluated chunk holds: its struct and the Arrow buffers behind its fields.
///
/// The grid is shared by every chunk addressed on it, so it is not counted per chunk.
fn evaluated_bytes(chunk: &EvaluatedChunk) -> usize {
    std::mem::size_of::<EvaluatedChunk>().saturating_add(fields_bytes(&chunk.series.fields))
}

/// The bytes one evaluated series holds: its header, the axis that addresses it, the identity it
/// carries and the sample buffers behind its fields.
///
/// It is the figure a fragment of an affected window is charged with as it arrives. The axis's runs
/// and the segments of its fields are counted in one pass over the series, so the estimate costs
/// the fragment rather than a walk over everything the node holds. A payload a fragment shares with
/// another — a histogram table behind two slices of it, the grid behind every axis — is counted
/// once per fragment: that over-count is accepted here rather than a deduplication that would have
/// to keep state across fragments.
fn series_bytes(series: &Series) -> usize {
    let runs = series.time.runs().count();
    let segments: usize = series
        .fields
        .iter()
        .map(|field| field.segments().len())
        .sum();
    std::mem::size_of::<Series>()
        .saturating_add(std::mem::size_of::<TimeAxis>())
        .saturating_add(runs.saturating_mul(std::mem::size_of::<(usize, usize)>()))
        .saturating_add(
            segments.saturating_mul(std::mem::size_of::<Samples>() + std::mem::size_of::<usize>()),
        )
        .saturating_add(fields_bytes(&series.fields))
        .saturating_add(metric_bytes(&series.metric))
}

/// The bytes one chunk a binary operand retains holds on its own.
///
/// It is [`series_bytes`] with the samples a chunk's rows occupy in place of the allocations they
/// are slices of: a series delivered in a thousand chunks holds a thousand slices of one buffer, so
/// counting that buffer once per chunk would make the operand's figure grow with the number of
/// chunks rather than with the data in them. The slices of one series add up to the allocation they
/// share, which is the figure a deduplication across the chunks would arrive at.
///
/// The histogram tables a chunk's segments reference are not part of the figure: a table is one
/// allocation however many chunks index into it, so it is not a per-chunk cost at all. Dropping it
/// here is therefore only half the story -- it must not become a figure the operand never charges.
/// [`unbilled_tables`] is the other half, and the operand bills each table once through it.
fn operand_chunk_bytes(series: &Series) -> usize {
    series_bytes(series)
        .saturating_sub(fields_bytes(&series.fields))
        .saturating_add(sample_slice_bytes(&series.fields))
}

/// The bytes the samples of one series' fields take as the slices they are.
///
/// A dense segment is counted by the rows it holds -- not by the buffer its slice belongs to -- and a
/// field of native histograms by its column of indices, because the table those indices point into
/// is one allocation the chunks of the series share. A nested segment holds rows of its own in the
/// chunks it is made of, so it is left to them.
fn sample_slice_bytes(fields: &SeriesFields) -> usize {
    fields
        .iter()
        .flat_map(|field| field.segments())
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

/// The histogram tables `series` is the first to reference, and the bytes they hold.
///
/// A table is one allocation the chunks of a series share, so it is neither a per-chunk cost --
/// counting it per chunk would make a series delivered in a thousand chunks cost a thousand tables
/// -- nor no cost at all, which is what leaving it out of every chunk leaves the figure at. What it
/// is, is a cost of the operand that first retains a chunk referencing it: the identities it returns
/// are the tables `billed` does not hold yet, and the figure is what those tables hold
/// ([`table_bytes`]). The caller bills the figure as it stands and adds the identities to `billed`
/// once the chunk is retained, which is what makes the two halves of the figure agree.
///
/// The identity is the address of the allocation behind the `Arc`. It names one table for as long as
/// it is remembered, because a chunk referencing a table keeps it alive and no later allocation can
/// be given the same address. A table two segments or two fields of the one chunk reference is
/// listed once. Each identity is also one slot in the set that remembers it, which is charged as it
/// is written -- one entry at a time, like the boundary list's, and not from the capacity the set
/// grows to -- so remembering a table is paid for beside the table itself.
fn unbilled_tables(series: &Series, billed: &HashSet<usize>) -> (Vec<usize>, usize) {
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

/// A rough workspace estimate for merging fragments that hold `rows` rows between them.
///
/// It allows for sorted row indices, readers, sample descriptors and output headers. Like the
/// retained-state estimates, it is not an allocation bound: vector slack, temporary conversions
/// and null arrays for complementary fields are not measured individually.
///
/// Existing values and histogram tables remain shared with the fragments, whose estimates already
/// account for their payloads.
fn merge_workspace_bytes(rows: usize, fragments: usize, fields: usize) -> usize {
    let sorted_rows = rows.saturating_mul(
        std::mem::size_of::<(usize, usize, usize)>() + std::mem::size_of::<usize>(),
    );
    let readers = fragments.saturating_mul(
        fields.saturating_mul(std::mem::size_of::<Samples>() + 4 * std::mem::size_of::<usize>()),
    );
    // Allow for run and flattened-field descriptors when rows alternate between fragments or
    // missing values; this is an estimate rather than a count of the allocations made.
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

/// The bytes the fields of a chunk hold: the sample buffers of every segment, and the histogram
/// tables those segments reference.
fn fields_bytes(fields: &SeriesFields) -> usize {
    fields
        .iter()
        .map(|field| {
            field.segments().iter().fold(0usize, |total, segment| {
                total.saturating_add(segment_bytes(segment))
            })
        })
        .fold(0usize, usize::saturating_add)
}

/// The bytes one dense segment of samples holds.
fn segment_bytes(segment: &Samples) -> usize {
    match segment.dense() {
        Some(DenseSamplesRef::Float(values)) => values.get_array_memory_size(),
        Some(DenseSamplesRef::Histogram(histograms)) => histograms
            .indices()
            .get_array_memory_size()
            .saturating_add(table_bytes(histograms.batch())),
        Some(DenseSamplesRef::Mixed(mixed)) => mixed
            .floats()
            .get_array_memory_size()
            .saturating_add(mixed.histograms().indices().get_array_memory_size())
            .saturating_add(table_bytes(mixed.histograms().batch())),
        // `segments` hands out dense segments only, so a nested chunk never reaches here.
        None => 0,
    }
}

/// The bytes one histogram table holds: the batch struct and the arrays its columns view.
fn table_bytes(table: &Arc<HistogramBatch>) -> usize {
    std::mem::size_of::<HistogramBatch>().saturating_add(table.as_arrow().get_array_memory_size())
}

/// The bytes the allocation `buffer` is a slice of.
///
/// The capacity is the whole allocation, which is what a slice keeps alive: a key of a few bytes
/// sliced out of a decoded dictionary values buffer pins that buffer. A buffer that owns no memory
/// of its own reports no capacity, and then the bytes it shows are all there is.
fn buffer_bytes(buffer: &Buffer) -> usize {
    buffer.capacity().max(buffer.len())
}

/// Which node kinds the runtime executes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NodeTag {
    /// Forwards the chunks it is given.
    Input,
    /// A constant scalar: the value its consumer broadcasts, and no chunk of its own.
    ///
    /// It is a value node and not a stream. Its output type is [`NativeType::Scalar`], and the
    /// runtime produces evaluated chunks only, so the node hands nothing over: what its consumer
    /// reads is the constant bound to it, which is why it has no input and no run-time state. A
    /// `pull` of it answers `None` for exactly that reason, and that is not a stall to fix.
    Scalar,
    /// Buffers one series and selects the last sample at or before each of its grid points.
    Instant,
    /// Feeds its input to its own [`RangeEvaluator`].
    Window,
    /// Applies point-wise operations to its input chunks.
    Map,
    /// Matches two evaluated vectors and applies one operation to every matched pair of series, or
    /// broadcasts one constant over the chunks of one evaluated vector.
    Binary,
}

impl NodeTag {
    /// What this tag is called in a rejection.
    fn describe(self) -> &'static str {
        match self {
            Self::Input => "input",
            Self::Scalar => "scalar",
            Self::Instant => "instant selection",
            Self::Window => "window",
            Self::Map => "map",
            Self::Binary => "binary",
        }
    }
}

/// One node of the flattened tree.
#[derive(Clone, Copy, Debug)]
struct NodeSpec {
    tag: NodeTag,
    /// The index of this node's input, which is the node the build pass reaches next: the one
    /// input of a unary node, the left operand of a binary node, and none for an input node.
    input: Option<usize>,
}

/// The series whose first chunk this buffer received and whose closing chunk it has not seen.
///
/// A window reads one series at a time, to the chunk carrying `is_last`, so the chunks of one series
/// arrive together and each of them continues the one before. What is kept here is the identity of
/// the open series and the last position it decided, which is what says whether the next chunk still
/// belongs to it and still moves forward.
struct OpenSeries {
    /// The identity of the open series, as the chunk that opened it named it.
    metric: MetricName,
    /// The last absolute position the open series decided, when it decided any.
    last_position: Option<usize>,
}

/// The input a window with a [`NodeRuntime::merge`] collects until its input ends.
///
/// Such a window cannot evaluate the series under one canonical label set one after another: its
/// evaluator closes a series on the chunk carrying `is_last`, and the points a window across the
/// boundary needs are exactly the ones that closed series held. The fragments are therefore kept
/// whole, merged into one series per label set with
/// [`crate::grid_series::merge_same_labelset`], and only then evaluated.
///
/// Collecting is also where the contract a series keeps is checked, because the merge that follows
/// would hide a stream that broke it: it sorts the rows by position and groups them by label set, so
/// runs that went backwards inside one series and an identity that changed inside one series both
/// come out as a plausible merged series. A streaming window refuses such a stream on the chunk that
/// breaks it, and so does [`MergeInput::check`] before the chunk is kept. An input that ends inside
/// a series is the one thing left to the executor's own check: it is reported before anything is
/// flushed, because the close a merge synthesises would make such a series look like one that ended.
struct MergeInput {
    /// The schema the fragments are addressed on: their grid and their field count.
    schema: Arc<SeriesSchema>,
    /// The series received so far, in the order they arrived.
    fragments: Vec<Series>,
    /// An estimate of the bytes the fragments and the merge that follows them stand for.
    ///
    /// A fragment is measured once as it arrives; the merge's workspace is added once, before the
    /// helper runs, from the rows, segments and fields of the fragments as they were collected.
    /// Neither figure walks the buffer again, and the space the fragments' payload shares is
    /// counted once per fragment rather than deduplicated.
    bytes: usize,
    /// The series the next chunk has to continue, if the last chunk this buffer took did not close
    /// one. Nothing is kept once a series closed: the next chunk, if any, starts another series.
    open: Option<OpenSeries>,
}

impl MergeInput {
    /// A buffer for the fragments of one window's input, which are addressed on `schema`.
    fn new(schema: Arc<SeriesSchema>) -> Self {
        Self {
            schema,
            fragments: Vec::new(),
            bytes: 0,
            open: None,
        }
    }

    /// Checks one received chunk against the contract a series keeps, and follows the open series.
    ///
    /// The checks are the ones a streaming window makes on every delivery it reads: the chunk is
    /// addressed on this window's input grid, carries one sample per row in every field, holds a
    /// position unless it closes its series, continues the identity of the series it belongs to, and
    /// only ever moves forward within it. They cost a constant number of reads of the chunk's own
    /// axis -- its first and last positions, not its rows, which the merge then copies nothing of --
    /// and they are what keeps the sort below honest: a stream that broke one of them is refused
    /// here, on the chunk that broke it, rather than merged into an answer.
    ///
    /// A chunk carrying `is_last` closes the open series, whether it holds rows or none at all, and
    /// nothing is remembered of it: the next chunk starts a series whose identity is its own.
    fn check(&mut self, path: &NodePath, series: &Series, is_last: bool) -> Result<()> {
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
            return Err(DataFusionError::Execution(format!(
                "the window at {path} was handed an evaluated series that does not match its input \
                 schema"
            )));
        }
        let (first, last) = (series.time.first_position(), series.time.last_position());
        if !is_last && first.is_none() {
            return Err(DataFusionError::Execution(format!(
                "the window at {path} was handed an evaluated series that holds no position before \
                 the last run of its series"
            )));
        }
        if let Some(open) = &self.open
            && open.metric != series.metric
        {
            return Err(DataFusionError::Execution(format!(
                "the window at {path} was handed an evaluated series that changed identity before \
                 its last run"
            )));
        }
        if let Some(previous) = self.open.as_ref().and_then(|open| open.last_position)
            && let Some(first) = first
            && first <= previous
        {
            return Err(DataFusionError::Execution(format!(
                "the window at {path} was handed evaluated series runs that are not increasing: \
                 {first} after {previous}"
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

    /// The bytes of the fragments and of the workspace merging them needs.
    ///
    /// The workspace is what the helper allocates for the rows the fragments hold; the sample
    /// payload is shared with them, so it is not counted twice here.
    fn workspace_bytes(&self) -> usize {
        let rows: usize = self
            .fragments
            .iter()
            .map(|fragment| fragment.time.len())
            .sum();
        merge_workspace_bytes(rows, self.fragments.len(), self.schema.fields.len())
    }
}

/// What every node holds: its metric handles and the bytes it reserves.
///
/// The reservation is the pool's, so a node that buffers input competes with every other operator
/// in the query; the metrics report the same figure so a slow node can be told apart from a wide
/// one. The reservation is also the figure itself: a node holds nothing beside what it reserved, so
/// a gauge reads its size rather than a copy that could drift from it.
struct NodeBuffer {
    metrics: Arc<NodeMetrics>,
    reservation: MemoryReservation,
}

impl NodeBuffer {
    /// Registers a reservation for the node at `path` with `pool`.
    fn new(path: &NodePath, metrics: Arc<NodeMetrics>, pool: &Arc<dyn MemoryPool>) -> Self {
        let consumer = MemoryConsumer::new(format!("native.{}", path.as_str()));
        Self {
            metrics,
            reservation: consumer.register(pool),
        }
    }

    /// Reserves `bytes` beside what this node already holds.
    fn grow(&mut self, bytes: usize) -> Result<()> {
        self.reservation.try_grow(bytes)?;
        self.record();
        Ok(())
    }

    /// Reserves exactly `bytes`, which is what this node holds from now on.
    ///
    /// What a node that consumes its input chunk by chunk holds afterwards is not the sum of what
    /// it consumed, so the reservation is set from the state that is left rather than grown per
    /// chunk.
    fn resize(&mut self, bytes: usize) -> Result<()> {
        self.reservation.try_resize(bytes)?;
        self.record();
        Ok(())
    }

    /// Gives `bytes` of what this node holds back to the pool.
    fn shrink(&mut self, bytes: usize) -> Result<()> {
        self.reservation.try_shrink(bytes)?;
        self.record();
        Ok(())
    }

    /// Reports what this node now reserves.
    fn record(&self) {
        self.metrics.record_reserved_bytes(self.reservation.size());
    }

    /// Returns every byte this node holds to the pool.
    fn release(&mut self) {
        self.reservation.free();
        self.record();
    }
}

/// One run of grid positions, half-open, that hold a value in one field.
#[derive(Clone, Copy, Debug)]
struct Run {
    start: usize,
    end: usize,
}

impl Run {
    /// Whether two runs share a position.
    fn overlaps(self, other: Self) -> bool {
        self.start < other.end && other.start < self.end
    }
}

/// The positions one node has handed out, per output label set and field.
///
/// A node that rewrites labels — a `Map` dropping the metric name, a `Window` dropping the name
/// and the empty-valued labels — can turn two input series into one label set, and the node above
/// reads the two chunks as one series. What is kept here is which positions of each field were
/// already handed out under each label set, so a second series onto a set can be refused before
/// the node above evaluates it.
///
/// Occupancy is per field and per position, not per chunk range: a chunk reports the positions it
/// decided, which include the ones holding no value, and series that share a decided range while
/// sharing no sample merge cleanly, as they do in [`crate::grid_series::merge_same_labelset`]. A
/// chunk holding no value occupies nothing and changes nothing.
///
/// A set is never retired: `is_last` closes the source series whose data the chunk carries, and a
/// later series may still fill the positions the closed one left empty. Every run handed out stays
/// charged until the node is released, which is what lets a later chunk be refused against the
/// positions an earlier series already occupied.
///
/// The table is one node's own: a node knows what it has handed out itself and sees nothing of what
/// the node below kept, so a label set costs its bytes again in every node that keeps a table, and
/// what is deduplicated here is one node's history rather than a plan's.
#[derive(Default)]
struct Emitted {
    sets: HashMap<LabelSet, History>,
    /// How many run vectors, runs and label payload bytes every entry holds, counted as the entries
    /// change rather than by walking them.
    ///
    /// A node records every label set it has ever handed out and charges for all of them, so the
    /// figure is read after every output; walking the table there would cost a pass over every entry
    /// recorded so far for each output. Folding one entry's change in as it happens keeps the read
    /// constant, and the update is the shape of the change itself: a vector's length before and
    /// after [`absorb`], and one vector per field a history gains.
    counts: Counts,
}

/// What the entries of an [`Emitted`] table hold, as running totals.
#[derive(Default)]
struct Counts {
    /// The run vectors, one per field of one entry.
    vectors: usize,
    /// The occupied runs in every vector.
    runs: usize,
    /// The payload of every label set: the bytes of its names and values with one row per label.
    payload: usize,
}

/// What one node has handed out under one output label set.
#[derive(Default)]
struct History {
    /// The occupied runs of each field, in position order and disjoint.
    fields: Vec<Vec<Run>>,
}

/// The bytes a label set holds: each label's name and value with the pair that refers to them.
fn label_bytes(labels: &LabelSet) -> usize {
    labels
        .iter()
        .map(|(name, value)| std::mem::size_of::<(Arc<str>, Arc<str>)>() + name.len() + value.len())
        .sum()
}

/// The bytes a metric name holds: its name and each tag's name and value, with the tags that refer
/// to them.
///
/// This is the convention [`label_bytes`] keeps: the payload of the shared strings and one row per
/// label.
fn metric_bytes(metric: &MetricName) -> usize {
    metric.name().map_or(0, |name| name.len())
        + std::mem::size_of_val(metric.tags())
        + metric
            .tags()
            .iter()
            .map(|tag| tag.name.len() + tag.value.len())
            .sum::<usize>()
}

impl Emitted {
    /// Records what `chunk` occupies, rejecting it against what this node already handed out.
    ///
    /// A second series onto a label set is legal when it holds its values where the first one held
    /// none: the two merge into one series, which is what
    /// [`crate::grid_series::merge_same_labelset`] does. What no merge resolves is two values of one
    /// field at one position, so that is the only thing refused here. `is_last` is not consulted: it
    /// closes the source series the chunk carries, not the label set, and a later series may still
    /// fill the positions that one left empty.
    fn occupy(&mut self, path: &NodePath, chunk: &EvaluatedChunk) -> Result<()> {
        let labels = label_set(&chunk.series.metric);
        let occupied: Vec<Vec<Run>> = chunk
            .series
            .fields
            .iter()
            .enumerate()
            .map(|(index, field)| occupied_runs(path, index, field, &chunk.series.time))
            .collect::<Result<_>>()?;
        let empty = occupied.iter().all(Vec::is_empty);
        if let Some(history) = self.sets.get(&labels) {
            for (index, runs) in occupied.iter().enumerate() {
                let stored = history.fields.get(index).map_or(&[][..], Vec::as_slice);
                for run in runs {
                    // The stored runs are sorted and disjoint, so only the one holding `run.start`
                    // or the one just before it can share a position. Scanning them all would make
                    // a sparse series split across chunks cost a pass over its whole history per
                    // chunk.
                    let at = stored.partition_point(|stored| stored.end <= run.start);
                    let previous = stored.get(at).filter(|stored| stored.overlaps(*run));
                    if let Some(previous) = previous {
                        return Err(occupied_position(path, &labels, index, run, previous));
                    }
                }
            }
        }
        if empty {
            // A chunk holding no value records nothing and closes nothing: a series that answered
            // empty leaves every earlier series on this label set exactly where it was.
            return Ok(());
        }
        let fields = chunk.series.fields.len();
        let history = match self.sets.entry(labels) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                self.counts.payload += label_bytes(entry.key());
                entry.insert(History::default())
            }
        };
        if history.fields.len() < fields {
            self.counts.vectors += fields - history.fields.len();
            history.fields.resize_with(fields, Vec::new);
        }
        for (index, runs) in occupied.into_iter().enumerate() {
            let stored = &mut history.fields[index];
            let before = stored.len();
            absorb(stored, &runs);
            self.counts.runs = self.counts.runs - before + stored.len();
        }
        Ok(())
    }

    /// An estimate of the bytes this table holds, which the node reserves for.
    ///
    /// It counts one label-set slot and one history header per entry, one vector per field of a
    /// history, the runs in those vectors, and the payload of every label set. Each of those is a
    /// running total, so the figure is read in constant time however many series the node has
    /// handed out; it leaves out the map's buckets, the slack of a vector's capacity and the `Arc`
    /// headers of the strings it shares, so it is an approximation rather than a measurement.
    fn bytes(&self) -> usize {
        let slots =
            self.sets.len() * (std::mem::size_of::<LabelSet>() + std::mem::size_of::<History>());
        slots
            + self.counts.vectors * std::mem::size_of::<Vec<Run>>()
            + self.counts.runs * std::mem::size_of::<Run>()
            + self.counts.payload
    }
}

/// The runs of positions holding a value in field `index`, as the axis `time` places its rows.
///
/// A missing sample holds no value; a `NaN` and a native histogram do, as they do for a merge.
/// A hole between two rows is not a position either: the axis skips it, so a run open before the
/// hole ends where the hole begins rather than reaching across it. A chunk of several runs is
/// therefore recorded as the runs it holds, and a series that occupies the hole is not refused.
///
/// The axis is what says how many rows there are, so every field it reads has to hold one sample per
/// row. [`Series`] keeps its fields public, so a chunk built by struct literal can carry fewer, and
/// that is refused here rather than read past the end of the field.
fn occupied_runs(
    path: &NodePath,
    index: usize,
    field: &Samples,
    time: &TimeAxis,
) -> Result<Vec<Run>> {
    let mut runs: Vec<Run> = Vec::new();
    let mut open: Option<usize> = None;
    let mut previous: Option<usize> = None;
    let mut cursor = field.cursor();
    for (position, row) in time.positions() {
        // The axis names the absolute position of every row, so the hole between two rows is
        // visible as a jump in `position` while no row of this chunk sits in it.
        if let Some(previous) = previous
            && position != previous + 1
            && let Some(start) = open.take()
        {
            runs.push(Run {
                start,
                end: previous + 1,
            });
        }
        let sample = cursor
            .at(row)
            .ok_or_else(|| misaligned_field(path, index, time, field))?;
        let present = !matches!(sample, SampleRef::Missing);
        match (present, open) {
            (true, None) => open = Some(position),
            (false, Some(start)) => {
                runs.push(Run {
                    start,
                    end: position,
                });
                open = None;
            }
            _ => {}
        }
        previous = Some(position);
    }
    if let Some(start) = open {
        // A run still open at the last row ends just past it, not where the field's row count says:
        // the rows need not be adjacent to the positions above them.
        runs.push(Run {
            start,
            end: previous.map_or(start, |last| last + 1),
        });
    }
    Ok(runs)
}

/// The rejection of a series whose axis and fields disagree on how many rows there are.
///
/// [`Series`] keeps its fields public, so a chunk built by struct literal — by a caller of this crate
/// as much as by a mistake here — can carry fewer samples than the axis places rows, and then the row
/// the axis names has nothing to read.
fn misaligned_field(
    path: &NodePath,
    index: usize,
    time: &TimeAxis,
    field: &Samples,
) -> DataFusionError {
    DataFusionError::Execution(format!(
        "the node at {path} was handed a series whose axis and fields are misaligned: the axis \
         places {} rows, but field {index} holds {} samples, so the axis names a row the field does \
         not have",
        time.len(),
        field.len(),
    ))
}

// How many times `absorb` rebuilt the stored runs instead of appending to them.
//
// Test-only, and per thread: the runs stored afterwards are the same either way, so a rebuild shows
// only in what it costs, and a test that reads its own thread's figure is not disturbed by another
// test's chunks.
#[cfg(test)]
thread_local! {
    static ABSORB_REBUILDS: std::cell::Cell<usize> = const { std::cell::Cell::new(0) };
}

/// How many times this thread's [`absorb`] rebuilt the stored runs.
#[cfg(test)]
fn absorb_rebuilds() -> usize {
    ABSORB_REBUILDS.with(std::cell::Cell::get)
}

/// Merges `incoming` into `stored`, keeping the result in position order and joining the runs that
/// touch.
///
/// Within a chunk one pass over both sides is what keeps this linear, since each side is sorted and
/// disjoint. Adding the runs one at a time and rebuilding the array after each would cost the square
/// of the runs a chunk yields: a chunk of one hundred thousand positions alternating between values
/// and nulls yields fifty thousand runs, and re-merging after each is over a billion element moves
/// for a single series.
///
/// One pass per chunk is still a pass over everything the series has stored so far, so it is not
/// free once the *history* grows across chunks: a series split into `K` chunks of one position each,
/// a hole apart, stores one more run per chunk and moves `K(K+1)/2` of them, with no label collision
/// and no illegal input anywhere. A chunk that starts no earlier than the end of the last stored run
/// only ever adds to the end — the stored runs are sorted and disjoint, so every one of them ends
/// before anything this chunk holds — and it is appended instead, which is amortised constant per
/// run. That is the shape a scan hands over as its positions move forward chunk after chunk. A chunk
/// that starts before the last stored run can split a run or reach back past it, so it still costs
/// the full merge.
///
/// Either way the result is sorted, disjoint, and has the runs that touch joined, which is what
/// [`Emitted::occupy`] looks a position up by.
fn absorb(stored: &mut Vec<Run>, incoming: &[Run]) {
    if incoming.is_empty() {
        return;
    }
    if stored
        .last()
        .is_none_or(|last| incoming[0].start >= last.end)
    {
        append_runs(stored, incoming);
    } else {
        merge_runs(stored, incoming);
    }
}

/// Appends `incoming` to `stored`, all of which lies past the last stored run.
///
/// Nothing already stored has to be placed again: the two sides are each in position order and
/// nothing `incoming` holds can reach back into what is stored, so its runs are pushed on the end and
/// the vector's own growth is the only move — amortised over the runs appended. Where its first run
/// touches the end of the last stored run the two are one run, as they are after a merge, so it is
/// joined rather than left beside it.
fn append_runs(stored: &mut Vec<Run>, incoming: &[Run]) {
    let mut incoming = incoming;
    if let Some(last) = stored.last_mut()
        && let Some(first) = incoming.first()
        && first.start <= last.end
    {
        last.end = last.end.max(first.end);
        incoming = &incoming[1..];
    }
    stored.extend_from_slice(incoming);
}

/// Merges `incoming` into `stored`, where the two can interleave.
///
/// One pass is enough because each side is sorted and disjoint: whichever run starts first is
/// placed, and it joins the run before it where the two touch.
fn merge_runs(stored: &mut Vec<Run>, incoming: &[Run]) {
    #[cfg(test)]
    ABSORB_REBUILDS.with(|rebuilds| rebuilds.set(rebuilds.get() + 1));
    let mut merged: Vec<Run> = Vec::with_capacity(stored.len() + incoming.len());
    let (mut a, mut b) = (0, 0);
    while a < stored.len() || b < incoming.len() {
        let larger = match (stored.get(a), incoming.get(b)) {
            (Some(left), Some(right)) if right.start < left.start => {
                b += 1;
                *right
            }
            (Some(left), _) => {
                a += 1;
                *left
            }
            (None, Some(right)) => {
                b += 1;
                *right
            }
            (None, None) => break,
        };
        match merged.last_mut() {
            Some(last) if larger.start <= last.end => last.end = last.end.max(larger.end),
            _ => merged.push(larger),
        }
    }
    *stored = merged;
}

/// What a map node applies.
struct MapRuntime {
    ops: Arc<[MathOp]>,
    /// The schema of what the map produces, which is what its input is checked against.
    schema: Arc<SeriesSchema>,
}

/// What an instant node holds: the chunks of the one series it is selecting, until that series ends.
///
/// [`select_instants`] takes a complete source axis, so this node cannot decide a grid point from
/// the samples one chunk carries the way a window decides a range from what it has retained: it
/// buffers a whole series and hands one chunk over for it. What it holds is therefore the series'
/// own bytes, not a fixed window of them, and the reservation follows the buffer.
struct InstantRuntime {
    /// The bound selection: how far back it reads, the offset it reads at, and the grid its values
    /// are addressed on.
    selection: BoundInstant,
    /// The layout the chunk it hands out is addressed on: its input's fields on that grid.
    schema: Arc<SeriesSchema>,
    /// Where the rows the selection picked are gathered, one series at a time.
    output: GridSeriesBuilder,
    /// The chunks of the series being selected, in arrival order; empty while no series is open.
    chunks: Vec<RawSeriesChunk>,
    /// What `chunks` hold, added as each chunk arrives and given back when the series is gathered.
    ///
    /// Each chunk is measured once, with the estimate a queued chunk is charged with: an allocation
    /// two chunks of the series share is counted once per chunk, which is the over-count a figure
    /// that keeps no state across chunks accepts.
    held: usize,
}

impl InstantRuntime {
    /// The run-time state of the instant selection `op`, reading the samples of `input`.
    fn try_new(input: &SeriesSchema, op: &BoundInstant) -> Result<Self> {
        // The chunk this node hands out is addressed on the selection's own grid, so the grid the
        // rows are gathered on is the binding's rather than a copy of it.
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

    /// The bytes the buffered chunks of the open series hold.
    fn buffered_bytes(&self) -> usize {
        self.held
    }

    /// The bytes gathering the series `closing` ends is about to allocate beside its chunks.
    ///
    /// An upper bound derived from what the selection has in hand rather than a measurement: the
    /// rows the series holds once `closing` has joined it, the fields it carries, the points the
    /// selection can decide for it, and, per field, the largest histogram a row of it can carry.
    /// Each term is one thing the gather allocates:
    ///
    /// - The concatenated axis ([`buffered_time`]): one `i64` per row, reserved by the row count.
    /// - The descriptors that read it ([`buffered_fields`]): one [`Samples`] per (chunk, field) for
    ///   the per-field vectors, three per segment for the vector [`Samples::try_from_chunks`]
    ///   flattens them into -- the segment it came from, the vector it is flattened into and the
    ///   `Arc<[Samples]>` the result is copied into -- and one `Vec<Samples>` header per field.
    /// - The two arrays [`select_instants`] returns: a `u64` source row index and an `i64`
    ///   evaluation timestamp per point, reserved by the count they can reach at most.
    /// - The rows [`GridSeriesBuilder`] keeps: one `usize` grid index, one `f64` per field and a
    ///   validity byte per (row, field). A field of native histograms adds one `Option<u32>` index
    ///   per row and the payload of the histogram that index names, charged at the largest one the
    ///   field holds and per *point* rather than per row: one source row can be selected by every
    ///   point the selection decides, so the point count is what that payload is multiplied by.
    /// - The row the selection fills, one [`GridSample`] slot per field, and one [`RowCursor`] per
    ///   field to read the axis with.
    /// - Doubling: every column the builder keeps is a `Vec` that grows by doubling, so each is
    ///   charged twice what it ends up holding, which bounds its capacity rather than its length.
    ///
    /// Left out, deliberately: the headers and Arrow buffers behind the buffered chunks (the
    /// node's own reservation already holds them, allocation by allocation), the `Vec` headers the
    /// gather drops before it returns, `Series`' own header and the runs [`GridSeriesBuilder`]
    /// hands the output over with, which the output chunk is charged for by whoever reads it, and
    /// the temporaries [`FloatHistogram::push_to`] builds for one row. None of those is
    /// proportional to the points the selection decides, which is the amplification the pool has to
    /// be asked for, and all of them are constant per gather or already counted row by row.
    ///
    /// The estimate applies to every gather: nothing refuses one before it allocates, so the points
    /// the series can be decided at are reserved for whatever their number is.
    ///
    /// [`FloatHistogram::push_to`]: crate::histogram_eval::float_histogram::FloatHistogram::push_to
    fn gather_bytes(&self, closing: &RawSeriesChunk) -> usize {
        // The points the selection decides, not the points the grid plans: the gather clips its
        // scan to the series it holds, so a figure over the whole grid would charge the pool for
        // rows this series can never produce.
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

    /// The number of grid points [`select_instants`] clips the buffered series to.
    ///
    /// The gather clips its scan to the series it holds instead of reading the whole planned grid:
    /// the candidate points run from the first sample's instant to the last sample's plus the
    /// lookback, aligned to the grid and held inside the query range. A workspace is charged per
    /// point, so the count has to be the clipped one -- charging the planned grid charges the pool
    /// for rows a short series can never produce, and a pool sized for what the series costs then
    /// refuses the gather. The clipping below is [`select_instants`]' own, repeated rather than
    /// approximated: an estimate that disagrees with the kernel is a bug here rather than a
    /// conservative bound. It is the kernel's own candidate count -- the one it sizes its arrays
    /// with -- so it bounds the rows it can report even though a candidate at the lookback edge
    /// itself selects nothing. Both ends come from chunks the node already holds, so the count
    /// reads two timestamps rather than the rows.
    fn selected_points(&self, closing: &RawSeriesChunk) -> usize {
        let spec = self.selection.output_grid.spec();
        // The selection refuses these itself, before it can decide anything.
        if spec.step_ms <= 0 || self.selection.lookback_ms < 0 {
            return 0;
        }
        let query_start = i128::from(spec.start_ms);
        let query_end = i128::from(grid_end_ms(spec));
        if query_end < query_start {
            return 0;
        }
        let series = || self.chunks.iter().chain(std::iter::once(closing));
        let (Some(first), Some(last)) = (
            series().find_map(|chunk| chunk.time.as_buffer().first().copied()),
            series()
                .rev()
                .find_map(|chunk| chunk.time.as_buffer().last().copied()),
        ) else {
            // A series of no sample decides no point.
            return 0;
        };
        let offset = i128::from(self.selection.offset_ms);
        let lookback = i128::from(self.selection.lookback_ms);
        let first = i128::from(first) + offset;
        let last = i128::from(last) + offset;
        let useful_last = if lookback == 0 {
            last
        } else {
            last + lookback - 1
        };
        let max_start = first.max(query_start);
        let min_end = useful_last.min(query_end);
        if max_start > min_end {
            return 0;
        }
        let step = i128::from(spec.step_ms);
        let aligned_start = query_start + (max_start - query_start) / step * step;
        let aligned_end = query_end - (query_end - min_end) / step * step;
        let points = (aligned_end - aligned_start) / step + 1;
        usize::try_from(points).unwrap_or(usize::MAX)
    }

    /// The largest histogram a row of each field can carry, `None` for a field of floats.
    ///
    /// A row of native histograms is copied into the builder once per grid point that selects it,
    /// so what such a field costs is one histogram per point: the largest row is the one to charge,
    /// because any row can be the one every decided point picks.
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

    /// Buffers one chunk of the series being selected, and gathers that series when it ends.
    ///
    /// `None` means the series is still open: the selection reads one whole axis, so nothing is
    /// decided for a series until the chunk carrying `is_last` arrives.
    fn push(&mut self, path: &NodePath, chunk: RawSeriesChunk) -> Result<Option<EvaluatedChunk>> {
        if let Some(open) = self.chunks.first()
            && (open.primary_key.as_slice() != chunk.primary_key.as_slice()
                || open.metric != chunk.metric)
        {
            // One axis is gathered before the kernel reads any of it, so a chunk of another series
            // has nowhere to go: it would be selected as though its samples were the open series'.
            return Err(series_changed(path));
        }
        let closing = chunk.is_last;
        self.held = self.held.saturating_add(raw_bytes(&chunk));
        self.chunks.push(chunk);
        if !closing {
            return Ok(None);
        }
        self.gather(path).map(Some)
    }

    /// Selects the instants of the buffered series, hands it over closed, and drops the buffer.
    fn gather(&mut self, path: &NodePath) -> Result<EvaluatedChunk> {
        let chunk = self.select(path)?;
        // The selection copied what it needed out of the buffered chunks, so a series that has been
        // answered must not keep charging the pool for them.
        self.chunks.clear();
        self.chunks.shrink_to_fit();
        self.held = 0;
        Ok(chunk)
    }

    /// The last sample at or before each grid point of the buffered series, as one chunk.
    fn select(&mut self, path: &NodePath) -> Result<EvaluatedChunk> {
        let Some(first) = self.chunks.first() else {
            return Err(DataFusionError::Execution(format!(
                "the instant selection at {path} holds no series to select"
            )));
        };
        let metric = first.metric.clone().ok_or_else(|| no_labels(path))?;
        // The buffered chunks are one series, so they are read as the one axis the selection takes.
        let time = buffered_time(&self.chunks)?;
        let fields = buffered_fields(&self.chunks, self.schema.fields.len())?;
        let grid = Arc::clone(&self.selection.output_grid);
        let spec = grid.spec();
        // The grid's own points are what the selection decides, so the range and the step it is
        // asked for are the grid's: `start..=start + (len - 1) * step`, step by step.
        let (indices, timestamps) = select_instants(
            &time,
            &fields,
            spec.start_ms..=grid_end_ms(spec),
            spec.step_ms,
            self.selection.lookback_ms,
            self.selection.offset_ms,
        )?;
        self.output.begin_series();
        // One cursor per field, so a row is read as the field holds it rather than by walking the
        // chunks of a field from its start once per row.
        let mut cursors: Vec<RowCursor<'_>> = fields.iter().map(Samples::cursor).collect();
        // `GridSample` is not `Clone`, so the row is filled slot by slot rather than from one value.
        let mut row: Vec<Option<GridSample>> = (0..fields.len()).map(|_| None).collect();
        let mut read: Option<usize> = None;
        for (index, timestamp) in indices.iter().zip(&timestamps) {
            let index = *index as usize;
            // The selection steps the grid's range by its step, so every timestamp it reports is a
            // point of the grid and the position is the point's index in it.
            let position = grid_position(&grid, *timestamp)?;
            // Two grid points can select one source row, and the row is read once for both.
            if read != Some(index) {
                for (slot, cursor) in row.iter_mut().zip(&mut cursors) {
                    let sample = cursor.at(index).ok_or_else(|| missing_row(path, index))?;
                    *slot = owned(sample)?;
                }
                read = Some(index);
            }
            // A position no field holds a value at is not a row: the builder drops it, which is
            // what keeps a hole in the grid out of the result instead of padding it.
            self.output.push_samples(position, &row)?;
        }
        // The rows the selection picked are the rows this series holds, and the whole series is
        // decided at once, so this one chunk closes it.
        match self.output.finish(metric.clone())? {
            Some(series) => EvaluatedChunk::try_new(series, true),
            None => no_row_chunk(&self.schema, metric),
        }
    }
}

/// One operand of a binary node, chunk by chunk, until it ends.
///
/// What is kept is the series each chunk carries, not a concatenation of them: a chunk is retained
/// as it arrives and nothing is copied on the way in, because the chunks of a series can share an
/// allocation and the figure the operand charges for is the data it holds rather than a running copy
/// of it.
///
/// The kernel matches whole series by label set, so the chunks are assembled into one series per
/// logical series once the input has ended: a series that arrived in a thousand chunks is otherwise
/// a thousand candidates on this side for every candidate of the other, and its own rows would be
/// paired a chunk at a time.
#[derive(Default)]
struct BinaryOperand {
    /// The series whose last chunk did not close it, with the position after its last row.
    ///
    /// `None` when no series is open: nothing has arrived yet, or the last series closed. A label
    /// set may be closed more than once -- a node that rewrites labels can make two series one --
    /// so a chunk that starts a series again is not an error, only one that starts it before the
    /// series already open ended, or names another series while it is open.
    open: Option<(MetricName, usize)>,
    /// The chunks this operand handed over, in arrival order.
    series: Vec<Series>,
    /// The chunk count at which each logical series closed, exclusive, in arrival order.
    ///
    /// The chunks `start..end` of one entry are one logical series: a label set that closes and
    /// starts again is two of them, so a list of ends is what keeps the two apart where a label set
    /// alone would read them as one. Only a chunk carrying `is_last` records an end, and it does so
    /// whether or not it carries a row.
    ends: Vec<usize>,
    /// The histogram tables the chunks this operand holds index into, by the address of the
    /// allocation behind their `Arc`.
    ///
    /// A table is one allocation however many chunks of a series index into it, so it is billed
    /// once -- by the first chunk that references it -- rather than once per chunk, and the tables
    /// this set names are the ones already billed. That is what keeps a series delivered in a
    /// thousand chunks from costing a thousand tables, and what keeps a table from costing nothing:
    /// a chunk's own figure leaves its tables out ([`operand_chunk_bytes`]), so a table billed by
    /// neither this operand nor this set is a table no reservation covers. One slot per billed
    /// table is charged with it, so the set's own storage is not a figure no reservation covers
    /// either.
    tables: HashSet<usize>,
    /// The bytes `series` and `ends` hold, folded in as chunks arrive.
    ///
    /// Each chunk is measured once, with the figure of what it holds on its own
    /// ([`operand_chunk_bytes`]): the samples its rows occupy and the descriptors it keeps, not the
    /// allocation those samples are slices of, so a series delivered in many chunks is charged the
    /// data it holds rather than the same buffer once per chunk. The histogram tables the chunks
    /// reference are the other half of that figure: one allocation the chunks share, billed once --
    /// with the one slot that remembers it -- the first time a chunk referencing it arrives
    /// ([`unbilled_tables`]).
    held: usize,
}

impl BinaryOperand {
    /// The bytes `chunk` would add to what this operand holds, read without retaining it.
    ///
    /// It is what [`Self::enqueue`] adds for the same chunk: what the chunk holds on its own, the
    /// histogram tables it is the first to reference, and the one entry a chunk that closes its
    /// series records in the boundary list. The boundary list holds one `usize` per closed series and
    /// is charged one entry at a time as it is written, rather than from the capacity it grows to --
    /// following that `Vec`'s first-four-then-double policy would be a second copy of it to keep in
    /// step, and one entry within a factor of two is the granularity the accounting aims at. The
    /// figure is what a caller reserves before it retains the chunk, so the pool admits it before
    /// the operand holds it.
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

    /// Retains one chunk, folding the estimate it is charged with into what the operand holds.
    fn enqueue(&mut self, chunk: EvaluatedChunk) {
        let closing = chunk.is_last;
        // The tables this chunk is the first to reference, read before they are recorded: the same
        // figure `pending` billed, so what is reserved for the chunk and what it grows the operand
        // by are one.
        let (tables, bytes) = unbilled_tables(&chunk.series, &self.tables);
        // The boundary list is storage the operand keeps, and it grows before the chunk is recorded
        // in it: an end is charged for before it exists, the same figure `pending` reported.
        self.held = self
            .held
            .saturating_add(if closing {
                std::mem::size_of::<usize>()
            } else {
                0
            })
            .saturating_add(operand_chunk_bytes(&chunk.series))
            .saturating_add(bytes);
        // From here the operand keeps those tables alive itself, so a chunk arriving later that
        // references one is charged its own slices and not the table again.
        self.tables.extend(tables);
        self.series.push(chunk.series);
        if closing {
            self.ends.push(self.series.len());
        }
    }

    /// The chunk ranges of the logical series this operand closed, in arrival order.
    ///
    /// Read after the input ended: a series that is still open has no end, and
    /// [`BinaryRuntime::closed`] refuses such an input before anything is assembled.
    fn boundaries(&self) -> impl Iterator<Item = std::ops::Range<usize>> + '_ {
        self.ends.iter().scan(0, |start, &end| {
            let range = *start..end;
            *start = end;
            Some(range)
        })
    }

    /// The bytes assembling this operand's chunks into logical series allocate, before they do.
    ///
    /// One series per closed logical series, on `schema`'s grid, and nothing that copies a sample:
    /// the descriptors the chunks' fields are made of are collected and shared. What is counted is
    /// therefore metadata only, and it is a rough figure per field, per segment and per run rather
    /// than a model of the containers the work builds:
    ///
    /// - per series, the assembled entry and the field list ([`SeriesFields`] takes a one-field list
    ///   out of its vector and boxes a wider one, so neither copies);
    /// - per chunk and field, the descriptor collected out of it;
    /// - per segment, three descriptors: the one the flatten copies out, the slot the `Arc<[Samples]>`
    ///   it is copied into holds, and the doubling of the vector it is copied out of;
    /// - per run the boundary covers, the span [`Self::assemble`] collects and the span and
    ///   row-offset arrays [`TimeAxis::from_grid_spans`] copies them into.
    ///
    /// No term follows how a `Vec` happens to grow -- there is no branch on a capacity here and no
    /// first-allocation floor -- so a change to the flatten's growth policy cannot make this figure
    /// wrong. What it is instead is a rough per-field, per-segment and per-run estimate, within a
    /// small factor of what the assembly holds at its peak, which is the granularity a reservation
    /// is made at.
    fn assembly_bytes(&self, schema: &SeriesSchema) -> Option<usize> {
        let samples = std::mem::size_of::<Samples>();
        let grids = std::mem::size_of::<GridSpan>();
        let width = schema.fields.len();
        let mut bytes = 0usize;
        for boundary in self.boundaries() {
            let chunks = &self.series[boundary];
            // The runs the boundary covers, and the segments its fields hold: a chunk's field can
            // itself be chunked, and flattening it copies one descriptor per segment.
            let mut runs = 0usize;
            let mut segments = 0usize;
            for chunk in chunks {
                runs = runs.checked_add(chunk.time.runs().count())?;
                for field in chunk.fields.iter() {
                    segments = segments.checked_add(field.segments().len())?;
                }
            }
            // The descriptors of a field's chunks, the flatten's descriptors and the `Arc` they are
            // copied into; a field with no segment still builds an empty `Samples`, which is one
            // descriptor.
            let descriptors = chunks
                .len()
                .checked_mul(width)?
                .checked_add(segments.checked_mul(3)?.checked_add(width)?)?;
            // The axis: the spans collected, and the span and offset arrays a run layout copies
            // them into.
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

    /// The logical series this operand closed, each as one series on `schema`'s grid.
    ///
    /// The chunks of a logical series are collected, not concatenated: the fields keep one
    /// descriptor per segment of every chunk, the axis keeps the runs the chunks cover, and the
    /// buffers behind both are shared with the chunks this operand still holds. The series is what
    /// the kernel matches and reads, so it must be on the schema's grid and hold as many rows as its
    /// axis places, which is what [`Series::try_new`] checks. What it allocates is
    /// [`Self::assembly_bytes`], which a caller reserves before this runs.
    fn assemble(&self, schema: &SeriesSchema, path: &NodePath, side: usize) -> Result<Vec<Series>> {
        let grid = schema.eval_grid.clone().ok_or_else(|| {
            DataFusionError::Execution(format!(
                "the binary node at {path} assembles its {} operand on an evaluation grid, and the \
                 operand's schema names none",
                side_name(side)
            ))
        })?;
        let mut assembled = Vec::with_capacity(self.ends.len());
        for boundary in self.boundaries() {
            let chunks = &self.series[boundary];
            let metric = chunks.first().map(|chunk| chunk.metric.clone()).ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "the binary node at {path} closed a logical series of its {} operand without a \
                     chunk to assemble",
                    side_name(side)
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
                        DataFusionError::Execution(format!(
                            "the binary node at {path} cannot assemble its {} operand: a chunk of \
                             it carries {} fields, not {}",
                            side_name(side),
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
        // The tables the cleared chunks were keeping alive are gone with them, so their addresses
        // name nothing this operand holds any more.
        self.tables.clear();
        self.tables.shrink_to_fit();
        self.held = 0;
    }
}

/// What a binary node holds: both operands until they end.
///
/// The operation pairs two whole vectors point by point, so this node cannot answer from the chunks
/// in hand the way a map does: it retains every chunk each operand hands over, assembles the chunks
/// of each logical series into the one series the kernel matches, and runs the kernel once, after the
/// input has ended and both operands closed. The chunks it produces are queued in the node's own
/// output queue and handed over from there.
struct BinaryRuntime {
    /// The operation and its modifiers, bound by the compiler.
    matching: Arc<BinaryMatching>,
    /// The layout each operand is bound to, what a chunk of it is checked against: `0` is the left
    /// operand and `1` the right one, the order the tree declares them in.
    schemas: [Arc<SeriesSchema>; 2],
    /// The retained chunks of each operand, in the same order.
    operands: [BinaryOperand; 2],
    /// Whether the kernel has run. It runs at most once: the operands it read are gone afterwards.
    computed: bool,
}

/// What side `index` is called in an error.
fn side_name(index: usize) -> &'static str {
    match index {
        0 => "left",
        _ => "right",
    }
}

impl BinaryRuntime {
    /// The run-time state of the binary operation `matching` over its two operands.
    fn new(matching: Arc<BinaryMatching>, schemas: [Arc<SeriesSchema>; 2]) -> Self {
        Self {
            matching,
            schemas,
            operands: [BinaryOperand::default(), BinaryOperand::default()],
            computed: false,
        }
    }

    /// The bytes the retained operands hold.
    fn retained_bytes(&self) -> usize {
        self.operands
            .iter()
            .map(|operand| operand.held)
            .fold(0usize, usize::saturating_add)
    }

    /// The bytes assembling both operands into logical series allocates, before it does.
    fn assembly_bytes(&self) -> Option<usize> {
        let [left, right] = &self.operands;
        let [left_schema, right_schema] = &self.schemas;
        left.assembly_bytes(left_schema)?
            .checked_add(right.assembly_bytes(right_schema)?)
    }

    /// The logical series both operands closed, ready for the kernel to match.
    fn assemble(&self, path: &NodePath) -> Result<[Vec<Series>; 2]> {
        let [left, right] = &self.operands;
        let [left_schema, right_schema] = &self.schemas;
        Ok([
            left.assemble(left_schema, path, 0)?,
            right.assemble(right_schema, path, 1)?,
        ])
    }

    /// The bytes the chunk of `side` would add to that operand's retention.
    ///
    /// It is read from the operand before the chunk is retained, so the figure is the one the
    /// operand grows by: what the chunk holds on its own and the room its end takes in the operand's
    /// boundary list.
    fn pending(&self, path: &NodePath, side: usize, chunk: &EvaluatedChunk) -> Result<usize> {
        self.operands
            .get(side)
            .map(|operand| operand.pending(chunk))
            .ok_or_else(|| no_binary(path))
    }

    /// Reports an operand that ended inside a series.
    ///
    /// A side hands over one series at a time, so the only series that can be open is the last one
    /// it handed over: a chunk that did not say `is_last` leaves it open, and nothing came after it
    /// to close it. The kernel is not run over such an operand: it would answer the pairs of a
    /// series whose remaining rows were never read.
    fn closed(&self, path: &NodePath) -> Result<()> {
        for (side, operand) in self.operands.iter().enumerate() {
            let Some((metric, _)) = &operand.open else {
                continue;
            };
            return Err(DataFusionError::Execution(format!(
                "the input of the binary node at {path} ended before the last chunk of a series: \
                 its {} operand handed over {metric:?} without the chunk that closes it",
                side_name(side)
            )));
        }
        Ok(())
    }

    /// Retains one chunk of the operand at `side`, checking it against that operand's binding.
    fn retain(&mut self, path: &NodePath, side: usize, chunk: EvaluatedChunk) -> Result<()> {
        // What a bound tree says an operand produces is not proof of what a chunk carries: a
        // `Series` keeps its fields public, so a chunk built by struct literal can carry another
        // number of fields, or fewer samples than its axis places rows. The kernel's own check is
        // run here, before the chunk enters the buffer, and the position and side are added to what
        // it reports.
        let Some(schema) = self.schemas.get(side) else {
            return Err(no_binary(path));
        };
        validate_binary_operand(schema.as_ref(), std::slice::from_ref(&chunk.series)).map_err(
            |error| {
                DataFusionError::Execution(format!(
                    "the binary node at {path} rejected its {} operand: {error}",
                    side_name(side)
                ))
            },
        )?;
        // An empty chunk closes a series without holding a row, so it says nothing about where that
        // series ended: only a chunk that carries rows can be read as going backwards.
        let (first, last) = (
            chunk.series.time.first_position(),
            chunk.series.time.last_position(),
        );
        let metric = chunk.series.metric.clone();
        let Some(operand) = self.operands.get_mut(side) else {
            return Err(no_binary(path));
        };
        // The position a chunk of the open series may start at: the row after the last one that
        // series handed over. Nothing is open after a series closed, so the next one starts over.
        let end = operand.open.as_ref().map_or(0, |(_, end)| *end);
        if let Some((open, _)) = &operand.open
            && *open != metric
        {
            return Err(DataFusionError::Execution(format!(
                "the binary node at {path} combines whole series, and its {} operand handed over \
                 {metric:?} before {open:?} ended",
                side_name(side)
            )));
        }
        if let Some(first) = first
            && first < end
        {
            return Err(DataFusionError::Execution(format!(
                "the chunks of {metric:?} handed to the {} operand of the binary node at {path} do \
                 not continue each other: one starts at position {first}, after the series already \
                 reached {end}",
                side_name(side)
            )));
        }
        operand.open = (!chunk.is_last).then(|| (metric, last.map_or(end, |last| last + 1)));
        operand.enqueue(chunk);
        Ok(())
    }

    /// Runs the kernel over both operands and returns the series it produced.
    ///
    /// The chunks of each operand are assembled into the logical series they make up first, because
    /// the kernel matches whole series by label set: a series that arrived in many chunks would
    /// otherwise be a candidate per chunk on its own side and be paired with every candidate of the
    /// other, which is quadratic in the chunk count of one series rather than in the series count.
    ///
    /// The reservation covers the retained chunks, the assembled series the kernel reads and the
    /// kernel's own live workspace, and it is what the pool admits before each of them allocates.
    /// The kernel reports that workspace coarsely -- where a long-lived result buffer grows, once per
    /// phase of its walk, and in the blocks a growing output buffer is reserved in -- rather than per
    /// grid point, and the total is resized as a phase's storage is released. It keeps covering the
    /// returned results until the caller queues them.
    fn compute(&mut self, path: &NodePath, buffer: &mut NodeBuffer) -> Result<Option<Vec<Series>>> {
        // The kernel reads the operands once and drops them, so a second call has nothing to read
        // and nothing left to produce: what it produced the first time is in the node's queue, and
        // the reservation is already the queue's.
        if self.computed {
            return Ok(None);
        }
        self.closed(path)?;
        let [left_schema, right_schema] = &self.schemas;
        let retained = self.retained_bytes();
        // The assembled series are live while the kernel reads them and share the buffers the
        // retained chunks hold, so what they add is metadata, and it is reserved before it exists:
        // the assembly allocates as it goes, and a figure read from it afterwards would be a figure
        // the pool never admitted.
        let assembly = self
            .assembly_bytes()
            .ok_or_else(|| reservation_overflow(path))?;
        buffer.grow(assembly)?;
        let assembled = match self.assemble(path) {
            Ok(assembled) => assembled,
            Err(error) => {
                // Nothing the assembly allocated outlives this call, so the node falls back to what
                // it still holds. A failure of that resize is the pool's to report; the driver stops
                // the execution either way and releases every node.
                buffer.resize(retained)?;
                return Err(error);
            }
        };
        let [left, right] = &assembled;
        // The operands are still held while the kernel runs, so what this node holds is the operands,
        // the assembled series and the workspace the kernel reports; the callback resizes the
        // reservation to that sum as the live workspace changes.
        let metrics = Arc::clone(&buffer.metrics);
        let output = {
            // The timer covers this node's own work only: it is dropped before the driver polls
            // the input again.
            let _timer = metrics.timer();
            binary_vectors_with_reservation(
                &self.matching,
                left_schema,
                left,
                right_schema,
                right,
                &mut |workspace| {
                    let bytes = retained
                        .checked_add(assembly)
                        .and_then(|held| held.checked_add(workspace))
                        .ok_or_else(|| reservation_overflow(path))?;
                    buffer.resize(bytes)
                },
            )
        };
        // The operands and the assembled series have been handed to the kernel and the reservation
        // covers the workspace it allocated past them, so a failure is the driver's to release: it
        // stops the execution and every node with it.
        let output = output?;
        // The assembled series were the kernel's input and are read by nothing after it, so they go
        // before the chunks they share their buffers with: what this node holds from here on is what
        // the kernel returned, and the pool is not charged for a vector nothing reads again.
        drop(assembled);
        for operand in &mut self.operands {
            operand.release();
        }
        self.computed = true;
        Ok(Some(output))
    }
}

/// The operation a scalar broadcast applies, with the side the constant sits on already resolved.
#[derive(Clone, Copy, Debug)]
enum BroadcastOp {
    /// Float arithmetic between the vector and the constant.
    Arithmetic(ArithmeticOp),
    /// A comparison: `Filter` keeps the positions that passed, `Bool` reports `1`/`0` for them.
    Compare(Comparison),
}

/// What a binary node holds when one operand is a constant: the operation, its scalar operand and
/// the layout of the vector side.
///
/// A broadcast applies to each chunk as it arrives; unlike [`BinaryRuntime`], it does not retain
/// both operands for whole-vector matching.
struct BroadcastRuntime {
    /// The scalar and its position in the operation.
    operand: ScalarOperand,
    /// The operation applied to every value of the vector.
    op: BroadcastOp,
    /// The layout of the vector side: the fields a chunk of it carries, on the grid it is addressed
    /// on. A chunk of the vector side is checked against it by the kernel itself.
    sample_schema: Arc<SeriesSchema>,
}

impl BroadcastRuntime {
    /// Whether the operation rewrites the label set of what it hands out.
    ///
    /// Arithmetic and a `bool` comparison drop the metric name, so two input series can become one
    /// label set under this node and what it hands out is recorded in an [`Emitted`] table. A
    /// filtering comparison keeps the label set it read, so it keeps none. The answer is read off
    /// [`Self::op`] rather than stored beside it, so an operation and its label policy cannot
    /// disagree.
    fn drops_labels(&self) -> bool {
        match self.op {
            BroadcastOp::Arithmetic(_) | BroadcastOp::Compare(Comparison::Bool(_)) => true,
            BroadcastOp::Compare(Comparison::Filter(_)) => false,
        }
    }

    /// The label set a chunk of the vector side's `metric` becomes under this operation.
    ///
    /// It is what a series closes under when the operation emptied every position: the labels say
    /// which series closed, and they are the labels the operation produces, not the ones it read.
    fn output_metric(&self, metric: &MetricName) -> Result<MetricName> {
        if self.drops_labels() {
            drop_metric_name(metric)
        } else {
            Ok(metric.clone())
        }
    }

    /// Applies the operation to one chunk of the vector side.
    ///
    /// `None` means the operation emptied every position and the chunk does not close its series:
    /// the input may still decide more, so nothing is handed on. A chunk that does close its series
    /// is answered even then — with a series carrying no row — because a consumer tells "nothing
    /// more here" from "no more input" by that chunk alone.
    fn apply(&self, chunk: &EvaluatedChunk) -> Result<Option<Series>> {
        let output = match self.op {
            BroadcastOp::Arithmetic(op) => Some(map_arithmetic(
                &self.sample_schema,
                &chunk.series,
                op,
                self.operand,
            )?),
            BroadcastOp::Compare(comparison) => {
                map_compare(&self.sample_schema, &chunk.series, comparison, self.operand)?
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

/// The buffered chunks as the one source axis they make up: their timestamps in arrival order.
///
/// The axis type validates the result as strictly increasing, which is also what refuses a series
/// whose chunks do not continue each other.
fn buffered_time(chunks: &[RawSeriesChunk]) -> Result<RawTimestamps> {
    let len = chunks.iter().map(|chunk| chunk.time.len()).sum();
    let mut timestamps: Vec<i64> = Vec::with_capacity(len);
    for chunk in chunks {
        timestamps.extend_from_slice(chunk.time.as_buffer());
    }
    RawTimestamps::try_new(ScalarBuffer::from(timestamps)).map_err(series_error)
}

/// The buffered chunks as one axis: per field, the chunks' samples in arrival order.
///
/// A field retains the chunks it arrived in instead of copying them out, so the axis costs one
/// descriptor per chunk and the selection reads the values where they already are.
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

/// The bytes the builder's table holds for one row of the field, `None` for a field of floats.
///
/// The largest row of the field rather than its total: every grid point that selects one source row
/// copies that row, so the widest histogram the field holds is what a point can cost.
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

/// The bytes the histogram table holds for one row of `view`.
///
/// The row `HistogramBatchBuilder::push` writes: its scalar columns, the offset each of the seven
/// bucket lists pays for it, and the values those lists hold -- the custom bounds, and each side's
/// bucket counts with the span offsets and lengths that place them. The spans are the view's own,
/// because a histogram handed on unchanged keeps its layout through
/// [`crate::histogram_eval::float_histogram::FloatHistogram::push_to`].
fn histogram_bytes(view: &HistogramView<'_>) -> usize {
    // Schema, zero threshold, sum, count, zero count, reset hint and start timestamp, the narrow
    // columns counted at the width of the widest.
    let scalars = 7 * std::mem::size_of::<i64>();
    // One offset per list column: custom bounds, two span columns and one bucket column per side.
    let list_offsets = 7 * std::mem::size_of::<u32>();
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
    scalars
        .saturating_add(list_offsets)
        .saturating_add(custom)
        .saturating_add(sides)
}

/// The last timestamp of `spec`, which is inclusive: its `len` points step from its start.
///
/// [`GridSpec::try_new`] checked that the point fits `i64`, so widening it here cannot overflow.
fn grid_end_ms(spec: GridSpec) -> i64 {
    (i128::from(spec.start_ms) + i128::from(spec.step_ms) * (i128::from(spec.len) - 1)) as i64
}

/// The position of the grid point at `timestamp`, which the selection reported as one of its own.
///
/// The selection steps the range it is given by the step it is given, so every timestamp it reports
/// is `start_ms + position * step_ms` for one position of the grid: subtracting the start and
/// dividing by the step gives that position back. A timestamp off that lattice, or one past the end
/// of the grid, is a disagreement between the selection and its binding rather than a sample to
/// skip, so it is reported instead of dropped.
fn grid_position(grid: &EvalGrid, timestamp: i64) -> Result<usize> {
    let spec = grid.spec();
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

/// A closing chunk carrying `metric` and no row at all.
///
/// A series whose selection picked no source row still closes exactly once, on a chunk that holds
/// nothing: the labels say which series closed, and the address says it closed before its first
/// point, as the empty chunk a window hands out for such a series does.
fn no_row_chunk(schema: &SeriesSchema, metric: MetricName) -> Result<EvaluatedChunk> {
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

/// The per-node run-time state of a node.
struct NodeRuntime {
    buffer: NodeBuffer,
    /// The chunks this node holds for its consumer, each with the estimate it was queued with.
    ///
    /// An input node's queue is filled by the caller, a window's by the chunks it computed while
    /// flushing its collected input, and a binary node's by the results its kernel produced; no node
    /// uses it for both. A chunk is measured once, as it enters, and the figure it was queued with
    /// is given back as it leaves, so neither the queue nor the handover walks it.
    queued: VecDeque<(NativeChunk, usize)>,
    /// What an instant node holds: the chunks of the series it is selecting, and, while it gathers
    /// that series, the workspace the gather reserved for. `None` for every other kind of node.
    instant: Option<InstantRuntime>,
    /// A window node's evaluator, with its own input and output grid; `None` before it is built and
    /// once it is closed.
    window: Option<RangeEvaluator>,
    /// What a map node applies.
    map: Option<MapRuntime>,
    /// What a binary node holds when it matches two vectors: both operands, until the input ends.
    ///
    /// `None` for every other node, and for a binary node that broadcasts a constant instead: that
    /// one holds no operand at all and is described by [`NodeRuntime::broadcast`].
    binary: Option<BinaryRuntime>,
    /// What a binary node holds when one operand is a constant: the operation and the layout of the
    /// vector side.
    ///
    /// The two modes are exclusive. A broadcast transforms each chunk of the vector side as it
    /// arrives and retains none of them, so what it holds is its own configuration and not data.
    broadcast: Option<BroadcastRuntime>,
    /// The layout the caller's chunks of an input node are checked against.
    ///
    /// A tree with a binary node reads two inputs, and each of them is bound to its own layout, so
    /// the binding is kept per input rather than once for the tree.
    source_schema: Option<Arc<SeriesSchema>>,
    /// What this node has already handed out under each output label set, for a node that rewrites
    /// labels: a window drops the metric name and the empty-valued labels, a map drops the name.
    emitted: Option<Emitted>,
    /// The input a window collects until its input ends, for a window whose input can hold two
    /// series under one label set; `None` for a window that evaluates its chunks as they arrive,
    /// and once the flush that merges the collected input has taken it.
    merge: Option<MergeInput>,
    /// Whether the series the last chunk handed over belongs to is still open.
    ///
    /// Only an input node ever sets this, because its queue is the one the caller fills; the flag is
    /// what reports a truncated input to a tree that has no window, since nothing else in such a
    /// tree holds a series and so nothing else could notice one that never ended. It is assigned on
    /// every handover -- `false` when that chunk carried `is_last`, `true` when it did not -- and
    /// cleared when the node is released, so a true flag says exactly one thing: the last chunk the
    /// consumer saw did not close its series, and no chunk came after it. Nothing else writes it, so
    /// it cannot report a truncation that did not happen.
    ///
    /// "The series of the last chunk handed over" is the whole answer because no more than one
    /// series can be open here: [`NativeExecutionState::try_new`] refuses an input declaring
    /// `SeriesOrdering::Interleaved`, so the chunks of a series arrive together and the series last
    /// handed over is the only one that could still be open.
    open_source_series: bool,
}

impl NodeRuntime {
    /// Queues a source chunk, holding what it is worth until it leaves.
    ///
    /// What this node holds is its queue and nothing else, so the reservation is the sum of the
    /// estimates of the chunks in it. A chunk is measured once, as it enters, and the figure it was
    /// queued with is given back as it leaves, so neither operation walks the queue.
    fn enqueue(&mut self, chunk: NativeChunk) -> Result<()> {
        let positions = chunk.positions();
        let bytes = chunk.bytes();
        // The chunk is held from here on, so the pool admits it before the queue keeps it: a
        // rejected chunk leaves nothing behind.
        self.buffer.grow(bytes)?;
        self.queued.push_back((chunk, bytes));
        self.buffer.metrics.record_input(1, positions);
        Ok(())
    }

    /// Hands over the next queued chunk, if there is one, giving back what it was queued with.
    ///
    /// It is the one way a chunk leaves this node's queue: an input node hands over what the caller
    /// queued, and a binary node hands over what its kernel produced. `values` is how that node
    /// counts the values it hands over, because the two do not count the same thing: an input's
    /// figure has always been its positions, while a binary result holds one value per field and a
    /// field may have none at a position it decided.
    fn pop_queued(&mut self, values: fn(&NativeChunk) -> usize) -> Result<Option<NativeChunk>> {
        let Some((chunk, bytes)) = self.queued.pop_front() else {
            return Ok(None);
        };
        let metrics = Arc::clone(&self.buffer.metrics);
        {
            // An input node's own work is handing the chunk over, so its self time covers that and
            // nothing its producer did. The chunk leaves this node, so the pool no longer has to
            // hold it.
            let _timer = metrics.timer();
            self.buffer.shrink(bytes)?;
        }
        metrics.record_output(1, values(&chunk));
        // An evaluated chunk is addressed on its grid, so its decided positions are grid points; a
        // raw source carries timestamps, which are not positions on any grid.
        if let NativeChunk::Evaluated(chunk) = &chunk {
            metrics.record_grid_points(chunk.series.time.len());
        }
        Ok(Some(chunk))
    }

    /// Hands over the next queued source chunk, if there is one.
    ///
    /// The handover is what [`Self::open_source_series`] follows: after this call the flag says
    /// whether the series the consumer just received a chunk of is still open, which is what closing
    /// the execution reads to tell a truncated input from one that ended cleanly. Only an input node
    /// follows that flag, so only it reads the handover this way: a binary node's queue holds the
    /// results of series that already closed on their way in.
    fn pop_source(&mut self) -> Result<Option<NativeChunk>> {
        let Some(chunk) = self.pop_queued(NativeChunk::positions)? else {
            return Ok(None);
        };
        // Assigned rather than only set: a chunk that closes its series is what says no series is
        // open any more, so the flag follows the handover and not the history of handovers.
        self.open_source_series = !chunk.is_last();
        if chunk.is_last() {
            self.buffer.metrics.record_completed_series(1);
        }
        Ok(Some(chunk))
    }

    /// Maps one evaluated chunk, keeping its axis and its closing.
    ///
    /// `None` means the operation emptied every position and the chunk does not close its series:
    /// there is nothing to hand on, and the input may still decide more.
    fn map(&mut self, path: &NodePath, chunk: EvaluatedChunk) -> Result<Option<EvaluatedChunk>> {
        let (positions, closing) = (chunk.series.time.len(), chunk.is_last);
        // The chunk is held for as long as this node works on it.
        self.buffer.grow(evaluated_bytes(&chunk))?;
        let metrics = Arc::clone(&self.buffer.metrics);
        let outcome = {
            // The timer covers this node's own work only: it is dropped before the driver polls
            // the input again.
            let _timer = metrics.timer();
            match self.map.as_ref() {
                Some(map) => map_chunk(&map.schema, &chunk, &map.ops),
                None => Err(DataFusionError::Execution(format!(
                    "the node at {path} holds no map"
                ))),
            }
        };
        metrics.record_input(1, positions);
        if closing {
            metrics.record_completed_series(1);
        }
        // An error leaves the reservation to the driver, which releases every node when it stops.
        let output = outcome?;
        if let Some(output) = &output {
            metrics.record_grid_points(output.series.time.len());
            metrics.record_output(1, valid_values(output));
            // What it handed out is what the label sets of this node now hold: `map_chunk` has
            // already rewritten the output's label set, so the label set of the output is the one
            // two input series can share.
            self.track(path, output)?;
        }
        // The chunk is gone and the label sets this node has handed out are what is left.
        let emitted = self.emitted_bytes();
        self.buffer.resize(emitted)?;
        Ok(output)
    }

    /// Broadcasts one chunk of the vector side with the constant this node was bound to.
    ///
    /// `None` means the operation emptied every position and the chunk does not close its series:
    /// there is nothing to hand on, and the vector side may still decide more. Nothing of the chunk
    /// is retained — the operation pairs one value with one constant — so what this node holds
    /// afterwards is the label sets it has handed out and nothing else.
    fn broadcast(&mut self, path: &NodePath, chunk: NativeChunk) -> Result<Option<EvaluatedChunk>> {
        let NativeChunk::Evaluated(chunk) = chunk else {
            return Err(DataFusionError::Execution(format!(
                "the binary node at {path} broadcasts over an evaluated vector, not raw samples"
            )));
        };
        let (positions, closing) = (chunk.series.time.len(), chunk.is_last);
        // The chunk is held for as long as this node works on it.
        self.buffer.grow(evaluated_bytes(&chunk))?;
        let metrics = Arc::clone(&self.buffer.metrics);
        let outcome = {
            // The timer covers this node's own work only: it is dropped before the driver polls
            // the vector side again.
            let _timer = metrics.timer();
            match self.broadcast.as_ref() {
                Some(broadcast) => broadcast.apply(&chunk),
                None => Err(no_broadcast(path)),
            }
        };
        metrics.record_input(1, positions);
        if closing {
            // A series closes exactly once, on the chunk carrying `is_last`: the broadcast kept it
            // on every chunk it handed up, so the series it read is closed here.
            metrics.record_completed_series(1);
        }
        // An error leaves the reservation to the driver, which releases every node when it stops.
        let output = outcome?;
        let output = match output {
            Some(series) => Some(EvaluatedChunk::try_new(series, closing)?),
            None => None,
        };
        if let Some(output) = &output {
            metrics.record_grid_points(output.series.time.len());
            metrics.record_output(1, valid_values(output));
            // Only an operation that rewrites the label set is where two input series can become
            // one, and only that one keeps the table `track` records into. A filtering comparison
            // keeps the label set it read, so recording it would refuse two input series that
            // share it without this node having made them share it.
            if self
                .broadcast
                .as_ref()
                .is_some_and(BroadcastRuntime::drops_labels)
            {
                self.track(path, output)?;
            }
        }
        // The chunk is gone and the label sets this node has handed out are what is left.
        let emitted = self.emitted_bytes();
        self.buffer.resize(emitted)?;
        Ok(output)
    }

    /// Feeds one chunk to this node's window.
    ///
    /// A window that collects its input keeps the whole series of every chunk it is handed and
    /// evaluates nothing yet: the label sets of those fragments are only canonical once the input
    /// has ended, and reading them as separate series would close the state of the one before in
    /// the evaluator. Every other window evaluates what it is handed as it arrives, which is what
    /// lets it answer before its input series ends.
    fn window(&mut self, path: &NodePath, chunk: NativeChunk) -> Result<Option<EvaluatedChunk>> {
        if self.merge.is_some() {
            let NativeChunk::Evaluated(chunk) = chunk else {
                return Err(DataFusionError::Execution(format!(
                    "the window at {path} collects evaluated fragments, but it was handed raw \
                     samples"
                )));
            };
            return self.collect(path, chunk);
        }
        let (positions, closing) = (chunk.positions(), chunk.is_last());
        // The chunk is held for as long as the evaluator consumes it.
        self.buffer.grow(chunk.bytes())?;
        let metrics = Arc::clone(&self.buffer.metrics);
        let outcome = {
            let _timer = metrics.timer();
            match (self.window.as_mut(), &chunk) {
                // Raw samples are read window by window by the evaluator itself.
                (Some(evaluator), NativeChunk::Raw(chunk)) => evaluator.push(chunk),
                // An evaluated input is read as computed samples: the evaluator carries the window
                // state of one series across its chunks and closes it on the chunk that says so.
                // Handing each chunk over as it arrives is what lets this window report a decision
                // before its input series has ended; buffering the chunks into a whole series first
                // would hold the input's bytes and delay every result to the end of the series.
                (Some(evaluator), NativeChunk::Evaluated(chunk)) => {
                    evaluator.push_series(&chunk.series, chunk.is_last)
                }
                (None, _) => Err(closed(path)),
            }
        };
        metrics.record_input(1, positions);
        if closing {
            // A series closes exactly once, on the chunk carrying `is_last`. A window that decided
            // nothing for it still closed it, so it is counted here rather than on its output.
            metrics.record_completed_series(1);
        }
        // An error leaves the reservation to the driver, which releases every node when it stops.
        let output = outcome?;
        if let Some(output) = &output {
            metrics.record_grid_points(output.series.time.len());
            metrics.record_output(1, valid_values(output));
            self.track(path, output)?;
        }
        // The chunk has been consumed: what this node still holds is what the evaluator kept of it
        // and the label sets it has already handed out. Both are charged whether or not this chunk
        // produced one -- a chunk that yields nothing leaves the evaluator's samples and every
        // earlier series' history exactly where they were -- and neither is charged per chunk
        // consumed, which would make a fixed window over a long series read as an allocation
        // failure even though the window holds no more samples.
        let retained = self
            .window
            .as_ref()
            .map_or(0, RangeEvaluator::retained_bytes);
        let emitted = self.emitted_bytes();
        self.buffer.resize(retained.saturating_add(emitted))?;
        Ok(output)
    }

    /// Retains one operand chunk of this node's binary operation.
    ///
    /// Nothing is decided for a point yet: the kernel needs both operands whole, so a chunk is only
    /// kept until the input ends. What the node holds is the estimate of every chunk it retained,
    /// and the reservation is set to that figure once the chunk is in: the operand's own total is
    /// what the node keeps, so a second chunk does not have to be admitted beside the first one's
    /// estimate again.
    ///
    /// The chunk is reserved for before it is retained, by the figure the operand reports for it, so
    /// the pool admits the memory before the node holds it and a refused chunk leaves the operand
    /// where it was.
    fn retain_binary(&mut self, path: &NodePath, side: usize, chunk: NativeChunk) -> Result<()> {
        let NativeChunk::Evaluated(chunk) = chunk else {
            return Err(DataFusionError::Execution(format!(
                "the binary node at {path} consumes evaluated chunks, not raw samples"
            )));
        };
        // The figure is read before the chunk is retained, because reserving after the fact would be
        // reserving for memory the node already holds.
        let pending = self
            .binary
            .as_ref()
            .ok_or_else(|| no_binary(path))?
            .pending(path, side, &chunk)?;
        let positions = chunk.series.time.len();
        self.buffer.grow(pending)?;
        let metrics = Arc::clone(&self.buffer.metrics);
        {
            let _timer = metrics.timer();
            let binary = self.binary.as_mut().ok_or_else(|| no_binary(path))?;
            binary.retain(path, side, chunk)?;
            let retained = binary.retained_bytes();
            self.buffer.resize(retained)?;
        }
        metrics.record_input(1, positions);
        Ok(())
    }

    /// Runs this node's binary kernel and queues the chunks it produced.
    fn compute_binary(&mut self, path: &NodePath) -> Result<()> {
        let output = {
            let binary = self.binary.as_mut().ok_or_else(|| no_binary(path))?;
            binary.compute(path, &mut self.buffer)?
        };
        // A kernel that already ran has moved its result into the queue, which the handover below
        // charges for: leaving the reservation alone here is what keeps a queued result covered.
        let Some(output) = output else {
            return Ok(());
        };
        // The results are queued the way a node that produces a chunk per input queues what it
        // hands on: the queue is the one source of chunk order and of the bytes queued chunks hold.
        let mut closed = 0usize;
        for series in output {
            // The kernel decided every point of a result series at once, so the one chunk it is
            // handed over as closes it.
            let chunk = NativeChunk::Evaluated(EvaluatedChunk::try_new(series, true)?);
            // The result is already admitted: the kernel kept it inside the workspace its callback
            // reserved. What the queue keeps is the estimate of the chunk that holds it.
            let bytes = chunk.bytes();
            self.queued.push_back((chunk, bytes));
            closed += 1;
        }
        // This node's own accounting of closed series is the results it closed, one per matched
        // series: an operand chunk that closed its input series is a closure of the node below, and
        // counting it here as well would report the same work twice.
        self.buffer.metrics.record_completed_series(closed);
        // The operands the reservation covered are gone and the queue holds what the kernel
        // returned, so the reservation settles on the queue: what a queued result is given back
        // with as it leaves is exactly the figure it is held for here.
        let queued = self
            .queued
            .iter()
            .fold(0usize, |total, (_, bytes)| total.saturating_add(*bytes));
        self.buffer.resize(queued)
    }

    /// Feeds one chunk to this node's instant selection.
    ///
    /// `None` means the series this chunk belongs to has not ended: the selection reads one whole
    /// axis, so it decides nothing until the chunk carrying `is_last` arrives.
    fn instant(&mut self, path: &NodePath, chunk: NativeChunk) -> Result<Option<EvaluatedChunk>> {
        let NativeChunk::Raw(chunk) = chunk else {
            return Err(DataFusionError::Execution(format!(
                "the instant selection at {path} consumes raw samples, not an evaluated chunk"
            )));
        };
        let (positions, closing) = (chunk.time.len(), chunk.is_last);
        // The chunk is held from here on: a selection keeps every chunk of a series until the one
        // that closes it, so what this node reserves for is the series it has buffered.
        self.buffer.grow(raw_bytes(&chunk))?;
        // The chunk that closes the series gathers it in one go, and the gather allocates a
        // workspace of its own beside the buffered chunks: the axis it concatenates, the descriptors
        // that read it, the two arrays the selection returns and the rows the builder keeps. That
        // workspace is reserved here, before it exists, so a pool too small for it refuses the
        // gather instead of learning about it afterwards. Without this the reservation would fall
        // back to the buffered chunks alone -- nothing at all, on the chunk that ends the series --
        // and a selection over a million grid points would take its tens of megabytes without
        // asking. [`InstantRuntime::gather_bytes`] derives the figure, because the chunks it counts
        // are that node's.
        let Some(instant) = self.instant.as_ref() else {
            return Err(DataFusionError::Execution(format!(
                "the node at {path} holds no instant selection"
            )));
        };
        let scratch = if closing {
            instant.gather_bytes(&chunk)
        } else {
            0
        };
        self.buffer.grow(scratch)?;
        let metrics = Arc::clone(&self.buffer.metrics);
        let outcome = {
            // The timer covers this node's own work only: it is dropped before the driver polls
            // the input again.
            let _timer = metrics.timer();
            match self.instant.as_mut() {
                Some(instant) => instant.push(path, chunk),
                // Rejected when the state was built, since a selection reads raw samples; kept as
                // an error rather than a panic so an impossible tree cannot take the process down.
                None => Err(DataFusionError::Execution(format!(
                    "the node at {path} holds no instant selection"
                ))),
            }
        };
        metrics.record_input(1, positions);
        if closing {
            // A series closes exactly once, on the chunk carrying `is_last`: the selection gathers
            // the whole series on it, so it is counted here rather than on the chunk handed out.
            metrics.record_completed_series(1);
        }
        // A failed gather gives its workspace back here rather than leaving it to the driver: the
        // driver releases every node when the execution stops, but the workspace is this call's and
        // not something the node keeps, so holding the pool's budget for it until the stopped
        // executor is dropped would charge the pool for memory nothing reads any more.
        let output = match outcome {
            Ok(output) => output,
            Err(error) => {
                self.settle_buffered()?;
                return Err(error);
            }
        };
        if let Some(output) = &output {
            metrics.record_grid_points(output.series.time.len());
            metrics.record_output(1, valid_values(output));
        }
        // What this node still holds is the chunks of the series it is selecting: a whole series is
        // held until it ends, so the reservation is what those chunks hold rather than a figure
        // that grows with the chunk count of anything already answered. The chunk just consumed and
        // the workspace of the gather it closed are both gone, so this is what gives them back.
        self.settle_buffered()?;
        Ok(output)
    }

    /// Sets this node's reservation to the chunks of the series it is still gathering.
    ///
    /// The chunk the node just consumed and the workspace of a gather it closed are not part of it:
    /// the first has left the buffer and the second has been handed to the output.
    fn settle_buffered(&mut self) -> Result<()> {
        let buffered = self
            .instant
            .as_ref()
            .map_or(0, InstantRuntime::buffered_bytes);
        self.buffer.resize(buffered)
    }

    /// Keeps one fragment of this node's collected input, charged for as it arrives.
    ///
    /// The fragment is measured once, here, and its estimate stands until the flush that merges it
    /// is over: a later chunk adds its own estimate rather than walking the buffer again. What is
    /// estimated is what the fragment keeps alive, so a sample payload two fragments share is
    /// counted once per fragment; the merge itself copies no values, and its workspace is charged
    /// separately before it runs.
    fn collect(
        &mut self,
        path: &NodePath,
        chunk: EvaluatedChunk,
    ) -> Result<Option<EvaluatedChunk>> {
        let Some(merge) = self.merge.as_mut() else {
            return Err(DataFusionError::Execution(format!(
                "the window at {path} holds no collected input"
            )));
        };
        let EvaluatedChunk { series, is_last } = chunk;
        // The chunk is checked before it is charged for or kept, so a stream that breaks the
        // contract leaves nothing behind.
        merge.check(path, &series, is_last)?;
        let positions = series.time.len();
        let bytes = series_bytes(&series);
        // The fragment is held from here on, so the pool admits it before this node keeps it: a
        // rejected fragment leaves nothing behind.
        self.buffer.grow(bytes)?;
        merge.bytes = merge.bytes.saturating_add(bytes);
        merge.fragments.push(series);
        let metrics = Arc::clone(&self.buffer.metrics);
        metrics.record_input(1, positions);
        if is_last {
            // The source series whose data this fragment carries ended, which is what `is_last`
            // says; the label set it shares with another series is not closed by it.
            metrics.record_completed_series(1);
        }
        // What this node holds is the fragments and the label sets it has already handed out; its
        // evaluator has read none of them yet.
        let retained = self
            .window
            .as_ref()
            .map_or(0, RangeEvaluator::retained_bytes);
        let emitted = self.emitted_bytes();
        let pending = self.merge.as_ref().map_or(0, |merge| merge.bytes);
        self.buffer
            .resize(retained.saturating_add(emitted).saturating_add(pending))?;
        Ok(None)
    }

    /// Queues one output this node computed while flushing its collected input.
    ///
    /// The chunk waits for the pull that hands it up the tree, and its estimate is added as it is
    /// queued and given back as it leaves, so the queue costs the sum of what it holds.
    fn enqueue_output(&mut self, chunk: EvaluatedChunk) -> Result<()> {
        let bytes = evaluated_bytes(&chunk);
        self.buffer.grow(bytes)?;
        self.queued
            .push_back((NativeChunk::Evaluated(chunk), bytes));
        Ok(())
    }

    /// Hands over the next output this node computed while flushing, if there is one.
    ///
    /// This is not a source handover: [`Self::open_source_series`] and the input metrics follow the
    /// chunks the caller queued, and a chunk this node produced itself is neither.
    fn take_output(&mut self) -> Result<Option<NativeChunk>> {
        let Some((chunk, bytes)) = self.queued.pop_front() else {
            return Ok(None);
        };
        self.buffer.shrink(bytes)?;
        Ok(Some(chunk))
    }

    /// Records what this node hands out under its output label set.
    ///
    /// A node that rewrites labels is where two input series become indistinguishable, so this is
    /// where a second one is refused: a node above reads what this one hands over, and a window
    /// above would evaluate the two as two series and report both. Checking at the root would be
    /// too late for that — the answer would already have been computed.
    fn track(&mut self, path: &NodePath, output: &EvaluatedChunk) -> Result<()> {
        match self.emitted.as_mut() {
            Some(emitted) => emitted.occupy(path, output),
            // A node that keeps its input's labels cannot make two series one.
            None => Ok(()),
        }
    }

    /// The bytes the label sets this node has handed out hold.
    ///
    /// Zero for a node that keeps its input's labels, which keeps no such table.
    fn emitted_bytes(&self) -> usize {
        self.emitted.as_ref().map_or(0, Emitted::bytes)
    }

    /// Returns everything this node holds, including queued chunks.
    fn release(&mut self) {
        // A node's own state goes first: dropping the evaluator and the buffered series is what
        // gives back the memory their reservation was taken for. Returning the reservation while
        // they still held the samples would let the pool admit memory the process has not released,
        // for as long as the caller keeps the stopped executor alive. The label sets this node
        // handed out and the fragments it collected go with it.
        self.window = None;
        self.instant = None;
        self.binary = None;
        self.broadcast = None;
        self.source_schema = None;
        self.emitted = None;
        self.merge = None;
        // A node that holds no chunk holds no series either, so nothing it handed over is still
        // open for it: the flag is what closing the execution reads, and it must not survive the
        // state that could have kept a series alive.
        self.open_source_series = false;
        self.queued.clear();
        self.queued.shrink_to_fit();
        // The reservation is returned once, below, for everything this node still held.
        self.buffer.release();
    }
}

/// The run-time state of one execution of a bound tree.
///
/// The state is keyed by tree position: the same positions [`crate::native::metrics::walk`]
/// derives, so a node's state, its metrics and the name it is shown under cannot drift apart.
/// Because none of it lives on the tree, one compiled tree serves every execution that builds a
/// state from it.
pub struct NativeExecutionState {
    /// The tree's structure, flattened in traversal order, so the root is at index 0.
    nodes: Vec<NodeSpec>,
    /// The position of each node, in the same order.
    paths: Vec<NodePath>,
    /// The per-node state, in the same order.
    runtimes: Vec<NodeRuntime>,
    /// The index of every node, so a caller names a node the way the metrics do.
    by_path: HashMap<NodePath, usize>,
    /// The index of every input node, in traversal order: where a caller's chunks enter.
    ///
    /// A tree of unary nodes has one, and a binary node adds one per operand: a tree of nested
    /// binary nodes has as many as it has leaves, and a queued chunk names the one it belongs to.
    sources: Vec<usize>,
}

impl NativeExecutionState {
    /// Builds the state of one execution of `tree`.
    ///
    /// Every node registers its metrics in `set` under its position and partition, and every node
    /// that buffers input gets its own reservation from `pool`.
    pub fn try_new(
        tree: &NativeTree,
        pool: Arc<dyn MemoryPool>,
        metrics_set: &ExecutionPlanMetricsSet,
        partition: usize,
    ) -> Result<Self> {
        // One pass over the tree names and registers every node in the order
        // `crate::native::metrics::walk` gives it: the same rule `EXPLAIN` uses, so a node cannot be
        // shown at one position and registered at another. That order reaches a node's left operand
        // before its right one, so the input a spec names -- the one input of a unary node, the left
        // operand of a binary node -- is the node the pass reaches next.
        let mut nodes: Vec<NodeSpec> = Vec::new();
        let mut paths: Vec<NodePath> = Vec::new();
        let mut runtimes: Vec<NodeRuntime> = Vec::new();
        // The bound expression of each node, in the same order: the pass below reads the node it
        // configures from here, so nothing about an operation is copied onto the node's spec.
        let mut exprs: Vec<&NativeExpr> = Vec::new();
        // The nodes the pass has not reached yet, the next one last: a node pushes its inputs back
        // to front, so the one popped next is the one its parent's spec names.
        let mut pending: Vec<(&NativeNode, NodePath)> = vec![(tree.root(), NodePath::root())];
        while let Some((node, path)) = pending.pop() {
            let index = nodes.len();
            let handles = Arc::new(NodeMetrics::default());
            handles.register(&path, metrics_set, partition);
            let tag = match &node.expr {
                NativeExpr::Input(bound) => {
                    // Every node here reads one series at a time and assumes its chunks arrive
                    // together: a window feeds a series to its evaluator up to the chunk carrying
                    // `is_last`, and an input node keeps one flag for the series it handed over,
                    // which is what reports one that never closed. An input that declares
                    // `Interleaved` can leave a series open across another series' chunks, which
                    // either of those would misread, so the tree is refused here rather than run.
                    // That is a precondition of the runtime, not a limitation to document: the tree
                    // stays free to declare the ordering -- the compiler's job is not narrowed here
                    // -- and no tree this runtime executes declares it.
                    if bound.ordering == SeriesOrdering::Interleaved {
                        return Err(unsupported_ordering(bound.ordering, &path));
                    }
                    NodeTag::Input
                }
                NativeExpr::Instant { .. } => NodeTag::Instant,
                NativeExpr::Window { .. } => NodeTag::Window,
                NativeExpr::Map { .. } => NodeTag::Map,
                // A binary node is built here and narrowed below: legality is a wider set of operand
                // types than the kernels take, so which of its two modes it is -- a match between
                // two vectors, or a constant broadcast over one -- is decided where the operands'
                // layouts are read.
                NativeExpr::Binary { .. } => NodeTag::Binary,
                // A scalar is a value node: it produces the constant it was bound to, hands over no
                // chunk and holds no state, so it is built like any other node and pulled like none.
                NativeExpr::Scalar(_) => NodeTag::Scalar,
                // An aggregate is legal in a bound tree, and `validate` stays the authority on that;
                // this runtime simply does not execute it yet. Rejected before a single chunk is
                // read: a tree the runtime cannot execute must not start, because a partial answer
                // would be one the caller never asked for.
                NativeExpr::Aggregate { .. } => return Err(not_implemented("Aggregate", &path)),
            };
            let buffer = NodeBuffer::new(&path, handles, &pool);
            let mut runtime = NodeRuntime {
                buffer,
                queued: VecDeque::new(),
                instant: None,
                window: None,
                map: None,
                binary: None,
                broadcast: None,
                source_schema: None,
                emitted: None,
                merge: None,
                open_source_series: false,
            };
            match &node.expr {
                NativeExpr::Input(_) => {
                    // What the caller's chunks of this input are checked against: a raw source
                    // carries timestamps, an evaluated one is addressed on a grid.
                    runtime.source_schema = node.output.samples().cloned();
                }
                NativeExpr::Instant { input, op } => {
                    let input_schema = input.output.samples().ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "the instant selection at {path} consumes samples, but its input \
                             produces none"
                        ))
                    })?;
                    runtime.instant = Some(InstantRuntime::try_new(input_schema, op)?);
                    // A selection keeps the labels of the series it reads, empty-valued ones
                    // included, so it is not one of the nodes that can turn two series into one
                    // label set and it keeps no table of what it handed out: the node that drops a
                    // label is the one that can collide, and it records its own.
                }
                NativeExpr::Map { op, .. } => {
                    let schema = node.output.samples().cloned().ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "the map node at {path} does not produce samples"
                        ))
                    })?;
                    runtime.map = Some(MapRuntime {
                        ops: Arc::clone(&op.ops),
                        schema,
                    });
                    // A map drops the metric name, so it is one of the nodes that can turn two
                    // series into one label set.
                    runtime.emitted = Some(Emitted::default());
                }
                NativeExpr::Window { input, op } => {
                    let input_schema = input.output.samples().ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "the window at {path} consumes samples, but its input produces none"
                        ))
                    })?;
                    // One evaluator per window, with the grids the node was bound to: the output
                    // grid is the node's own grid, which is also the grid its chunks are addressed
                    // on.
                    runtime.window = Some(RangeEvaluator::try_new(
                        input_schema,
                        Arc::clone(&op.output_grid),
                        op.window,
                        op.function,
                    )?);
                    // A window drops the metric name and the empty-valued labels, so a `rate` over
                    // a series whose tag is empty and one where it is absent produces one label set
                    // from two series.
                    runtime.emitted = Some(Emitted::default());
                }
                NativeExpr::Binary {
                    left,
                    right,
                    matching,
                } => {
                    // A binary node has two modes here, and the operands' layouts are what tell
                    // them apart. Two evaluated vectors are matched by label set and retain both
                    // operands; one evaluated vector and a constant is a broadcast, which pairs
                    // each vector value with the constant as the chunk carrying it arrives. All
                    // that is left is refused as this node, before the pass reaches the operands
                    // that produced them, so the position named is the one that asked for them.
                    match (left.output.samples(), right.output.samples()) {
                        (Some(left), Some(right))
                            if left.eval_grid.is_some() && right.eval_grid.is_some() =>
                        {
                            // Each operand's layout is the one its own child produces, and the walk
                            // only built this node when both produce evaluated samples.
                            runtime.binary = Some(BinaryRuntime::new(
                                Arc::new(matching.clone()),
                                [Arc::clone(left), Arc::clone(right)],
                            ));
                        }
                        _ => {
                            let broadcast = broadcast_runtime(&path, left, right, matching)?;
                            if broadcast.drops_labels() {
                                // The operation rewrites the label set it hands out, so this node
                                // is one where two input series can become one: it keeps the table
                                // that refuses a second value at a position the first one filled.
                                runtime.emitted = Some(Emitted::default());
                            }
                            runtime.broadcast = Some(broadcast);
                        }
                    }
                    // A binary operation over two vectors matches by label set, so a result label
                    // set may be produced at points that do not collide; the kernel groups and
                    // merges them itself, which is why that mode keeps no ledger of what it handed
                    // out. A broadcast that keeps the labels it reads is not where two series
                    // become one either, so it keeps none; the one that drops them does, above.
                }
                NativeExpr::Scalar(_) | NativeExpr::Aggregate { .. } => {
                    // A scalar holds nothing: what its consumer reads is the constant the node was
                    // bound to, which is on the tree rather than in this state. The aggregate is
                    // rejected above, where the pass has the position to name.
                }
            }
            // The inputs this node reads, in the order the tree declares them: a unary node's one
            // input, a binary node's left and right operand, and none for an input node. They are
            // pushed back to front, so the node the spec names is the one the pass reaches next and
            // nothing but its own subtree lies between the two.
            let reads_input = match &node.expr {
                NativeExpr::Input(_) | NativeExpr::Scalar(_) | NativeExpr::Aggregate { .. } => {
                    false
                }
                NativeExpr::Instant { input, .. }
                | NativeExpr::Window { input, .. }
                | NativeExpr::Map { input, .. } => {
                    pending.push((input.as_ref(), path.input()));
                    true
                }
                NativeExpr::Binary { left, right, .. } => {
                    pending.push((right.as_ref(), path.right()));
                    pending.push((left.as_ref(), path.left()));
                    true
                }
            };
            nodes.push(NodeSpec {
                tag,
                input: reads_input.then_some(index + 1),
            });
            exprs.push(&node.expr);
            paths.push(path.clone());
            runtimes.push(runtime);
        }

        // A caller names a node by the position the metrics give it, and a chunk it queues is bound
        // to the input that position names, so every position is indexed once the pass has given the
        // whole tree one. The reverse pass below reads it as well, because an operand is not always
        // the node the spec's `input` names: a broadcast's vector side may be its right operand.
        let by_path: HashMap<NodePath, usize> = paths
            .iter()
            .enumerate()
            .map(|(index, path)| (path.clone(), index))
            .collect();

        // One reverse pass sets where a merge has to happen, from the input upwards: the pass above
        // walked the root first, so a node's input is the node after it and has been decided by the
        // time the node itself is.
        //
        // A source is a stream of series, each with its own label set, so its output needs no merge:
        // that is what keeps `Input -> Window` streaming and reporting as its input arrives. A map
        // drops the metric name, so its output can hold two input series under one label set, and
        // every node above it needs the merge. A window collects its input and merges it when its
        // input can hold two series under one label set: reading them as series one after another
        // would end the first in the evaluator before the second arrives, and a window across the
        // boundary needs both.
        //
        // A window's own output needs the same when it rewrites labels -- `rate` drops the name, so
        // two input series can become one label set -- or when it did not merge its input:
        // `last_over_time` keeps the name, but a tag holding an empty value and an absent tag are
        // two input series and one canonical label set, so the pair only becomes one series if it
        // was merged before the window read it.
        //
        // The flags this needs are the pass's own: one per node, filled from the input upwards, and
        // they and the expressions go when the pass ends. A node's spec therefore stays what the
        // tree said, and nothing about an operation is kept once the state is built.
        {
            let mut holds_shared: Vec<bool> = vec![false; nodes.len()];
            for (index, expr) in exprs.into_iter().enumerate().rev() {
                let input_holds_shared =
                    nodes[index].input.is_some_and(|child| holds_shared[child]);
                match nodes[index].tag {
                    NodeTag::Input => {}
                    NodeTag::Instant => {
                        // A selection keeps the labels it read, so it is not where two series
                        // become one label set and its own output carries its input's flag. Its
                        // input is the source -- `validate` binds a selection to raw samples, and
                        // no node above a source produces them -- and a source hands each series'
                        // chunks over together, so the flag is false there in every tree this
                        // runtime runs. Should the pass ever compute it true, the selection still
                        // needs no collected input: it reads one series at a time and refuses a
                        // chunk of another before it is buffered.
                        holds_shared[index] = input_holds_shared;
                    }
                    NodeTag::Map => holds_shared[index] = true,
                    NodeTag::Scalar => {
                        // A scalar produces one value and no labels, so it is not a stream of series
                        // and nothing above it can read two of them under one label set. The flag
                        // is false rather than inherited: the node has no input at all.
                        holds_shared[index] = false;
                    }
                    NodeTag::Binary => {
                        // A vector match files one result row per label set and point, merging the
                        // rows of one set whatever piece they came from: what it hands out is one
                        // series per label set, so nothing above has to join pieces of a series. Its
                        // own operands are read whole -- it retains them until the input ends -- so
                        // its flag is false whatever theirs are.
                        //
                        // A broadcast hands each input chunk up transformed instead, so it merges
                        // nothing: what its output can hold under one label set is what its vector
                        // side held, plus the pair its own label rewrite can create. One that drops
                        // the metric name -- arithmetic and a `bool` comparison -- is therefore where
                        // two input series become one label set, and one that keeps the name only
                        // inherits the pair its input already had. The flag of the *vector* side is
                        // the one that matters: the constant is a node with no input at all.
                        holds_shared[index] = match runtimes[index].broadcast.as_ref() {
                            Some(broadcast) => {
                                let vector = match broadcast.operand {
                                    ScalarOperand::Right(_) => nodes[index].input,
                                    ScalarOperand::Left(_) => {
                                        by_path.get(&paths[index].right()).copied()
                                    }
                                };
                                broadcast.drops_labels()
                                    || vector.is_some_and(|vector| holds_shared[vector])
                            }
                            None => false,
                        };
                    }
                    NodeTag::Window => {
                        let (drops_metric_name, input_schema) = match expr {
                            NativeExpr::Window { input, op } => (
                                op.function.drops_metric_name(),
                                input.output.samples().cloned(),
                            ),
                            _ => {
                                return Err(DataFusionError::Execution(format!(
                                    "the node at {} is a window without a bound window operation",
                                    paths[index]
                                )));
                            }
                        };
                        if input_holds_shared {
                            let schema = input_schema.ok_or_else(|| {
                                DataFusionError::Execution(format!(
                                    "the window at {} merges its input, but its input produces no \
                                 samples",
                                    paths[index]
                                ))
                            })?;
                            runtimes[index].merge = Some(MergeInput::new(schema));
                        }
                        // A window's own output can hold two series under one label set when it drops
                        // the name, or when it did not merge its input and so never joined the two.
                        holds_shared[index] = drops_metric_name || !input_holds_shared;
                    }
                }
            }
        }

        // The runtime's output is an evaluated chunk. Raw samples have no grid, and a scalar or
        // aggregate state is not a query value, so neither can be the tree's answer.
        match tree.root().output.samples() {
            Some(schema) if schema.eval_grid.is_some() => {}
            Some(_) => {
                return Err(DataFusionError::Execution(
                    "the native runtime produces evaluated chunks, but the root of the tree \
                     produces raw samples"
                        .to_string(),
                ));
            }
            None => {
                return Err(DataFusionError::Execution(
                    "the native runtime produces evaluated chunks, but the root of the tree \
                     produces a scalar or aggregate state"
                        .to_string(),
                ));
            }
        }

        // Every input node is where a caller's chunks can enter, and each of them is named by its
        // own position when a caller queues a chunk. A tree of unary nodes has one input; a binary
        // node adds one per operand, so a tree of nested binary nodes has as many as it has leaves.
        let sources: Vec<usize> = nodes
            .iter()
            .enumerate()
            .filter(|(_, node)| node.tag == NodeTag::Input)
            .map(|(index, _)| index)
            .collect();
        if sources.is_empty() {
            return Err(DataFusionError::Execution(
                "the native tree has no input node".to_string(),
            ));
        }
        // A caller's chunks are samples: a source that hands over aggregate state is not something
        // this runtime can be handed, and it is refused when the state is built rather than when the
        // first chunk is queued at it.
        for &source in &sources {
            if runtimes[source].source_schema.is_none() {
                return Err(DataFusionError::Execution(format!(
                    "the input node at {} does not produce samples",
                    paths[source]
                )));
            }
        }
        Ok(Self {
            nodes,
            paths,
            runtimes,
            by_path,
            sources,
        })
    }

    /// The metrics of the node at `path`.
    pub fn metrics(&self, path: &NodePath) -> Option<&Arc<NodeMetrics>> {
        let index = self.paths.iter().position(|candidate| candidate == path)?;
        Some(&self.runtimes[index].buffer.metrics)
    }

    /// The position of the right operand of the binary node at `index`.
    ///
    /// A spec names one input, the node the build pass reaches next: a unary node's only input and a
    /// binary node's left operand. The right operand is the one input a spec cannot name, so it is
    /// read from the position the node itself sits at, which the pass resolved and indexed.
    fn right_input(&self, index: usize) -> Result<usize> {
        let path = &self.paths[index];
        self.by_path.get(&path.right()).copied().ok_or_else(|| {
            DataFusionError::Execution(format!(
                "the native tree has no node at the right input of {path}"
            ))
        })
    }

    /// The bytes this execution currently reserves from the pool.
    pub fn reserved_bytes(&self) -> usize {
        self.runtimes
            .iter()
            .map(|runtime| runtime.buffer.reservation.size())
            .sum()
    }

    /// How many nodes still hold the state a window buffers into.
    ///
    /// Test-only: releasing a stopped execution has to drop the evaluators and their sample
    /// buffers, not only the numbers that say how many bytes they were reserved for, and that is
    /// not observable from the pool.
    #[cfg(test)]
    pub(crate) fn nodes_holding_windows(&self) -> usize {
        self.runtimes
            .iter()
            .filter(|runtime| runtime.window.is_some())
            .count()
    }

    /// How many nodes still hold the chunks of a series an instant selection was gathering.
    ///
    /// Test-only, and for the same reason as [`Self::nodes_holding_windows`]: the buffered chunks
    /// have to be dropped, not only the bytes they were reserved for, and that is not observable
    /// from the pool.
    #[cfg(test)]
    pub(crate) fn nodes_holding_instants(&self) -> usize {
        self.runtimes
            .iter()
            .filter(|runtime| runtime.instant.is_some())
            .count()
    }

    /// How many nodes still hold the operands a binary operation retained.
    ///
    /// Test-only, and for the same reason as [`Self::nodes_holding_windows`]: an execution that
    /// stopped has to drop the vectors it retained, not only the number that says how many bytes
    /// they were reserved for.
    #[cfg(test)]
    pub(crate) fn nodes_holding_binary(&self) -> usize {
        self.runtimes
            .iter()
            .filter(|runtime| runtime.binary.is_some())
            .count()
    }

    /// The input that ended before its last chunk, if there is one.
    ///
    /// It is read before anything is merged: a window that collects its input closes every series
    /// it merges with a synthetic `is_last`, so a series the caller never ended would be
    /// indistinguishable from one that ended afterwards.
    fn truncated(&self) -> Option<DataFusionError> {
        self.paths
            .iter()
            .zip(self.runtimes.iter())
            .find(|(_, runtime)| runtime.open_source_series)
            .map(|(path, _)| truncated_input(path))
    }

    /// Reports a stream that ended inside a series, without closing any node.
    ///
    /// Closing is destructive -- a window's evaluator takes itself apart -- and the binary nodes
    /// still have to compute after this check, so what is read here is only the state that follows
    /// what each node was handed: an input node reports the series it handed over without the chunk
    /// that ends it, and a binary node reports an operand that is still open on either side. A
    /// truncation is therefore refused before a binary kernel allocates its workspace, rather than
    /// after one has already been computed on half a vector.
    fn unclosed(&self) -> Result<()> {
        // A source series that never ended is refused before anything is merged: the close a merge
        // synthesises would make it look like one that did.
        if let Some(error) = self.truncated() {
            return Err(error);
        }
        for (path, runtime) in self.paths.iter().zip(&self.runtimes) {
            if let Some(binary) = &runtime.binary {
                binary.closed(path)?;
            }
        }
        Ok(())
    }

    /// Closes every node, reporting an input that ended mid-series.
    ///
    /// A drain finds nothing left to emit: a window reports its results on the chunk that closes
    /// the series, so what is left to do is to check that every series did close, and to return
    /// everything the execution reserved. A window's evaluator is what holds a series and so
    /// notices one that never ended; a tree with no window has none, so there the input node answers
    /// the same question from [`NodeRuntime::open_source_series`], which follows what it handed over.
    /// The first error found is the one reported, and the nodes are walked from the root down, so a
    /// window above the input reports its own truncation as it always did.
    fn close(&mut self) -> Result<()> {
        let mut result = Ok(());
        for (path, runtime) in self.paths.iter().zip(self.runtimes.iter_mut()) {
            if let Some(window) = runtime.window.take()
                && let Err(error) = window.finish()
                && result.is_ok()
            {
                result = Err(error);
            }
            if runtime.open_source_series && result.is_ok() {
                result = Err(truncated_input(path));
            }
            runtime.release();
        }
        result
    }
}

/// Drives one execution of a bound tree.
///
/// The driver owns the state and nothing else: every decision a node makes was bound before the
/// tree existed, so a pull only reads the position and runs the bound operation.
///
/// A window directly over a source evaluates its chunks as they arrive, a map at the root hands
/// its chunks on as it produces them, and a binary node over a constant transforms each chunk of
/// the vector side as it arrives: all three stream. A window whose input can hold two series under
/// one canonical label set collects that input instead, merges each set once and evaluates it, so
/// it reports at [`Self::finish`] rather than as its input arrives.
pub struct NativeExecutor {
    state: NativeExecutionState,
    phase: Phase,
    /// Whether the caller ended the input.
    ///
    /// Until then a pull that finds nothing means nothing has settled yet, and a binary node
    /// retains what its operands hand over without computing: the whole vector has to be there
    /// before a point of it is answered. [`NativeExecutor::finish`] drains once, checks that every
    /// stream closed, and only then sets this flag and drains again, which is what lets a binary
    /// node answer its result -- and a binary node beneath another one do the same in turn.
    inputs_finished: bool,
    /// Whether the caller ended the input.
    ///
    /// Until then a pull that finds nothing means nothing has settled yet. After it, the same pull
    /// runs the next stage of the ending -- the truncation check, a binary node's computation, the
    /// merge a collecting window owes -- and only a pull that finds every stage done closes the
    /// nodes and returns their reservations.
    ended: bool,
}

/// Whether the execution can still produce chunks.
#[derive(Clone, Debug, PartialEq, Eq)]
enum Phase {
    /// Chunks may still be queued and pulled.
    Running,
    /// The input ended and every node closed.
    Finished,
    /// A chunk failed, so the tree stopped: the evaluators must not be reused after an error.
    Failed(String),
}

impl NativeExecutor {
    /// Drives `state`, which was built from one execution's tree.
    pub fn new(state: NativeExecutionState) -> Self {
        Self {
            state,
            phase: Phase::Running,
            inputs_finished: false,
            ended: false,
        }
    }

    /// The state this driver runs.
    pub fn state(&self) -> &NativeExecutionState {
        &self.state
    }

    /// Queues one raw source chunk at the tree's only input.
    ///
    /// A tree with a binary node reads two inputs, and a chunk queued at "the input" would be one
    /// the caller cannot say the source of: such a tree is refused here rather than answered from a
    /// source the caller did not name, and [`Self::push_raw_at`] is what names one.
    pub fn push_raw(&mut self, chunk: RawSeriesChunk) -> Result<()> {
        self.push_source(None, NativeChunk::Raw(chunk))
    }

    /// Queues one evaluated source chunk at the tree's only input.
    pub fn push_evaluated(&mut self, chunk: EvaluatedChunk) -> Result<()> {
        self.push_source(None, NativeChunk::Evaluated(chunk))
    }

    /// Queues one raw source chunk at the input node at `path`.
    ///
    /// The path is the position the metrics and `EXPLAIN` name the node by, so a caller that reads
    /// either can say which of the two inputs a chunk belongs to.
    pub fn push_raw_at(&mut self, path: &NodePath, chunk: RawSeriesChunk) -> Result<()> {
        self.push_source(Some(path), NativeChunk::Raw(chunk))
    }

    /// Queues one evaluated source chunk at the input node at `path`.
    pub fn push_evaluated_at(&mut self, path: &NodePath, chunk: EvaluatedChunk) -> Result<()> {
        self.push_source(Some(path), NativeChunk::Evaluated(chunk))
    }

    /// The next chunk the root produces, or `None` when nothing has settled yet.
    ///
    /// A window and a map take whole chunks from their input, so nothing beyond the chunk in hand
    /// is materialised on their edge -- except below a window that collects its input, which holds
    /// that input until [`Self::finish`]. A binary node that matches two vectors is the other
    /// exception: it pairs two whole vectors point by point, so it retains every chunk each operand
    /// hands over and reports `None` until [`Self::finish`] ends the input, which is when it
    /// computes its result. A binary node that broadcasts a constant is not one: it pairs each
    /// value of the vector side with the constant and hands the transformed chunk up as it arrives.
    ///
    /// A chunk's `is_last` says the series whose data it carries ended. It does not say its label
    /// set is finished: two source series whose labels were rewritten into one set are two pieces
    /// of one logical series, and the merge that makes them one is
    /// [`crate::grid_series::merge_same_labelset`]'s. Every window that collects its input performs
    /// that merge itself, so a root below such a window hands over one series per label set; a map
    /// at the root hands over pieces, and a caller that needs one series per label set merges them.
    pub fn next_chunk(&mut self) -> Result<Option<EvaluatedChunk>> {
        self.check_running()?;
        // The end of the input is walked here rather than in one call, so a chunk is handed over as
        // soon as the tree settles it: the caller pulls the answer instead of receiving it as one
        // collected value, and what it has not pulled yet stays charged to the node holding it.
        loop {
            // The root is the node the state was built from, which the pass puts first.
            let pulled: Option<NativeChunk> = match self.pull(0) {
                Ok(Some(NativeChunk::Evaluated(chunk))) => return Ok(Some(chunk)),
                // Rejected when the state was built; kept as an error rather than a panic so an
                // impossible tree cannot take the process down.
                Ok(Some(NativeChunk::Raw(_))) => {
                    let error = DataFusionError::Execution(
                        "the native runtime produces evaluated chunks, but its root produced raw \
                         samples"
                            .to_string(),
                    );
                    self.stop(&error);
                    return Err(error);
                }
                Ok(None) => None,
                Err(error) => {
                    self.stop(&error);
                    return Err(error);
                }
            };
            let _ = pulled;
            // Nothing has settled. While the caller is still pushing, that is a stall and the
            // answer is simply not ready; once it has ended the input, each stage below runs on the
            // first pull that finds the tree quiet, and the loop looks for what it produced.
            if !self.ended {
                return Ok(None);
            }
            if !self.inputs_finished {
                // The source queues are empty and every node has consumed what it was given, so a
                // series the caller never closed is still open here and nowhere else. It is
                // refused before a binary node computes: half a vector is not a vector, so the
                // kernel must not allocate a workspace to match one.
                if let Err(error) = self.state.unclosed() {
                    self.stop(&error);
                    return Err(error);
                }
                self.inputs_finished = true;
                continue;
            }
            // A window that collects its input answers only once its own input is finished, and
            // what a binary node above it reads is that answer. Flushing the deepest ready window
            // first is what keeps the two in order: the window produces, the pull that follows
            // carries its output to whoever reads it, and only then can a binary node decide
            // whether both of its operands are complete.
            match self.flush_ready_merges() {
                // A window answered: what it queued is pulled on the next turn, and that pull is
                // also what carries its answer to a node above it.
                Ok(true) => continue,
                Ok(false) => {}
                Err(error) => {
                    self.stop(&error);
                    return Err(error);
                }
            }
            if self.any_merge_pending() {
                // A window still holds collected input, so something below it has not finished:
                // pulling again is what gives that node its turn.
                continue;
            }
            // Every node has answered everything it can. Closing them is what hands back the
            // reservations they were charged, so pulling to exhaustion leaves the pool as it found
            // it.
            return match self.state.close() {
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

    /// Ends the input, which is what tells the tree that nothing more will be pushed.
    ///
    /// It hands nothing over: [`Self::next_chunk`] runs the ending stage by stage as the caller
    /// pulls, so a chunk reaches the caller as soon as it settles and the chunks not pulled yet
    /// stay charged to the node holding them. A caller that wants the whole answer at once calls
    /// [`Self::finish`] instead.
    pub fn end_of_input(&mut self) -> Result<()> {
        if self.ended {
            return Err(DataFusionError::Execution(
                "the native execution already ended its input".to_string(),
            ));
        }
        self.ended = true;
        Ok(())
    }

    /// Ends the input and collects everything the tree answers.
    ///
    /// It is [`Self::end_of_input`] followed by a drain, kept for callers that want the whole
    /// answer in one value. A caller that would rather stream it calls `end_of_input` and then
    /// pulls with [`Self::next_chunk`] until it returns `None`, which is also when the execution
    /// closes and returns its reservations.
    pub fn finish(&mut self) -> Result<Vec<EvaluatedChunk>> {
        self.end_of_input()?;
        self.drain()
    }

    /// Pulls what has settled until nothing is left.
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

    /// Merges the collected input of every window that collects it, and drains the root after each
    /// merged series.
    ///
    /// Deeper windows go first: a window above one of them collects the chunks the one below hands
    /// up, so flushing the shallow one first would merge an input it has not received yet. What a
    /// flush queues is handed up by the caller's own pulls, which is why nothing here collects the
    /// answer into a value of its own.
    fn flush_ready_merges(&mut self) -> Result<bool> {
        let mut flushed = false;
        for index in (0..self.state.runtimes.len()).rev() {
            // A window that collects its input is the one node holding a [`MergeInput`], and the
            // flush takes it: the same node is not visited twice.
            if self.state.runtimes[index].merge.is_none() {
                continue;
            }
            // Its input has to be finished before it is merged: a node below still holding collected
            // input has not answered yet, and merging what it has not sent would answer a series
            // from half of its contributions.
            let Some(child) = self.state.nodes[index].input else {
                continue;
            };
            if !self.settled(child) {
                continue;
            }
            self.flush_merge(index)?;
            flushed = true;
        }
        Ok(flushed)
    }

    /// Whether any window still holds collected input that has not been merged.
    fn any_merge_pending(&self) -> bool {
        self.state
            .runtimes
            .iter()
            .any(|runtime| runtime.merge.is_some())
    }

    /// Whether the node at `index` has produced everything it ever will.
    ///
    /// A pull that finds nothing does not answer it: a collecting window reports nothing until it
    /// is flushed, and a binary node that matches two vectors reports nothing until both nodes it
    /// reads from are finished. Each node therefore answers from its own state and from its
    /// children's, which is what keeps a binary node from computing on an operand a window below it
    /// has not handed over yet.
    fn settled(&self, index: usize) -> bool {
        let runtime = &self.state.runtimes[index];
        if !runtime.queued.is_empty() || runtime.merge.is_some() {
            return false;
        }
        match self.state.nodes[index].tag {
            NodeTag::Input => true,
            // A scalar produces its value without reading anything, so it is settled from the
            // start: there is no chunk it owes its consumer.
            NodeTag::Scalar => true,
            NodeTag::Map | NodeTag::Window | NodeTag::Instant => self.state.nodes[index]
                .input
                .is_none_or(|child| self.settled(child)),
            NodeTag::Binary => {
                // A broadcast owes only what its vector side has not handed over yet: it answers
                // each chunk as it reads it and keeps nothing, so it is settled exactly when that
                // side is. A vector match is settled once its kernel ran and both operands are.
                if let Some(broadcast) = &runtime.broadcast {
                    let vector = match broadcast.operand {
                        ScalarOperand::Right(_) => self.state.nodes[index].input,
                        ScalarOperand::Left(_) => self.state.right_input(index).ok(),
                    };
                    return vector.is_some_and(|vector| self.settled(vector));
                }
                runtime
                    .binary
                    .as_ref()
                    .is_some_and(|binary| binary.computed)
                    && self.state.nodes[index]
                        .input
                        .is_none_or(|child| self.settled(child))
                    && self
                        .state
                        .right_input(index)
                        .is_ok_and(|right| self.settled(right))
            }
        }
    }

    /// Merges the collected input of the node at `index` and evaluates it, one merged series at a
    /// time.
    ///
    /// Each merged series is a whole series: it is handed to the evaluator as one closing series, so
    /// the window reports on it once, and its chunk waits in the node's queue at its own estimate.
    /// The fragments and the merge's workspace stay charged for the whole flush and go back when it
    /// ends; the queued answers stay charged until the caller pulls them.
    fn flush_merge(&mut self, index: usize) -> Result<()> {
        let path = self.state.paths[index].clone();
        let Some(merge) = self.state.runtimes[index].merge.take() else {
            return Ok(());
        };
        if merge.fragments.is_empty() {
            // Nothing arrived, so there is nothing to merge and nothing to answer: the window's
            // evaluator never read a series and the charge goes back.
            return self.settle_merge(index, merge.bytes);
        }
        // The workspace the merge allocates is charged before it runs, from the rows, segments and
        // fields of the fragments as they were collected.
        let workspace = merge.workspace_bytes();
        let held = merge.bytes.saturating_add(workspace);
        let schema = Arc::clone(&merge.schema);
        self.state.runtimes[index].buffer.grow(workspace)?;
        // The fragments' charge stands for the merged series too -- a merged run is a slice of a
        // fragment's own segment, so no value is copied -- and the workspace stays charged until
        // the flush is over.
        let merged = merge_same_labelset(&schema, merge.fragments)?;
        for series in merged {
            let output = {
                let runtime = &mut self.state.runtimes[index];
                let Some(evaluator) = runtime.window.as_mut() else {
                    return Err(closed(&path));
                };
                let metrics = Arc::clone(&runtime.buffer.metrics);
                let _timer = metrics.timer();
                evaluator.push_series(&series, true)?
            };
            if let Some(output) = output {
                let runtime = &mut self.state.runtimes[index];
                runtime
                    .buffer
                    .metrics
                    .record_grid_points(output.series.time.len());
                runtime
                    .buffer
                    .metrics
                    .record_output(1, valid_values(&output));
                runtime.track(&path, &output)?;
                let retained = runtime
                    .window
                    .as_ref()
                    .map_or(0, RangeEvaluator::retained_bytes);
                let emitted = runtime.emitted_bytes();
                runtime
                    .buffer
                    .resize(retained.saturating_add(emitted).saturating_add(held))?;
                runtime.enqueue_output(output)?;
            }
        }
        // The fragments and the merge's workspace go back, and whatever the flush produced stays
        // charged in the node's queue until the caller pulls it.
        self.settle_merge(index, held)
    }

    /// Gives back the `give_back` bytes the flush took, leaving the rest of the node's
    /// reservation alone.
    ///
    /// It must not resize to a figure computed here: the chunks the flush queued are still charged
    /// to this node, and they stay charged until the pull that hands each of them up returns its
    /// estimate.
    fn settle_merge(&mut self, index: usize, give_back: usize) -> Result<()> {
        self.state.runtimes[index].buffer.shrink(give_back)
    }

    /// The next chunk of the node at `index`.
    fn pull(&mut self, index: usize) -> Result<Option<NativeChunk>> {
        let tag = self.state.nodes[index].tag;
        let input = self.state.nodes[index].input;
        match tag {
            NodeTag::Input => self.state.runtimes[index].pop_source(),
            // A scalar owes its consumer no chunk: it produces one value and no series, and the
            // value is the constant on the tree, which the consumer reads off its own binding. A
            // pull of it is therefore empty by construction and says nothing has stalled.
            NodeTag::Scalar => Ok(None),
            NodeTag::Map => {
                let child = input.ok_or_else(|| missing_input(tag))?;
                // An operation that empties every position yields no chunk, and the map asks its
                // input again rather than reporting a decision it did not make.
                loop {
                    let Some(chunk) = self.pull(child)? else {
                        return Ok(None);
                    };
                    if let Some(output) = self.map_node(index, chunk)? {
                        return Ok(Some(output));
                    }
                }
            }
            NodeTag::Window => {
                // A chunk this window computed while flushing its collected input leaves first: it
                // was produced before the window was drained, and the pull that hands it up is the
                // one that empties the queue.
                if let Some(chunk) = self.state.runtimes[index].take_output()? {
                    return Ok(Some(chunk));
                }
                let child = input.ok_or_else(|| missing_input(tag))?;
                // A window reports on the chunk that closes a series, so a chunk that decides
                // nothing yields nothing and the window asks its input again.
                loop {
                    let Some(chunk) = self.pull(child)? else {
                        return Ok(None);
                    };
                    if let Some(output) = self.window_node(index, chunk)? {
                        return Ok(Some(NativeChunk::Evaluated(output)));
                    }
                }
            }
            NodeTag::Instant => {
                let child = input.ok_or_else(|| missing_input(tag))?;
                // A selection decides a whole series at once, so every chunk before the one that
                // closes it yields nothing and the selection asks its input again.
                loop {
                    let Some(chunk) = self.pull(child)? else {
                        return Ok(None);
                    };
                    if let Some(output) = self.instant_node(index, chunk)? {
                        return Ok(Some(NativeChunk::Evaluated(output)));
                    }
                }
            }
            NodeTag::Binary => {
                // The spec names the left operand, the node the build pass reaches next; the right
                // one is the input a spec of two fields would have had to carry.
                let left = input.ok_or_else(|| missing_input(tag))?;
                let right = self.state.right_input(index)?;
                // A broadcast reads the vector side only: the constant is on the node's binding, so
                // there is no second stream to pull and no chunk to retain. Each chunk is
                // transformed as it arrives and handed up, which is what keeps a broadcast over a
                // whole vector from costing the vector.
                if self.state.runtimes[index].broadcast.is_some() {
                    return self.broadcast_node(index, left, right);
                }
                // Both operands are read to the end of what they hand over: before the input ends a
                // binary node only retains the chunks, because a pair is not decided until both
                // sides are whole, and after it ends this is the drain that empties the two.
                for (side, child) in [left, right].into_iter().enumerate() {
                    while let Some(chunk) = self.pull(child)? {
                        let path = &self.state.paths[index];
                        self.state.runtimes[index].retain_binary(path, side, chunk)?;
                    }
                }
                // Nothing has settled for a point that has an operand still arriving, and an
                // operand below a collecting window has not arrived until that window is flushed.
                if !self.inputs_finished || !self.settled(left) || !self.settled(right) {
                    return Ok(None);
                }
                let path = &self.state.paths[index];
                let runtime = &mut self.state.runtimes[index];
                runtime.compute_binary(path)?;
                // A result holds one value per field, and a field may hold none where another
                // does, so its values are counted rather than read as its positions.
                runtime.pop_queued(evaluated_values)
            }
        }
    }

    /// Pulls the vector side of the broadcast at `index` until it hands over a chunk, and returns
    /// that chunk transformed.
    ///
    /// Only the vector side is pulled: the scalar side is the constant the node was bound to. The
    /// chunk is transformed inside this call and nothing of it is kept, so what a broadcast holds
    /// does not grow with the vector it reads.
    fn broadcast_node(
        &mut self,
        index: usize,
        left: usize,
        right: usize,
    ) -> Result<Option<NativeChunk>> {
        let vector = {
            let broadcast = self.state.runtimes[index]
                .broadcast
                .as_ref()
                .ok_or_else(|| no_broadcast(&self.state.paths[index]))?;
            match broadcast.operand {
                ScalarOperand::Right(_) => left,
                ScalarOperand::Left(_) => right,
            }
        };
        // An operation that empties every position yields no chunk, and the node asks its vector
        // side again rather than reporting a decision it did not make.
        loop {
            let Some(chunk) = self.pull(vector)? else {
                // Nothing is left in the vector side. Before the input ends that is a stall rather
                // than an end, and so is a vector side still holding collected input: the node has
                // answered everything it can and owes whatever that side has not produced yet.
                return Ok(None);
            };
            let path = &self.state.paths[index];
            let runtime = &mut self.state.runtimes[index];
            if let Some(output) = runtime.broadcast(path, chunk)? {
                return Ok(Some(NativeChunk::Evaluated(output)));
            }
        }
    }

    /// Maps one chunk the node at `index` pulled from its input.
    fn map_node(&mut self, index: usize, chunk: NativeChunk) -> Result<Option<NativeChunk>> {
        let NativeChunk::Evaluated(chunk) = chunk else {
            return Err(DataFusionError::Execution(format!(
                "the map node at {} consumes an evaluated chunk, not raw samples",
                self.state.paths[index]
            )));
        };
        let path = &self.state.paths[index];
        let runtime = &mut self.state.runtimes[index];
        Ok(runtime.map(path, chunk)?.map(NativeChunk::Evaluated))
    }

    /// Feeds one chunk to the window at `index`.
    fn window_node(&mut self, index: usize, chunk: NativeChunk) -> Result<Option<EvaluatedChunk>> {
        let path = &self.state.paths[index];
        let runtime = &mut self.state.runtimes[index];
        runtime.window(path, chunk)
    }

    /// Feeds one chunk to the instant selection at `index`.
    fn instant_node(&mut self, index: usize, chunk: NativeChunk) -> Result<Option<EvaluatedChunk>> {
        let path = &self.state.paths[index];
        let runtime = &mut self.state.runtimes[index];
        runtime.instant(path, chunk)
    }

    /// Queues a source chunk at the input node `path` names, or at the tree's only one.
    fn push_source(&mut self, path: Option<&NodePath>, chunk: NativeChunk) -> Result<()> {
        self.check_running()?;
        let result = self.queue_source(path, chunk);
        if let Err(error) = &result {
            self.stop(error);
        }
        result
    }

    /// Queues a source chunk at one input node, checking it against that input's binding.
    fn queue_source(&mut self, path: Option<&NodePath>, chunk: NativeChunk) -> Result<()> {
        let source = self.source_index(path)?;
        let at = &self.state.paths[source];
        let Some(binding) = self.state.runtimes[source].source_schema.as_ref() else {
            return Err(DataFusionError::Execution(format!(
                "the input node at {at} does not produce samples"
            )));
        };
        let bound = binding.eval_grid.is_some();
        let evaluated = matches!(chunk, NativeChunk::Evaluated(_));
        if evaluated != bound {
            return Err(DataFusionError::Execution(format!(
                "the native source at {at} is bound to {}, but {} chunk was queued at it",
                if bound {
                    "an evaluated series"
                } else {
                    "raw samples"
                },
                if evaluated { "an evaluated" } else { "a raw" },
            )));
        }
        self.state.runtimes[source].enqueue(chunk)
    }

    /// The input node at `path`, or the tree's only input when the caller named none.
    ///
    /// A tree with a binary node has two inputs, and a chunk queued at "the input" of such a tree
    /// cannot be assigned to either of them: the caller's chunks would be matched as the wrong
    /// operand, so the ambiguity is refused instead of answered from the first input found.
    fn source_index(&self, path: Option<&NodePath>) -> Result<usize> {
        let Some(path) = path else {
            return match self.state.sources.as_slice() {
                [only] => Ok(*only),
                _ => Err(DataFusionError::Execution(format!(
                    "the native execution has {} inputs, so a queued chunk has to name the input it \
                     belongs to",
                    self.state.sources.len()
                ))),
            };
        };
        let index = *self.state.by_path.get(path).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "the native tree has no node at {path} to queue a chunk at"
            ))
        })?;
        let tag = self.state.nodes[index].tag;
        if tag != NodeTag::Input {
            return Err(DataFusionError::Execution(format!(
                "the node at {path} is a {} node, and only an input node takes queued chunks",
                tag.describe()
            )));
        }
        Ok(index)
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

    /// Stops the execution, returning every byte it reserved to the pool.
    ///
    /// A stopped execution is not resumed: an evaluator that failed must not be reused, so the
    /// chunks it already produced are the only ones the caller will see.
    fn stop(&mut self, error: &DataFusionError) {
        for runtime in &mut self.state.runtimes {
            runtime.release();
        }
        self.phase = Phase::Failed(error.to_string());
    }
}

/// Maps one chunk with `ops`, keeping its axis and its closing.
///
/// Mapping is point-wise over valued positions, so a chunk whose values survive keeps exactly the
/// rows it came with: a position no field fills is not a row of any chunk, and mapping does not
/// invent one. `None` means the operation emptied every position, which yields no chunk at all
/// unless this chunk closed its series: a closing chunk is still handed over, holding no position,
/// because a consumer tells "nothing more here" from "no more input" by that chunk alone.
fn map_chunk(
    schema: &SeriesSchema,
    chunk: &EvaluatedChunk,
    ops: &[MathOp],
) -> Result<Option<EvaluatedChunk>> {
    let mut series = chunk.series.clone();
    for &op in ops {
        series = match map_math(schema, &series, op)? {
            Some(mapped) => mapped,
            // `clamp` with `max < min` is the empty vector: every position it held was decided and
            // holds no value now, so there is no row to hand over.
            None => {
                return if chunk.is_last {
                    // A map drops the metric name, so the empty series closes under the labels the
                    // map produces.
                    let metric = drop_metric_name(&chunk.series.metric)?;
                    let empty = no_row_series(schema, &chunk.series, metric)?;
                    EvaluatedChunk::try_new(empty, true).map(Some)
                } else {
                    Ok(None)
                };
            }
        };
    }
    EvaluatedChunk::try_new(series, chunk.is_last).map(Some)
}

/// A series carrying `metric` and no row at all.
///
/// It is what a closing chunk holds when the operation emptied its input: the labels say which
/// series closed, and the axis is empty because no position holds a value. `metric` is the identity
/// the operation produces — a map drops the name, a filtering comparison keeps it — so the caller
/// says which, rather than this function deciding for it.
fn no_row_series(schema: &SeriesSchema, series: &Series, metric: MetricName) -> Result<Series> {
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

/// The values a chunk a binary node hands over holds.
///
/// A binary result is an evaluated chunk, so this is [`valid_values`]: one value per field sample
/// that holds one. A raw chunk cannot reach a binary node's queue, and is answered as its positions
/// so an impossible queue cannot under-count.
fn evaluated_values(chunk: &NativeChunk) -> usize {
    match chunk {
        NativeChunk::Evaluated(chunk) => valid_values(chunk),
        NativeChunk::Raw(chunk) => chunk.time.len(),
    }
}

/// The rejection of a node kind this runtime does not execute yet.
///
/// It names the kind and the node's position: the position is what the metrics and `EXPLAIN` call
/// the same node, so a reader can find it in the tree. An aggregate is the one kind left here: a
/// scalar executes as a value node, and a binary node executes both as a vector match and as a
/// constant broadcast.
fn not_implemented(kind: &str, path: &NodePath) -> DataFusionError {
    DataFusionError::NotImplemented(format!(
        "the native runtime does not execute {kind} nodes yet: {kind} at {path}"
    ))
}

/// The error for a window whose evaluator this execution already closed.
fn closed(path: &NodePath) -> DataFusionError {
    DataFusionError::Execution(format!("the window at {path} is already closed"))
}

/// The error for a chunk of another series arriving while one is being selected.
///
/// [`select_instants`] reads one whole axis, so the buffered chunks have to be one series: a chunk
/// of another series has nowhere to go, and appending it would select its samples as though they
/// were the open series'.
fn series_changed(path: &NodePath) -> DataFusionError {
    DataFusionError::Execution(format!(
        "the instant selection at {path} combines one series into one axis, and a chunk of another \
         series arrived before the first one ended"
    ))
}

/// The error for a series an instant selection has no labels to report it under.
///
/// A source bound in label mode carries its identity encoded and no decoded labels, and a chunk
/// that is handed on has to name the series it holds.
fn no_labels(path: &NodePath) -> DataFusionError {
    DataFusionError::Execution(format!(
        "the instant selection at {path} requires materialized series labels"
    ))
}

/// The error for a source row the selection named but the series does not hold.
///
/// The selection returns indices into the axis it was given, so a row outside it means the two
/// disagree rather than that a sample is missing.
fn missing_row(path: &NodePath, index: usize) -> DataFusionError {
    DataFusionError::Execution(format!(
        "the instant selection at {path} selected the source row {index}, which the series it read \
         does not hold"
    ))
}

/// The rejection of an input ordering this runtime cannot execute.
///
/// It names the input that declares the ordering and the ordering found, so a reader can tell a
/// tree this runtime cannot run from one it cannot run correctly.
fn unsupported_ordering(ordering: SeriesOrdering, input: &NodePath) -> DataFusionError {
    DataFusionError::NotImplemented(format!(
        "the native runtime does not execute {ordering:?} input yet: the nodes here read one series \
         at a time, and the input at {input} is {ordering:?}, so the chunks of two of them can \
         arrive interleaved"
    ))
}

/// The rejection of a chunk holding a value where the same field already holds one.
///
/// No merge resolves this one: at `position`, field `field` of the label set `labels` would carry
/// two values, which is what Prometheus rejects for two series whose label sets became equal.
fn occupied_position(
    path: &NodePath,
    labels: &LabelSet,
    field: usize,
    run: &Run,
    previous: &Run,
) -> DataFusionError {
    DataFusionError::Execution(format!(
        "the node at {path} produces two series under the label set {labels:?} that both hold a \
         value of field {field} at position {position}: one occupies {previous:?} and another \
         {run:?}, and no merge resolves two values of one field at one position",
        position = run.start.max(previous.start),
    ))
}

/// The rejection of an input that ended without the chunk closing its series.
///
/// It is what a window's evaluator reports for the input it reads, said by the node that handed the
/// chunks over instead: a tree with no window has no evaluator to hold the series, so the input node
/// is the one node left that can tell a series that ended from one that was cut off.
fn truncated_input(path: &NodePath) -> DataFusionError {
    DataFusionError::Execution(format!(
        "the input at {path} ended before the last chunk of a series: the last chunk it handed over \
         did not say `is_last`"
    ))
}

/// The error for a node asked to compute without the runtime its kind holds.
fn no_binary(path: &NodePath) -> DataFusionError {
    DataFusionError::Execution(format!("the node at {path} holds no binary operation"))
}

/// The error for a node asked to broadcast without the broadcast its kind holds.
fn no_broadcast(path: &NodePath) -> DataFusionError {
    DataFusionError::Execution(format!("the node at {path} holds no scalar broadcast"))
}

/// The rejection of a scalar broadcast whose matching is not the one a constant can take.
///
/// A constant has no labels, so there is nothing to match it against: Prometheus does not consult
/// the matching modifiers of a vector–scalar operation at all. A tree that carries them therefore
/// describes an operation that is not the one it would compute, which is refused rather than run
/// as if the modifiers were absent.
fn matched_scalar(path: &NodePath) -> DataFusionError {
    DataFusionError::NotImplemented(format!(
        "the native runtime does not execute a scalar broadcast whose constant carries matching \
         modifiers: a constant has no labels to match, so the operation this Binary at {path} \
         describes is not the one it would compute"
    ))
}

/// The rejection of a binary node whose operands are neither two evaluated vectors nor one
/// evaluated vector and one constant.
///
/// `operands` says which shape arrived and why that shape has no path here, so a tree a later
/// stage compiles is refused for a reason a reader can act on. The kind and the position are named
/// the way every other rejection in this runtime names them.
fn unsupported_binary(path: &NodePath, operands: &str) -> DataFusionError {
    DataFusionError::NotImplemented(format!(
        "the native runtime does not execute this Binary node: it reads {operands}, and this \
         runtime reads two evaluated vectors or one evaluated vector and one constant, at {path}"
    ))
}

/// The broadcast a binary node with one constant operand is, read off its binding.
///
/// The two sides say which mode this is: one evaluated vector and one constant is a broadcast. The
/// constant comes from the scalar node's own binding — the value the node produces — and the
/// operation from the matching the compiler bound; [`ScalarOperand`] preserves the constant's side.
///
/// Of the rejection arms in the operand-type match below, only the two-constant one is a shape a
/// tree that `validate` accepts can put here; the others are backstops that keep the match
/// exhaustive. That is not the wide safety margin it looks like: [`NativeNode::validate`] refuses a
/// binary operand that produces neither evaluated values nor a scalar before any of this runs, so a
/// raw side or an aggregate-state side can never reach this function at all. The refusals after the
/// match — a constant operand that is not a literal scalar node, and a constant carrying matching
/// modifiers — are reachable through a validated tree.
fn broadcast_runtime(
    path: &NodePath,
    left: &NativeNode,
    right: &NativeNode,
    matching: &BinaryMatching,
) -> Result<BroadcastRuntime> {
    let (scalar_operand, vector, constant) = match (&left.output, &right.output) {
        (NativeType::Samples(vector), NativeType::Scalar(_)) if vector.eval_grid.is_some() => (
            ScalarOperand::Right as fn(f64) -> ScalarOperand,
            vector,
            right,
        ),
        (NativeType::Scalar(_), NativeType::Samples(vector)) if vector.eval_grid.is_some() => (
            ScalarOperand::Left as fn(f64) -> ScalarOperand,
            vector,
            left,
        ),
        // The two-constant pair is reachable through the state builder, which is what this arm
        // guards: `validate` accepts a binary node of two constants (it binds them to a scalar), and
        // a compiler that could not fold a scalar–scalar operation would emit one. No constant has
        // labels or fields to broadcast over, so there is no vector to apply the operation to. The
        // executor test `an_unimplemented_node_is_reported_not_panicked` builds this pair directly.
        (NativeType::Scalar(_), NativeType::Scalar(_)) => {
            return Err(unsupported_binary(
                path,
                "two constants, which broadcast over no vector",
            ));
        }
        // Raw samples or aggregate state: `validate` already refused either as a binary operand, so
        // this arm is the exhaustiveness backstop rather than a reachable path.
        (_, _) => {
            return Err(unsupported_binary(
                path,
                "an operand that is neither evaluated samples nor a constant, which no validated \
                 tree binds as a binary operand",
            ));
        }
    };
    let NativeExpr::Scalar(BoundScalar { value }) = &constant.expr else {
        // NotImplemented rather than Execution: this is a shape the runtime does not support, not a
        // tree whose values are wrong. A caller that falls back to another planner on NotImplemented
        // then still can, which an Execution would deny it.
        return Err(DataFusionError::NotImplemented(format!(
            "the native runtime does not execute the Binary node at {path}, which broadcasts an \
             operand that is not a literal scalar: the constant side has to be a Scalar node, and a \
             computed constant such as `1 < bool 2` is not one until it is folded"
        )));
    };
    // A constant has no labels, so the modifiers a vector–vector operation would use have no
    // meaning here: a tree that carries them describes another operation than the one it computes.
    if matching.labels != MatchingLabels::Ignoring(Vec::new())
        || matching.cardinality != Cardinality::OneToOne
        || matching.fill != FillValues::default()
    {
        return Err(matched_scalar(path));
    }
    Ok(BroadcastRuntime {
        operand: scalar_operand(*value),
        op: broadcast_op(path, matching)?,
        sample_schema: Arc::clone(vector),
    })
}

/// The operation a binary node's `matching` describes.
///
/// A comparison is a filter unless it was bound as `bool`, and a filter keeps the label set it
/// read while every other operation drops the metric name. Arithmetic has no `bool` form, so one
/// bound that way describes a node the compiler does not produce and is refused.
fn broadcast_op(path: &NodePath, matching: &BinaryMatching) -> Result<BroadcastOp> {
    let arithmetic = |op: ArithmeticOp| {
        if matching.return_bool {
            Err(DataFusionError::Execution(format!(
                "the binary node at {path} carries `bool` on an arithmetic operator, which has no \
                 bool form"
            )))
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
    match matching.op {
        BinaryOp::Add => arithmetic(ArithmeticOp::Add),
        BinaryOp::Sub => arithmetic(ArithmeticOp::Subtract),
        BinaryOp::Mul => arithmetic(ArithmeticOp::Multiply),
        BinaryOp::Div => arithmetic(ArithmeticOp::Divide),
        BinaryOp::Mod => arithmetic(ArithmeticOp::Modulo),
        BinaryOp::Pow => arithmetic(ArithmeticOp::Power),
        BinaryOp::Atan2 => arithmetic(ArithmeticOp::Atan2),
        BinaryOp::Eq => comparison(ComparisonOp::Equal),
        BinaryOp::Ne => comparison(ComparisonOp::NotEqual),
        BinaryOp::Gt => comparison(ComparisonOp::Greater),
        BinaryOp::Lt => comparison(ComparisonOp::Less),
        BinaryOp::Ge => comparison(ComparisonOp::GreaterEqual),
        BinaryOp::Le => comparison(ComparisonOp::LessEqual),
    }
}

/// The error for a reservation the platform cannot express.
///
/// The binary node reserves its retained operands and the kernel's live workspace together, and
/// the kernel reports the second as a size of its own. A sum of the two that does not fit an
/// address space cannot be reserved, so the execution is refused rather than run with a figure
/// that wrapped.
fn reservation_overflow(path: &NodePath) -> DataFusionError {
    DataFusionError::Execution(format!(
        "the operands and the kernel's workspace of the binary node at {path} do not fit one \
         address space: the bytes it would reserve overflow"
    ))
}

/// The error for a node the flattened tree lost its input for.
fn missing_input(tag: NodeTag) -> DataFusionError {
    DataFusionError::Execution(format!(
        "the native tree has no input for a {} node",
        tag.describe()
    ))
}

fn series_error(error: datatypes::error::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
