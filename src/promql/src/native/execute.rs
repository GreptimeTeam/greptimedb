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
//! A node kind this runtime does not execute yet is rejected when the execution is built, naming
//! the kind and its position. An unsupported tree therefore never starts, rather than answering
//! half a query or panicking halfway through one.
//!
//! A node that rewrites labels — a window drops the metric name and the empty-valued labels, a map
//! drops the name — can turn two distinct input series into one output label set. That pair is
//! legal when the two hold values at disjoint positions, which is exactly what
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
use std::collections::{HashMap, VecDeque};
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
use datatypes::series::time::{EvalGrid, GridRange, GridSpec, TimeAxis};
use datatypes::series::{MetricName, SeriesOrdering};

use crate::grid_series::{
    GridSample, GridSeriesBuilder, LabelSet, label_set, merge_same_labelset, owned,
};
use crate::instant_eval::select_instants;
use crate::map::{MathOp, drop_metric_name, map_math};
use crate::native::batch::EvaluatedChunk;
use crate::native::metrics::{NodeMetrics, NodePath};
use crate::native::plan::{BoundInstant, NativeExpr, NativeTree};
use crate::range_eval::RangeEvaluator;

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
    /// Buffers one series and selects the last sample at or before each of its grid points.
    Instant,
    /// Feeds its input to its own [`RangeEvaluator`].
    Window,
    /// Applies point-wise operations to its input chunks.
    Map,
}

impl NodeTag {
    /// What this tag is called in a rejection.
    fn describe(self) -> &'static str {
        match self {
            Self::Input => "input",
            Self::Instant => "instant selection",
            Self::Window => "window",
            Self::Map => "map",
        }
    }
}

/// One node of the flattened tree.
#[derive(Clone, Copy, Debug)]
struct NodeSpec {
    tag: NodeTag,
    /// The index of this node's input, which is the node the build pass reaches next.
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
    /// with and refuses against a point limit -- so it bounds the rows it can report even though a
    /// candidate at the lookback edge itself selects nothing. Both ends come from chunks the node
    /// already holds, so the count reads two timestamps rather than the rows.
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
    /// An input node's queue is filled by the caller, and a window's by the chunks it computed while
    /// flushing its collected input; no node uses it for both. A chunk is measured once, as it
    /// enters, and the figure it was queued with is given back as it leaves, so neither the queue
    /// nor the handover walks it.
    queued: VecDeque<(NativeChunk, usize)>,
    /// What an instant node holds: the chunks of the series it is selecting, and, while it gathers
    /// that series, the workspace the gather reserved for. `None` for every other kind of node.
    instant: Option<InstantRuntime>,
    /// A window node's evaluator, with its own input and output grid; `None` before it is built and
    /// once it is closed.
    window: Option<RangeEvaluator>,
    /// What a map node applies.
    map: Option<MapRuntime>,
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

    /// Hands over the next queued source chunk, if there is one.
    ///
    /// The handover is what [`Self::open_source_series`] follows: after this call the flag says
    /// whether the series the consumer just received a chunk of is still open, which is what closing
    /// the execution reads to tell a truncated input from one that ended cleanly.
    fn pop_source(&mut self) -> Result<Option<NativeChunk>> {
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
        metrics.record_output(1, chunk.positions());
        // An evaluated source is already addressed on its grid, so its decided positions are grid
        // points; a raw source carries timestamps, which are not positions on any grid.
        if let NativeChunk::Evaluated(chunk) = &chunk {
            metrics.record_grid_points(chunk.series.time.len());
        }
        // Assigned rather than only set: a chunk that closes its series is what says no series is
        // open any more, so the flag follows the handover and not the history of handovers.
        self.open_source_series = !chunk.is_last();
        if chunk.is_last() {
            metrics.record_completed_series(1);
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
    /// The index of the tree's input node.
    source: usize,
    /// Whether the source hands over evaluated chunks rather than raw samples.
    source_is_evaluated: bool,
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
        // One pass down the tree names and registers every node in the order
        // `crate::native::metrics::walk` gives it: the same rule `EXPLAIN` uses, so a node cannot be
        // shown at one position and registered at another. Every node this runtime executes is
        // unary, so that order is the chain from the root to the input, and a node's input is the
        // next one the pass reaches.
        let mut nodes: Vec<NodeSpec> = Vec::new();
        let mut paths: Vec<NodePath> = Vec::new();
        let mut runtimes: Vec<NodeRuntime> = Vec::new();
        // The bound expression of each node, in the same order: the pass below reads the node it
        // configures from here, so nothing about an operation is copied onto the node's spec.
        let mut exprs: Vec<&NativeExpr> = Vec::new();
        let mut source: Option<(usize, Arc<SeriesSchema>)> = None;
        let mut node = tree.root();
        let mut path = NodePath::root();
        loop {
            let index = nodes.len();
            let handles = Arc::new(NodeMetrics::default());
            handles.register(&path, metrics_set, partition);
            let (tag, child) = match &node.expr {
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
                    let samples = node.output.samples().cloned().ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "the input node at {path} does not produce samples"
                        ))
                    })?;
                    source = Some((index, samples));
                    (NodeTag::Input, None)
                }
                NativeExpr::Instant { input, .. } => (NodeTag::Instant, Some(input.as_ref())),
                NativeExpr::Window { input, .. } => (NodeTag::Window, Some(input.as_ref())),
                NativeExpr::Map { input, .. } => (NodeTag::Map, Some(input.as_ref())),
                // The kinds below are legal in a bound tree, and `validate` stays the authority on
                // that; this runtime simply does not execute them yet. Rejected before a single
                // chunk is read: a tree the runtime cannot execute must not start, because a
                // partial answer would be one the caller never asked for.
                NativeExpr::Scalar(_) => return Err(not_implemented("Scalar", &path)),
                NativeExpr::Aggregate { .. } => return Err(not_implemented("Aggregate", &path)),
                NativeExpr::Binary { .. } => return Err(not_implemented("Binary", &path)),
            };
            let buffer = NodeBuffer::new(&path, handles, &pool);
            let mut runtime = NodeRuntime {
                buffer,
                queued: VecDeque::new(),
                instant: None,
                window: None,
                map: None,
                emitted: None,
                merge: None,
                open_source_series: false,
            };
            match &node.expr {
                NativeExpr::Input(_) => {}
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
                NativeExpr::Scalar(_)
                | NativeExpr::Aggregate { .. }
                | NativeExpr::Binary { .. } => {
                    // Rejected above, where the pass has the position to name.
                }
            }
            // The input is the node the pass reaches next: the chain has no other node between
            // this one and its input, and nothing is read off the positions afterwards.
            nodes.push(NodeSpec {
                tag,
                input: child.map(|_| index + 1),
            });
            exprs.push(&node.expr);
            paths.push(path.clone());
            runtimes.push(runtime);
            let Some(child) = child else {
                break;
            };
            node = child;
            path = path.input();
        }

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

        // The supported nodes are unary, so a tree of them has exactly one input node; it is where
        // the caller's chunks enter.
        let (source, source_schema) = source.ok_or_else(|| {
            DataFusionError::Execution("the native tree has no input node".to_string())
        })?;
        Ok(Self {
            nodes,
            paths,
            runtimes,
            source,
            source_is_evaluated: source_schema.eval_grid.is_some(),
        })
    }

    /// The metrics of the node at `path`.
    pub fn metrics(&self, path: &NodePath) -> Option<&Arc<NodeMetrics>> {
        let index = self.paths.iter().position(|candidate| candidate == path)?;
        Some(&self.runtimes[index].buffer.metrics)
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
/// A window directly over a source evaluates its chunks as they arrive, and a map at the root hands
/// its chunks on as it produces them: both stream. A window whose input can hold two series under
/// one canonical label set collects that input instead, merges each set once and evaluates it, so
/// it reports at [`Self::finish`] rather than as its input arrives.
pub struct NativeExecutor {
    state: NativeExecutionState,
    phase: Phase,
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
        }
    }

    /// The state this driver runs.
    pub fn state(&self) -> &NativeExecutionState {
        &self.state
    }

    /// Queues one raw source chunk.
    pub fn push_raw(&mut self, chunk: RawSeriesChunk) -> Result<()> {
        self.push_source(NativeChunk::Raw(chunk))
    }

    /// Queues one evaluated source chunk.
    pub fn push_evaluated(&mut self, chunk: EvaluatedChunk) -> Result<()> {
        self.push_source(NativeChunk::Evaluated(chunk))
    }

    /// The next chunk the root produces, or `None` when nothing has settled yet.
    ///
    /// Every node takes whole chunks from its input, so a caller that keeps pushing and pulling
    /// materialises nothing beyond the chunk in hand -- except below a window that collects its
    /// input, which holds that input until [`Self::finish`].
    ///
    /// A chunk's `is_last` says the series whose data it carries ended. It does not say its label
    /// set is finished: two source series whose labels were rewritten into one set are two pieces
    /// of one logical series, and the merge that makes them one is
    /// [`crate::grid_series::merge_same_labelset`]'s. Every window that collects its input performs
    /// that merge itself, so a root below such a window hands over one series per label set; a map
    /// at the root hands over pieces, and a caller that needs one series per label set merges them.
    pub fn next_chunk(&mut self) -> Result<Option<EvaluatedChunk>> {
        self.check_running()?;
        // The root is the node the state was built from, which the pass puts first.
        let result = self.pull(0).and_then(|chunk| match chunk {
            Some(NativeChunk::Evaluated(chunk)) => Ok(Some(chunk)),
            // Rejected when the state was built; kept as an error rather than a panic so an
            // impossible tree cannot take the process down.
            Some(NativeChunk::Raw(_)) => Err(DataFusionError::Execution(
                "the native runtime produces evaluated chunks, but its root produced raw samples"
                    .to_string(),
            )),
            None => Ok(None),
        });
        if let Err(error) = &result {
            self.stop(error);
        }
        result
    }

    /// Ends the input: drains what has settled, merges what the windows that collect their input
    /// hold, closes every node and releases every reservation.
    ///
    /// Every series must have closed by now, in every tree: a window's evaluator reports the series
    /// it is still holding, and a tree with no window has its input node report one it handed over
    /// without the chunk that ends it. An input that ends mid-series is an error here rather than a
    /// series the caller never sees close, and it is refused before any collected input is merged:
    /// merging closes what it merges.
    ///
    /// The merge comes next, deepest window first, and what it hands up is drained as it is
    /// produced. What the root hands over is therefore a piece stream: a chunk's `is_last` says the
    /// source series it carries ended, and two of them can share one label set. A caller that needs
    /// one series per label set from the root merges them with
    /// [`crate::grid_series::merge_same_labelset`] as well, exactly as this runtime merges the input
    /// of the windows it collects for.
    pub fn finish(&mut self) -> Result<Vec<EvaluatedChunk>> {
        let mut drained = Vec::new();
        loop {
            match self.next_chunk() {
                Ok(Some(chunk)) => drained.push(chunk),
                Ok(None) => break,
                Err(error) => return Err(error),
            }
        }
        // A source series that never ended is refused before anything is merged: the close a merge
        // synthesises would make it look like one that did.
        if let Some(error) = self.state.truncated() {
            self.stop(&error);
            return Err(error);
        }
        if let Err(error) = self.flush_merges(&mut drained) {
            self.stop(&error);
            return Err(error);
        }
        match self.state.close() {
            Ok(()) => {
                self.phase = Phase::Finished;
                Ok(drained)
            }
            Err(error) => {
                self.stop(&error);
                Err(error)
            }
        }
    }

    /// Merges the collected input of every window that collects it, and drains the root after each
    /// merged series.
    ///
    /// Deeper windows go first: a window above one of them collects the chunks the one below hands
    /// up, so flushing the shallow one first would merge an input it has not received yet. The
    /// queue of a flushed node is emptied by the root drain that follows each of its merged series,
    /// so a chunk it produced never stands beside the fragments it was computed from.
    fn flush_merges(&mut self, drained: &mut Vec<EvaluatedChunk>) -> Result<()> {
        for index in (0..self.state.runtimes.len()).rev() {
            // A window that collects its input is the one node holding a [`MergeInput`], and the
            // flush takes it: the same node is not visited twice.
            if self.state.runtimes[index].merge.is_none() {
                continue;
            }
            self.flush_merge(index, drained)?;
        }
        Ok(())
    }

    /// Merges the collected input of the node at `index` and evaluates it, one merged series at a
    /// time.
    ///
    /// Each merged series is a whole series: it is handed to the evaluator as one closing series, so
    /// the window reports on it once, and its chunk is queued and drained up the tree before the
    /// next one is evaluated. That is what keeps the reservation honest: the fragments and the
    /// merge's workspace stay charged while the flush runs, and the queue of a node never holds an
    /// earlier series beside the fragments of a later one.
    fn flush_merge(&mut self, index: usize, drained: &mut Vec<EvaluatedChunk>) -> Result<()> {
        let path = self.state.paths[index].clone();
        let Some(merge) = self.state.runtimes[index].merge.take() else {
            return Ok(());
        };
        if merge.fragments.is_empty() {
            // Nothing arrived, so there is nothing to merge and nothing to answer: the window's
            // evaluator never read a series and the charge goes back.
            return self.settle_merge(index);
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
            self.drain_root(drained)?;
        }
        self.settle_merge(index)
    }

    /// Sets the reservation of the node at `index` to what it holds once its flush is over.
    fn settle_merge(&mut self, index: usize) -> Result<()> {
        let runtime = &mut self.state.runtimes[index];
        let retained = runtime
            .window
            .as_ref()
            .map_or(0, RangeEvaluator::retained_bytes);
        let emitted = runtime.emitted_bytes();
        runtime.buffer.resize(retained.saturating_add(emitted))
    }

    /// Hands the root's outputs to `drained` until the tree has nothing settled.
    fn drain_root(&mut self, drained: &mut Vec<EvaluatedChunk>) -> Result<()> {
        loop {
            match self.pull(0)? {
                Some(NativeChunk::Evaluated(chunk)) => drained.push(chunk),
                Some(NativeChunk::Raw(_)) => {
                    return Err(DataFusionError::Execution(
                        "the native runtime produces evaluated chunks, but its root produced raw \
                         samples"
                            .to_string(),
                    ));
                }
                None => return Ok(()),
            }
        }
    }

    /// The next chunk of the node at `index`.
    fn pull(&mut self, index: usize) -> Result<Option<NativeChunk>> {
        let tag = self.state.nodes[index].tag;
        let input = self.state.nodes[index].input;
        match tag {
            NodeTag::Input => self.state.runtimes[index].pop_source(),
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

    /// Queues a source chunk at the tree's input node.
    fn push_source(&mut self, chunk: NativeChunk) -> Result<()> {
        self.check_running()?;
        let evaluated = matches!(chunk, NativeChunk::Evaluated(_));
        if evaluated != self.state.source_is_evaluated {
            let error = DataFusionError::Execution(format!(
                "the native source is bound to {}, but an {} chunk was queued at it",
                if self.state.source_is_evaluated {
                    "an evaluated series"
                } else {
                    "raw samples"
                },
                if evaluated { "evaluated" } else { "raw" },
            ));
            self.stop(&error);
            return Err(error);
        }
        let source = self.state.source;
        let result = self.state.runtimes[source].enqueue(chunk);
        if let Err(error) = &result {
            self.stop(error);
        }
        result
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
                    let empty = no_row_series(schema, &chunk.series)?;
                    EvaluatedChunk::try_new(empty, true).map(Some)
                } else {
                    Ok(None)
                };
            }
        };
    }
    EvaluatedChunk::try_new(series, chunk.is_last).map(Some)
}

/// A series carrying the identity of `series` and no row at all.
///
/// It is what a closing chunk holds when the operation emptied its input: the labels say which
/// series closed, and the axis is empty because no position holds a value.
fn no_row_series(schema: &SeriesSchema, series: &Series) -> Result<Series> {
    let grid = series.time.grid().ok_or_else(|| {
        DataFusionError::Execution("a mapped chunk must be addressed on a grid".to_string())
    })?;
    let fields = (0..schema.fields.len())
        .map(|_| Samples::from_float(Float64Array::from(Vec::<f64>::new())))
        .collect();
    let fields = SeriesFields::try_from_vec(fields).map_err(series_error)?;
    let metric = drop_metric_name(&series.metric)?;
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

/// The rejection of a node kind this runtime does not execute yet.
///
/// It names the kind and the node's position: the position is what the metrics and `EXPLAIN` call
/// the same node, so a reader can find it in the tree.
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
