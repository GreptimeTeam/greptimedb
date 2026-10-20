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
//! by the position [`metrics::walk`] gives each node. The tree is what the compiler produced and
//! carries no state of its own, so a second execution builds a second state from the same tree and
//! the two never share a buffer, an evaluator or a memory reservation.
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
//! drops the name — can turn two distinct series into one label set, and each of them still closes
//! on its own chunk. The node that did the rewriting is where the second one is refused, before the
//! node above evaluates it: a label set that has closed cannot be produced again, and a chunk that
//! holds a value where the same field already holds one cannot be merged without holding whole
//! series. A chunk that continues a label set where it ended, or that fills positions the set has
//! not occupied, is handed on.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, VecDeque};
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datatypes::arrow::array::{Array, Float64Array};
use datatypes::arrow::buffer::{Buffer, ScalarBuffer};
use datatypes::series::histogram::HistogramBatch;
use datatypes::series::raw::{RawSeriesChunk, RawTimestamps};
use datatypes::series::samples::{
    DenseSamplesRef, RowCursor, SampleRef, Samples, Series, SeriesFields, SeriesSchema,
};
use datatypes::series::time::{EvalGrid, GridRange, GridSpec, TimeAxis};
use datatypes::series::{MetricName, SeriesOrdering};

use crate::grid_series::{GridSample, GridSeriesBuilder, LabelSet, label_set};
use crate::histogram_eval::float_histogram::FloatHistogram;
use crate::instant_eval::select_instants;
use crate::map::{MathOp, drop_metric_name, map_math};
use crate::native::batch::EvaluatedChunk;
use crate::native::metrics::{self, NodeMetrics, NodePath};
use crate::native::plan::{BoundInstant, BoundWindow, NativeExpr, NativeTree};
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

    /// Whether this chunk closes its series.
    fn is_last(&self) -> bool {
        match self {
            Self::Raw(chunk) => chunk.is_last,
            Self::Evaluated(chunk) => chunk.is_last,
        }
    }

    /// The bytes this chunk holds, counting each shared allocation it references that `charged` has
    /// not met yet.
    fn bytes(&self, charged: &mut ChargedAllocations) -> usize {
        match self {
            Self::Raw(chunk) => raw_bytes(chunk, charged),
            Self::Evaluated(chunk) => evaluated_bytes(chunk, charged),
        }
    }

    /// The bytes this chunk occupies on its own, which no other chunk holds a reference to.
    ///
    /// It is the struct term of [`Self::bytes`] and nothing else: everything beside it is a buffer or
    /// a histogram table, which a sibling chunk of the series can share and which the queue counts
    /// per allocation rather than per chunk.
    fn own_bytes(&self) -> usize {
        match self {
            Self::Raw(_) => std::mem::size_of::<RawSeriesChunk>(),
            Self::Evaluated(_) => std::mem::size_of::<EvaluatedChunk>(),
        }
    }
}

/// The shared allocations one accounting pass has already counted.
///
/// A chunk holds very little of what it costs: a field of native histograms holds no floats at all,
/// only indices into an immutable `Arc<HistogramBatch>` table, and both that table and the buffers
/// behind the field's Arrow arrays are batch-wide allocations that a chunk can hold a slice of. Two
/// chunks of one series can share one table by cloning the `Arc`, and both can hold sibling slices
/// of one buffer, so a pass counts an allocation the first time it meets it. Charging it per
/// referencing chunk instead would make the reservation grow with the number of chunks a node holds
/// rather than with the data in them -- the figure the pool has to rank.
///
/// One pass walks one chunk, so the list stays as short as the allocations that chunk met and is
/// dropped with the pass; nothing outlives the call it was made for. The queue of one node is not
/// recounted by such a pass: [`QueuedCharge`] folds a chunk in as it enters and out as it leaves,
/// carrying the same dedupe across the queue.
#[derive(Default)]
struct ChargedAllocations {
    /// The address of every histogram table this pass has counted, with the bytes it contributes.
    tables: Vec<(*const HistogramBatch, usize)>,
    /// The address of every Arrow buffer this pass has counted, with the bytes it contributes.
    buffers: Vec<(*const u8, usize)>,
}

impl ChargedAllocations {
    /// The bytes `table` holds, or zero when this pass has already counted it.
    fn table(&mut self, table: &Arc<HistogramBatch>) -> usize {
        let address = Arc::as_ptr(table);
        if self.tables.iter().any(|(counted, _)| *counted == address) {
            return 0;
        }
        // The batch struct holds the struct array and the column arrays beside it, which are views
        // of that array's children: the array reports the buffers of all of them, once.
        let bytes = std::mem::size_of::<HistogramBatch>()
            .saturating_add(table.as_arrow().get_array_memory_size());
        self.tables.push((address, bytes));
        bytes
    }

    /// The bytes the allocation `buffer` is a slice of, or zero when this pass has already counted
    /// it.
    ///
    /// The capacity is the whole allocation, which is what a slice keeps alive: a key of a few
    /// bytes sliced out of a decoded dictionary values buffer pins that buffer. A buffer that owns
    /// no memory of its own reports no capacity, and then the bytes it shows are all there is.
    fn buffer(&mut self, buffer: &Buffer) -> usize {
        let address = buffer.data_ptr().as_ptr().cast_const();
        if self.buffers.iter().any(|(counted, _)| *counted == address) {
            return 0;
        }
        let bytes = buffer.capacity().max(buffer.len());
        self.buffers.push((address, bytes));
        bytes
    }

    /// The bytes the buffers of one Arrow array hold.
    fn array(&mut self, array: &dyn Array) -> usize {
        let data = array.to_data();
        let values = data.buffers().iter().fold(0usize, |total, buffer| {
            total.saturating_add(self.buffer(buffer))
        });
        // The validity bitmap is not one of `buffers`: a field of a series with a missing sample
        // carries one, and it is an allocation like any other.
        let nulls = data
            .nulls()
            .map_or(0, |nulls| self.buffer(nulls.inner().inner()));
        values.saturating_add(nulls)
    }
}

/// The bytes the chunks one node queues hold, kept as chunks enter and leave the queue.
///
/// A node is asked for its queue's figure on every enqueue and every pop, and the queue can hold a
/// whole series at once, for a caller that pushes a series before pulling it. Recounting the queue
/// with a [`ChargedAllocations`] pass each time is one pass over every queued chunk per operation,
/// which is quadratic in the chunks that caller queues. What is kept here instead is a count per
/// shared allocation, with the bytes that allocation contributes and the running total they add up
/// to, so an operation walks only the allocations of the one chunk it names.
///
/// The dedupe is the one [`ChargedAllocations`] makes inside a chunk, carried across the queue. It
/// is per queued chunk that references an allocation, not per field or per segment that does: the
/// chunk is walked with one pass, so a chunk whose two fields index one table is one reference to
/// it, and the walk a pop makes of the same chunk meets the same references. A chunk is therefore
/// charged its own struct once, and an allocation once for as long as at least one queued chunk
/// references it -- the figure a whole-queue pass would give, without the pass.
#[derive(Default)]
struct QueuedCharge {
    /// Every histogram table the queued chunks reference, by address, with how many queued chunks
    /// reference it and the bytes it contributes to the total while at least one does.
    tables: HashMap<usize, Counted>,
    /// Every Arrow buffer the queued chunks reference, likewise: two chunks of one series hold
    /// sibling slices of one buffer, and the address of the allocation is what makes them one.
    buffers: HashMap<usize, Counted>,
    /// What the queued chunks hold: their own structs, and the allocations above added once each.
    /// This is what the node reserves for its queue.
    bytes: usize,
    /// How many chunks the accounting has walked, test-only.
    ///
    /// An enqueue or a pop walks the chunk it names, so this grows by one per operation whatever
    /// the queue holds. A figure recounted from the whole queue would grow with the depth of the
    /// queue, which is what a test refuses.
    #[cfg(test)]
    walked: usize,
}

/// How many queued chunks reference one shared allocation, and the bytes it contributes while at
/// least one of them does.
#[derive(Clone, Copy)]
struct Counted {
    chunks: usize,
    bytes: usize,
}

impl QueuedCharge {
    /// Counts the allocations `chunk` references, as that chunk enters the queue, and returns the
    /// queue's new total.
    fn enqueue(&mut self, chunk: &NativeChunk) -> usize {
        let charged = self.walk(chunk);
        self.add(chunk.own_bytes(), &charged)
    }

    /// Counts the allocations a raw chunk references as a node buffers it, and returns the new
    /// total.
    ///
    /// It is the same fold [`Self::enqueue`] makes, for a node that buffers the chunks of one series
    /// itself rather than queueing the chunks it is handed: its chunks can share a histogram table
    /// or an Arrow buffer just as queued ones can, and counting them chunk by chunk would charge one
    /// allocation once per chunk of the series instead of once for the allocation.
    fn enqueue_raw(&mut self, chunk: &RawSeriesChunk) -> usize {
        let mut charged = ChargedAllocations::default();
        let _ = raw_bytes(chunk, &mut charged);
        // The struct a chunk is held by is its own; only what it points at can be shared.
        self.add(std::mem::size_of::<RawSeriesChunk>(), &charged)
    }

    /// Adds one chunk's own struct and the allocations it references to the total.
    fn add(&mut self, own_bytes: usize, charged: &ChargedAllocations) -> usize {
        self.bytes = self.bytes.saturating_add(own_bytes);
        for &(table, bytes) in &charged.tables {
            count_reference(&mut self.tables, &mut self.bytes, table as usize, bytes);
        }
        for &(buffer, bytes) in &charged.buffers {
            count_reference(&mut self.buffers, &mut self.bytes, buffer as usize, bytes);
        }
        self.bytes
    }

    /// Gives the allocations `chunk` references back, as that chunk leaves the queue, and returns
    /// the queue's new total.
    ///
    /// An allocation another queued chunk still references stays charged: only the last reference
    /// takes its bytes out, which is what keeps a shared table counted once for as long as the
    /// queue holds it.
    fn pop(&mut self, chunk: &NativeChunk) -> usize {
        let charged = self.walk(chunk);
        self.bytes = self.bytes.saturating_sub(chunk.own_bytes());
        for &(table, _) in &charged.tables {
            drop_reference(&mut self.tables, &mut self.bytes, table as usize);
        }
        for &(buffer, _) in &charged.buffers {
            drop_reference(&mut self.buffers, &mut self.bytes, buffer as usize);
        }
        self.bytes
    }

    /// The shared allocations one chunk references, each met once.
    ///
    /// The pass is over `chunk` alone: what the queue holds is folded in from the allocations the
    /// pass met rather than recounted by walking the queue. The same chunk walked again meets the
    /// same addresses, which is what makes the subtraction on the way out match the addition on the
    /// way in.
    fn walk(&mut self, chunk: &NativeChunk) -> ChargedAllocations {
        let mut charged = ChargedAllocations::default();
        // The figure this sums is the chunk's own; the queue needs the allocations behind it, which
        // `charged` now holds.
        let _ = chunk.bytes(&mut charged);
        #[cfg(test)]
        {
            self.walked += 1;
        }
        charged
    }
}

/// Counts one more reference to the allocation at `address`, which contributes `bytes`, and adds
/// those bytes to `total` when the queue did not already hold it.
fn count_reference(
    counts: &mut HashMap<usize, Counted>,
    total: &mut usize,
    address: usize,
    bytes: usize,
) {
    let entry = counts
        .entry(address)
        .or_insert(Counted { chunks: 0, bytes });
    if entry.chunks == 0 {
        // The first chunk to meet the allocation is what makes the queue pay for it: a chunk
        // sharing it afterwards adds a reference, not bytes.
        *total = total.saturating_add(entry.bytes);
    }
    entry.chunks += 1;
}

/// Gives one reference to the allocation at `address` back, and takes its bytes out of `total` when
/// it was the last one, so a queue that emptied leaves no entry and no bytes behind.
fn drop_reference(counts: &mut HashMap<usize, Counted>, total: &mut usize, address: usize) {
    let last = match counts.get_mut(&address) {
        // The chunk entered the queue before it left it, so one leaving cannot reach zero while
        // another chunk of the queue still references the allocation.
        Some(entry) if entry.chunks > 1 => {
            entry.chunks -= 1;
            false
        }
        Some(_) => true,
        // Every chunk that leaves was queued, so its allocations were counted on the way in. An
        // address missing here is exactly the asymmetry that would leak or undercharge silently.
        None => {
            debug_assert!(false, "the queue released an allocation it never counted");
            false
        }
    };
    if last && let Some(entry) = counts.remove(&address) {
        *total = total.saturating_sub(entry.bytes);
    }
}

/// The bytes a raw chunk holds: its identity, its timestamps and the Arrow buffers behind its
/// fields.
///
/// The figure is what the chunk keeps alive rather than one float per field and row: a histogram
/// field holds no floats at all, only indices into the table its samples live in.
fn raw_bytes(chunk: &RawSeriesChunk, charged: &mut ChargedAllocations) -> usize {
    std::mem::size_of::<RawSeriesChunk>()
        .saturating_add(charged.buffer(&chunk.primary_key))
        .saturating_add(charged.buffer(chunk.time.as_buffer().inner()))
        .saturating_add(fields_bytes(&chunk.fields, charged))
}

/// The bytes an evaluated chunk holds: its struct and the Arrow buffers behind its fields.
///
/// The figure is what the chunk keeps alive rather than one float per field and decided position:
/// a histogram field holds no floats at all, only indices into the table its samples live in. The
/// grid itself is shared by every chunk addressed on it, so it is not counted per chunk.
fn evaluated_bytes(chunk: &EvaluatedChunk, charged: &mut ChargedAllocations) -> usize {
    std::mem::size_of::<EvaluatedChunk>()
        .saturating_add(fields_bytes(&chunk.series.fields, charged))
}

/// The bytes the fields of a chunk hold: the sample buffers of every segment, and the histogram
/// tables those segments reference.
fn fields_bytes(fields: &SeriesFields, charged: &mut ChargedAllocations) -> usize {
    fields
        .iter()
        .map(|field| {
            field
                .segments()
                .iter()
                .map(|segment| segment_bytes(segment, charged))
                .fold(0usize, usize::saturating_add)
        })
        .fold(0usize, usize::saturating_add)
}

/// The bytes one dense segment of samples holds.
fn segment_bytes(segment: &Samples, charged: &mut ChargedAllocations) -> usize {
    match segment.dense() {
        Some(DenseSamplesRef::Float(values)) => charged.array(values),
        Some(DenseSamplesRef::Histogram(histograms)) => charged
            .array(histograms.indices())
            .saturating_add(charged.table(histograms.batch())),
        Some(DenseSamplesRef::Mixed(mixed)) => charged
            .array(mixed.floats())
            .saturating_add(charged.array(mixed.histograms().indices()))
            .saturating_add(charged.table(mixed.histograms().batch())),
        // `segments` hands out dense segments only, so a nested chunk never reaches here; one that
        // somehow did is charged for the descriptor alone rather than for buffers it nests.
        None => 0,
    }
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

/// The operation a node binds, kept aside until the whole tree has been visited.
///
/// A window's evaluator needs its input's schema, and the input is visited after the window, so
/// the binding is collected first and the run-time state is built from the complete set.
enum Seed {
    Input,
    Instant(BoundInstant),
    Window(BoundWindow),
    Map(Arc<[MathOp]>),
}

/// One visited node, before its input's position is resolved.
struct PendingNode {
    tag: NodeTag,
    path: NodePath,
    metrics: Arc<NodeMetrics>,
    /// What this node produces, when it produces samples.
    output_schema: Option<Arc<SeriesSchema>>,
    seed: Seed,
}

/// One node of the flattened tree.
#[derive(Clone, Copy, Debug)]
struct NodeSpec {
    tag: NodeTag,
    /// The position of this node's input, resolved from the tree's own positions.
    input: Option<usize>,
}

/// What every node holds: its metric handles and the bytes it reserves.
///
/// The reservation is the pool's, so a node that buffers input competes with every other operator
/// in the query; the metrics report the same figure so a slow node can be told apart from a wide
/// one.
struct NodeBuffer {
    metrics: Arc<NodeMetrics>,
    reservation: MemoryReservation,
    /// The bytes `reservation` holds, mirrored so a chunk leaving returns exactly them.
    held: usize,
}

impl NodeBuffer {
    /// Registers a reservation for the node at `path` with `pool`.
    fn new(path: &NodePath, metrics: Arc<NodeMetrics>, pool: &Arc<dyn MemoryPool>) -> Self {
        let consumer = MemoryConsumer::new(format!("native.{}", path.as_str()));
        Self {
            metrics,
            reservation: consumer.register(pool),
            held: 0,
        }
    }

    /// Reserves the bytes of a chunk this node starts holding.
    fn hold(&mut self, bytes: usize) -> Result<()> {
        self.reservation.try_grow(bytes)?;
        self.held += bytes;
        self.metrics.record_reserved_bytes(self.held);
        Ok(())
    }

    /// Reserves exactly `bytes`, which is what this node holds from now on.
    ///
    /// A node that consumes its input chunk by chunk what it holds afterwards is not the sum of
    /// what it consumed, so the reservation is set from the state that is left rather than grown
    /// per chunk.
    fn hold_exactly(&mut self, bytes: usize) -> Result<()> {
        self.reservation.try_resize(bytes)?;
        self.held = bytes;
        self.metrics.record_reserved_bytes(bytes);
        Ok(())
    }

    /// Returns every byte this node holds to the pool.
    fn release(&mut self) {
        self.held = 0;
        self.reservation.free();
        self.metrics.record_reserved_bytes(0);
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
/// sharing no sample merge cleanly, as they do in [`crate::grid_series::merge_same_labelset`].
///
/// The table is one node's own: a node knows what it has handed out itself and sees nothing of what
/// the node below kept, so a label set costs its bytes again in every node that keeps a table, and
/// what is deduplicated here is one node's history rather than a plan's.
#[derive(Default)]
struct Emitted {
    sets: HashMap<LabelSet, History>,
    /// The bytes the entries in `sets` hold, kept as a running total.
    ///
    /// A node records every label set it has ever handed out and must charge for all of them, so
    /// the figure is read after every output. Recomputing it there walks every entry recorded so
    /// far, which is quadratic over a series that keeps producing label sets; folding one entry's
    /// change in as it happens keeps the read constant.
    entries: usize,
}

/// What one node has handed out under one output label set.
#[derive(Default)]
struct History {
    /// The occupied runs of each field, in position order and disjoint.
    fields: Vec<Vec<Run>>,
    /// Whether the series on this label set closed. A second series cannot close it again.
    closed: bool,
    /// The heap this entry owns: the runs its per-field vectors hold, kept current by
    /// [`Emitted::occupy`]. The header and the slot it sits in are charged by the table's capacity.
    bytes: usize,
}

/// The bytes a label set holds: each label's name and value with the pair that refers to them.
fn label_bytes(labels: &LabelSet) -> usize {
    labels
        .iter()
        .map(|(name, value)| std::mem::size_of::<(Arc<str>, Arc<str>)>() + name.len() + value.len())
        .sum()
}

/// The bytes a history holds: the per-field vectors and the runs in them.
///
/// This is what the entry itself allocates, not the slot it is filed under: the `History` lives in
/// the map's slot, and that slot is what [`Emitted::bytes`] charges with the table's capacity. A
/// `size_of::<History>()` term here billed every entry one header for a value it does not own,
/// once in this figure and again in the slot term beside it.
fn history_bytes(history: &History) -> usize {
    history.fields.capacity() * std::mem::size_of::<Vec<Run>>()
        + history
            .fields
            .iter()
            .map(|runs| runs.capacity() * std::mem::size_of::<Run>())
            .sum::<usize>()
}

impl Emitted {
    /// Records what `chunk` occupies, rejecting it against what this node already handed out.
    ///
    /// A label set that has closed cannot be reached by a second series: the node would hand on two
    /// chunks that its consumer cannot tell apart, and the merge the caller wants spans the first
    /// series, whose state the evaluator above has already closed. Such a pair does merge in
    /// Prometheus, so this is a limitation of the streaming evaluator rather than a collision; it is
    /// refused instead of being answered wrongly (see [`closed_label_set`]). A chunk of a set still
    /// open is refused only where it holds a value at a position that same field already holds one
    /// at, which is the case no merge can resolve.
    fn occupy(&mut self, path: &NodePath, chunk: &EvaluatedChunk) -> Result<()> {
        let labels = label_set(&chunk.series.metric);
        let occupied: Vec<Vec<Run>> = chunk
            .series
            .fields
            .iter()
            .enumerate()
            .map(|(index, field)| occupied_runs(path, index, field, &chunk.series.time))
            .collect::<Result<_>>()?;
        // A chunk holding no value records nothing. If the label set is unknown or already closed,
        // the series this chunk belongs to contributed no sample at all -- either it is the first
        // such series, or a later one that also answers empty -- so there is nothing to report,
        // nothing to merge and nothing to refuse. A label set still open is the other case: the
        // chunk closes the series that *did* produce the values, so it falls through to close it.
        let empty = occupied.iter().all(Vec::is_empty);
        if empty && self.sets.get(&labels).is_none_or(|history| history.closed) {
            return Ok(());
        }
        if let Some(history) = self.sets.get(&labels) {
            if history.closed {
                return Err(closed_label_set(path, &labels));
            }
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
        let fields = chunk.series.fields.len();
        // The change is folded in here rather than recomputed when the figure is read: a node
        // keeps every label set it has ever handed out, so a fresh walk after each output costs a
        // pass over the whole table for a table that only ever grows.
        let history = match self.sets.entry(labels) {
            Entry::Occupied(entry) => entry.into_mut(),
            Entry::Vacant(entry) => {
                self.entries += label_bytes(entry.key());
                entry.insert(History::default())
            }
        };
        let before = history.bytes;
        if history.fields.len() < fields {
            history.fields.resize_with(fields, Vec::new);
        }
        for (index, runs) in occupied.into_iter().enumerate() {
            absorb(&mut history.fields[index], &runs);
        }
        if chunk.is_last {
            history.closed = true;
            // No later chunk reads these runs: a closed label set refuses every series that would
            // use them. Keeping them charges the pool for a series that has already been answered.
            history.fields = Vec::new();
        }
        let after = history_bytes(history);
        history.bytes = after;
        self.entries = self.entries - before + after;
        Ok(())
    }

    /// An estimate of the bytes this table holds, which the node reserves for.
    ///
    /// It counts each label's name and value bytes with a tuple per label, the runs a history keeps
    /// in its per-field vectors, and the table's capacity at one label-set slot plus one history
    /// slot per entry -- which is where a `History` header itself is charged, once, for the entry
    /// that lives in it. It leaves out the map's buckets and the `Arc` headers of the strings it
    /// shares, so it is a lower bound rather than a measurement.
    ///
    /// The per-entry part is a running total, so reading this after every output does not walk the
    /// table: only the entry that changed and the table's own capacity are read here.
    fn bytes(&self) -> usize {
        self.entries
            + self.sets.capacity()
                * (std::mem::size_of::<LabelSet>() + std::mem::size_of::<History>())
    }

    /// The same figure by walking every entry, used by a test to keep the running total honest.
    #[cfg(test)]
    fn bytes_recomputed(&self) -> usize {
        self.sets
            .iter()
            .map(|(labels, history)| label_bytes(labels) + history_bytes(history))
            .sum::<usize>()
            + self.sets.capacity()
                * (std::mem::size_of::<LabelSet>() + std::mem::size_of::<History>())
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

/// What a window node holds.
struct WindowRuntime {
    /// This window's own evaluator, with its own input and output grid; `None` once closed.
    evaluator: Option<RangeEvaluator>,
}

impl WindowRuntime {
    /// The run-time state of the window `op`, reading the samples of `input`.
    fn try_new(input: &SeriesSchema, op: &BoundWindow) -> Result<Self> {
        // One evaluator per window, with the grids the node was bound to: the output grid is the
        // node's own grid, which is also the grid its chunks are addressed on.
        let evaluator =
            RangeEvaluator::try_new(input, Arc::clone(&op.output_grid), op.window, op.function)?;
        Ok(Self {
            evaluator: Some(evaluator),
        })
    }

    /// The bytes this window's charge accounts for, as its evaluator's storage stands now.
    ///
    /// Between pushes this is the accounted retained size: the sample buffers the evaluator keeps
    /// per field, by capacity, with the payload of every native histogram they retain, the row a
    /// push fills with the histograms it boxes, the pending batch's inner vectors, and the identity
    /// payload of the series still open -- the allocation behind its primary key, not the bytes the
    /// key shows. It is a lower bound on what is held, not a measurement of it.
    ///
    /// The kernel's scratch is left out, being a constant factor of the window samples already
    /// counted. The `Emitted` table's own map overhead and the `Arc` headers of the strings it
    /// shares are charged for beside this figure rather than inside it. Of the pending batch only
    /// the inner vectors are counted, not the outer `spans` and `columns` that hold them.
    ///
    /// During a push the evaluator also holds the incoming chunk, which the caller charges while
    /// the call runs; vector growth during a push is not reserved at all, and is not bounded by one
    /// doubling either, since a bulk append can allocate more than twice the previous capacity and
    /// several vectors can grow at once. So this is the storage's current size, not a peak over a
    /// push.
    ///
    /// The evaluator is the only one that knows: it keeps samples per field, each field with its
    /// own timestamps, values, reset index and native histograms, and it drops them by its own
    /// rule. Mirroring that rule here would be a second copy of it, free to drift from the first.
    fn retained_bytes(&self) -> usize {
        self.evaluator
            .as_ref()
            .map_or(0, RangeEvaluator::retained_bytes)
    }

    /// Feeds one input chunk to the evaluator and returns the chunk it yields, if any.
    fn push(&mut self, path: &NodePath, chunk: &NativeChunk) -> Result<Option<EvaluatedChunk>> {
        let evaluator = self.evaluator.as_mut().ok_or_else(|| closed(path))?;
        match chunk {
            // Raw samples are read window by window by the evaluator itself.
            NativeChunk::Raw(chunk) => evaluator.push(chunk),
            // An evaluated input is read as computed samples: the evaluator carries the window
            // state of one series across its chunks and closes it on the chunk that says so.
            // Handing each chunk over as it arrives is what lets this window report a decision
            // before its input series has ended; buffering the chunks into a whole series first
            // would hold the input's bytes and delay every result to the end of the series.
            NativeChunk::Evaluated(chunk) => evaluator.push_series(&chunk.series, chunk.is_last),
        }
    }

    /// Closes this window, reporting an input that ended mid-series.
    ///
    /// The evaluator is what knows whether a series is still open, for evaluated input as much as
    /// for raw: a chunk that did not say `is_last` leaves it holding the series.
    fn finish(&mut self) -> Result<()> {
        match self.evaluator.take() {
            Some(evaluator) => evaluator.finish(),
            None => Ok(()),
        }
    }
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
    /// What `chunks` hold, folded in as chunks arrive and given back when the series is gathered.
    held: QueuedCharge,
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
            held: QueuedCharge::default(),
        })
    }

    /// The bytes the buffered chunks of the open series hold.
    fn buffered_bytes(&self) -> usize {
        self.held.bytes
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
        self.held.enqueue_raw(&chunk);
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
        self.held = QueuedCharge::default();
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
                    *slot = grid_sample(sample)?;
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

    /// Reports an input that ended inside a series this node was gathering.
    fn finish(&self, path: &NodePath) -> Result<()> {
        if self.chunks.is_empty() {
            Ok(())
        } else {
            Err(unfinished_series(path))
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

/// The value a grid series holds for `sample`, or `None` when the sample is missing.
///
/// It is the mapping every kernel applies when it turns a source sample into a grid value: a float
/// stays the float it is -- a `NaN` that is not the stale marker included -- a native histogram is
/// read out of the table it indexes, and a missing sample holds no value.
fn grid_sample(sample: SampleRef<'_>) -> Result<Option<GridSample>> {
    Ok(match sample {
        SampleRef::Missing => None,
        SampleRef::Float(value) => Some(GridSample::Float(value)),
        SampleRef::Histogram(view) => Some(GridSample::Histogram(Box::new(
            FloatHistogram::try_from_view(&view)?,
        ))),
    })
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
    /// Source chunks queued by the caller, for an input node.
    queued: VecDeque<NativeChunk>,
    /// What `queued` holds, folded in as chunks enter and out as they leave.
    queued_charge: QueuedCharge,
    /// What an instant node holds.
    instant: Option<InstantRuntime>,
    /// What a window node holds.
    window: Option<WindowRuntime>,
    /// What a map node applies.
    map: Option<MapRuntime>,
    /// What this node has already handed out under each output label set, for a node that rewrites
    /// labels: a window drops the metric name and the empty-valued labels, a map drops the name.
    emitted: Option<Emitted>,
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
    /// One flag is enough under the `Contiguous` and `KeyThenTime` orderings, which keep the chunks
    /// of a series together: only the series last handed over can be open, and one that closed
    /// cannot resume. An `Interleaved` input does reach a tree with no window above it -- that
    /// ordering is refused only where a window reads a series at a time -- and there an earlier
    /// series left open behind another one's chunk is not reported. The flag under-reports such a
    /// stream; it never over-reports one.
    open_source_series: bool,
}

impl NodeRuntime {
    /// Queues a source chunk, holding the bytes it needs until it leaves.
    ///
    /// What this node holds is its queue and nothing else, so the reservation is the queue's
    /// running total: a chunk of a series can share its histogram table with a chunk of the same
    /// series beside it, and counting the queue chunk by chunk would charge that one table once per
    /// chunk instead of once for the allocation.
    ///
    /// The total is kept incrementally rather than recounted from the queue: recomputing it here
    /// costs a pass over every queued chunk per chunk queued, so a caller that queues a whole series
    /// before pulling it pays quadratically in the chunks it holds. What this costs instead is one
    /// pass over the chunk entering, and the same on the way out.
    fn enqueue(&mut self, chunk: NativeChunk) -> Result<()> {
        let positions = chunk.positions();
        let bytes = self.queued_charge.enqueue(&chunk);
        self.queued.push_back(chunk);
        self.buffer.hold_exactly(bytes)?;
        self.buffer.metrics.record_input(1, positions);
        Ok(())
    }

    /// The bytes the chunks this node still queues hold, counted as one accounting pass.
    ///
    /// Test-only: what the node reserves for its queue is the running total of [`QueuedCharge`], so
    /// this walk is the figure that total is checked against rather than the figure it is read from.
    #[cfg(test)]
    fn queued_bytes_recomputed(&self) -> usize {
        let mut charged = ChargedAllocations::default();
        self.queued
            .iter()
            .map(|chunk| chunk.bytes(&mut charged))
            .fold(0usize, usize::saturating_add)
    }

    /// Hands over the next queued source chunk, if there is one.
    ///
    /// The handover is what [`Self::open_source_series`] follows: after this call the flag says
    /// whether the series the consumer just received a chunk of is still open, which is what closing
    /// the execution reads to tell a truncated input from one that ended cleanly.
    fn pop_source(&mut self) -> Result<Option<NativeChunk>> {
        let Some(chunk) = self.queued.pop_front() else {
            return Ok(None);
        };
        // The leaving chunk gives back only what it alone held: an allocation a chunk still in the
        // queue references stays charged, and no chunk left behind is walked to find that out.
        let bytes = self.queued_charge.pop(&chunk);
        let metrics = Arc::clone(&self.buffer.metrics);
        {
            // An input node's own work is handing the chunk over, so its self time covers that and
            // nothing its producer did.
            let _timer = metrics.timer();
            // The chunk leaves this node, so the pool no longer has to hold it.
            self.buffer.hold_exactly(bytes)?;
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
        let (bytes, positions, closing) = (
            evaluated_bytes(&chunk, &mut ChargedAllocations::default()),
            chunk.series.time.len(),
            chunk.is_last,
        );
        self.buffer.hold(bytes)?;
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
            // What it handed out is what the label sets of this node now hold.
            self.track(path, output)?;
        }
        let bytes = self.emitted_bytes();
        self.buffer.hold_exactly(bytes)?;
        Ok(output)
    }

    /// Feeds one chunk to this node's window.
    fn window(&mut self, path: &NodePath, chunk: NativeChunk) -> Result<Option<EvaluatedChunk>> {
        let (bytes, positions, closing) = (
            chunk.bytes(&mut ChargedAllocations::default()),
            chunk.positions(),
            chunk.is_last(),
        );
        // The chunk is held for as long as the evaluator consumes it.
        self.buffer.hold(bytes)?;
        let metrics = Arc::clone(&self.buffer.metrics);
        let outcome = {
            let _timer = metrics.timer();
            match self.window.as_mut() {
                Some(window) => window.push(path, &chunk),
                None => Err(DataFusionError::Execution(format!(
                    "the node at {path} holds no window"
                ))),
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
            // What it handed out is what the label sets of this node now hold.
            self.track(path, output)?;
        }
        // The chunk has been consumed: what this node still holds is what the evaluator kept of it
        // and the label sets it has already handed out. Between pushes the charge for both is the
        // accounted size of that storage, a lower bound on what is held rather than a measurement:
        // the evaluator's buffers by capacity -- with the payload of the histograms they retain and
        // of the ones the row boxes -- and every label set handed out, leaving out the kernel's
        // scratch, this table's map overhead with the `Arc` headers of its strings and the outer
        // vectors of the pending batch. During one push the evaluator additionally holds the
        // incoming chunk, charged by `hold` above for as long as the call runs, and vector growth
        // is not reserved and not bounded by one doubling — a bulk append can allocate more than
        // twice the previous capacity, and several vectors can grow at once — so this figure is
        // what the storage stands at, not a peak over the push.
        //
        // The reason the figure must not be per-chunk: charging every chunk consumed makes the
        // reservation grow with the chunk count, so a fixed window over a long series reads as an
        // allocation failure even though the window holds no more samples.
        //
        // Both terms are charged whether or not this chunk produced one: a chunk that yields
        // nothing leaves the evaluator's samples and every earlier series' history exactly where
        // they were, so dropping the charge here would release memory this node still holds.
        let retained = self
            .window
            .as_ref()
            .map_or(0, WindowRuntime::retained_bytes);
        let emitted = self.emitted_bytes();
        self.buffer.hold_exactly(retained.saturating_add(emitted))?;
        Ok(output)
    }

    /// Buffers one chunk for this node's instant selection.
    ///
    /// `None` means the series this chunk belongs to has not ended: the selection reads one whole
    /// axis, so it decides nothing until the chunk carrying `is_last` arrives.
    fn instant(&mut self, path: &NodePath, chunk: NativeChunk) -> Result<Option<EvaluatedChunk>> {
        let (bytes, positions, closing) = (
            chunk.bytes(&mut ChargedAllocations::default()),
            chunk.positions(),
            chunk.is_last(),
        );
        // The chunk is held for as long as it enters the buffer.
        self.buffer.hold(bytes)?;
        let metrics = Arc::clone(&self.buffer.metrics);
        let outcome = {
            // The timer covers this node's own work only: it is dropped before the driver polls
            // the input again.
            let _timer = metrics.timer();
            match (self.instant.as_mut(), chunk) {
                (Some(instant), NativeChunk::Raw(chunk)) => instant.push(path, chunk),
                // Rejected when the state was built, since a selection reads raw samples; kept as
                // an error rather than a panic so an impossible tree cannot take the process down.
                (Some(_), NativeChunk::Evaluated(_)) => Err(DataFusionError::Execution(format!(
                    "the instant selection at {path} consumes raw samples, not an evaluated chunk"
                ))),
                (None, _) => Err(DataFusionError::Execution(format!(
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
        // An error leaves the reservation to the driver, which releases every node when it stops.
        let output = outcome?;
        if let Some(output) = &output {
            metrics.record_grid_points(output.series.time.len());
            metrics.record_output(1, valid_values(output));
        }
        // What this node still holds is the chunks of the series it is selecting: a whole series is
        // held until it ends, so the reservation is what those chunks hold rather than a figure
        // that grows with the chunk count of anything already answered.
        let buffered = self
            .instant
            .as_ref()
            .map_or(0, InstantRuntime::buffered_bytes);
        self.buffer.hold_exactly(buffered)?;
        Ok(output)
    }

    /// Records what this node hands out under its output label set.
    ///
    /// A node that rewrites labels is where two input series become indistinguishable, so this is
    /// where the second one is refused: the node above reads what this one hands over, and a window
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
        // handed out go with it.
        self.window = None;
        self.instant = None;
        self.emitted = None;
        // A node that holds no chunk holds no series either, so nothing it handed over is still
        // open for it: the flag is what closing the execution reads, and it must not survive the
        // state that could have kept a series alive.
        self.open_source_series = false;
        self.queued.clear();
        self.queued.shrink_to_fit();
        // The queue's counts go with the queue: they are what a later chunk would be folded onto,
        // and a chunk that is dropped is a chunk the pool no longer has to hold. The reservation is
        // returned below, once, for everything this node still held.
        self.queued_charge = QueuedCharge::default();
        self.buffer.release();
    }
}

/// The run-time state of one execution of a bound tree.
///
/// The state is keyed by tree position: the same positions [`metrics::walk`] derives, so a node's
/// state, its metrics and the name it is shown under cannot drift apart. Because none of it lives
/// on the tree, one compiled tree serves every execution that builds a state from it.
pub struct NativeExecutionState {
    /// The tree's structure, flattened in traversal order.
    nodes: Vec<NodeSpec>,
    /// The position of each node, in the same order.
    paths: Vec<NodePath>,
    /// The per-node state, in the same order.
    runtimes: Vec<NodeRuntime>,
    /// The index of every node, so a caller names a node the way the metrics do.
    by_path: HashMap<NodePath, usize>,
    /// The index of the root node.
    root: usize,
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
        // One traversal names and registers every node: the same walk `EXPLAIN` uses, so a node
        // cannot be shown at one position and registered at another.
        let mut pending: Vec<PendingNode> = Vec::new();
        let mut unsupported: Option<DataFusionError> = None;
        // The order the source streams its series in, and where it says so: a window evaluates one
        // series at a time, so it is only executable over an input that keeps each series
        // contiguous.
        let mut ordering: Option<(SeriesOrdering, NodePath)> = None;
        let mut first_window: Option<NodePath> = None;
        metrics::walk(tree.root(), &mut |node, path| {
            let handles = Arc::new(NodeMetrics::default());
            handles.register(path, metrics_set, partition);
            let plan = match &node.expr {
                NativeExpr::Input(bound) => {
                    ordering.get_or_insert_with(|| (bound.ordering, path.clone()));
                    Some((NodeTag::Input, Seed::Input))
                }
                NativeExpr::Window { op, .. } => {
                    first_window.get_or_insert_with(|| path.clone());
                    Some((NodeTag::Window, Seed::Window(op.clone())))
                }
                NativeExpr::Instant { op, .. } => {
                    Some((NodeTag::Instant, Seed::Instant(op.clone())))
                }
                NativeExpr::Map { op, .. } => Some((NodeTag::Map, Seed::Map(Arc::clone(&op.ops)))),
                // The kinds below are legal in a bound tree, and `validate` stays the authority on
                // that; this runtime simply does not execute them yet.
                NativeExpr::Scalar(_) => {
                    unsupported.get_or_insert_with(|| not_implemented("Scalar", path));
                    None
                }
                NativeExpr::Aggregate { .. } => {
                    unsupported.get_or_insert_with(|| not_implemented("Aggregate", path));
                    None
                }
                NativeExpr::Binary { .. } => {
                    unsupported.get_or_insert_with(|| not_implemented("Binary", path));
                    None
                }
            };
            if let Some((tag, seed)) = plan {
                pending.push(PendingNode {
                    tag,
                    path: path.clone(),
                    metrics: handles,
                    output_schema: node.output.samples().cloned(),
                    seed,
                });
            }
        });
        // Rejected before a single chunk is read: a tree the runtime cannot execute must not
        // start, because a partial answer would be one the caller never asked for.
        if let Some(error) = unsupported {
            return Err(error);
        }

        // A window feeds one series at a time to its evaluator, so it can only run over an input
        // that keeps each series contiguous. The tree stays free to say `Interleaved` -- the
        // compiler's job is not narrowed here -- but this runtime says so up front instead of
        // failing on the second series of a chunk stream it advertised a chunk at a time.
        if let Some(window) = &first_window
            && let Some((ordering, source)) = &ordering
        {
            match ordering {
                SeriesOrdering::Contiguous | SeriesOrdering::KeyThenTime => {}
                SeriesOrdering::Interleaved => {
                    return Err(unsupported_ordering(*ordering, window, source));
                }
            }
        }

        // A node's input is the node at the same position extended by the step the traversal took,
        // so the structure is read off the positions instead of a second traversal that could
        // disagree with the first.
        let mut by_path = HashMap::with_capacity(pending.len());
        for (index, node) in pending.iter().enumerate() {
            by_path.insert(node.path.clone(), index);
        }

        let mut nodes = Vec::with_capacity(pending.len());
        let mut paths = Vec::with_capacity(pending.len());
        let mut runtimes = Vec::with_capacity(pending.len());
        for (index, node) in pending.iter().enumerate() {
            let input = match node.tag {
                NodeTag::Input => None,
                NodeTag::Instant | NodeTag::Window | NodeTag::Map => {
                    let path = node.path.input();
                    Some(*by_path.get(&path).ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "the native tree has no node at the input of {}",
                            node.path
                        ))
                    })?)
                }
            };
            let buffer = NodeBuffer::new(&node.path, Arc::clone(&node.metrics), &pool);
            let mut runtime = NodeRuntime {
                buffer,
                queued: VecDeque::new(),
                queued_charge: QueuedCharge::default(),
                instant: None,
                window: None,
                map: None,
                emitted: None,
                open_source_series: false,
            };
            match &node.seed {
                Seed::Input => {}
                Seed::Instant(instant) => {
                    let child = input.ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "the instant selection at {} has no input",
                            node.path
                        ))
                    })?;
                    let input_schema = pending[child].output_schema.clone().ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "the instant selection at {} consumes samples, but its input produces \
                             none",
                            node.path
                        ))
                    })?;
                    runtime.instant = Some(InstantRuntime::try_new(&input_schema, instant)?);
                    // A selection keeps the labels of the series it reads, empty-valued ones
                    // included, so it is not one of the nodes that can turn two series into one
                    // label set and it keeps no table of what it handed out: the node that drops a
                    // label is the one that can collide, and it records its own.
                }
                Seed::Map(ops) => {
                    let schema = node.output_schema.clone().ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "the map node at {} does not produce samples",
                            node.path
                        ))
                    })?;
                    runtime.map = Some(MapRuntime {
                        ops: Arc::clone(ops),
                        schema,
                    });
                    // A map drops the metric name, so it is one of the nodes that can turn two
                    // series into one label set.
                    runtime.emitted = Some(Emitted::default());
                }
                Seed::Window(window) => {
                    let child = input.ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "the window at {} has no input",
                            node.path
                        ))
                    })?;
                    let input_schema = pending[child].output_schema.clone().ok_or_else(|| {
                        DataFusionError::Execution(format!(
                            "the window at {} consumes samples, but its input produces none",
                            node.path
                        ))
                    })?;
                    runtime.window = Some(WindowRuntime::try_new(&input_schema, window)?);
                    // A window drops the metric name and the empty-valued labels, so a `rate` over
                    // a series whose tag is empty and one where it is absent produces one label set
                    // from two series.
                    runtime.emitted = Some(Emitted::default());
                }
            }
            debug_assert_eq!(nodes.len(), index);
            nodes.push(NodeSpec {
                tag: node.tag,
                input,
            });
            paths.push(node.path.clone());
            runtimes.push(runtime);
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
        let root = *by_path.get(&NodePath::root()).ok_or_else(|| {
            DataFusionError::Execution("the native tree has no root node".to_string())
        })?;
        let source = nodes
            .iter()
            .position(|node| node.tag == NodeTag::Input)
            .ok_or_else(|| {
                DataFusionError::Execution("the native tree has no input node".to_string())
            })?;
        let source_schema = pending[source].output_schema.clone().ok_or_else(|| {
            DataFusionError::Execution(format!(
                "the input node at {} does not produce samples",
                pending[source].path
            ))
        })?;
        Ok(Self {
            nodes,
            paths,
            runtimes,
            by_path,
            root,
            source,
            source_is_evaluated: source_schema.eval_grid.is_some(),
        })
    }

    /// The metrics of the node at `path`.
    pub fn metrics(&self, path: &NodePath) -> Option<&Arc<NodeMetrics>> {
        let index = *self.by_path.get(path)?;
        Some(&self.runtimes[index].buffer.metrics)
    }

    /// The bytes this execution currently reserves from the pool.
    pub fn reserved_bytes(&self) -> usize {
        self.runtimes
            .iter()
            .map(|runtime| runtime.buffer.held)
            .sum()
    }

    /// The number of nodes this execution runs.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Whether the tree this state was built from has no nodes at all.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
    }

    /// What the node at `path` charges for: the bytes its window's evaluator retains and the bytes
    /// of the label sets it has handed out.
    ///
    /// Test-only: a window's reservation has to be exactly these two numbers, so a test compares
    /// the reservation against them rather than against a figure the runtime keeps on the side.
    #[cfg(test)]
    fn window_charge(&self, path: &NodePath) -> (usize, usize) {
        let index = *self
            .by_path
            .get(path)
            .expect("the path names a node of this tree");
        let runtime = &self.runtimes[index];
        (
            runtime
                .window
                .as_ref()
                .map_or(0, WindowRuntime::retained_bytes),
            runtime.emitted_bytes(),
        )
    }

    /// The bytes the label sets at `path` charge for, and the same figure walked in full.
    ///
    /// Test-only: the running total is only correct if folding one entry's change in agrees with
    /// recounting the table, so a test compares the two after the table has changed shape.
    #[cfg(test)]
    fn emitted_charge_pair(&self, path: &NodePath) -> (usize, usize) {
        let index = *self
            .by_path
            .get(path)
            .expect("the path names a node of this tree");
        self.runtimes[index]
            .emitted
            .as_ref()
            .map_or((0, 0), |emitted| {
                (emitted.bytes(), emitted.bytes_recomputed())
            })
    }

    /// The bytes the queue at `path` charges for, and the same figure recounted by walking it.
    ///
    /// Test-only: the queue's running total is only correct if folding one chunk in as it enters and
    /// out as it leaves agrees with one pass over what is left, so a test compares the two after the
    /// queue has changed shape.
    #[cfg(test)]
    fn queued_charge_pair(&self, path: &NodePath) -> (usize, usize) {
        let index = *self
            .by_path
            .get(path)
            .expect("the path names a node of this tree");
        let runtime = &self.runtimes[index];
        (
            runtime.queued_charge.bytes,
            runtime.queued_bytes_recomputed(),
        )
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
    /// have to be dropped, not only their accounting.
    #[cfg(test)]
    pub(crate) fn nodes_holding_instants(&self) -> usize {
        self.runtimes
            .iter()
            .filter(|runtime| runtime.instant.is_some())
            .count()
    }

    /// The bytes the buffered series at `path` charges for, and the same figure walked in full.
    ///
    /// Test-only: a selection holds a whole series, so what it reserves is the sum over the chunks
    /// it holds, with an allocation two chunks share counted once. The running total the node keeps
    /// is only correct if it agrees with one pass over the buffer.
    #[cfg(test)]
    fn instant_charge_pair(&self, path: &NodePath) -> (usize, usize) {
        let index = *self
            .by_path
            .get(path)
            .expect("the path names a node of this tree");
        let instant = self.runtimes[index]
            .instant
            .as_ref()
            .expect("the path names an instant selection");
        let mut charged = ChargedAllocations::default();
        let walked = instant
            .chunks
            .iter()
            .map(|chunk| raw_bytes(chunk, &mut charged))
            .fold(0usize, usize::saturating_add);
        (instant.buffered_bytes(), walked)
    }

    /// Closes every node, reporting an input that ended mid-series.
    ///
    /// A drain finds nothing left to emit: a window reports its results on the chunk that closes
    /// the series and a selection gathers the whole series on it, so what is left to do is to check
    /// that every series did close, and to return everything the execution reserved. A window's
    /// evaluator and a selection's buffer are what hold a series and so notice one that never
    /// ended; a tree with neither has nothing holding one, so there the input node answers the same
    /// question from [`NodeRuntime::open_source_series`], which follows what it handed over. The
    /// first error found is the one reported, and the nodes are walked from the root down, so a
    /// window or a selection above the input reports its own truncation as it always did.
    fn close(&mut self) -> Result<()> {
        let mut result = Ok(());
        for (path, runtime) in self.paths.iter().zip(self.runtimes.iter_mut()) {
            if let Some(window) = runtime.window.as_mut()
                && let Err(error) = window.finish()
                && result.is_ok()
            {
                result = Err(error);
            }
            if let Some(instant) = runtime.instant.as_ref()
                && let Err(error) = instant.finish(path)
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
    /// materialises nothing beyond the chunk in hand.
    pub fn next_chunk(&mut self) -> Result<Option<EvaluatedChunk>> {
        self.check_running()?;
        let root = self.state.root;
        let result = self.pull(root).and_then(|chunk| match chunk {
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

    /// Ends the input: drains what has settled, closes every node and releases every reservation.
    ///
    /// Every series must have closed by now, in every tree: a window's evaluator reports the series
    /// it is still holding, and a tree with no window has its input node report one it handed over
    /// without the chunk that ends it. An input that ends mid-series is an error here rather than a
    /// series the caller never sees close.
    pub fn finish(&mut self) -> Result<Vec<EvaluatedChunk>> {
        let mut drained = Vec::new();
        loop {
            match self.next_chunk() {
                Ok(Some(chunk)) => drained.push(chunk),
                Ok(None) => break,
                Err(error) => return Err(error),
            }
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

/// The error for an input that ended inside a series an instant selection was gathering.
fn unfinished_series(path: &NodePath) -> DataFusionError {
    DataFusionError::Execution(format!(
        "the input of the instant selection at {path} ended before the last chunk of a series"
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

/// The rejection of an input ordering this runtime cannot execute under a window.
///
/// It names the window that needs contiguity, the input that does not keep it, and the ordering
/// that was found, so a reader can tell a tree this runtime cannot run from one it cannot run
/// correctly.
fn unsupported_ordering(
    ordering: SeriesOrdering,
    window: &NodePath,
    input: &NodePath,
) -> DataFusionError {
    DataFusionError::NotImplemented(format!(
        "the native runtime does not execute windows over {ordering:?} input yet: the window at \
         {window} reads one series at a time, but the input at {input} is {ordering:?}"
    ))
}

/// The rejection of a second series onto a label set the node already closed.
///
/// This is a limitation of this streaming evaluator, not a PromQL collision. Two series whose
/// distinct label sets were rewritten into one do merge when they hold values in different places:
/// [`crate::grid_series::merge_same_labelset`] accepts exactly that, rejecting only two values of
/// one field at one position. What this runtime cannot do is merge across a series that has already
/// closed, because the evaluator closes its per-series state on the chunk carrying `is_last`:
/// nothing is left to merge the second series into, and the node hands chunks on as it produces
/// them, so it cannot hold the first series back until it knows whether a second one follows.
///
/// The case is therefore refused rather than answered wrongly: reporting the two as one series, or
/// the first series twice, would both be a claim this layer cannot stand behind.
fn closed_label_set(path: &NodePath, labels: &LabelSet) -> DataFusionError {
    DataFusionError::Execution(format!(
        "the node at {path} already closed the label set {labels:?}, and a second input series \
         produces it: answering would report one series twice, and merging the two would need \
         whole series"
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
         {run:?}. Merging them would need whole series, which this layer does not hold",
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
