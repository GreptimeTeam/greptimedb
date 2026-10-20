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
use datatypes::arrow::array::Float64Array;
use datatypes::series::SeriesOrdering;
use datatypes::series::raw::RawSeriesChunk;
use datatypes::series::samples::{SampleRef, Samples, Series, SeriesFields, SeriesSchema};
use datatypes::series::time::{GridRange, TimeAxis};

use crate::grid_series::{LabelSet, label_set};
use crate::map::{MathOp, drop_metric_name, map_math};
use crate::native::batch::EvaluatedChunk;
use crate::native::metrics::{self, NodeMetrics, NodePath};
use crate::native::plan::{BoundWindow, NativeExpr, NativeTree};
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

    /// An estimate of the bytes this chunk holds.
    fn bytes(&self) -> usize {
        match self {
            Self::Raw(chunk) => raw_bytes(chunk),
            Self::Evaluated(chunk) => evaluated_bytes(chunk),
        }
    }
}

/// An estimate of the bytes a raw chunk holds: its timestamps, one float per field and its
/// identity.
fn raw_bytes(chunk: &RawSeriesChunk) -> usize {
    std::mem::size_of::<RawSeriesChunk>()
        + chunk.primary_key.len()
        + chunk.time.len()
            * (std::mem::size_of::<i64>() + chunk.fields.len() * std::mem::size_of::<f64>())
}

/// An estimate of the bytes an evaluated chunk holds: one float per field and decided position.
///
/// The grid itself is shared by every chunk addressed on it, so it is not counted per chunk. The
/// figure only has to rank one chunk against another for the memory pool, which is what a
/// reservation is for.
fn evaluated_bytes(chunk: &EvaluatedChunk) -> usize {
    std::mem::size_of::<EvaluatedChunk>()
        + chunk.series.time.len() * chunk.series.fields.len() * std::mem::size_of::<f64>()
}

/// Which node kinds the runtime executes.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum NodeTag {
    /// Forwards the chunks it is given.
    Input,
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
    /// The bytes `reservation` holds, mirrored so a chunk leaving shrinks it exactly.
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

    /// Returns the bytes of a chunk this node no longer holds.
    fn shrink(&mut self, bytes: usize) -> Result<()> {
        let held = self.held.saturating_sub(bytes);
        self.reservation.try_resize(held)?;
        self.held = held;
        self.metrics.record_reserved_bytes(held);
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
    /// The bytes this entry's labels and runs hold, kept current by [`Emitted::occupy`].
    bytes: usize,
}

/// The bytes a label set holds: each label's name and value with the pair that refers to them.
fn label_bytes(labels: &LabelSet) -> usize {
    labels
        .iter()
        .map(|(name, value)| std::mem::size_of::<(Arc<str>, Arc<str>)>() + name.len() + value.len())
        .sum()
}

/// The bytes a history holds: the entry itself, the per-field vectors and the runs in them.
fn history_bytes(history: &History) -> usize {
    std::mem::size_of::<History>()
        + history.fields.capacity() * std::mem::size_of::<Vec<Run>>()
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
            .map(|field| occupied_runs(field, &chunk.series.time))
            .collect();
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
    /// It counts each label's name and value bytes with a tuple per label, a history with its runs,
    /// and the table's capacity at a label-set slot plus a history per entry. It leaves out the
    /// map's buckets and the `Arc` headers of the strings it shares, so it is a lower bound rather
    /// than a measurement.
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

/// The runs of positions holding a value in `field`, as the axis `time` places its rows.
///
/// A missing sample holds no value; a `NaN` and a native histogram do, as they do for a merge.
/// A hole between two rows is not a position either: the axis skips it, so a run open before the
/// hole ends where the hole begins rather than reaching across it. A chunk of several runs is
/// therefore recorded as the runs it holds, and a series that occupies the hole is not refused.
fn occupied_runs(field: &Samples, time: &TimeAxis) -> Vec<Run> {
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
        let present = !matches!(
            cursor.at(row).expect("the axis and the fields are aligned"),
            SampleRef::Missing
        );
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
    runs
}

/// Merges `incoming` into `stored`, keeping the result in position order and joining the runs that
/// touch.
///
/// Both sides are sorted and disjoint, so one pass merges them. Adding the runs one at a time and
/// rebuilding the whole array after each would cost the square of the runs a chunk yields: a chunk
/// of one hundred thousand positions alternating between values and nulls yields fifty thousand
/// runs, and re-merging after each is over a billion element moves for a single series.
fn absorb(stored: &mut Vec<Run>, incoming: &[Run]) {
    if incoming.is_empty() {
        return;
    }
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
    /// per field, by capacity, the row a push fills, the pending batch's inner vectors, and the
    /// identity payload of the series still open. It is a lower bound on what is held, not a
    /// measurement of it.
    ///
    /// The kernel's scratch is left out, being a constant factor of the window samples already
    /// counted. The `Emitted` table's own map overhead and the `Arc` headers of the strings it
    /// shares are charged for beside this figure rather than inside it. Of the pending batch only
    /// the inner vectors are counted, not the outer `spans` and `columns` that hold them. And a
    /// sliced buffer's backing can be larger than the bytes its primary key shows.
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

/// The per-node run-time state of a node.
struct NodeRuntime {
    buffer: NodeBuffer,
    /// Source chunks queued by the caller, for an input node.
    queued: VecDeque<NativeChunk>,
    /// What a window node holds.
    window: Option<WindowRuntime>,
    /// What a map node applies.
    map: Option<MapRuntime>,
    /// What this node has already handed out under each output label set, for a node that rewrites
    /// labels: a window drops the metric name and the empty-valued labels, a map drops the name.
    emitted: Option<Emitted>,
}

impl NodeRuntime {
    /// Queues a source chunk, holding the bytes it needs until it leaves.
    fn enqueue(&mut self, chunk: NativeChunk) -> Result<()> {
        let (bytes, positions) = (chunk.bytes(), chunk.positions());
        self.buffer.hold(bytes)?;
        self.buffer.metrics.record_input(1, positions);
        self.queued.push_back(chunk);
        Ok(())
    }

    /// Hands over the next queued source chunk, if there is one.
    fn pop_source(&mut self) -> Result<Option<NativeChunk>> {
        let Some(chunk) = self.queued.pop_front() else {
            return Ok(None);
        };
        let metrics = Arc::clone(&self.buffer.metrics);
        {
            // An input node's own work is handing the chunk over, so its self time covers that and
            // nothing its producer did.
            let _timer = metrics.timer();
            // The chunk leaves this node, so the pool no longer has to hold it.
            self.buffer.shrink(chunk.bytes())?;
        }
        metrics.record_output(1, chunk.positions());
        // An evaluated source is already addressed on its grid, so its decided positions are grid
        // points; a raw source carries timestamps, which are not positions on any grid.
        if let NativeChunk::Evaluated(chunk) = &chunk {
            metrics.record_grid_points(chunk.series.time.len());
        }
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
            evaluated_bytes(&chunk),
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
        let (bytes, positions, closing) = (chunk.bytes(), chunk.positions(), chunk.is_last());
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
        // the evaluator's buffers by capacity and every label set handed out, leaving out the
        // kernel's scratch, this table's map overhead with the `Arc` headers of its strings, the
        // outer vectors of the pending batch and a sliced buffer's extra backing. During one push
        // the evaluator additionally holds the incoming chunk, charged by `hold` above for as long
        // as the call runs, and vector growth is not reserved and not bounded by one doubling —
        // a bulk append can allocate more than twice the previous capacity, and several vectors can
        // grow at once — so this figure is what the storage stands at, not a peak over the push.
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
        // The window goes first: dropping the evaluator is what gives back the memory its
        // reservation was taken for. Returning the reservation while the evaluator still held the
        // samples would let the pool admit memory the process has not released, for as long as the
        // caller keeps the stopped executor alive. The label sets this node handed out go with it.
        self.window = None;
        self.emitted = None;
        self.queued.clear();
        self.queued.shrink_to_fit();
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
                NativeExpr::Map { op, .. } => Some((NodeTag::Map, Seed::Map(Arc::clone(&op.ops)))),
                // The kinds below are legal in a bound tree, and `validate` stays the authority on
                // that; this runtime simply does not execute them yet.
                NativeExpr::Scalar(_) => {
                    unsupported.get_or_insert_with(|| not_implemented("Scalar", path));
                    None
                }
                NativeExpr::Instant { .. } => {
                    unsupported.get_or_insert_with(|| not_implemented("Instant", path));
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
                NodeTag::Window | NodeTag::Map => {
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
                window: None,
                map: None,
                emitted: None,
            };
            match &node.seed {
                Seed::Input => {}
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

    /// Closes every node, reporting an input that ended mid-series.
    ///
    /// A drain finds nothing left to emit: a window reports its results on the chunk that closes
    /// the series, so what is left to do is to check that every series did close, and to return
    /// everything the execution reserved.
    fn close(&mut self) -> Result<()> {
        let mut result = Ok(());
        for runtime in &mut self.runtimes {
            if let Some(window) = runtime.window.as_mut()
                && let Err(error) = window.finish()
                && result.is_ok()
            {
                result = Err(error);
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
