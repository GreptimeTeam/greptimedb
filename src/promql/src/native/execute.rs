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
use datatypes::arrow::buffer::Buffer;
use datatypes::series::histogram::HistogramBatch;
use datatypes::series::raw::RawSeriesChunk;
use datatypes::series::samples::{
    DenseSamplesRef, SampleRef, Samples, Series, SeriesFields, SeriesSchema,
};
use datatypes::series::time::{GridRange, TimeAxis};
use datatypes::series::{MetricName, SeriesOrdering};

use crate::grid_series::{LabelSet, label_set};
use crate::map::{MathOp, drop_metric_name, map_math};
use crate::native::batch::EvaluatedChunk;
use crate::native::metrics::{NodeMetrics, NodePath};
use crate::native::plan::{NativeExpr, NativeTree};
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

    /// The identity of the series this chunk belongs to, when the chunk names it.
    ///
    /// A raw chunk in bound-label mode carries its identity in its key and no metric name, so there
    /// is nothing to compare; an evaluated chunk always names the series it holds.
    fn metric(&self) -> Option<&MetricName> {
        match self {
            Self::Raw(chunk) => chunk.metric.as_ref(),
            Self::Evaluated(chunk) => Some(&chunk.series.metric),
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

/// One node of the flattened tree.
#[derive(Clone, Copy, Debug)]
struct NodeSpec {
    tag: NodeTag,
    /// The index of this node's input, which is the node the build pass reaches next.
    input: Option<usize>,
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
/// sharing no sample merge cleanly, as they do in [`crate::grid_series::merge_same_labelset`].
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
    /// after [`absorb`], one vector per field a history gains, and one terminal pass over a closing
    /// history's vector lengths.
    counts: Counts,
}

/// What the entries of an [`Emitted`] table hold, as running totals.
#[derive(Default)]
struct Counts {
    /// The run vectors, one per field of one entry.
    vectors: usize,
    /// The occupied runs in every vector.
    runs: usize,
    /// The payload of every label set and of every owner: the bytes of their names and values with
    /// one row per label.
    payload: usize,
}

/// What one node has handed out under one output label set.
#[derive(Default)]
struct History {
    /// The occupied runs of each field, in position order and disjoint.
    fields: Vec<Vec<Run>>,
    /// The source series that filled this label set, when the chunk that did named it.
    ///
    /// A chunk holding no value is handed over to close a series, never to fill a label set, so it
    /// closes this one only when it belongs to the series that filled it: a second series that
    /// produced nothing here is a series this label set never saw, and closing on its `is_last`
    /// would leave the series that did produce the values unable to finish. `None` is a set filled
    /// by chunks that named no series, where there is nothing to compare a closing chunk against.
    owner: Option<MetricName>,
    /// Whether the series on this label set closed. A second series cannot close it again.
    closed: bool,
}

impl History {
    /// Whether a chunk of `source` holding no value is the one that closes this label set.
    ///
    /// Only the series that filled it can: a series that produced nothing here is one this label
    /// set never saw, so closing on its `is_last` would take the close away from the series that
    /// did fill it. A chunk naming no series is compared with nothing and closes the set, which is
    /// what every chunk did before the owner was kept.
    fn closed_by(&self, source: Option<&MetricName>) -> bool {
        match (self.owner.as_ref(), source) {
            (Some(owner), Some(source)) => owner == source,
            _ => true,
        }
    }
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
/// label. The `Option<MetricName>` a [`History`] keeps its owner in is not counted here, being part
/// of the header that entry's slot is charged for.
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
    /// `source` is the identity of the input series `chunk` came from, when the chunk names one: a
    /// label set is filled by the series that contributes its values, and it is that series whose
    /// closing chunk closes it (see [`History::owner`]).
    ///
    /// A label set that has closed cannot be reached by a second series: the node would hand on two
    /// chunks that its consumer cannot tell apart, and the merge the caller wants spans the first
    /// series, whose state the evaluator above has already closed. Such a pair does merge in
    /// Prometheus, so this is a limitation of the streaming evaluator rather than a collision; it is
    /// refused instead of being answered wrongly (see [`closed_label_set`]). A chunk of a set still
    /// open is refused only where it holds a value at a position that same field already holds one
    /// at, which is the case no merge can resolve.
    fn occupy(
        &mut self,
        path: &NodePath,
        source: Option<&MetricName>,
        chunk: &EvaluatedChunk,
    ) -> Result<()> {
        let labels = label_set(&chunk.series.metric);
        let occupied: Vec<Vec<Run>> = chunk
            .series
            .fields
            .iter()
            .enumerate()
            .map(|(index, field)| occupied_runs(path, index, field, &chunk.series.time))
            .collect::<Result<_>>()?;
        let empty = occupied.iter().all(Vec::is_empty);
        let recorded = self.sets.get(&labels);
        if let Some(history) = recorded {
            if history.closed {
                // A series that produced nothing under a closed label set contributes no sample, so
                // there is nothing to report, nothing to merge and nothing to refuse. One that did
                // produce values is the second series this set cannot take.
                return if empty {
                    Ok(())
                } else {
                    Err(closed_label_set(path, &labels))
                };
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
            // A chunk holding no value records nothing. Past a closed set the same series is the
            // one that answered empty, and on an open set it is the close of the series that filled
            // it -- but only of that series: a different one closes a label set it never wrote to,
            // which would leave the series that did write to it unable to finish.
            if empty && !(chunk.is_last && history.closed_by(source)) {
                return Ok(());
            }
        } else if empty {
            // The first chunk onto a label set holds no value: a series that answered empty, with
            // nothing to record, nothing to close and nothing to merge.
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
        // The series that contributes the values is the one that owns the label set, so a chunk of
        // another one cannot close it later on.
        if !empty
            && let Some(source) = source
            && history.owner.as_ref() != Some(source)
        {
            self.counts.payload = self
                .counts
                .payload
                .saturating_sub(history.owner.as_ref().map_or(0, metric_bytes))
                .saturating_add(metric_bytes(source));
            history.owner = Some(source.clone());
        }
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
        if chunk.is_last {
            history.closed = true;
            // No later chunk reads these runs: a closed label set refuses every series that would
            // use them. Keeping them charges the pool for a series that has already been answered.
            self.counts.runs -= history.fields.iter().map(Vec::len).sum::<usize>();
            self.counts.vectors -= history.fields.len();
            history.fields = Vec::new();
        }
        Ok(())
    }

    /// An estimate of the bytes this table holds, which the node reserves for.
    ///
    /// It counts one label-set slot and one history header per entry, one vector per field of a
    /// history, the runs in those vectors, and the payload of every label set and owner. Each of
    /// those is a running total, so the figure is read in constant time however many series the node
    /// has handed out; it leaves out the map's buckets, the slack of a vector's capacity and the
    /// `Arc` headers of the strings it shares, so it is an approximation rather than a measurement.
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

/// The per-node run-time state of a node.
struct NodeRuntime {
    buffer: NodeBuffer,
    /// Source chunks queued by the caller, for an input node, each with the estimate it was queued
    /// with: what a chunk is worth is read once, as it enters, and given back as it leaves.
    queued: VecDeque<(NativeChunk, usize)>,
    /// A window node's evaluator, with its own input and output grid; `None` before it is built and
    /// once it is closed.
    window: Option<RangeEvaluator>,
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
            // What it handed out is what the label sets of this node now hold. The identity is the
            // input's: `map_chunk` has already rewritten the output's label set, and a series is
            // the one that arrived.
            self.track(path, Some(&chunk.series.metric), output)?;
        }
        // The chunk is gone and the label sets this node has handed out are what is left.
        let emitted = self.emitted_bytes();
        self.buffer.resize(emitted)?;
        Ok(output)
    }

    /// Feeds one chunk to this node's window.
    fn window(&mut self, path: &NodePath, chunk: NativeChunk) -> Result<Option<EvaluatedChunk>> {
        let (positions, closing) = (chunk.positions(), chunk.is_last());
        // Which series this chunk came from, for the label-set table below. A window consumes one
        // series to its `is_last` before the next, so the chunk that closes an output label set is
        // the one of the series that produced its values, which is what lets the table accept a
        // closing chunk of a series that wrote to it. A raw chunk in bound-label mode names no
        // series, so there is nothing to pass on.
        let source = chunk.metric();
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
            // What it handed out is what the label sets of this node now hold, and the series that
            // filled one is the series its chunks came from.
            self.track(path, source, output)?;
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

    /// Records what this node hands out under its output label set.
    ///
    /// `source` is the identity of the input series the chunk came from, when there is one: a node
    /// that rewrites labels is where two input series become indistinguishable, so this is where
    /// the second one is refused, and it is also the only place that knows which series filled the
    /// label set a chunk closes. A node above reads what this one hands over, and a window above
    /// would evaluate the two as two series and report both. Checking at the root would be too late
    /// for that — the answer would already have been computed.
    fn track(
        &mut self,
        path: &NodePath,
        source: Option<&MetricName>,
        output: &EvaluatedChunk,
    ) -> Result<()> {
        match self.emitted.as_mut() {
            Some(emitted) => emitted.occupy(path, source, output),
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
                NativeExpr::Window { input, .. } => (NodeTag::Window, Some(input.as_ref())),
                NativeExpr::Map { input, .. } => (NodeTag::Map, Some(input.as_ref())),
                // The kinds below are legal in a bound tree, and `validate` stays the authority on
                // that; this runtime simply does not execute them yet. Rejected before a single
                // chunk is read: a tree the runtime cannot execute must not start, because a
                // partial answer would be one the caller never asked for.
                NativeExpr::Scalar(_) => return Err(not_implemented("Scalar", &path)),
                NativeExpr::Instant { .. } => return Err(not_implemented("Instant", &path)),
                NativeExpr::Aggregate { .. } => return Err(not_implemented("Aggregate", &path)),
                NativeExpr::Binary { .. } => return Err(not_implemented("Binary", &path)),
            };
            let buffer = NodeBuffer::new(&path, handles, &pool);
            let mut runtime = NodeRuntime {
                buffer,
                queued: VecDeque::new(),
                window: None,
                map: None,
                emitted: None,
                open_source_series: false,
            };
            match &node.expr {
                NativeExpr::Input(_) => {}
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
                | NativeExpr::Instant { .. }
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
            paths.push(path.clone());
            runtimes.push(runtime);
            let Some(child) = child else {
                break;
            };
            node = child;
            path = path.input();
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

    /// The number of nodes this execution runs.
    pub fn len(&self) -> usize {
        self.nodes.len()
    }

    /// Whether the tree this state was built from has no nodes at all.
    pub fn is_empty(&self) -> bool {
        self.nodes.is_empty()
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
