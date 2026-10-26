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
//! Each execution owns a recursive runtime tree; the compiled [`NativeTree`] remains immutable and
//! can be used to build independent executions. A caller registers the tree's per-node metric
//! handles into its own metrics set once, before the execution starts.

use std::collections::hash_map::Entry;
use std::collections::{HashMap, HashSet, VecDeque};
use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datatypes::arrow::array::Float64Array;
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
    GridSample, GridSeriesBuilder, LabelSet, fields_bytes, label_set, merge_same_labelset, owned,
    series_bytes, table_bytes,
};
use crate::histogram_eval::{evaluate_histograms, map_native_histogram};
use crate::instant_eval::{
    select_instant_at, select_instants, timestamp_of_evaluated, timestamp_of_selection,
};
use crate::map::{
    ArithmeticOp, Comparison, ComparisonOp, ScalarOperand, drop_metric_name, map_arithmetic,
    map_compare, map_math,
};
use crate::native::aggregate::AggregateRuntime;
use crate::native::batch::EvaluatedChunk;
use crate::native::metrics::NodeMetrics;
use crate::native::plan::{
    BoundInstant, BoundScalar, BoundSetOp, BoundVectorOp, Grouping, MapOp, NativeExpr, NativeNode,
    NativeTree, NativeType,
};
#[cfg(test)]
pub(crate) use crate::native::runs::absorb_rebuilds;
use crate::native::runs::{Run, absorb};
use crate::range_eval::RangeEvaluator;
use crate::select::{self, SortOrder};
use crate::vector_match::{
    BinaryMatching, BinaryOp, Cardinality, FillValues, MatchingLabels,
    binary_vectors_with_reservation, set_vectors_with_reservation, validate_binary_operand,
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
///
/// It is what an aggregate node charges for a chunk it is folding: that node hands the chunk over
/// to its fold and drops it, so the chunk alone is what it holds for as long as it holds anything.
pub(super) fn evaluated_bytes(chunk: &EvaluatedChunk) -> usize {
    std::mem::size_of::<EvaluatedChunk>().saturating_add(fields_bytes(&chunk.series.fields))
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

/// The bytes the allocation `buffer` is a slice of.
///
/// The capacity is the whole allocation, which is what a slice keeps alive: a key of a few bytes
/// sliced out of a decoded dictionary values buffer pins that buffer. A buffer that owns no memory
/// of its own reports no capacity, and then the bytes it shows are all there is.
fn buffer_bytes(buffer: &Buffer) -> usize {
    buffer.capacity().max(buffer.len())
}

/// Which of an execution's inputs a caller's chunk belongs to.
///
/// An input is a leaf of the tree, and a tree of unary nodes has one while a binary node adds one
/// per operand, so a caller has to say which leaf it is feeding. The id is the leaf's position
/// among the leaves counted left to right, which is stable for one tree and means nothing across
/// trees: it is a routing handle, not an identity a node carries.
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
///
/// A window reads one series at a time, to the chunk carrying `is_last`, so the chunks of one series
/// arrive together and each of them continues the one before. What is kept here is the identity of
/// the open series and the last position it decided, which is what says whether the next chunk still
/// belongs to it and still moves forward.
struct OpenSeries {
    /// The identity of the open series, as the chunk that opened it named it.
    metric: SeriesLabels,
    /// The last absolute position the open series decided, when it decided any.
    last_position: Option<usize>,
}

/// The input a window with a [`RuntimeOp::Window`] collects until its input ends.
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
                "the window was handed an evaluated series that does not match its input schema"
                    .to_string(),
            ));
        }
        let (first, last) = (series.time.first_position(), series.time.last_position());
        if !is_last && first.is_none() {
            return Err(DataFusionError::Execution(
                "the window was handed an evaluated series that holds no position before the last \
                 run of its series"
                    .to_string(),
            ));
        }
        if let Some(open) = &self.open
            && open.metric != series.metric
        {
            return Err(DataFusionError::Execution(
                "the window was handed an evaluated series that changed identity before its last \
                 run"
                .to_string(),
            ));
        }
        if let Some(previous) = self.open.as_ref().and_then(|open| open.last_position)
            && let Some(first) = first
            && first <= previous
        {
            return Err(DataFusionError::Execution(format!(
                "the window was handed evaluated series runs that are not increasing: {first} after \
                 {previous}"
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
// Visible to the aggregate node so that its fold can reserve for what it is about to allocate; the
// reservation belongs to a node, not to a module.
pub(super) struct NodeBuffer {
    metrics: Arc<NodeMetrics>,
    reservation: MemoryReservation,
}

impl NodeBuffer {
    /// Registers a reservation for the node called `name` with `pool`.
    ///
    /// The name is what the operator is — `native.input`, `native.window` — rather than where it
    /// sits in the tree: a consumer is named once, when the node owning it is built, so a tree
    /// executed twice registers the same consumers again instead of a second set of them.
    fn new(name: &'static str, metrics: Arc<NodeMetrics>, pool: &Arc<dyn MemoryPool>) -> Self {
        let consumer = MemoryConsumer::new(name);
        Self {
            metrics,
            reservation: consumer.register(pool),
        }
    }

    /// Reserves `bytes` beside what this node already holds.
    pub(super) fn grow(&mut self, bytes: usize) -> Result<()> {
        self.reservation.try_grow(bytes)?;
        self.record();
        Ok(())
    }

    /// Reserves exactly `bytes`, which is what this node holds from now on.
    ///
    /// What a node that consumes its input chunk by chunk holds afterwards is not the sum of what
    /// it consumed, so the reservation is set from the state that is left rather than grown per
    /// chunk.
    pub(super) fn resize(&mut self, bytes: usize) -> Result<()> {
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
fn metric_bytes(metric: &SeriesLabels) -> usize {
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
    fn occupy(&mut self, chunk: &EvaluatedChunk) -> Result<()> {
        let labels = label_set(&chunk.series.metric);
        let occupied: Vec<Vec<Run>> = chunk
            .series
            .fields
            .iter()
            .enumerate()
            .map(|(index, field)| occupied_runs(index, field, &chunk.series.time))
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
                        return Err(occupied_position(&labels, index, run, previous));
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
fn occupied_runs(index: usize, field: &Samples, time: &TimeAxis) -> Result<Vec<Run>> {
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
            .ok_or_else(|| misaligned_field(index, time, field))?;
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
fn misaligned_field(index: usize, time: &TimeAxis, field: &Samples) -> DataFusionError {
    DataFusionError::Execution(format!(
        "a series was handed to the node whose axis and fields are misaligned: the axis places {} \
         rows, but field {index} holds {} samples, so the axis names a row the field does not have",
        time.len(),
        field.len(),
    ))
}

/// What a map node applies.
struct MapRuntime {
    ops: Arc<[MapOp]>,
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
        if self.selection.at_ms.is_some() {
            return usize::try_from(spec.len).unwrap_or(usize::MAX);
        }
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
    fn push(&mut self, chunk: RawSeriesChunk) -> Result<Option<EvaluatedChunk>> {
        if let Some(open) = self.chunks.first()
            && (open.primary_key.as_slice() != chunk.primary_key.as_slice()
                || open.metric != chunk.metric)
        {
            // One axis is gathered before the kernel reads any of it, so a chunk of another series
            // has nowhere to go: it would be selected as though its samples were the open series'.
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

    /// Selects the instants of the buffered series, hands it over closed, and drops the buffer.
    fn gather(&mut self) -> Result<EvaluatedChunk> {
        let chunk = self.select()?;
        // The selection copied what it needed out of the buffered chunks, so a series that has been
        // answered must not keep charging the pool for them.
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
        // The buffered chunks are one series, so they are read as the one axis the selection takes.
        let time = buffered_time(&self.chunks)?;
        let fields = buffered_fields(&self.chunks, self.schema.fields.len())?;
        let grid = Arc::clone(&self.selection.output_grid);
        let spec = grid.spec();
        // The grid's own points are what the selection decides, so the range and the step it is
        // asked for are the grid's: `start..=start + (len - 1) * step`, step by step.
        if let Some(at_ms) = self.selection.at_ms {
            let Some(index) = select_instant_at(
                &time,
                &fields,
                at_ms,
                self.selection.lookback_ms,
                self.selection.offset_ms,
            )?
            else {
                let metric = if self.selection.timestamp {
                    drop_metric_name(&metric)?
                } else {
                    metric
                };
                return no_row_chunk(&self.schema, metric);
            };
            let grid_len = usize::try_from(spec.len).unwrap_or(usize::MAX);
            if self.selection.timestamp {
                let indices = vec![index; grid_len];
                let timestamps = grid
                    .timestamps()
                    .map_err(crate::grid_series::external)?
                    .to_vec();
                return match timestamp_of_selection(
                    &self.schema,
                    &metric,
                    &time,
                    &fields,
                    &indices,
                    &timestamps,
                )? {
                    Some(series) => EvaluatedChunk::try_new(series, true),
                    None => no_row_chunk(&self.schema, drop_metric_name(&metric)?),
                };
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
        if self.selection.timestamp {
            return match timestamp_of_selection(
                &self.schema,
                &metric,
                &time,
                &fields,
                &indices,
                &timestamps,
            )? {
                Some(series) => EvaluatedChunk::try_new(series, true),
                None => no_row_chunk(&self.schema, drop_metric_name(&metric)?),
            };
        }
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
                    let sample = cursor.at(index).ok_or_else(|| missing_row(index))?;
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

/// An input a node reads whole before it computes -- one operand of a binary node, or the input of a
/// vector operation -- chunk by chunk, until it ends.
///
/// What is kept is the series each chunk carries, not a concatenation of them: a chunk is retained
/// as it arrives and nothing is copied on the way in, because the chunks of a series can share an
/// allocation and the figure the input charges for is the data it holds rather than a running copy
/// of it.
///
/// The kernels read whole series, so the chunks are assembled into one series per logical series
/// once the input has ended: a series that arrived in a thousand chunks is otherwise a thousand
/// candidates for a binary match or a ranking, and its own rows would be read a chunk at a time.
///
#[derive(Default)]
struct RetainedInput {
    /// The series whose last chunk did not close it, with the position after its last row.
    ///
    /// `None` when no series is open: nothing has arrived yet, or the last series closed. A label
    /// set may be closed more than once -- a node that rewrites labels can make two series one --
    /// so a chunk that starts a series again is not an error, only one that starts it before the
    /// series already open ended, or names another series while it is open.
    open: Option<(SeriesLabels, usize)>,
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

impl RetainedInput {
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

    /// Retains one chunk after checking that it continues what this input handed over.
    ///
    /// An input hands over one series at a time, each chunk of it after the rows the series already
    /// reached; a chunk that starts another series while one is open, or goes back inside the open
    /// one, is refused before it is retained.
    fn retain(&mut self, chunk: EvaluatedChunk, reader: Reader) -> Result<()> {
        // An empty chunk closes a series without holding a row, so it says nothing about where that
        // series ended: only a chunk that carries rows can be read as going backwards.
        let (first, last) = (
            chunk.series.time.first_position(),
            chunk.series.time.last_position(),
        );
        let metric = chunk.series.metric.clone();
        // The position a chunk of the open series may start at: the row after the last one that
        // series handed over. Nothing is open after a series closed, so the next one starts over.
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

    /// Refuses an input that ended inside a series.
    ///
    /// An input hands over one series at a time, so the only series that can be open is the last
    /// one it handed over: a chunk that did not say `is_last` leaves it open, and nothing came after
    /// it to close it.
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
    /// [`Self::ensure_closed`] refuses such an input before anything is assembled.
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
        // The tables the cleared chunks were keeping alive are gone with them, so their addresses
        // name nothing this operand holds any more.
        self.tables.clear();
        self.tables.shrink_to_fit();
        self.held = 0;
    }
}

/// What a binary or set node holds: both operands until they end.
///
/// The operation pairs two whole vectors point by point, so this node cannot answer from the chunks
/// in hand the way a map does: it retains every chunk each operand hands over, assembles the chunks
/// of each logical series into the one series the kernel matches, and runs the kernel once, after the
/// input has ended and both operands closed. The chunks it produces are queued in the node's own
/// output queue and handed over from there.
struct BinaryRuntime {
    /// The kernel and the modifiers the compiler bound it with.
    kernel: PairKernel,
    /// The layout each operand is bound to, what a chunk of it is checked against: `0` is the left
    /// operand and `1` the right one, the order the tree declares them in.
    schemas: [Arc<SeriesSchema>; 2],
    /// The retained chunks of each operand, in the same order.
    operands: [RetainedInput; 2],
    /// Whether the kernel has run. It runs at most once: the operands it read are gone afterwards.
    computed: bool,
}

/// The kernel a [`BinaryRuntime`] runs over its two assembled operands.
enum PairKernel {
    /// An arithmetic or comparison operation between matched series.
    Binary(Arc<BinaryMatching>),
    /// `and`, `or` or `unless`, which keeps samples of either side as they are.
    Set(BoundSetOp),
}

impl PairKernel {
    /// What the node running this kernel is called in an error.
    fn node(&self) -> &'static str {
        match self {
            Self::Binary(_) => "the binary node",
            Self::Set(_) => "the set node",
        }
    }
}

/// What side `index` is called in an error.
fn side_name(index: usize) -> &'static str {
    match index {
        0 => "left",
        _ => "right",
    }
}

/// The node a [`RetainedInput`] belongs to, which its messages name; formatted only when one is.
#[derive(Clone, Copy)]
enum Reader {
    /// The operand at `side` of `node`, a binary or set node.
    Operand { side: usize, node: &'static str },
    /// The input of a vector operation.
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
    /// The run-time state of `kernel` over its two operands.
    fn new(kernel: PairKernel, schemas: [Arc<SeriesSchema>; 2]) -> Self {
        Self {
            kernel,
            schemas,
            operands: [RetainedInput::default(), RetainedInput::default()],
            computed: false,
        }
    }

    /// The reader of the operand at `side`, which names this node in its messages.
    fn reader(&self, side: usize) -> Reader {
        Reader::Operand {
            side,
            node: self.kernel.node(),
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
    fn assemble(&self) -> Result<[Vec<Series>; 2]> {
        let [left, right] = &self.operands;
        let [left_schema, right_schema] = &self.schemas;
        Ok([
            left.assemble(left_schema, self.reader(0))?,
            right.assemble(right_schema, self.reader(1))?,
        ])
    }

    /// The bytes the chunk of `side` would add to that operand's retention.
    ///
    /// It is read from the operand before the chunk is retained, so the figure is the one the
    /// operand grows by: what the chunk holds on its own and the room its end takes in the operand's
    /// boundary list.
    fn pending(&self, side: usize, chunk: &EvaluatedChunk) -> Result<usize> {
        self.operands
            .get(side)
            .map(|operand| operand.pending(chunk))
            .ok_or_else(no_binary)
    }

    /// Reports an operand that ended inside a series.
    ///
    /// The kernel is not run over such an operand: it would answer the pairs of a series whose
    /// remaining rows were never read.
    fn closed(&self) -> Result<()> {
        for (side, operand) in self.operands.iter().enumerate() {
            operand.ensure_closed(self.reader(side))?;
        }
        Ok(())
    }

    /// Retains one chunk of the operand at `side`, checking it against that operand's binding.
    fn retain(&mut self, side: usize, chunk: EvaluatedChunk) -> Result<()> {
        // What a bound tree says an operand produces is not proof of what a chunk carries: a
        // `Series` keeps its fields public, so a chunk built by struct literal can carry another
        // number of fields, or fewer samples than its axis places rows. The kernel's own check is
        // run here, before the chunk enters the buffer, and the side is added to what it reports.
        let Some(schema) = self.schemas.get(side) else {
            return Err(no_binary());
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
            return Err(no_binary());
        };
        operand.retain(chunk, reader)
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
    fn compute(&mut self, buffer: &mut NodeBuffer) -> Result<Option<Vec<Series>>> {
        // The kernel reads the operands once and drops them, so a second call has nothing to read
        // and nothing left to produce: what it produced the first time is in the node's queue, and
        // the reservation is already the queue's.
        if self.computed {
            return Ok(None);
        }
        self.closed()?;
        let [left_schema, right_schema] = &self.schemas;
        let retained = self.retained_bytes();
        // The assembled series are live while the kernel reads them and share the buffers the
        // retained chunks hold, so what they add is metadata, and it is reserved before it exists:
        // the assembly allocates as it goes, and a figure read from it afterwards would be a figure
        // the pool never admitted.
        let node = self.kernel.node();
        let assembly = self
            .assembly_bytes()
            .ok_or_else(|| reservation_overflow(node))?;
        buffer.grow(assembly)?;
        let assembled = match self.assemble() {
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
            let mut reserve = |workspace: usize| {
                let bytes = retained
                    .checked_add(assembly)
                    .and_then(|held| held.checked_add(workspace))
                    .ok_or_else(|| reservation_overflow(node))?;
                buffer.resize(bytes)
            };
            match &self.kernel {
                PairKernel::Binary(matching) => binary_vectors_with_reservation(
                    matching,
                    left_schema,
                    left,
                    right_schema,
                    right,
                    &mut reserve,
                ),
                PairKernel::Set(set) => set_vectors_with_reservation(
                    set.op,
                    &set.labels,
                    left_schema,
                    left,
                    right_schema,
                    right,
                    &mut reserve,
                ),
            }
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

/// What a vector node holds: its whole input until it ends.
///
/// Which samples `topk` or `limitk` keeps at a point depends on every series present there, so,
/// like a binary node, this node retains every chunk its input hands over, assembles the chunks of
/// each logical series, and runs its kernel once, after the input has ended. The results are queued
/// in the node's own output queue and handed over from there.
struct VectorRuntime {
    op: BoundVectorOp,
    /// The layout of the input, which is also what the node produces.
    schema: Arc<SeriesSchema>,
    input: RetainedInput,
    /// Whether the input's pieces of one label set are merged before the kernel reads them, which
    /// `limitk` needs when its input can hold them.
    merge_input: bool,
    /// Whether the kernel has run. It runs at most once: the input it read is gone afterwards.
    computed: bool,
}

impl VectorRuntime {
    fn new(op: BoundVectorOp, schema: Arc<SeriesSchema>) -> Self {
        Self {
            op,
            schema,
            input: RetainedInput::default(),
            merge_input: false,
            computed: false,
        }
    }

    /// Runs the kernel over the whole input and returns the series it kept, or `None` when it
    /// already ran.
    ///
    /// The reservation covers the retained chunks, the assembled series and a rough estimate of the kernel's workspace and of the series it rebuilds
    /// ([`vector_workspace_bytes`]). The kernels do not report their workspace as they allocate,
    /// so the estimate is reserved once, before the kernel runs.
    fn compute(&mut self, buffer: &mut NodeBuffer) -> Result<Option<Vec<Series>>> {
        if self.computed {
            return Ok(None);
        }
        self.input.ensure_closed(Reader::Vector)?;
        let retained = self.input.held;
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
        // `limitk` keeps the first series in the order its input hands them over, as Prometheus
        // takes the order of the vector it is given -- a sort below it decides that order. Pieces
        // of one label set are one series there, placed where the label set first arrived, so
        // they are merged first; ranked apart, a later piece would be counted after the series
        // that arrived between them.
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
        buffer.grow(vector_workspace_bytes(&self.op, &self.schema, &assembled))?;
        let metrics = Arc::clone(&buffer.metrics);
        let output = {
            let _timer = metrics.timer();
            vector_series(&self.op, &self.schema, assembled)?
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

/// Runs the kernel `op` binds over the whole input.
fn vector_series(
    op: &BoundVectorOp,
    schema: &Arc<SeriesSchema>,
    series: Vec<Series>,
) -> Result<Vec<Series>> {
    let grouping = |group: &Grouping| match group {
        Grouping::By(labels) => select::Grouping::By(labels.to_vec()),
        Grouping::Without(labels) => select::Grouping::Without(labels.to_vec()),
    };
    match op {
        BoundVectorOp::TopK { k, top, group } => {
            select::topk(*k, *top, &grouping(group), schema, &series)
        }
        BoundVectorOp::LimitK { k, group } => select::limitk(*k, &grouping(group), schema, &series),
        BoundVectorOp::LimitRatio { ratio } => select::limit_ratio(*ratio, schema, &series),
        // A sort hands back the series it was given, rebuilding only those `sort` and `sort_desc`
        // drop histogram samples from.
        BoundVectorOp::Sort(order) => select::sort_instant(order, schema, series),
        // The kernel groups bucket series by label set and takes one native histogram series per
        // set, so pieces of one label set -- an empty tag and an absent one, or what a map folded
        // -- are merged first, and two of them holding one point fail as they do elsewhere.
        BoundVectorOp::Histogram(function) => {
            evaluate_histograms(*function, schema, &merge_same_labelset(schema, series)?)
        }
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
            aggregate::count_values(schema, &series, label, grouping)
        }
        BoundVectorOp::Absent { labels } => Ok(select::absent(schema, &series, labels.clone())?
            .into_iter()
            .collect()),
    }
}

/// A rough estimate of what the vector operation `op` allocates over `series`, beside the input it reads.
/// Like [`merge_workspace_bytes`] it is not an allocation bound: vector slack and the hash table's
/// own layout are not measured.
///
/// - `topk`, `bottomk`, `limitk` and `limit_ratio`, per row: the `(point, row)` a kept sample is
///   recorded as, the candidate it is ranked as and the value each field rebuilds it into; per
///   series: its place in the label order and its group, its label pairs in the group key, its
///   cursor and the series and field descriptors of its result; and the histograms `limitk` and
///   `limit_ratio` copy into their results, at the widest histogram of each field.
/// - A sort: the value each series is keyed by, and the floats `sort` and `sort_desc` rebuild a
///   series into when it holds histograms; the label sorts move the series they were given.
/// - `absent`: a presence flag and a result value per grid point and field.
/// - `count_values`, per field sample, since every field's distinct values make groups of their
///   own and a gauge with distinct values makes about one group per sample: the group's label set
///   twice (its metric and its key) with the value written into it, its count per field, and the
///   result series it becomes.
/// - `histogram_quantile` and `histogram_fraction`: the merge of label-set pieces before the kernel
///   ([`merge_workspace_bytes`]); per series, its group entry and label pairs and a cursor per
///   field; per row, the bucket it contributes and a result value per field.
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
            // The value label at the length of a 17-digit value with sign, point and exponent;
            // the `'f'` form writes a large magnitude out in full, which this does not follow.
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
    fn output_metric(&self, metric: &SeriesLabels) -> Result<SeriesLabels> {
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

/// One unlabelled constant value per statement grid point, closed on its only chunk.
fn scalar_chunk(grid: Arc<EvalGrid>, value: f64) -> Result<EvaluatedChunk> {
    let schema = SeriesSchema {
        fields: Arc::from([FieldSpec {
            name: "value".into(),
        }]),
        eval_grid: Some(Arc::clone(&grid)),
    };
    let len = grid.spec().len as usize;
    let time = GridRange::try_new(grid, 0, len).map_err(series_error)?;
    let values = Float64Array::from(vec![value; len]);
    let fields = SeriesFields::one(Samples::from_float(values));
    let metric = SeriesLabels::try_new(None, Vec::new()).map_err(series_error)?;
    let series =
        Series::try_new(&schema, metric, TimeAxis::Grid(time), fields).map_err(series_error)?;
    EvaluatedChunk::try_new(series, true)
}

/// A closing chunk carrying `metric` and no row at all.
///
/// A series whose selection picked no source row still closes exactly once, on a chunk that holds
/// nothing: the labels say which series closed, and the address says it closed before its first
/// point, as the empty chunk a window hands out for such a series does.
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

enum RuntimeOp {
    Input(InputId),
    Scalar,
    Instant {
        input: Box<RuntimeNode>,
        instant: Option<InstantRuntime>,
        state: NodeState,
    },
    Merge {
        input: Box<RuntimeNode>,
        merge: Option<MergeInput>,
        state: NodeState,
    },
    Window {
        input: Box<RuntimeNode>,
        window: Option<RangeEvaluator>,
        merge: Option<MergeInput>,
        state: NodeState,
    },
    Map {
        input: Box<RuntimeNode>,
        map: Option<MapRuntime>,
        state: NodeState,
    },
    Timestamp {
        input: Box<RuntimeNode>,
        schema: Option<Arc<SeriesSchema>>,
        state: NodeState,
    },
    Binary {
        left: Box<RuntimeNode>,
        right: Box<RuntimeNode>,
        binary: Option<BinaryRuntime>,
        state: NodeState,
    },
    Broadcast {
        left: Box<RuntimeNode>,
        right: Box<RuntimeNode>,
        broadcast: Option<BroadcastRuntime>,
        state: NodeState,
    },
    Aggregate {
        input: Box<RuntimeNode>,
        aggregate: Option<AggregateRuntime>,
        state: NodeState,
    },
    Vector {
        input: Box<RuntimeNode>,
        vector: Option<VectorRuntime>,
        state: NodeState,
    },
}

/// State shared by the stateful operators.
struct NodeState {
    queued: VecDeque<(NativeChunk, usize)>,
    queued_tables: usize,
    emitted: Option<Emitted>,
    buffer: NodeBuffer,
}

/// The caller-owned state of one input leaf.
struct InputSlot {
    source_index: usize,
    schema: Arc<SeriesSchema>,
    open_source_series: bool,
    state: NodeState,
}

/// State for one execution of a bound tree.
///
/// The state owns its runtime tree outright: the topology is the one the compiled tree built, and
/// nothing outside reaches into it, so a descent and the release that follows a failure are both
/// borrows of this one owner.
pub struct NativeExecutionState {
    root: RuntimeNode,
    inputs: Vec<InputSlot>,
    /// A constant root waiting for its only pull; operand scalars remain chunkless.
    scalar_root: Option<(Arc<EvalGrid>, f64, NodeState)>,
}

impl NodeState {
    fn new(name: &'static str, metrics: Arc<NodeMetrics>, pool: &Arc<dyn MemoryPool>) -> Self {
        Self {
            queued: VecDeque::new(),
            queued_tables: 0,
            emitted: None,
            buffer: NodeBuffer::new(name, metrics, pool),
        }
    }
}

impl RuntimeNode {
    fn build(
        node: &NativeNode,
        pool: &Arc<dyn MemoryPool>,
        inputs: &mut Vec<InputSlot>,
    ) -> Result<(Self, bool)> {
        let metrics = Arc::new(NodeMetrics::default());
        let (op, shared) = match &node.expr {
            NativeExpr::Input(bound) => {
                if bound.ordering == SeriesOrdering::Interleaved {
                    return Err(DataFusionError::NotImplemented(format!(
                        "the native runtime does not accept {:?} input ordering",
                        bound.ordering
                    )));
                }
                let schema = node.output.samples().cloned().ok_or_else(|| {
                    DataFusionError::Execution("input node does not produce samples".to_string())
                })?;
                let id = InputId(inputs.len());
                inputs.push(InputSlot {
                    source_index: bound.source_index,
                    schema,
                    open_source_series: false,
                    state: NodeState::new("native.input", Arc::clone(&metrics), pool),
                });
                (RuntimeOp::Input(id), false)
            }
            NativeExpr::Scalar(_) => (RuntimeOp::Scalar, false),
            NativeExpr::Instant { input, op } => {
                let (child, shared) = Self::build(input, pool, inputs)?;
                let input_schema = input.output.samples().cloned().ok_or_else(|| {
                    DataFusionError::Execution(
                        "instant selection consumes non-sample input".to_string(),
                    )
                })?;
                (
                    RuntimeOp::Instant {
                        input: Box::new(child),
                        instant: Some(InstantRuntime::try_new(&input_schema, op)?),
                        state: {
                            let mut state =
                                NodeState::new("native.instant", Arc::clone(&metrics), pool);
                            if op.timestamp {
                                state.emitted = Some(Emitted::default());
                            }
                            state
                        },
                    },
                    shared || op.timestamp,
                )
            }
            NativeExpr::Merge { input } => {
                let (child, _) = Self::build(input, pool, inputs)?;
                let schema = node.output.samples().cloned().ok_or_else(|| {
                    DataFusionError::Execution("merge does not produce samples".to_string())
                })?;
                let mut state = NodeState::new("native.merge", Arc::clone(&metrics), pool);
                state.emitted = Some(Emitted::default());
                (
                    RuntimeOp::Merge {
                        input: Box::new(child),
                        merge: Some(MergeInput::new(schema)),
                        state,
                    },
                    false,
                )
            }
            NativeExpr::Window { input, op } => {
                let (child, child_shared) = Self::build(input, pool, inputs)?;
                let input_schema = input.output.samples().cloned().ok_or_else(|| {
                    DataFusionError::Execution("window consumes non-sample input".to_string())
                })?;
                let merge = child_shared.then(|| MergeInput::new(Arc::clone(&input_schema)));
                let shared = op.function.drops_metric_name() || !child_shared;
                let mut state = NodeState::new("native.window", Arc::clone(&metrics), pool);
                state.emitted = Some(Emitted::default());
                (
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
                    },
                    shared,
                )
            }
            NativeExpr::Map { input, op } => {
                let (child, _) = Self::build(input, pool, inputs)?;
                let schema = node.output.samples().cloned().ok_or_else(|| {
                    DataFusionError::Execution("map does not produce samples".to_string())
                })?;
                let mut state = NodeState::new("native.map", Arc::clone(&metrics), pool);
                state.emitted = Some(Emitted::default());
                (
                    RuntimeOp::Map {
                        input: Box::new(child),
                        map: Some(MapRuntime {
                            ops: Arc::clone(&op.ops),
                            schema,
                        }),
                        state,
                    },
                    true,
                )
            }
            NativeExpr::Timestamp { input } => {
                let (child, _) = Self::build(input, pool, inputs)?;
                let schema = node.output.samples().cloned().ok_or_else(|| {
                    DataFusionError::Execution("timestamp does not produce samples".to_string())
                })?;
                let mut state = NodeState::new("native.timestamp", Arc::clone(&metrics), pool);
                state.emitted = Some(Emitted::default());
                (
                    RuntimeOp::Timestamp {
                        input: Box::new(child),
                        schema: Some(schema),
                        state,
                    },
                    true,
                )
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
                        (
                            Some(BinaryRuntime::new(
                                PairKernel::Binary(Arc::new(matching.clone())),
                                [Arc::clone(left_schema), Arc::clone(right_schema)],
                            )),
                            None,
                        )
                    }
                    _ => (None, Some(broadcast_runtime(left, right, matching)?)),
                };
                let (left_node, left_shared) = Self::build(left, pool, inputs)?;
                let (right_node, right_shared) = Self::build(right, pool, inputs)?;
                match (binary, broadcast) {
                    (Some(binary), None) => (
                        RuntimeOp::Binary {
                            left: Box::new(left_node),
                            right: Box::new(right_node),
                            binary: Some(binary),
                            state: NodeState::new("native.binary", Arc::clone(&metrics), pool),
                        },
                        false,
                    ),
                    (None, Some(broadcast)) => {
                        let shared = broadcast.drops_labels()
                            || if left_schema.is_some() {
                                left_shared
                            } else {
                                right_shared
                            };
                        let mut state = NodeState::new("native.binary", Arc::clone(&metrics), pool);
                        if broadcast.drops_labels() {
                            state.emitted = Some(Emitted::default());
                        }
                        (
                            RuntimeOp::Broadcast {
                                left: Box::new(left_node),
                                right: Box::new(right_node),
                                broadcast: Some(broadcast),
                                state,
                            },
                            shared,
                        )
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
                let set = BinaryRuntime::new(
                    PairKernel::Set(op.clone()),
                    [Arc::clone(left_schema), Arc::clone(right_schema)],
                );
                let (left_node, _) = Self::build(left, pool, inputs)?;
                let (right_node, _) = Self::build(right, pool, inputs)?;
                // The kernel files one series per label set, so its output shares none.
                (
                    RuntimeOp::Binary {
                        left: Box::new(left_node),
                        right: Box::new(right_node),
                        binary: Some(set),
                        state: NodeState::new("native.set", Arc::clone(&metrics), pool),
                    },
                    false,
                )
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
                let mut state = NodeState::new("native.aggregate", Arc::clone(&metrics), pool);
                let aggregate = Some(AggregateRuntime::new(
                    op.clone(),
                    schema,
                    &mut state.buffer,
                )?);
                let (child, _) = Self::build(input, pool, inputs)?;
                (
                    RuntimeOp::Aggregate {
                        input: Box::new(child),
                        aggregate,
                        state,
                    },
                    false,
                )
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
                let mut vector = VectorRuntime::new(op.clone(), schema);
                let (child, child_shared) = Self::build(input, pool, inputs)?;
                let limitk = matches!(op, BoundVectorOp::LimitK { .. });
                if limitk && child_shared {
                    vector.merge_input = true;
                }
                let whole = limitk
                    || matches!(
                        op,
                        BoundVectorOp::Absent { .. }
                            | BoundVectorOp::Histogram(_)
                            | BoundVectorOp::CountValues { .. }
                    );
                (
                    RuntimeOp::Vector {
                        input: Box::new(child),
                        vector: Some(vector),
                        state: NodeState::new("native.vector", Arc::clone(&metrics), pool),
                    },
                    child_shared && !whole,
                )
            }
        };
        Ok((Self { metrics, op }, shared))
    }

    fn register_metrics(&self, name: &str, set: &ExecutionPlanMetricsSet, partition: usize) {
        self.metrics.register(name, set, partition);
        match &self.op {
            RuntimeOp::Input(_) | RuntimeOp::Scalar => {}
            RuntimeOp::Instant { input, .. }
            | RuntimeOp::Window { input, .. }
            | RuntimeOp::Merge { input, .. }
            | RuntimeOp::Map { input, .. }
            | RuntimeOp::Timestamp { input, .. }
            | RuntimeOp::Aggregate { input, .. }
            | RuntimeOp::Vector { input, .. } => {
                input.register_metrics(&format!("{name}.input"), set, partition)
            }
            RuntimeOp::Binary { left, right, .. } | RuntimeOp::Broadcast { left, right, .. } => {
                left.register_metrics(&format!("{name}.left"), set, partition);
                right.register_metrics(&format!("{name}.right"), set, partition);
            }
        }
    }

    fn reserved_bytes(&self) -> usize {
        match &self.op {
            RuntimeOp::Input(_) | RuntimeOp::Scalar => 0,
            RuntimeOp::Instant { input, state, .. }
            | RuntimeOp::Window { input, state, .. }
            | RuntimeOp::Merge { input, state, .. }
            | RuntimeOp::Map { input, state, .. }
            | RuntimeOp::Timestamp { input, state, .. }
            | RuntimeOp::Aggregate { input, state, .. }
            | RuntimeOp::Vector { input, state, .. } => state
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
        }
    }
}

/// What the driver asks of one node.
///
/// A node reads its own children directly instead of being told which node to read: a pull descends
/// the part of the tree it needs and returns, and the exclusive borrow the state takes of its root
/// is what makes the whole descent exclusive. No node therefore has to know where in the tree it
/// sits, and the topology stays the one the tree was built with.
impl RuntimeNode {
    /// Pulls the next chunk this node hands over, taking what its children owe it.
    ///
    /// `inputs_finished` is the caller's word that no more source chunks will arrive. A node that
    /// decides nothing until its whole input is there reads it to tell the last pull that found
    /// nothing -- "nothing more" -- from the ones before it, which meant "nothing yet".
    fn pull(
        &mut self,
        inputs: &mut [InputSlot],
        inputs_finished: bool,
    ) -> Result<Option<NativeChunk>> {
        match &mut self.op {
            // A source hands over what the caller queued at it. The id was assigned when the tree
            // was built, so an id past the leaves means this call and the tree disagree rather than
            // that the chunk belongs somewhere else.
            RuntimeOp::Input(id) => {
                let input = *id;
                let count = inputs.len();
                let slot = inputs
                    .get_mut(input.index())
                    .ok_or_else(|| no_input(count, input))?;
                slot.pop_source()
            }
            // A scalar owes its consumer no chunk: it produces one value and no series, and the
            // value is the constant on the tree, which the consumer reads off its own binding. A
            // pull of it is therefore empty by construction and says nothing has stalled.
            RuntimeOp::Scalar => Ok(None),
            RuntimeOp::Instant {
                input,
                instant,
                state,
            } => {
                let Some(selection) = instant.as_mut() else {
                    return Err(no_instant());
                };
                // A selection decides a whole series at once, so every chunk before the one that
                // closes it yields nothing and the selection asks its input again.
                loop {
                    let Some(chunk) = input.pull(inputs, inputs_finished)? else {
                        return Ok(None);
                    };
                    if let Some(output) = state.instant(selection, chunk)? {
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
                // A chunk this window computed while flushing its collected input leaves first: it
                // was produced before the window was drained, and the pull that hands it up is the
                // one that empties the queue.
                if let Some(chunk) = state.take_output()? {
                    return Ok(Some(chunk));
                }
                let Some(evaluator) = window.as_mut() else {
                    return Err(no_window());
                };
                // A window reports on the chunk that closes a series, so a chunk that decides
                // nothing yields nothing and the window asks its input again.
                loop {
                    let Some(chunk) = input.pull(inputs, inputs_finished)? else {
                        return Ok(None);
                    };
                    if let Some(output) = state.window(evaluator, merge, chunk)? {
                        return Ok(Some(NativeChunk::Evaluated(output)));
                    }
                }
            }
            RuntimeOp::Map { input, map, state } => {
                let Some(operation) = map.as_ref() else {
                    return Err(no_map());
                };
                // An operation that empties every position yields no chunk, and the map asks its
                // input again rather than reporting a decision it did not make.
                loop {
                    let Some(chunk) = input.pull(inputs, inputs_finished)? else {
                        return Ok(None);
                    };
                    let NativeChunk::Evaluated(chunk) = chunk else {
                        return Err(DataFusionError::Execution(
                            "the map node consumes an evaluated chunk, not raw samples".to_string(),
                        ));
                    };
                    if let Some(output) = state.map(operation, chunk)? {
                        return Ok(Some(NativeChunk::Evaluated(output)));
                    }
                }
            }
            RuntimeOp::Timestamp {
                input,
                schema,
                state,
            } => {
                let schema = schema.as_ref().ok_or_else(|| {
                    DataFusionError::Execution("timestamp schema was released".into())
                })?;
                loop {
                    let Some(chunk) = input.pull(inputs, inputs_finished)? else {
                        return Ok(None);
                    };
                    let NativeChunk::Evaluated(chunk) = chunk else {
                        return Err(DataFusionError::Execution(
                            "timestamp consumes evaluated samples, not raw samples".into(),
                        ));
                    };
                    if let Some(output) = state.timestamp(schema, chunk)? {
                        return Ok(Some(NativeChunk::Evaluated(output)));
                    }
                }
            }
            RuntimeOp::Binary {
                left,
                right,
                binary,
                state,
            } => {
                let Some(operation) = binary.as_mut() else {
                    return Err(no_binary());
                };
                // Both operands are read to the end of what they hand over: before the input ends a
                // binary node only retains the chunks, because a pair is not decided until both
                // sides are whole, and after it ends this is the drain that empties the two.
                while let Some(chunk) = left.pull(inputs, inputs_finished)? {
                    state.retain_binary(operation, 0, chunk)?;
                }
                while let Some(chunk) = right.pull(inputs, inputs_finished)? {
                    state.retain_binary(operation, 1, chunk)?;
                }
                // Nothing has settled for a point that has an operand still arriving, and an
                // operand below a collecting window has not arrived until that window is flushed:
                // a node that reports itself settled reports it for everything below it too.
                if !inputs_finished || !left.settled(inputs) || !right.settled(inputs) {
                    return Ok(None);
                }
                state.compute_binary(operation)?;
                // A result holds one value per field, and a field may hold none where another
                // does, so its values are counted rather than read as its positions.
                state.pop_queued(evaluated_values)
            }
            RuntimeOp::Broadcast {
                left,
                right,
                broadcast,
                state,
            } => {
                let Some(operation) = broadcast.as_mut() else {
                    return Err(no_broadcast());
                };
                // A broadcast reads the vector side only: the constant is on the node's binding, so
                // there is no second stream to pull and no chunk to retain. Each chunk is
                // transformed as it arrives and handed up, which is what keeps a broadcast over a
                // whole vector from costing the vector.
                let vector = match operation.operand {
                    ScalarOperand::Left(_) => &mut **right,
                    ScalarOperand::Right(_) => &mut **left,
                };
                // An operation that empties every position yields no chunk, and the node asks its
                // vector side again rather than reporting a decision it did not make.
                loop {
                    let Some(chunk) = vector.pull(inputs, inputs_finished)? else {
                        return Ok(None);
                    };
                    if let Some(output) = state.broadcast(operation, chunk)? {
                        return Ok(Some(NativeChunk::Evaluated(output)));
                    }
                }
            }
            RuntimeOp::Aggregate {
                input,
                aggregate,
                state,
            } => {
                let Some(fold) = aggregate.as_mut() else {
                    return Err(no_aggregate());
                };
                // What the fold reads is not retained -- a chunk is folded as it arrives and
                // dropped -- so reading the child dry costs the fold's states and not the input's
                // bytes. Nothing is answered before the input ends: until then a group's value at a
                // point is still missing the members that have not arrived.
                while let Some(chunk) = input.pull(inputs, inputs_finished)? {
                    state.retain_aggregate(fold, chunk)?;
                }
                if !inputs_finished || !input.settled(inputs) {
                    return Ok(None);
                }
                state.compute_aggregate(fold)?;
                // A group's series holds one value per field and point it decided, so its values
                // are counted rather than read as its positions, as a binary result's are.
                state.pop_queued(evaluated_values)
            }
            RuntimeOp::Vector {
                input,
                vector,
                state,
            } => {
                let Some(operation) = vector.as_mut() else {
                    return Err(no_vector());
                };
                // Which samples are kept at a point is not decided until the whole input has
                // arrived, so the input is read to the end before anything is handed over.
                while let Some(chunk) = input.pull(inputs, inputs_finished)? {
                    state.retain_vector(operation, chunk)?;
                }
                if !inputs_finished || !input.settled(inputs) {
                    return Ok(None);
                }
                state.compute_vector(operation)?;
                state.pop_queued(evaluated_values)
            }
        }
    }

    /// Whether this node has produced everything it ever will.
    ///
    /// A pull that finds nothing does not answer it: a collecting window reports nothing until it
    /// is flushed, and a binary node that matches two vectors reports nothing until both nodes it
    /// reads from are finished. Each node therefore answers from its own state and from its
    /// children's, which is what keeps a binary node from computing on an operand a window below it
    /// has not handed over yet.
    fn settled(&self, inputs: &[InputSlot]) -> bool {
        match &self.op {
            // A source has handed over everything it ever will once nothing is queued at it: what
            // it hands over is what the caller queued.
            RuntimeOp::Input(id) => inputs
                .get(id.index())
                .is_some_and(|slot| slot.state.queued.is_empty()),
            // A scalar produces its value without reading anything, so it is settled from the
            // start: there is no chunk it owes its consumer.
            RuntimeOp::Scalar => true,
            // These report as they are handed chunks, so a child with nothing left is all it takes
            // for them to have nothing left either.
            RuntimeOp::Instant { input, state, .. }
            | RuntimeOp::Map { input, state, .. }
            | RuntimeOp::Timestamp { input, state, .. } => {
                state.queued.is_empty() && input.settled(inputs)
            }
            // A window that collects its input has not answered until it has flushed it, so a merge
            // still held is an answer still owed.
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
            // An aggregate answers one series per group and only once its input ended, so its own
            // fold has to have run as well: a child that has handed everything up is not enough,
            // because every one of those chunks was folded and none of them answered yet.
            RuntimeOp::Aggregate {
                input,
                aggregate,
                state,
            } => {
                state.queued.is_empty()
                    && aggregate.as_ref().is_some_and(|fold| fold.computed)
                    && input.settled(inputs)
            }
            RuntimeOp::Vector {
                input,
                vector,
                state,
            } => {
                state.queued.is_empty()
                    && vector.as_ref().is_some_and(|operation| operation.computed)
                    && input.settled(inputs)
            }
            // A vector match is settled once its kernel ran and both operands are: what one side
            // answers at a point depends on the other side.
            RuntimeOp::Binary {
                left,
                right,
                binary,
                state,
            } => {
                state.queued.is_empty()
                    && binary.as_ref().is_some_and(|operation| operation.computed)
                    && left.settled(inputs)
                    && right.settled(inputs)
            }
            // A broadcast owes only what its vector side has not handed over yet: it answers each
            // chunk as it reads it and keeps nothing, so it is settled exactly when that side is.
            RuntimeOp::Broadcast {
                left,
                right,
                broadcast,
                state,
            } => {
                state.queued.is_empty()
                    && broadcast
                        .as_ref()
                        .is_some_and(|operation| match operation.operand {
                            ScalarOperand::Left(_) => right.settled(inputs),
                            ScalarOperand::Right(_) => left.settled(inputs),
                        })
            }
        }
    }

    /// Merges the collected input of every window that collects it, deepest first.
    ///
    /// Deeper windows go first: a window above one of them collects the chunks the one below hands
    /// up, so flushing the shallow one first would merge an input it has not received yet. What a
    /// flush queues is handed up by the caller's own pulls, which is why nothing here collects the
    /// answer into a value of its own: a window whose input has not settled is simply left for a
    /// later call, when what is below it has handed its answer over.
    fn flush_ready_merges(&mut self, inputs: &[InputSlot]) -> Result<bool> {
        match &mut self.op {
            RuntimeOp::Input(_) | RuntimeOp::Scalar => Ok(false),
            RuntimeOp::Instant { input, .. }
            | RuntimeOp::Map { input, .. }
            | RuntimeOp::Timestamp { input, .. }
            | RuntimeOp::Aggregate { input, .. }
            | RuntimeOp::Vector { input, .. } => input.flush_ready_merges(inputs),
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
                // A window collects its input only when it can hold two series under one canonical
                // label set, and that input has to be finished before it is merged: a node below
                // still holding collected input has not answered yet, and merging what it has not
                // sent would answer a series from half of its contributions.
                if merge.is_some() && input.settled(inputs) {
                    let Some(evaluator) = window.as_mut() else {
                        return Err(no_window());
                    };
                    state.flush_merge(evaluator, merge)?;
                    flushed = true;
                }
                Ok(flushed)
            }
        }
    }

    /// Whether any window still holds collected input that has not been merged.
    fn any_merge_pending(&self) -> bool {
        match &self.op {
            RuntimeOp::Input(_) | RuntimeOp::Scalar => false,
            RuntimeOp::Instant { input, .. }
            | RuntimeOp::Map { input, .. }
            | RuntimeOp::Timestamp { input, .. }
            | RuntimeOp::Aggregate { input, .. }
            | RuntimeOp::Vector { input, .. } => input.any_merge_pending(),
            RuntimeOp::Binary { left, right, .. } | RuntimeOp::Broadcast { left, right, .. } => {
                left.any_merge_pending() || right.any_merge_pending()
            }
            // Only a window that collects its input keeps collected input to merge.
            RuntimeOp::Window { input, merge, .. } => merge.is_some() || input.any_merge_pending(),
            RuntimeOp::Merge {
                input,
                merge,
                state,
            } => merge.is_some() || !state.queued.is_empty() || input.any_merge_pending(),
        }
    }

    /// Refuses a node that ended inside a series, in every node that would read the remainder.
    ///
    /// A node that retains its whole input is the one that can tell a series that ended from one
    /// that was cut off there: the assembly above would read half a vector as a whole one, and a
    /// window that collected its input would merge the pieces it has into a series that looks
    /// finished. Both are refused here, before any of them allocates a workspace.
    fn unclosed(&self) -> Result<()> {
        match &self.op {
            RuntimeOp::Input(_) | RuntimeOp::Scalar => Ok(()),
            RuntimeOp::Instant { input, .. }
            | RuntimeOp::Map { input, .. }
            | RuntimeOp::Timestamp { input, .. }
            | RuntimeOp::Window { input, .. }
            | RuntimeOp::Aggregate { input, .. } => input.unclosed(),
            RuntimeOp::Merge { input, merge, .. } => {
                input.unclosed()?;
                if merge.as_ref().is_some_and(|m| m.open.is_some()) {
                    return Err(DataFusionError::Execution(
                        "merge input ended inside a series".into(),
                    ));
                }
                Ok(())
            }
            RuntimeOp::Binary {
                left,
                right,
                binary,
                ..
            } => {
                if let Some(operation) = binary {
                    operation.closed()?;
                }
                left.unclosed()?;
                right.unclosed()
            }
            RuntimeOp::Broadcast { left, right, .. } => {
                left.unclosed()?;
                right.unclosed()
            }
            RuntimeOp::Vector { input, vector, .. } => {
                if let Some(operation) = vector {
                    operation.input.ensure_closed(Reader::Vector)?;
                }
                input.unclosed()
            }
        }
    }

    /// Finishes every window's evaluator that is still here, and reports the first failure.
    ///
    /// An evaluator that read the input says here whether that input ended inside a series: one
    /// still holding a series is what a truncated stream looks like above the leaf that could have
    /// named it. Every evaluator is finished even after one fails -- the first error is the one
    /// reported -- so that a failure leaves nothing to be read on the way out.
    fn finish_windows(&mut self) -> Result<()> {
        match &mut self.op {
            RuntimeOp::Input(_) | RuntimeOp::Scalar => Ok(()),
            RuntimeOp::Instant { input, .. }
            | RuntimeOp::Map { input, .. }
            | RuntimeOp::Timestamp { input, .. }
            | RuntimeOp::Aggregate { input, .. }
            | RuntimeOp::Vector { input, .. }
            | RuntimeOp::Merge { input, .. } => input.finish_windows(),
            RuntimeOp::Binary { left, right, .. } | RuntimeOp::Broadcast { left, right, .. } => {
                // Both sides are finished even when the first one fails: the failure is reported
                // from here, and an evaluator left holding a series would keep its bytes.
                let first = left.finish_windows();
                let second = right.finish_windows();
                first.and(second)
            }
            RuntimeOp::Window { input, window, .. } => {
                let child = input.finish_windows();
                let own = window.take().map_or(Ok(()), RangeEvaluator::finish);
                child.and(own)
            }
        }
    }

    /// Gives back everything this node and its children hold, keeping the topology.
    ///
    /// The operation's payload goes first: it is what the reservation was taken for, and dropping
    /// it after the reservation went back would hand the pool bytes it cannot admit while they are
    /// still read. A released node keeps its shape — an input keeps its routing slot, a binary both
    /// of its operands — because what a node is does not change when the bytes it held go back.
    fn release(&mut self) {
        match &mut self.op {
            // A source keeps its queue in the caller-owned slot, which is released beside the tree,
            // and a scalar holds nothing at all.
            RuntimeOp::Input(_) | RuntimeOp::Scalar => {}
            RuntimeOp::Instant {
                input,
                instant,
                state,
            } => {
                *instant = None;
                state.release();
                input.release();
            }
            RuntimeOp::Merge {
                input,
                merge,
                state,
            } => {
                *merge = None;
                state.release();
                input.release();
            }
            RuntimeOp::Window {
                input,
                window,
                merge,
                state,
            } => {
                *window = None;
                *merge = None;
                state.release();
                input.release();
            }
            RuntimeOp::Map { input, map, state } => {
                *map = None;
                state.release();
                input.release();
            }
            RuntimeOp::Timestamp {
                input,
                schema,
                state,
            } => {
                *schema = None;
                state.release();
                input.release();
            }
            RuntimeOp::Aggregate {
                input,
                aggregate,
                state,
            } => {
                *aggregate = None;
                state.release();
                input.release();
            }
            RuntimeOp::Vector {
                input,
                vector,
                state,
            } => {
                *vector = None;
                state.release();
                input.release();
            }
            RuntimeOp::Binary {
                left,
                right,
                binary,
                state,
            } => {
                *binary = None;
                state.release();
                left.release();
                right.release();
            }
            RuntimeOp::Broadcast {
                left,
                right,
                broadcast,
                state,
            } => {
                *broadcast = None;
                state.release();
                left.release();
                right.release();
            }
        }
    }
}

impl NativeExecutionState {
    pub fn try_new(tree: &NativeTree, pool: Arc<dyn MemoryPool>) -> Result<Self> {
        let mut inputs = Vec::new();
        let (root, _) = RuntimeNode::build(tree.root(), &pool, &mut inputs)?;
        let scalar_root = if inputs.is_empty() {
            match (&tree.root().expr, &tree.root().output) {
                (NativeExpr::Scalar(BoundScalar { value }), NativeType::Scalar(grid)) => Some((
                    Arc::clone(grid),
                    *value,
                    NodeState::new("native.scalar", Arc::clone(&root.metrics), &pool),
                )),
                _ => {
                    return Err(DataFusionError::Execution(
                        "native tree without inputs must be a scalar root".into(),
                    ));
                }
            }
        } else {
            None
        };
        match &tree.root().output {
            NativeType::Scalar(_) if inputs.is_empty() => {}
            NativeType::Scalar(_) => {
                return Err(DataFusionError::Execution(
                    "native scalar root unexpectedly has input nodes".into(),
                ));
            }
            NativeType::Samples(schema) if schema.eval_grid.is_some() => {}
            NativeType::Samples(_) => {
                return Err(DataFusionError::Execution(
                    "native runtime root produces raw samples".to_string(),
                ));
            }
            NativeType::AggregateState(_) => {
                return Err(DataFusionError::Execution(
                    "native runtime root produces no sample output".to_string(),
                ));
            }
        }
        Ok(Self {
            root,
            inputs,
            scalar_root,
        })
    }

    /// Registers the metric handles of every node of this execution's tree with `set`.
    ///
    /// A caller invokes this once, before the execution starts: what `set` holds afterwards is the
    /// nodes' own counters and gauges, so a later read of it reports what they recorded and keeps
    /// no topology of this execution alive.
    pub(crate) fn register_metrics(&self, set: &ExecutionPlanMetricsSet, partition: usize) {
        self.root.register_metrics("root", set, partition);
    }

    /// Returns the runtime input slots bound to `source_index`.
    pub fn input_ids(&self, source_index: usize) -> impl Iterator<Item = InputId> + '_ {
        self.inputs
            .iter()
            .enumerate()
            .filter(move |(_, slot)| slot.source_index == source_index)
            .map(|(index, _)| InputId::new(index))
    }

    pub fn reserved_bytes(&self) -> usize {
        self.inputs
            .iter()
            .map(|slot| slot.state.buffer.reservation.size())
            .fold(0usize, usize::saturating_add)
            .saturating_add(self.root.reserved_bytes())
            .saturating_add(
                self.scalar_root
                    .as_ref()
                    .map_or(0, |(_, _, state)| state.buffer.reservation.size()),
            )
    }
}

/// What the driver asks of one execution's state.
///
/// Every operation borrows the tree this state owns — exclusively to pull, shared to inspect it —
/// while it works on the nodes below, and gives the borrow back before returning. Nothing else
/// reaches into the tree, so a failed operation leaves nothing held behind it and the release that
/// follows a failure borrows the same tree again.
impl NativeExecutionState {
    /// Pulls the next chunk the root of this state's tree hands over.
    ///
    /// `inputs_finished` is the caller's word that no more source chunks will arrive; it is passed
    /// down unchanged, because only a node that reads its whole input needs it.
    fn pull(&mut self, inputs_finished: bool) -> Result<Option<NativeChunk>> {
        if let Some((grid, value, mut state)) = self.scalar_root.take() {
            let len = usize::try_from(grid.spec().len).map_err(|_| {
                DataFusionError::Execution("scalar grid length does not fit memory size".into())
            })?;
            let bytes = std::mem::size_of::<EvaluatedChunk>()
                .checked_add(std::mem::size_of::<Float64Array>())
                .and_then(|bytes| {
                    len.checked_mul(std::mem::size_of::<f64>())
                        .and_then(|values| bytes.checked_add(values))
                })
                .ok_or_else(|| {
                    DataFusionError::Execution("scalar allocation size overflow".into())
                })?;
            state.buffer.grow(bytes)?;
            let chunk = scalar_chunk(grid, value)?;
            state.buffer.metrics.record_completed_series(1);
            state
                .queued
                .push_back((NativeChunk::Evaluated(chunk), bytes));
            return state.pop_queued(evaluated_values);
        }
        self.root.pull(&mut self.inputs, inputs_finished)
    }

    /// Refuses an input the caller never closed, and every node that ended inside a series.
    ///
    /// The slots are read first: an open series there is the input the caller can still close, and
    /// the name it is reported under is the routing slot it was queued at. Only then are the nodes
    /// asked, because a node that retains its whole input reports the same truncation in its own
    /// terms.
    fn unclosed(&self) -> Result<()> {
        if let Some(index) = self.inputs.iter().position(|slot| slot.open_source_series) {
            return Err(truncated_input(InputId::new(index)));
        }
        self.root.unclosed()
    }

    /// Merges what every window that collects its input has been handed, deepest first.
    fn flush_ready_merges(&mut self) -> Result<bool> {
        self.root.flush_ready_merges(&self.inputs)
    }

    /// Whether any window still holds collected input that has not been merged.
    fn any_merge_pending(&self) -> bool {
        self.root.any_merge_pending()
    }

    /// Closes the execution: finishes every window's evaluator and gives everything back.
    ///
    /// The evaluators are finished before anything is released, so the first failure is reported
    /// from the state that caused it. Everything is released whether or not one of them failed:
    /// the caller reads the error either way, and holding the bytes past it would keep the query's
    /// memory in use for as long as the executor is alive.
    fn close(&mut self) -> Result<()> {
        let finished = self.root.finish_windows();
        self.root.release();
        if let Some((_, _, mut state)) = self.scalar_root.take() {
            state.release();
        }
        // The slots are taken rather than only released: what a released slot still holds is its
        // schema and the grid that schema is addressed on, and a caller that keeps the stopped
        // executor would otherwise keep the input's schema and materialized timestamps alive past
        // the execution. The leaf itself stays -- the root keeps its `Input(InputId)` and the
        // nodes keep their metric handles -- and it holds neither.
        for mut slot in std::mem::take(&mut self.inputs) {
            slot.release();
        }
        finished
    }

    /// Gives back everything this execution still holds: its payloads, its slots and the bytes
    /// they reserved.
    ///
    /// The slots are taken here as they are when the execution closes, so the input's schema and
    /// grid do not outlive it in an executor its caller still holds; a second call finds an empty
    /// vector and nothing left to take.
    fn release(&mut self) {
        self.root.release();
        if let Some((_, _, mut state)) = self.scalar_root.take() {
            state.release();
        }
        for mut slot in std::mem::take(&mut self.inputs) {
            slot.release();
        }
    }
}

impl Drop for NativeExecutionState {
    /// Returns what the execution never gave back itself.
    ///
    /// The tree goes with this state, so its payloads would be dropped here in any case; the
    /// release is what gives the pool their bytes back and leaves the counters reporting what the
    /// nodes did rather than what they held.
    fn drop(&mut self) {
        self.release();
    }
}

impl NodeState {
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
    /// queued, and a binary or vector node hands over what its kernel produced. `values` is how
    /// that node counts the values it hands over, because the two do not count the same thing: an
    /// input's figure has always been its positions, while a kernel's result holds one value per
    /// field and a field may have none at a position it decided.
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
            self.release_queued_tables()?;
        }
        metrics.record_output(1, values(&chunk));
        // An evaluated chunk is addressed on its grid, so its decided positions are grid points; a
        // raw source carries timestamps, which are not positions on any grid.
        if let NativeChunk::Evaluated(chunk) = &chunk {
            metrics.record_grid_points(chunk.series.time.len());
        }
        Ok(Some(chunk))
    }

    /// Maps one evaluated chunk, keeping its axis and its closing.
    ///
    /// `None` means the operation emptied every position and the chunk does not close its series:
    /// there is nothing to hand on, and the input may still decide more.
    fn map(&mut self, map: &MapRuntime, chunk: EvaluatedChunk) -> Result<Option<EvaluatedChunk>> {
        let (positions, closing) = (chunk.series.time.len(), chunk.is_last);
        // The chunk is held for as long as this node works on it.
        self.buffer.grow(evaluated_bytes(&chunk))?;
        let metrics = Arc::clone(&self.buffer.metrics);
        let outcome = {
            // The timer covers this node's own work only: it is dropped before the driver polls
            // the input again.
            let _timer = metrics.timer();
            map_chunk(&map.schema, &chunk, &map.ops, &mut |bytes| {
                self.buffer.grow(bytes)
            })
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
            self.track(output)?;
        }
        // The chunk is gone and the label sets this node has handed out are what is left.
        let emitted = self.emitted_bytes();
        self.buffer.resize(emitted)?;
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
        let grid_points = schema.eval_grid.as_ref().map_or(0, |grid| {
            usize::try_from(grid.spec().len).unwrap_or(usize::MAX)
        });
        let scratch = grid_points
            .saturating_mul(std::mem::size_of::<i64>())
            .saturating_add(
                positions
                    .saturating_mul(
                        std::mem::size_of::<usize>().saturating_add(
                            schema
                                .fields
                                .len()
                                .saturating_mul(std::mem::size_of::<f64>() + 1),
                        ),
                    )
                    .saturating_mul(2),
            );
        self.buffer
            .grow(evaluated_bytes(&chunk).saturating_add(scratch))?;
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
        let output = match result? {
            Some(series) => Some(EvaluatedChunk::try_new(series, closing)?),
            None if closing => Some(no_row_chunk(schema, labels)?),
            None => None,
        };
        if let Some(output) = &output {
            metrics.record_grid_points(output.series.time.len());
            metrics.record_output(1, valid_values(output));
            self.track(output)?;
        }
        self.buffer.resize(self.emitted_bytes())?;
        Ok(output)
    }

    /// Broadcasts one chunk of the vector side with the constant this node was bound to.
    ///
    /// `None` means the operation emptied every position and the chunk does not close its series:
    /// there is nothing to hand on, and the vector side may still decide more. Nothing of the chunk
    /// is retained — the operation pairs one value with one constant — so what this node holds
    /// afterwards is the label sets it has handed out and nothing else.
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
        // The chunk is held for as long as this node works on it.
        self.buffer.grow(evaluated_bytes(&chunk))?;
        let metrics = Arc::clone(&self.buffer.metrics);
        let outcome = {
            // The timer covers this node's own work only: it is dropped before the driver polls
            // the vector side again.
            let _timer = metrics.timer();
            broadcast.apply(&chunk)
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
            if broadcast.drops_labels() {
                self.track(output)?;
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
    fn window(
        &mut self,
        window: &mut RangeEvaluator,
        merge: &mut Option<MergeInput>,
        chunk: NativeChunk,
    ) -> Result<Option<EvaluatedChunk>> {
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
        // The chunk is held for as long as the evaluator consumes it.
        self.buffer.grow(chunk.bytes())?;
        let metrics = Arc::clone(&self.buffer.metrics);
        let outcome = {
            let _timer = metrics.timer();
            match &chunk {
                // Raw samples are read window by window by the evaluator itself.
                NativeChunk::Raw(chunk) => window.push(chunk),
                // An evaluated input is read as computed samples: the evaluator carries the window
                // state of one series across its chunks and closes it on the chunk that says so.
                // Handing each chunk over as it arrives is what lets this window report a decision
                // before its input series has ended; buffering the chunks into a whole series first
                // would hold the input's bytes and delay every result to the end of the series.
                NativeChunk::Evaluated(chunk) => window.push_series(&chunk.series, chunk.is_last),
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
            self.track(output)?;
        }
        // The chunk has been consumed: what this node still holds is what the evaluator kept of it
        // and the label sets it has already handed out. Both are charged whether or not this chunk
        // produced one -- a chunk that yields nothing leaves the evaluator's samples and every
        // earlier series' history exactly where they were -- and neither is charged per chunk
        // consumed, which would make a fixed window over a long series read as an allocation
        // failure even though the window holds no more samples.
        let retained = window.retained_bytes();
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
        // The figure is read before the chunk is retained, because reserving after the fact would be
        // reserving for memory the node already holds.
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
        self.queue_results(output, false)
    }

    /// Retains one chunk of a vector node's input, reserved for before it is retained, as
    /// [`Self::retain_binary`] retains an operand's.
    fn retain_vector(&mut self, vector: &mut VectorRuntime, chunk: NativeChunk) -> Result<()> {
        let NativeChunk::Evaluated(chunk) = chunk else {
            return Err(DataFusionError::Execution(
                "the vector operation consumes evaluated chunks, not raw samples".to_string(),
            ));
        };
        // What a bound tree says the input produces is not proof of what a chunk carries, so the
        // chunk is checked against the layout before it is retained.
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
            let retained = vector.input.held;
            self.buffer.resize(retained)?;
        }
        metrics.record_input(1, positions);
        Ok(())
    }

    /// Runs this node's vector kernel and queues the series it kept.
    fn compute_vector(&mut self, vector: &mut VectorRuntime) -> Result<()> {
        let shares_input = matches!(vector.op, BoundVectorOp::Sort(_));
        let output = vector.compute(&mut self.buffer)?;
        let Some(output) = output else {
            return Ok(());
        };
        self.queue_results(output, shares_input)
    }

    /// Queues the series a kernel that reads its whole input produced, and settles the reservation
    /// on the queue.
    ///
    /// `shares_input` says the series are the assembled input itself, as a sort hands it back: a
    /// field there is one slice per input chunk of buffers and histogram tables the chunks shared,
    /// so each series is measured the way the input was ([`operand_chunk_bytes`]) rather than by
    /// the allocations its slices belong to, which would count a buffer once per chunk. The tables
    /// are billed once, to the queue ([`NodeState::queued_tables`]), since several results can
    /// keep one alive.
    fn queue_results(&mut self, output: Vec<Series>, shares_input: bool) -> Result<()> {
        let mut tables = HashSet::new();
        // The results are queued the way a node that produces a chunk per input queues what it
        // hands on: the queue is the one source of chunk order and of the bytes queued chunks hold.
        let mut closed = 0usize;
        for series in output {
            // The kernel decided every point of a result series at once, so the one chunk it is
            // handed over as closes it.
            // The result is already admitted, within the workspace reserved for the kernel. What the
            // queue keeps is the result as it is owned: its samples and its labels, which the chunk
            // keeps alive until it is handed on.
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
        // This node's own accounting of closed series is the results it closed, one per result
        // series: an input chunk that closed its series is a closure of the node below, and
        // counting it here as well would report the same work twice.
        self.buffer.metrics.record_completed_series(closed);
        // The input the reservation covered is gone and the queue holds what the kernel returned,
        // so the reservation settles on the queue: what a queued result is given back with as it
        // leaves is exactly the figure it is held for here.
        let queued = self
            .queued
            .iter()
            .fold(self.queued_tables, |total, (_, bytes)| {
                total.saturating_add(*bytes)
            });
        self.buffer.resize(queued)
    }

    /// Folds one chunk into this node's aggregate.
    ///
    /// The chunk is reserved for before it is folded, by what the node reports it is worth, and the
    /// reservation settles afterwards on the fold that is left. Unlike [`Self::retain_binary`], what
    /// stops being held here is the chunk itself: the fold keeps its states, so a chunk that closes
    /// its series leaves nothing behind and the node is empty of input again.
    fn retain_aggregate(
        &mut self,
        aggregate: &mut AggregateRuntime,
        chunk: NativeChunk,
    ) -> Result<()> {
        let NativeChunk::Evaluated(chunk) = chunk else {
            return Err(DataFusionError::Execution(
                "the aggregate consumes evaluated chunks, not raw samples".to_string(),
            ));
        };
        // The figure is read before the chunk is folded, because reserving after the fact would be
        // reserving for memory the fold already read.
        let pending = aggregate.pending(&chunk);
        let positions = chunk.series.time.len();
        self.buffer.grow(pending)?;
        let metrics = Arc::clone(&self.buffer.metrics);
        {
            let _timer = metrics.timer();
            aggregate.accumulate(chunk, &mut self.buffer, pending)?;
        }
        metrics.record_input(1, positions);
        // A chunk that closed its series closed it for the node below: a group is not a series, so
        // the fold completing nothing on it is why nothing is counted here. The series this node
        // closes are the ones it hands out, counted when it queues them.
        Ok(())
    }

    /// Finishes this node's fold and queues one chunk per group it produced.
    ///
    /// Each group is one series, decided in full by the fold, so the one chunk it is handed over as
    /// closes it: a consumer above reads "nothing more here" from that chunk rather than from another
    /// one arriving.
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
        // The fold has returned its series; reserve their handover before appending them to the queue.
        let Some(output) = output else {
            return Ok(());
        };
        let mut closed = 0usize;
        for series in output {
            let bytes = series_bytes(&series);
            let chunk = NativeChunk::Evaluated(EvaluatedChunk::try_new(series, true)?);
            self.queued.push_back((chunk, bytes));
            closed += 1;
        }
        // A group is one series and this node closes each of them exactly once, on the one chunk it
        // hands it over as.
        self.buffer.metrics.record_completed_series(closed);
        // The output Vec has been consumed; the node now holds only queue backing and chunks.
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

    /// Feeds one chunk to this node's instant selection.
    ///
    /// `None` means the series this chunk belongs to has not ended: the selection reads one whole
    /// axis, so it decides nothing until the chunk carrying `is_last` arrives.
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
            instant.push(chunk)
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
                self.settle_buffered(instant)?;
                return Err(error);
            }
        };
        if let Some(output) = &output {
            metrics.record_grid_points(output.series.time.len());
            metrics.record_output(1, valid_values(output));
            if instant.selection.timestamp {
                self.track(output)?;
            }
        }
        // What this node still holds is the chunks of the series it is selecting: a whole series is
        // held until it ends, so the reservation is what those chunks hold rather than a figure
        // that grows with the chunk count of anything already answered. The chunk just consumed and
        // the workspace of the gather it closed are both gone, so this is what gives them back.
        self.settle_buffered(instant)?;
        Ok(output)
    }

    /// Sets this node's reservation to the chunks of the series it is still gathering.
    ///
    /// The chunk the node just consumed and the workspace of a gather it closed are not part of it:
    /// the first has left the buffer and the second has been handed to the output.
    fn settle_buffered(&mut self, instant: &InstantRuntime) -> Result<()> {
        self.buffer.resize(
            instant
                .buffered_bytes()
                .saturating_add(self.emitted_bytes()),
        )
    }

    /// Retains and validates a fragment for standalone canonicalization.
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
        self.buffer
            .resize(merge.bytes.saturating_add(self.emitted_bytes()))
    }

    /// Canonicalizes all fragments before exposing any output.
    fn flush_canonical_merge(&mut self, merge: &mut Option<MergeInput>) -> Result<()> {
        let Some(merge) = merge.take() else {
            return Ok(());
        };
        if merge.open.is_some() {
            return Err(DataFusionError::Execution(
                "merge input ended inside a series".into(),
            ));
        }
        if merge.fragments.is_empty() {
            return self.buffer.shrink(merge.bytes);
        }
        let workspace = merge.workspace_bytes();
        self.buffer.grow(workspace)?;
        let held = merge.bytes.saturating_add(workspace);
        let merged = merge_same_labelset(&merge.schema, merge.fragments)?;
        let mut queued = 0usize;
        for series in merged {
            let chunk = EvaluatedChunk::try_new(series, true)?;
            self.buffer
                .metrics
                .record_grid_points(chunk.series.time.len());
            self.buffer.metrics.record_output(1, valid_values(&chunk));
            self.track(&chunk)?;
            // Label occupancy is retained alongside the held fragments/workspace before queuing.
            self.buffer.resize(
                held.saturating_add(self.emitted_bytes())
                    .saturating_add(queued),
            )?;
            let bytes = evaluated_bytes(&chunk);
            self.enqueue_output(chunk)?;
            queued = queued.saturating_add(bytes);
        }
        self.buffer.shrink(held)?;
        self.buffer
            .resize(self.emitted_bytes().saturating_add(queued))
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
        merge: &mut MergeInput,
        window: &RangeEvaluator,
        chunk: EvaluatedChunk,
    ) -> Result<Option<EvaluatedChunk>> {
        let EvaluatedChunk { series, is_last } = chunk;
        // The chunk is checked before it is charged for or kept, so a stream that breaks the
        // contract leaves nothing behind.
        merge.check(&series, is_last)?;
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
        let retained = window.retained_bytes();
        let emitted = self.emitted_bytes();
        self.buffer
            .resize(retained.saturating_add(emitted).saturating_add(merge.bytes))?;
        Ok(None)
    }

    /// Merges this window's collected input and evaluates it, one merged series at a time.
    ///
    /// Each merged series is a whole series: it is handed to the evaluator as one closing series, so
    /// the window reports on it once, and its chunk waits in this node's queue at its own estimate.
    /// The fragments and the merge's workspace stay charged for the whole flush and go back when it
    /// ends; the queued answers stay charged until the pull that hands each of them up.
    ///
    /// `merge` is taken here, so a window whose collected input failed to merge does not merge it
    /// again: the error has stopped the execution, and the fragments went with it.
    fn flush_merge(
        &mut self,
        window: &mut RangeEvaluator,
        merge: &mut Option<MergeInput>,
    ) -> Result<()> {
        let Some(merge) = merge.take() else {
            return Ok(());
        };
        if merge.fragments.is_empty() {
            // Nothing arrived, so there is nothing to merge and nothing to answer: the window's
            // evaluator never read a series and the charge goes back.
            return self.buffer.shrink(merge.bytes);
        }
        // The workspace the merge allocates is charged before it runs, from the rows, segments and
        // fields of the fragments as they were collected.
        let workspace = merge.workspace_bytes();
        let held = merge.bytes.saturating_add(workspace);
        let schema = Arc::clone(&merge.schema);
        self.buffer.grow(workspace)?;
        // What the queue holds before this flush adds to it: a queued answer is charged until the
        // pull that hands it up returns its estimate, so every resize below has to leave it charged.
        // The figure is folded forward rather than read back off the queue, because walking the
        // queue once per answer would make a flush cost a pass over what it produced so far.
        let mut queued_bytes = self
            .queued
            .iter()
            .fold(self.queued_tables, |total, (_, bytes)| {
                total.saturating_add(*bytes)
            });
        // The fragments' charge stands for the merged series too -- a merged run is a slice of a
        // fragment's own segment, so no value is copied -- and the workspace stays charged until
        // the flush is over.
        let merged = merge_same_labelset(&schema, merge.fragments)?;
        for series in merged {
            let output = {
                let metrics = Arc::clone(&self.buffer.metrics);
                let _timer = metrics.timer();
                window.push_series(&series, true)?
            };
            let Some(output) = output else {
                continue;
            };
            self.buffer
                .metrics
                .record_grid_points(output.series.time.len());
            self.buffer.metrics.record_output(1, valid_values(&output));
            self.track(&output)?;
            let retained = window.retained_bytes();
            let emitted = self.emitted_bytes();
            self.buffer.resize(
                retained
                    .saturating_add(emitted)
                    .saturating_add(held)
                    .saturating_add(queued_bytes),
            )?;
            let bytes = evaluated_bytes(&output);
            self.enqueue_output(output)?;
            queued_bytes = queued_bytes.saturating_add(bytes);
        }
        // The fragments and the merge's workspace go back, and whatever the flush produced stays
        // charged in the node's queue until the caller pulls it.
        self.buffer.shrink(held)
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
    /// This is not a source handover: an input slot's open-series flag and the input metrics follow
    /// the chunks the caller queued, and a chunk this node produced itself is neither.
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

    /// Records what this node hands out under its output label set.
    ///
    /// A node that rewrites labels is where two input series become indistinguishable, so this is
    /// where a second one is refused: a node above reads what this one hands over, and a window
    /// above would evaluate the two as two series and report both. Checking at the root would be
    /// too late for that — the answer would already have been computed.
    fn track(&mut self, output: &EvaluatedChunk) -> Result<()> {
        match self.emitted.as_mut() {
            Some(emitted) => emitted.occupy(output),
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

    /// Returns the shared state this node holds: its queue, its tables and its reservation.
    ///
    /// The operation's own payload — the evaluator, the retained operands, the fold — is not this
    /// state's: it belongs to the node that owns this state, and it is dropped there before this
    /// runs. Dropping it first is what keeps the reservation from going back to the pool while the
    /// memory it was taken for is still held.
    fn release(&mut self) {
        // The label sets this node handed out and the chunks it queued go first, for the same
        // reason: their memory is what the reservation below returns.
        self.emitted = None;
        self.queued.clear();
        self.queued.shrink_to_fit();
        self.queued_tables = 0;
        // The reservation is returned once, below, for everything this node still held.
        self.buffer.release();
    }
}

impl InputSlot {
    /// Queues one source chunk at this input, checking it against the binding the leaf was built
    /// with.
    ///
    /// The leaf's own schema says which kind of chunk it produces: a leaf bound to a grid is fed
    /// evaluated chunks, and one bound to raw samples is fed raw ones. A chunk of the other kind has
    /// no address here, so it is refused rather than queued and read as though it were the kind this
    /// leaf promised.
    fn enqueue(&mut self, chunk: NativeChunk) -> Result<()> {
        let bound = self.schema.eval_grid.is_some();
        let evaluated = matches!(chunk, NativeChunk::Evaluated(_));
        if evaluated != bound {
            return Err(DataFusionError::Execution(format!(
                "the native source is bound to {}, but {} chunk was queued at it",
                if bound {
                    "an evaluated series"
                } else {
                    "raw samples"
                },
                if evaluated { "an evaluated" } else { "a raw" },
            )));
        }
        self.state.enqueue(chunk)
    }

    /// Hands over the next queued source chunk, if there is one.
    ///
    /// The handover is what [`Self::open_source_series`] follows: after this call the flag says
    /// whether the series the consumer just received a chunk of is still open, which is what closing
    /// the execution reads to tell a truncated input from one that ended cleanly. Only an input leaf
    /// follows that flag, so only it reads the handover this way: any other node's queue holds the
    /// results of series that already closed on their way in.
    fn pop_source(&mut self) -> Result<Option<NativeChunk>> {
        let Some(chunk) = self.state.pop_queued(NativeChunk::positions)? else {
            return Ok(None);
        };
        // Assigned rather than only set: a chunk that closes its series is what says no series is
        // open any more, so the flag follows the handover and not the history of handovers.
        self.open_source_series = !chunk.is_last();
        if chunk.is_last() {
            self.state.buffer.metrics.record_completed_series(1);
        }
        Ok(Some(chunk))
    }

    /// Returns what this input still holds: its open-series flag, its queue and its reservation.
    ///
    /// A leaf that holds no chunk holds no series either, so nothing it handed over is still open
    /// for it: the flag is what closing the execution reads, and it must not survive the state that
    /// could have kept a series alive.
    fn release(&mut self) {
        self.open_source_series = false;
        self.state.release();
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

    /// Queues one raw source chunk at the input `input` names.
    ///
    /// A tree of unary nodes has one input; a binary node adds one per operand, so a tree of nested
    /// binary nodes has as many as it has leaves. The id is what says which leaf a chunk belongs to,
    /// and a chunk queued at an id the tree has no input for is refused rather than assigned to
    /// whichever input was found first.
    pub fn push_raw(&mut self, input: InputId, chunk: RawSeriesChunk) -> Result<()> {
        self.push_source(input, NativeChunk::Raw(chunk))
    }

    /// Queues one evaluated source chunk at the input `input` names.
    pub fn push_evaluated(&mut self, input: InputId, chunk: EvaluatedChunk) -> Result<()> {
        self.push_source(input, NativeChunk::Evaluated(chunk))
    }

    /// The next chunk the root produces, or `None` when nothing has settled yet.
    ///
    /// A window and a map take whole chunks from their input, so nothing beyond the chunk in hand
    /// is materialised on their edge -- except below a window that collects its input, which holds
    /// that input until [`Self::finish`]. A binary node that matches two vectors is the other
    /// exception: it pairs two whole vectors point by point, so it retains every chunk each operand
    /// hands over and reports `None` until [`Self::finish`] ends the input, which is when it
    /// computes its result. A binary node that broadcasts a constant is not one: it pairs each
    /// value of the vector side with the constant and hands the transformed chunk up as it arrives. A vector node
    /// reads its one input whole the same way.
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
            // The state's tree is the one this execution was built from, and a pull is asked of its
            // root; the nodes below it read their own children from there.
            match self.state.pull(self.inputs_finished) {
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
                // Nothing has settled: the stages below decide whether that is a stall or an end.
                Ok(None) => {}
                Err(error) => {
                    self.stop(&error);
                    return Err(error);
                }
            }
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
            match self.state.flush_ready_merges() {
                // A window answered: what it queued is pulled on the next turn, and that pull is
                // also what carries its answer to a node above it.
                Ok(true) => continue,
                Ok(false) => {}
                Err(error) => {
                    self.stop(&error);
                    return Err(error);
                }
            }
            if self.state.any_merge_pending() {
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

    /// Queues a source chunk at the input leaf `input` names.
    fn push_source(&mut self, input: InputId, chunk: NativeChunk) -> Result<()> {
        self.check_running()?;
        // An execution whose input ended reads no more of it: the ending stages run as the caller
        // pulls, and a chunk queued behind them would arrive at a node that has already decided
        // what to answer. The refusal leaves the execution alone rather than stopping it, because
        // what it still holds is the answer the caller is pulling.
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

    /// Queues a source chunk at one input leaf, checking it against that input's binding.
    fn queue_source(&mut self, input: InputId, chunk: NativeChunk) -> Result<()> {
        // The id counts the leaves left to right, so an id past the last one names no leaf at all:
        // a caller that miscounted its tree is refused rather than answered from whichever leaf the
        // count happened to reach.
        let count = self.state.inputs.len();
        let slot = self
            .state
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

    /// Stops the execution, returning every byte it reserved to the pool.
    ///
    /// A stopped execution is not resumed: an evaluator that failed must not be reused, so the
    /// chunks it already produced are the only ones the caller will see. Every node is released
    /// here -- its payload, its queue and its reservation -- so a stopped execution holds nothing
    /// while the caller still owns it.
    fn stop(&mut self, error: &DataFusionError) {
        self.state.release();
        self.phase = Phase::Failed(error.to_string());
    }
}

/// Maps one chunk with `ops`, keeping its axis and its closing.
///
/// Mapping is point-wise over valued positions: a float chunk keeps the rows it came with under
/// math, a chunk holding native histograms loses the rows only histograms filled, as math skips
/// them, and a histogram function keeps only the rows histograms filled; a position no field fills
/// is not a row of any chunk, and mapping does not invent one. `None` means the operation emptied
/// every position, which yields no chunk at all unless this chunk closed its series: a closing
/// chunk is still handed over, holding no position, because a consumer tells "nothing more here"
/// from "no more input" by that chunk alone.
///
/// `admit` is handed the bytes a label change can write before the change builds them; an error
/// from it stops the map.
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
        // `clamp` with `max < min` is the empty vector, math skips native histograms and a
        // histogram function skips floats, so a series left without a value of the kind the
        // operation reads has none left: every position it held was decided and holds no value
        // now, so there is no row to hand over.
        if !chunk.is_last {
            return Ok(None);
        }
        // The closing chunk names the series the map would have produced, so the operations not
        // run still decide its labels.
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

/// A series carrying `metric` and no row at all.
///
/// It is what a closing chunk holds when the operation emptied its input: the labels say which
/// series closed, and the axis is empty because no position holds a value. `metric` is the identity
/// the operation produces — a map drops the name, a filtering comparison keeps it — so the caller
/// says which, rather than this function deciding for it.
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

/// The values an evaluated chunk hands over, or the timestamps a raw source hands over.
fn evaluated_values(chunk: &NativeChunk) -> usize {
    match chunk {
        NativeChunk::Evaluated(chunk) => valid_values(chunk),
        NativeChunk::Raw(chunk) => chunk.time.len(),
    }
}

/// The error for a chunk of another series arriving while one is being selected.
///
/// [`select_instants`] reads one whole axis, so the buffered chunks have to be one series: a chunk
/// of another series has nowhere to go, and appending it would select its samples as though they
/// were the open series'.
fn series_changed() -> DataFusionError {
    DataFusionError::Execution(
        "the instant selection combines one series into one axis, and a chunk of another series \
         arrived before the first one ended"
            .to_string(),
    )
}

/// The error for a series an instant selection has no labels to report it under.
///
/// A source bound in label mode carries its identity encoded and no decoded labels, and a chunk
/// that is handed on has to name the series it holds.
fn no_labels() -> DataFusionError {
    DataFusionError::Execution(
        "the instant selection requires materialized series labels".to_string(),
    )
}

/// The error for a source row the selection named but the series does not hold.
///
/// The selection returns indices into the axis it was given, so a row outside it means the two
/// disagree rather than that a sample is missing.
fn missing_row(index: usize) -> DataFusionError {
    DataFusionError::Execution(format!(
        "the instant selection selected the source row {index}, which the series it read does not \
         hold"
    ))
}

/// The rejection of a chunk holding a value where the same field already holds one.
///
/// No merge resolves this one: at `position`, field `field` of the label set `labels` would carry
/// two values, which is what Prometheus rejects for two series whose label sets became equal.
fn occupied_position(
    labels: &LabelSet,
    field: usize,
    run: &Run,
    previous: &Run,
) -> DataFusionError {
    DataFusionError::Execution(format!(
        "the node produces two series under the label set {labels:?} that both hold a value of field \
         {field} at position {position}: one occupies {previous:?} and another {run:?}, and no merge \
         resolves two values of one field at one position",
        position = run.start.max(previous.start),
    ))
}

/// The rejection of an input that ended without the chunk closing its series.
///
/// It is what a window's evaluator reports for the input it reads, said by the node that handed the
/// chunks over instead: a tree with no window has no evaluator to hold the series, so the input node
/// is the one node left that can tell a series that ended from one that was cut off. The input is
/// named by the routing slot its chunks are handed to, which is the one identity a caller feeding
/// the execution knows it by.
fn truncated_input(input: InputId) -> DataFusionError {
    DataFusionError::Execution(format!(
        "the input at routing slot {} ended before the last chunk of a series: the last chunk it \
         handed over did not say `is_last`",
        input.index()
    ))
}

/// The rejection of a chunk routed to an input the execution does not have.
///
/// The id counts the leaves left to right, so an id past the last one names no leaf at all: a
/// caller that miscounted its tree is refused rather than answered from whichever leaf the count
/// happened to reach.
fn no_input(inputs: usize, input: InputId) -> DataFusionError {
    DataFusionError::Execution(format!(
        "the native execution has {inputs} inputs, and no input {} was queued at",
        input.index()
    ))
}

/// The error for a node asked to compute without the runtime its kind holds.
fn no_binary() -> DataFusionError {
    DataFusionError::Execution("the node holds no binary operation".to_string())
}

/// The error for an aggregate node whose fold is missing.
fn no_aggregate() -> DataFusionError {
    DataFusionError::Execution("the node holds no aggregate".to_string())
}

/// The error for a node asked to run a vector operation without one.
fn no_vector() -> DataFusionError {
    DataFusionError::Execution("the node holds no vector operation".to_string())
}

/// The error for a node asked to broadcast without the broadcast its kind holds.
fn no_broadcast() -> DataFusionError {
    DataFusionError::Execution("the node holds no scalar broadcast".to_string())
}

/// The error for a node asked to map without the operation its kind holds.
fn no_map() -> DataFusionError {
    DataFusionError::Execution("the node holds no map operation".to_string())
}

/// The error for a node asked to select without the selection its kind holds.
fn no_instant() -> DataFusionError {
    DataFusionError::Execution("the node holds no instant selection".to_string())
}

/// The error for a node asked to evaluate a window without its evaluator.
fn no_window() -> DataFusionError {
    DataFusionError::Execution("the node holds no window evaluator".to_string())
}

/// The rejection of a scalar broadcast whose matching is not the one a constant can take.
///
/// A constant has no labels, so there is nothing to match it against: Prometheus does not consult
/// the matching modifiers of a vector–scalar operation at all. A tree that carries them therefore
/// describes an operation that is not the one it would compute, which is refused rather than run
/// as if the modifiers were absent.
fn matched_scalar() -> DataFusionError {
    DataFusionError::NotImplemented(
        "the native runtime does not execute a scalar broadcast whose constant carries matching \
         modifiers: a constant has no labels to match, so the operation this Binary describes is \
         not the one it would compute"
            .to_string(),
    )
}

/// The rejection of a binary node whose operands are neither two evaluated vectors nor one
/// evaluated vector and one constant.
///
/// `operands` says which shape arrived and why that shape has no place here, so a tree a later
/// stage compiles is refused for a reason a reader can act on.
fn unsupported_binary(operands: &str) -> DataFusionError {
    DataFusionError::NotImplemented(format!(
        "the native runtime does not execute this Binary node: it reads {operands}, and this \
         runtime reads two evaluated vectors or one evaluated vector and one constant"
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
                "two constants, which broadcast over no vector",
            ));
        }
        // Raw samples or aggregate state: `validate` already refused either as a binary operand, so
        // this arm is the exhaustiveness backstop rather than a reachable case.
        (_, _) => {
            return Err(unsupported_binary(
                "an operand that is neither evaluated samples nor a constant, which no validated \
                 tree binds as a binary operand",
            ));
        }
    };
    let NativeExpr::Scalar(BoundScalar { value }) = &constant.expr else {
        // NotImplemented rather than Execution: this is a shape the runtime does not support, not a
        // tree whose values are wrong. A caller that falls back to another planner on NotImplemented
        // then still can, which an Execution would deny it.
        return Err(DataFusionError::NotImplemented(
            "the native runtime does not execute this Binary node, which broadcasts an operand that \
             is not a literal scalar: the constant side has to be a Scalar node, and a computed \
             constant such as `1 < bool 2` is not one until it is folded"
                .to_string(),
        ));
    };
    // A constant has no labels, so the modifiers a vector–vector operation would use have no
    // meaning here: a tree that carries them describes another operation than the one it computes.
    if matching.labels != MatchingLabels::Ignoring(Vec::new())
        || matching.cardinality != Cardinality::OneToOne
        || matching.fill != FillValues::default()
    {
        return Err(matched_scalar());
    }
    Ok(BroadcastRuntime {
        operand: scalar_operand(*value),
        op: broadcast_op(matching)?,
        sample_schema: Arc::clone(vector),
    })
}

/// The operation a binary node's `matching` describes.
///
/// A comparison is a filter unless it was bound as `bool`, and a filter keeps the label set it
/// read while every other operation drops the metric name. Arithmetic has no `bool` form, so one
/// bound that way describes a node the compiler does not produce and is refused.
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
/// A binary or set node reserves its retained operands and the kernel's live workspace together,
/// and the kernel reports the second as a size of its own. A sum of the two that does not fit an
/// address space cannot be reserved, so the execution is refused rather than run with a figure
/// that wrapped.
fn reservation_overflow(node: &str) -> DataFusionError {
    DataFusionError::Execution(format!(
        "the operands and the kernel's workspace of {node} do not fit one address space: the bytes \
         it would reserve overflow"
    ))
}

fn series_error(error: datatypes::error::Error) -> DataFusionError {
    DataFusionError::External(Box::new(error))
}
