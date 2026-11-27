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

//! The run-time state of an aggregate node.
//!
//! An aggregate folds a whole vector into one series per group, so its node is the one that cannot
//! answer as its input arrives: a group's value at a grid point is the fold of every value every
//! member of the group has there, and the last member to arrive is the one that decides it. What the
//! node keeps between chunks is therefore the fold's states and nothing else -- a chunk is folded as
//! it arrives and dropped -- and it produces once, when the input ends.
//!
//! That is also why this runtime cannot tell an incomplete group from a complete one: it folds what
//! it is handed. A [`AggregateStage::Complete`] aggregate is correct only if every contribution to
//! each group lies within the streams this execution reads, which is the physical plan's job -- the
//! caller that schedules scans and exchanges guarantees it, and a plan that ran a `Complete`
//! aggregate once per partition and then combined the answers would be combining values that were
//! each missing the other partitions' contributions. The stages that exist for that shape are
//! [`AggregateStage::Partial`] and [`AggregateStage::Final`], which carry kernel accumulator
//! state between executions.
//!
//! `quantile` is the one aggregate here that is not a fold of each point's values. Its answer is the
//! φ-quantile of the group's whole sample set at that point, so there is no fixed-width accumulator
//! that answers it, and no state that combines two partitions' quantiles into the quantile of their
//! union. What the node keeps instead is its input: every series it was handed, until the input
//! ends, when [`crate::aggregate::aggregate_quantile`] answers from all of it at once. That is also
//! why `quantile` exists only as [`AggregateStage::Complete`] here, and why a plan that would have
//! to fold it per partition has to keep the previous route instead of splitting it.
//!
//! There is no spill either: a `quantile` keeps the whole input of every group, at once, until the
//! last chunk arrives, so what it holds is not bounded by the number of groups the way a fixed-width
//! accumulator's is -- it is bounded by the pool. Each chunk is admitted before it is kept and the
//! reservation settles on the input retained so far ([`AggregateRuntime::accumulate`]), so a
//! statement whose groups' samples do not fit the pool is refused rather than served from disk, and
//! a caller that needs an answer in bounded memory needs a wider pool rather than a wider node.
//! Everything else in this runtime trades memory for the width of a point: a fold's state per group
//! and point, this one's whole input.

use std::collections::HashSet;
use std::sync::Arc;

use common_query::promql_annotations::PromqlAnnotationCollector;
use datafusion::error::{DataFusionError, Result};
use datatypes::series::SeriesLabels;
use datatypes::series::samples::{FieldSpec, Series, SeriesSchema};
use datatypes::series::time::{EvalGrid, GridRange, TimeAxis, union_positions};

use crate::aggregate::{
    AggregateOp, FloatAggregator, FloatMerger, Grouping, HistogramEvidence, HistogramSamples,
    aggregate_quantile, histogram_samples, validate_aggregate_series,
};
use crate::evaluated::EvaluatedChunk;
use crate::grid_series::{GridSeriesBuilder, series_bytes};
use crate::map::{constant_series, validate_grid_series};
use crate::native::execute::{
    NodeBuffer, chunk_bytes_beside_samples, evaluated_bytes, unbilled_sample_buffers,
    unbilled_tables,
};
use crate::native::plan::{
    AggregateKind, AggregateStage, AggregateStateSchema, BoundAggregate, Grouping as BoundGrouping,
    aggregate_state_binding,
};
use crate::native::state::{
    StateChunk, state_bytes, state_chunk_from_partial, state_row_from_frame_row,
};

/// The output shape produced by an aggregate stage.
pub(crate) enum AggregateOutput {
    Samples(Vec<Series>),
    State(StateChunk),
}

/// What a `Complete` aggregate keeps while its input arrives.
///
/// The eight point-wise aggregates fold each grid point's values into fixed-width state; `quantile`
/// keeps the samples themselves, because the φ-quantile of a group is not a value its members' values
/// fold to.
#[derive(Clone, Copy)]
enum Fold {
    /// One fixed-width accumulator per point and field.
    Pointwise(AggregateOp),
    /// The whole input, kept until it ends.
    Quantile,
}

/// The input a `quantile` aggregate keeps, until it has all of it.
///
/// The members are the series the node was handed, in the order they arrived: that order is the order
/// [`aggregate_quantile`] folds a group's members in, and a series delivered in several chunks
/// contributes at each position it holds, so the pieces of one series answer what the whole series
/// would have. Nothing here is grouped: the kernel groups, and it groups the same way for a chunk as
/// for a series of its own.
struct QuantileInput {
    /// The φ the query wrote, when the query wrote it: a fold that bound its argument folds with
    /// this one number at every point. The kernel reads it as Prometheus does, so a value outside
    /// `[0, 1]` answers `-Infinity` or `+Infinity` rather than being refused here. `None` for a fold
    /// whose φ is read per point, which is what `parameter` holds instead.
    quantile: Option<f64>,
    /// The φ read per point, when the query left it to the statement's own evaluation: the one
    /// unlabelled scalar series the node held from its parameter child, on this node's grid.
    ///
    /// It is kept beside the retained input because the kernel reads it per point, exactly as the
    /// parameter a bound φ is turned into is read: `q` with no sample at a point leaves the point
    /// unanswered, as it does for the built one.
    parameter: Option<Series>,
    /// One entry per series this node was handed.
    series: Vec<Series>,
    /// The histogram tables the retained chunks index into, by the address of the allocation behind
    /// their `Arc`.
    ///
    /// A table is one allocation however many chunks of a series index into it, so it is billed once
    /// -- by the first chunk that references it -- exactly as a binary operand bills the tables of
    /// the chunks it retains. That is what keeps a series delivered in a thousand chunks from costing
    /// a thousand tables, and what keeps a table from costing nothing: the per-chunk figure leaves
    /// its tables out ([`chunk_bytes_beside_samples`]), so a table in neither this set nor a
    /// reservation is a table nothing covers.
    tables: HashSet<usize>,
    /// The Arrow allocations the retained chunks' samples live in, by the address of the allocation
    /// behind their `Buffer`.
    ///
    /// The chunks of one Arrow batch are the same story as its tables one level down, and the same
    /// answer: what a retainer keeps is the allocation its slices point into, and one allocation is
    /// one however many chunks slice it, so it is billed once -- by the first chunk that references
    /// it ([`unbilled_sample_buffers`]) -- rather than once per chunk. That is what keeps a series
    /// handed over as a thousand slices of one buffer from costing that buffer a thousand times. A
    /// chunk that holds a small slice of a large allocation is charged the allocation, because the
    /// allocation is what stays resident; its rows would be the smaller and wrong figure.
    buffers: HashSet<usize>,
    /// The bytes the retained chunks hold, folded in as they arrive.
    ///
    /// Each chunk is measured once, by what it holds on its own ([`chunk_bytes_beside_samples`]):
    /// its axis, its descriptors and its labels, beside the allocations it is the first to reference
    /// -- the Arrow buffers its samples live in and the histogram tables they index into, each
    /// billed once as [`unbilled_sample_buffers`] and [`unbilled_tables`] report them. Neither the
    /// allocation those samples are slices of nor the struct that carries the chunk is part of the
    /// figure: the first is billed as the allocation it is and not as the rows a chunk holds of it,
    /// and the second is the slot the vector holds the chunk in. [`Self::bytes`] adds that vector
    /// from the capacity it holds, which is why the struct is not counted here as well.
    held: usize,
}

impl QuantileInput {
    /// The bytes this input holds: the chunks it retained and the vector that carries them.
    ///
    /// The retained figure is [`Self::held`], which is folded in chunk by chunk, and the vector is
    /// counted by the capacity it really holds -- the capacity `retain_quantile` reserved before it
    /// grew -- so this is what the node has to reserve rather than a second estimate of it.
    ///
    /// The two do not overlap. A chunk's `Series` is carried inside one slot of that vector, so
    /// `held` counts what the chunk holds beside the struct ([`chunk_bytes_beside_samples`]) and the
    /// capacity here counts every slot the vector owns, occupied or not. Counting the struct in both
    /// would charge each occupied slot twice; counting it in neither would leave every slot the
    /// vector holds uncovered.
    ///
    /// The parameter is the third figure: a φ read per point is one series this node holds from the
    /// moment its parameter child hands it over until the kernel reads it, so it is counted from
    /// [`series_bytes`] however long it stays. A φ the query bound is not held at all - it is
    /// rebuilt from the binding's one number when the fold answers.
    ///
    /// That series sits inline, in the `Option<Series>` slot of this struct, so the descriptor is
    /// what `size_of::<Self>()` above already counts: the parameter adds the heap beside it and not
    /// a second descriptor. [`series_bytes`] measures a series it takes by reference, so it counts
    /// the struct as well; the subtraction here is what keeps one held series from being billed one
    /// descriptor twice.
    fn bytes(&self) -> usize {
        std::mem::size_of::<Self>()
            .saturating_add(
                self.series
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Series>()),
            )
            .saturating_add(self.parameter.as_ref().map_or(0, |parameter| {
                series_bytes(parameter).saturating_sub(std::mem::size_of::<Series>())
            }))
            .saturating_add(self.held)
    }
}

/// One aggregate node's states, from the first chunk it is handed to the series it finishes with.
pub(crate) struct AggregateRuntime {
    /// What the node computes, as the tree bound it: the kind it folds by and the stage it reports.
    op: BoundAggregate,
    /// The layout its input and output share: the fields every chunk has and the grid they are
    /// addressed on. A `Complete` aggregate neither adds nor drops a field, and it evaluates on its
    /// input's grid, so one schema describes both sides.
    schema: Arc<SeriesSchema>,
    /// What this node keeps between chunks, as the runtime's own enum, so the chunk check can ask
    /// what the operation accepts.
    fold: Fold,
    /// The query's annotation collector, once the driver installed it. Only an operation that skips
    /// histogram samples has anything to say, and it says it once.
    annotations: Option<PromqlAnnotationCollector>,
    /// The fold. `None` once [`Self::compute`] has taken it, which is also what says the node has
    /// nothing left to fold into.
    accumulator: Option<FloatAggregator>,
    /// The whole input of a `quantile` aggregate. `None` for every other kind, and `None` once the
    /// kernel has answered from it.
    retained: Option<QuantileInput>,
    merger: Option<FloatMerger>,
    state_schema: Option<Arc<AggregateStateSchema>>,
    /// Whether the fold has been finished and its series handed over. A node computes once: a second
    /// pull finds the series it queued rather than a fold that ran again.
    ///
    /// The driver reads this to decide whether the node settled: an aggregate answers only once its
    /// fold has run, so a child with nothing left to hand over is not an answer.
    pub(crate) computed: bool,
}

impl AggregateRuntime {
    /// The PromQL name of the aggregate this node folds, as an annotation names it.
    fn aggregate_name(&self) -> &'static str {
        match self.fold {
            Fold::Pointwise(AggregateOp::Sum) => "sum",
            Fold::Pointwise(AggregateOp::Count) => "count",
            Fold::Pointwise(AggregateOp::Avg) => "avg",
            Fold::Pointwise(AggregateOp::Min) => "min",
            Fold::Pointwise(AggregateOp::Max) => "max",
            Fold::Pointwise(AggregateOp::Group) => "group",
            Fold::Pointwise(AggregateOp::Stddev) => "stddev",
            Fold::Pointwise(AggregateOp::Stdvar) => "stdvar",
            Fold::Quantile => "quantile",
        }
    }

    /// Gives this node the query's collector, so it can report what it skipped.
    pub(crate) fn set_annotations(&mut self, collector: PromqlAnnotationCollector) {
        self.annotations = Some(collector);
    }

    /// What this node computes, for a rejection that has to say what it refused.
    ///
    /// It names the kind this node computes, which distinguishes it from another node in the tree.
    fn describe(&self) -> String {
        format!("{:?}", self.op.kind)
    }

    /// The states of a `Complete` aggregate over `schema`.
    ///
    /// A stage other than `Complete` and a kind without a supported kernel are refused here, before a single
    /// chunk is folded: a node this runtime cannot compute must not start, because a partial answer
    /// would be one the caller never asked for. The rejection names what was found, so a reader can
    /// tell a tree this runtime cannot run from one it cannot run correctly.
    pub(crate) fn new(
        op: BoundAggregate,
        schema: Arc<SeriesSchema>,
        buffer: &mut NodeBuffer,
    ) -> Result<Self> {
        let kind = match op.kind {
            AggregateKind::Sum => AggregateOp::Sum,
            AggregateKind::Count => AggregateOp::Count,
            AggregateKind::Avg => AggregateOp::Avg,
            AggregateKind::Min => AggregateOp::Min,
            AggregateKind::Max => AggregateOp::Max,
            AggregateKind::Group => AggregateOp::Group,
            AggregateKind::Stddev => AggregateOp::Stddev,
            AggregateKind::Stdvar => AggregateOp::Stdvar,
            // The one kind with no accumulator: it keeps its input instead, which is a state of its
            // own and is built by [`Self::new_quantile`]. The φ is the only parameter an aggregate
            // carries, and the arm that names the kind is the one place it is read.
            AggregateKind::Quantile { quantile } => {
                return Self::new_quantile(op, Some(quantile), schema, buffer);
            }
        };
        let (state_kind, state_components) = aggregate_state_binding(op.kind, op.field_kind)
            .ok_or_else(|| {
                DataFusionError::NotImplemented("aggregate kind has no mergeable state".into())
            })?;
        let state_schema = schema.eval_grid.as_ref().map(|grid| {
            Arc::new(AggregateStateSchema {
                group: op.group.clone(),
                kind: state_kind,
                components: state_components.into(),
                accumulators: schema.fields.clone(),
                grid: grid.clone(),
            })
        });
        if op.stage == AggregateStage::Final {
            let merger = FloatMerger::try_new(&schema, kind)?;
            buffer.resize(merger.state_bytes())?;
            return Ok(Self {
                op,
                fold: Fold::Pointwise(kind),
                annotations: None,
                schema,
                accumulator: None,
                retained: None,
                merger: Some(merger),
                state_schema,
                computed: false,
            });
        }
        let name_count = match &op.group {
            BoundGrouping::By(labels) | BoundGrouping::Without(labels) => labels.len(),
        };
        let name_bytes = name_count.saturating_mul(std::mem::size_of::<&str>());
        buffer.grow(name_bytes)?;
        let names: Vec<&str> = match &op.group {
            BoundGrouping::By(labels) | BoundGrouping::Without(labels) => {
                labels.iter().map(|label| label.as_ref()).collect()
            }
        };
        let grouping = match &op.group {
            BoundGrouping::By(_) => Grouping::By(&names),
            BoundGrouping::Without(_) => Grouping::Without(&names),
        };
        let name_bytes = names.len().saturating_mul(std::mem::size_of::<&str>());
        let initial = FloatAggregator::try_new_budgeted(&schema, kind, grouping, |bytes| {
            buffer.resize(bytes.saturating_add(name_bytes))
        })?;
        drop(names);
        buffer.resize(initial.state_bytes())?;
        Ok(Self {
            op,
            fold: Fold::Pointwise(kind),
            annotations: None,
            schema,
            accumulator: Some(initial),
            retained: None,
            merger: None,
            state_schema,
            computed: false,
        })
    }

    /// The states of a `Complete` `quantile` aggregate over `schema`, whose φ is `quantile`.
    ///
    /// A `quantile` is answered from its group's whole sample set, so a stage that saw only part of
    /// a group would answer the quantile of a subset of it: the quantile of two partitions'
    /// quantiles is not the quantile of their union, and there is no state to export or merge
    /// either. That is a property of the operation and not of this runtime, so what refuses a
    /// `Partial` or a `Final` one is the tree's own check -- [`aggregate_state_binding`] has no
    /// binding for a `quantile`, which [`crate::native::plan::NativeNode::validate`] reports, and
    /// [`crate::native::split`] refuses a `quantile` for the same reason -- and this starts the one
    /// stage that is left.
    ///
    /// The same check is what puts the input on a grid: a `Complete` aggregate is validated to
    /// answer its input's values on its input's grid, and `schema` is the layout the node reports,
    /// so the grid the kernel reads its parameter on is there before this runs.
    fn new_quantile(
        op: BoundAggregate,
        quantile: Option<f64>,
        schema: Arc<SeriesSchema>,
        buffer: &mut NodeBuffer,
    ) -> Result<Self> {
        let retained = QuantileInput {
            quantile,
            parameter: None,
            series: Vec::new(),
            tables: HashSet::new(),
            buffers: HashSet::new(),
            held: 0,
        };
        buffer.resize(retained.bytes())?;
        Ok(Self {
            op,
            fold: Fold::Quantile,
            annotations: None,
            schema,
            accumulator: None,
            retained: Some(retained),
            merger: None,
            state_schema: None,
            computed: false,
        })
    }

    /// The states of a `Complete` aggregate whose one argument is read at every grid point.
    ///
    /// Only `quantile` has such an argument, and its fold keeps no number of its own: the φ arrives
    /// as a series from the node's parameter child ([`Self::hold_parameter`]), so the binding's
    /// constant is not read here at all. Everything else about the fold is the bound one's -- the
    /// stage, the layout, and the sample set it keeps until the input ends -- which is why this is
    /// [`Self::new_quantile`] with no constant to fall back on rather than a second kind of state.
    pub(crate) fn new_parameterized(
        op: BoundAggregate,
        schema: Arc<SeriesSchema>,
        buffer: &mut NodeBuffer,
    ) -> Result<Self> {
        if !matches!(op.kind, AggregateKind::Quantile { .. }) {
            return Err(DataFusionError::NotImplemented(format!(
                "the runtime reads a per-point argument only for `quantile`, not for {:?}",
                op.kind
            )));
        }
        Self::new_quantile(op, None, schema, buffer)
    }

    /// Keeps the φ this node reads at every grid point, in the one unlabelled scalar series its
    /// parameter child handed over.
    ///
    /// The series is already reserved: it arrives through the driver's `hold_scalar`, which grows the
    /// same reservation by [`series_bytes`] before the chunk is kept, exactly as it does for a
    /// broadcast scalar or a `topk` parameter. Adopting it therefore only settles on what the node
    /// holds -- which counts it once, from [`QuantileInput::bytes`] -- rather than growing for a
    /// second allocation that was never made: a pool that holds the handoff must not refuse a
    /// handoff that allocates nothing.
    pub(crate) fn hold_parameter(
        &mut self,
        parameter: Series,
        buffer: &mut NodeBuffer,
    ) -> Result<()> {
        let Some(retained) = self.retained.as_mut() else {
            return Err(no_fold(&self.describe()));
        };
        if retained.parameter.is_some() {
            return Err(DataFusionError::Execution(
                "a `quantile` aggregate reads one parameter, and it already holds one".into(),
            ));
        }
        retained.parameter = Some(parameter);
        buffer.resize(retained.bytes())
    }

    /// What consuming `chunk` reserves while it is consumed.
    ///
    /// A point-wise fold drops the chunk at the end of the call and keeps only its states, so the
    /// charge is the chunk's own bytes -- the figure [`evaluated_bytes`] reports, which is one pass
    /// over the chunk and no state between chunks -- and not the `operand_chunk_bytes` figure a
    /// binary operand keeps, which exists because such an operand holds a series for the whole
    /// execution and so must count the slices of one buffer instead of the buffer once per chunk.
    ///
    /// A `quantile` keeps the chunk instead, and what it holds is not the chunk alone: it is the
    /// allocations the chunk's samples live in, billed once each, beside the chunk's own descriptors
    /// ([`Self::retain_quantile`], which is [`chunk_bytes_beside_samples`] plus
    /// [`unbilled_sample_buffers`] plus [`unbilled_tables`]). That figure is read from the chunk by
    /// the retainer itself, before it retains it, so nothing is held before it is reserved. Booking
    /// the chunk again here would reserve the same backing twice -- once for the chunk being handed
    /// over and once for the allocations it points into -- which is not a safer estimate but a
    /// refusal of an input whose own figure the pool admits.
    pub(crate) fn pending(&self, chunk: &EvaluatedChunk) -> usize {
        match self.fold {
            Fold::Pointwise(_) => evaluated_bytes(chunk),
            Fold::Quantile => 0,
        }
    }

    /// Folds one chunk into its group.
    ///
    /// A chunk that closes its series closes nothing here: a group is not a series, so the last
    /// contribution of one member says nothing about the group. The fold keeps its states, which is
    /// what makes a group's answer its whole group's answer once the input ends.
    ///
    /// A `quantile` keeps the chunk itself, for the same reason: the answer is the group's sample set,
    /// so the samples have to survive until the input does.
    pub(crate) fn accumulate(
        &mut self,
        chunk: EvaluatedChunk,
        buffer: &mut NodeBuffer,
        pending: usize,
    ) -> Result<()> {
        let Fold::Pointwise(kind) = self.fold else {
            return self.retain_quantile(chunk, buffer);
        };
        // `Series` keeps its fields public, so a chunk can be built by struct literal against
        // another schema; the states are indexed by absolute position on this node's grid, so a
        // chunk of another field count or another grid would be folded into the wrong places.
        validate_aggregate_series(&self.schema, &chunk.series, kind)
            .map_err(|error| folded_chunk(&self.describe(), error))?;
        if histogram_samples(kind) == HistogramSamples::Ignored
            && !crate::map::is_float_series(&chunk.series)
            && let Some(collector) = &self.annotations
        {
            // Prometheus reports once that it skipped samples it cannot order or average over; the
            // fold keeps going on the float samples it can read.
            collector.record_info(format!(
                "{}: ignored native histogram samples",
                self.aggregate_name()
            ));
            // The collector keeps the message, so the node has said its piece.
            self.annotations = None;
        }
        let Some(accumulator) = self.accumulator.as_mut() else {
            return Err(no_fold(&self.describe()));
        };
        // Keep the chunk covered while the fold decides whether it needs a new group or block.
        accumulator.update_budgeted(&chunk.series, |fold_bytes| {
            buffer.resize(pending.saturating_add(fold_bytes))
        })?;
        drop(chunk);
        buffer.resize(accumulator.state_bytes())
    }

    /// Keeps one chunk of a `quantile`'s input until the input ends.
    ///
    /// The chunk is kept whole rather than folded: the answer is the φ-quantile of the group's sample
    /// set, and no fixed-width state holds a sample set. What it costs is measured the way a binary
    /// operand measures the chunks it retains, and for the same reason: what a `quantile` keeps is a
    /// series for the rest of the execution, so the allocation one buffer its chunks are slices of is
    /// counted once rather than once per chunk -- and counted at all when the chunk holds a small
    /// slice of it, because the allocation is what stays resident. It is the chunk's own descriptors
    /// ([`chunk_bytes_beside_samples`]) beside those allocations: the Arrow buffers its samples live
    /// in, once each ([`unbilled_sample_buffers`]), and the histogram tables they index into, once
    /// each ([`unbilled_tables`]). The figure is reserved before the push -- the push is what makes
    /// the node hold it -- and the reservation then settles on the whole input, which is what this
    /// node holds between chunks.
    fn retain_quantile(&mut self, chunk: EvaluatedChunk, buffer: &mut NodeBuffer) -> Result<()> {
        // A quantile reads float values and skips a histogram sample, so a chunk only has to fit the
        // schema and the grid -- exactly what the kernel's own input check asks for.
        validate_grid_series(&self.schema, &chunk.series)
            .map_err(|error| folded_chunk(&self.describe(), error))?;
        if !crate::map::is_float_series(&chunk.series)
            && let Some(collector) = &self.annotations
        {
            // Prometheus reports once that it skipped samples it cannot order; the kernel folds the
            // float samples it can read.
            collector.record_info(format!(
                "{}: ignored native histogram samples",
                self.aggregate_name()
            ));
            self.annotations = None;
        }
        let Some(retained) = self.retained.as_mut() else {
            return Err(no_fold(&self.describe()));
        };
        // What this chunk adds to what the node holds: the allocations its samples live in and the
        // histogram tables it is the first chunk to reference -- each billed once, because one
        // allocation is one however many chunks slice it -- plus the descriptors the chunk keeps.
        let (buffers, buffer_bytes) = unbilled_sample_buffers(&chunk.series, &retained.buffers);
        let (tables, table_bytes) = unbilled_tables(&chunk.series, &retained.tables);
        let bytes = chunk_bytes_beside_samples(&chunk.series)
            .saturating_add(buffer_bytes)
            .saturating_add(table_bytes);
        buffer.grow(bytes)?;
        // The slot the chunk is about to take, and - when the vector has to grow for it - the
        // capacity it grows to. `Vec::push` doubles, so booking one slot would let the real
        // allocation outrun what the pool admitted; the target is booked instead, and the settle
        // below then reads back the exact capacity the vector holds. The vector's slots are not part
        // of `held`: [`QuantileInput::bytes`] counts them from that capacity, so booking them here
        // and folding them there would charge them twice.
        let needed = retained.series.len().saturating_add(1);
        if needed > retained.series.capacity() {
            let target = retained.series.capacity().saturating_mul(2).max(needed);
            buffer.grow(
                target
                    .saturating_sub(retained.series.capacity())
                    .saturating_mul(std::mem::size_of::<Series>()),
            )?;
            retained
                .series
                .try_reserve_exact(target.saturating_sub(retained.series.len()))
                .map_err(|error| {
                    DataFusionError::Execution(format!(
                        "cannot retain the `quantile` input: {error}"
                    ))
                })?;
        }
        retained.series.push(chunk.series);
        // From here the node keeps those tables and allocations alive itself, so a chunk arriving
        // later that references one is charged its own descriptors and not the allocation again.
        retained.tables.extend(tables);
        retained.buffers.extend(buffers);
        retained.held = retained.held.saturating_add(bytes);
        buffer.resize(retained.bytes())
    }

    /// Merges one state chunk into this final aggregate.
    ///
    /// State reaches a `Final` from exactly two places and is merged here, once: a producer in this
    /// process hands over the groups it folded ([`StateChunk::Series`]), and a region's exported
    /// frame is decoded into the row it carries ([`StateChunk::Row`], which a merge scan reads
    /// across processes). Both are read as the rows the merge kernel takes, so there is one merge
    /// and not one per source; the series form is turned into those rows here, inside the kernel,
    /// instead of by a caller that would have to know what the kernel eats.
    pub(crate) fn accumulate_state(
        &mut self,
        chunk: StateChunk,
        buffer: &mut NodeBuffer,
        pending: usize,
    ) -> Result<()> {
        let expected = self.state_schema.as_ref().ok_or_else(|| {
            DataFusionError::NotImplemented("this runtime does not merge aggregate state".into())
        })?;
        // The one conversion this edge performs, and it is here rather than at the API boundary: a
        // producer hands over the groups it folded, and the kernel merges rows. Each source is
        // validated once, where its rows are made: the series form by the row layout it is built
        // against, a decoded row by the checks that layout makes of a group, so the merge below only
        // merges and a group costs its own encoding.
        let rows = match chunk {
            StateChunk::Row(row) => {
                if row.fields.kind() != expected.kind
                    || row.fields.components() != expected.components.as_ref()
                {
                    return Err(DataFusionError::NotImplemented(
                        "this runtime does not merge that kind of state".into(),
                    ));
                }
                if row
                    .time
                    .grid()
                    .is_none_or(|grid| grid.spec() != expected.grid.spec())
                {
                    return Err(DataFusionError::Execution(
                        "aggregate state grid mismatch".into(),
                    ));
                }
                if row.fields.len() != self.schema.fields.len() {
                    return Err(DataFusionError::Execution(
                        "aggregate state field count mismatch".into(),
                    ));
                }
                vec![row]
            }
            StateChunk::Series {
                series,
                state_schema: _,
                reservation: _reservation,
            } => {
                // The state layout and the Arrow shape a row is encoded against depend on the chunk,
                // not on the group, so they are resolved once for all of its groups. The grid is
                // checked here for the same reason: every row of this chunk lands on the series grid
                // the layout encodes against, so comparing it once says what a comparison per row
                // said before.
                if self.schema.eval_grid.as_ref().map(|grid| grid.spec())
                    != Some(expected.grid.spec())
                {
                    return Err(DataFusionError::Execution(
                        "aggregate state grid mismatch".into(),
                    ));
                }
                let declared = datatypes::series::aggregate_state::AggregateStateSchema::try_new(
                    expected.kind,
                    expected.components.to_vec(),
                )
                .map_err(|error| DataFusionError::Execution(error.to_string()))?;
                let layout =
                    datatypes::series::frame::StateRowLayout::try_new(&declared, &self.schema)
                        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
                let mut rows = Vec::with_capacity(series.len());
                for group in series {
                    // A group a `Partial` hands over is whole: it folded the group's contribution of
                    // its partition, and the merge above joins partitions rather than continuing a
                    // chain.
                    let row = layout
                        .row(group, true)
                        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
                    rows.push(state_row_from_frame_row(row, expected)?);
                }
                rows
            }
        };
        let description = self.describe();
        let merger = self.merger.as_mut().ok_or_else(|| no_fold(&description))?;
        for row in &rows {
            merger.merge_row(row, |bytes| buffer.resize(pending.saturating_add(bytes)))?;
        }
        drop(rows);
        buffer.resize(merger.state_bytes())
    }

    /// Transient bytes required to consume a state chunk.
    pub(crate) fn pending_state(&self, chunk: &StateChunk) -> usize {
        match chunk {
            StateChunk::Series { .. } => state_bytes(chunk),
            StateChunk::Row(row) => crate::native::state::state_row_bytes(row),
        }
    }

    /// The number of retained groups, for budgeting the output queue backing storage.
    ///
    /// A point-wise fold knows its groups because it keyed them. A `quantile` has not grouped
    /// anything: the kernel does that when it answers. What it reports here is therefore the number
    /// of members it retained, which is an upper bound -- every group has at least one member -- and
    /// only ever sizes a reservation.
    pub(crate) fn groups(&self) -> usize {
        if let Some(input) = self.retained.as_ref() {
            return input.series.len();
        }
        self.accumulator.as_ref().map_or(0, FloatAggregator::groups)
    }

    /// Finishes the fold and returns one series per group, or `None` if the fold already ran.
    ///
    /// `None` is what a node reports after it has queued its series: the caller reads them from the
    /// queue, exactly as it reads the chunks a binary node queued, so a second pull does not finish
    /// a fold whose states are gone.
    pub(crate) fn compute(
        &mut self,
        buffer: &mut NodeBuffer,
        queue_bytes: usize,
    ) -> Result<Option<AggregateOutput>> {
        if self.computed {
            return Ok(None);
        }
        self.computed = true;
        if let Some(input) = self.retained.take() {
            let output = self.finish_quantile(input, buffer, queue_bytes)?;
            return Ok(Some(AggregateOutput::Samples(output)));
        }
        if let Some(accumulator) = self.accumulator.take() {
            return match self.op.stage {
                AggregateStage::Complete => {
                    // What the fold recorded about its inputs is reported here, where this node's
                    // answer is the whole answer.
                    let evidence = accumulator.evidence_summary();
                    let output = accumulator.finish_budgeted(|total| {
                        buffer.resize(total.saturating_add(queue_bytes))
                    })?;
                    let output_vec_bytes = output
                        .capacity()
                        .saturating_mul(std::mem::size_of::<Series>());
                    charge_samples(buffer, &output, output_vec_bytes, queue_bytes)?;
                    self.report_evidence(evidence);
                    Ok(Some(AggregateOutput::Samples(output)))
                }
                AggregateStage::Partial => {
                    let state_schema = self
                        .state_schema
                        .clone()
                        .ok_or_else(|| no_fold(&self.describe()))?;
                    // The fold builds the chunk's own representation, so handing it over costs no
                    // second copy and no second charge: what was admitted for the groups is what
                    // the chunk now holds.
                    //
                    // What the fold recorded travels in that state instead of being reported here:
                    // one partition's evidence is a claim about a point the merge has not decided
                    // yet, and a message cannot be taken back.
                    let partial = accumulator
                        .finish_partial_budgeted(|bytes| buffer.resize(bytes + queue_bytes))?;
                    let chunk = state_chunk_from_partial(partial, state_schema, &self.op)?;
                    Ok(Some(AggregateOutput::State(chunk)))
                }
                AggregateStage::Final => Err(no_fold(&self.describe())),
            };
        }
        if let Some(merger) = self.merger.take() {
            // The merge saw every partition's state, so this node is where the point's outcome is
            // final and where its evidence is reported.
            let evidence = merger.evidence_summary();
            let output =
                merger.finish_budgeted(|total| buffer.resize(total.saturating_add(queue_bytes)))?;
            let output_vec_bytes = output
                .capacity()
                .saturating_mul(std::mem::size_of::<Series>());
            charge_samples(buffer, &output, output_vec_bytes, queue_bytes)?;
            self.report_evidence(evidence);
            return Ok(Some(AggregateOutput::Samples(output)));
        }
        Err(no_fold(&self.describe()))
    }

    /// Reports what the fold or merge recorded about its inputs, once, where the answer is final.
    ///
    /// Only a `Complete` or a `Final` node reports: a `Partial` node hands its evidence on in the
    /// state it exports, because a point's outcome is not decided until every partition has been
    /// merged and a message cannot be withdrawn once it has been collected. The collector is the
    /// node's own handle, so its children keep theirs.
    ///
    /// A poisoned point answers nothing, and the reconciliation its inputs needed is not reported
    /// for it: the summary already dropped that bit, which is what makes the message set the same
    /// whichever order the partitions arrived in.
    fn report_evidence(&mut self, evidence: HistogramEvidence) {
        if self.op.stage == AggregateStage::Partial {
            return;
        }
        let Some(collector) = self.annotations.take() else {
            return;
        };
        let name = self.aggregate_name();
        if evidence.incompatible_layout {
            collector.record_warning(format!(
                "{name}: incompatible native histogram layouts, the result is removed"
            ));
        }
        if evidence.custom_bounds_reconciled {
            collector.record_info(format!("{name}: custom buckets reconciled"));
        }
        if evidence.counter_reset_collision {
            collector.record_warning(format!("{name}: contradictory reset hints"));
        }
    }
    /// Answers a `quantile` from the input it kept, and hands the series over.
    ///
    /// The kernel reads `q` per point from an unlabelled single-field scalar on the aggregate's own
    /// grid, and a point that scalar has no value at is a point with no result. Which scalar that is
    /// depends on what the query wrote: the φ the node held from its parameter child, or -- for a
    /// number bound on the tree -- one built over the points the input holds, so that every point it
    /// covers carries it. (A point the held series has no sample at is unanswered, exactly as a
    /// missing `q` is in the kernel; `scalar(...)`, the shape that produces one per point, answers
    /// NaN rather than no sample, so a statement whose `foo` holds nothing answers NaN there.)
    ///
    /// A built parameter covers the points the input holds and not the window they sit in: the
    /// kernel reads `q` only where a member has a sample, so a sparse input's parameter is as wide
    /// as its rows. A parameter over the envelope would instead allocate every position between two
    /// far-apart points -- a span sized by the statement's range rather than by the data it reads,
    /// which the pool never admitted. The parameter is booked from the bound its rows give before it
    /// exists, and settles on its exact figure once it is built. A held parameter is not built here
    /// and needs no such bound: it was reserved when the node adopted it, which is what
    /// [`QuantileInput::bytes`] counts ever since.
    ///
    /// The reservation covers what is held while the kernel runs -- the input, that parameter, and
    /// the kernel's workspace -- and then settles on the series the kernel answered, exactly as a
    /// point-wise fold's finish does. The input is what stops being held when the kernel is done with
    /// it, so it is dropped before the settle rather than when this call returns: a settle read
    /// beside a live input would name a set that is never held, and the figure it left behind would
    /// be the one the next node's admission had to fit into.
    fn finish_quantile(
        &self,
        input: QuantileInput,
        buffer: &mut NodeBuffer,
        queue_bytes: usize,
    ) -> Result<Vec<Series>> {
        let grid = self.schema.eval_grid.clone().ok_or_else(|| {
            DataFusionError::Execution(
                "a `quantile` aggregate needs an evaluated grid to read its parameter on".into(),
            )
        })?;
        // The points the kernel copies the parameter over. A held `φ` covers the grid the statement
        // evaluated it on, which the input's rows say nothing about: one row of input beside a
        // `time()` over a million grid points is a million pairs, not one. A built one covers the
        // points the input holds, and the input's rows bound those.
        let parameter_points = match &input.parameter {
            Some(parameter) => parameter.time.len(),
            None => input.series.iter().map(|series| series.time.len()).sum(),
        };
        let workspace = quantile_workspace_bytes(&self.schema, &input, parameter_points);
        // A built parameter is admitted before it exists, because building it is what makes this
        // node hold it: the bound is what a parameter over the input's points can take, and the
        // resize below settles on what it did take. A held one is already counted by
        // `input.bytes()`, so reserving for it again would charge the same series twice.
        let built_bound = match (&input.parameter, input.quantile) {
            (None, Some(_)) => quantile_parameter_bound_bytes(&input),
            _ => 0,
        };
        buffer.resize(
            input
                .bytes()
                .saturating_add(built_bound)
                .saturating_add(workspace)
                .saturating_add(queue_bytes),
        )?;
        // The φ the kernel reads: the series this node held from its parameter child, or -- when the
        // query bound the number -- one built over the points the input holds.
        let mut built = None;
        if input.parameter.is_none() {
            let Some(quantile) = input.quantile else {
                return Err(DataFusionError::Internal(
                    "a per-point `quantile` parameter was never handed over".into(),
                ));
            };
            let positions = quantile_positions(&input);
            built = Some(quantile_parameter(&grid, quantile, &positions)?);
            // The collected positions are what the bound above covered and what this node no longer
            // holds, so they go before the figure settles.
            drop(positions);
        }
        let parameter = input
            .parameter
            .as_ref()
            .or(built.as_ref())
            .expect("a parameter is either held or built just above");
        let held = input
            .bytes()
            .saturating_add(if input.parameter.is_some() {
                0
            } else {
                series_bytes(parameter)
            })
            .saturating_add(workspace);
        buffer.resize(held.saturating_add(queue_bytes))?;
        let names: Vec<&str> = match &self.op.group {
            BoundGrouping::By(labels) | BoundGrouping::Without(labels) => {
                labels.iter().map(|label| label.as_ref()).collect()
            }
        };
        let grouping = match &self.op.group {
            BoundGrouping::By(_) => Grouping::By(&names),
            BoundGrouping::Without(_) => Grouping::Without(&names),
        };
        let output = aggregate_quantile(&self.schema, &input.series, parameter, grouping)
            .map_err(|error| folded_chunk(&self.describe(), error))?;
        drop(names);
        // The parameter's use ends with the kernel call, and a built one is dropped here for the
        // same reason the input is: the settle below names what the node holds from then on. A
        // held one goes with the input it is part of.
        drop(built);
        // The input is what this node held until the kernel answered, and it is given back before
        // the reservation settles on the answer: the settle below is what the node holds from then
        // on, so it must be read from the set that is really alive -- the answer and the queue --
        // rather than from the answer beside an input this call only drops on its way out. The
        // kernel's answer holds nothing of the input: the builder copies the samples it keeps.
        drop(input);
        let output_vec_bytes = output
            .capacity()
            .saturating_mul(std::mem::size_of::<Series>());
        charge_samples(buffer, &output, output_vec_bytes, queue_bytes)?;
        Ok(output)
    }
}

/// The field the `quantile` parameter is bound under, which the kernel reads as its scalar.
const QUANTILE_PARAMETER_FIELD: &str = "scalar";

/// The points the retained input holds, once each, in ascending order.
///
/// A group's answer is decided at the points at least one of its members holds a sample at, and its
/// members are drawn from the input, so these are every point the kernel can ask its parameter for.
/// They are the positions the input's rows sit at -- not the window they span, which two far-apart
/// points make as wide as the grid.
fn quantile_positions(input: &QuantileInput) -> Vec<usize> {
    union_positions(input.series.iter().map(|series| &series.time)).collect()
}

/// The parameter a `quantile` reads at each point: the φ the query wrote, unlabelled and single
/// field, at `positions` and nowhere else.
///
/// The kernel reads `q` per point and treats a point it has no value at as a point with no result,
/// so the parameter has to cover exactly the points that can be asked for and may omit every other
/// one. [`GridSeriesBuilder`] is what makes that cheap: the axis keeps the runs of consecutive
/// positions rather than a row per position, so two far-apart points cost two rows and two spans
/// instead of every position between them.
///
/// An input holding no point has nothing to decide, which the kernel answers with no output: an
/// empty scalar over the grid says the same thing and costs nothing.
fn quantile_parameter(grid: &Arc<EvalGrid>, quantile: f64, positions: &[usize]) -> Result<Series> {
    let schema = Arc::new(SeriesSchema {
        fields: Arc::from([FieldSpec {
            name: QUANTILE_PARAMETER_FIELD.into(),
        }]),
        eval_grid: Some(Arc::clone(grid)),
    });
    let metric = SeriesLabels::try_new(None, Vec::new())
        .map_err(|error| DataFusionError::External(Box::new(error)))?;
    let mut builder = GridSeriesBuilder::try_new(Arc::clone(&schema))?;
    for position in positions {
        builder.push(*position, &[Some(quantile)])?;
    }
    match builder.finish(metric)? {
        Some(parameter) => Ok(parameter),
        None => constant_series(
            &schema,
            GridRange::try_new(Arc::clone(grid), 0, 0)
                .map_err(|error| DataFusionError::External(Box::new(error)))?,
            quantile,
        ),
    }
}

/// The most a `quantile` parameter over `input` can cost, before it is built.
///
/// The parameter holds one value and one position per point it covers, and one span per run of
/// them; a run is at least one point long, so the input's rows bound all of those. The figure is
/// what the pool admits ahead of the allocation, and [`series_bytes`] is what it settles on once
/// the parameter exists: what is admitted is what the parameter really takes, wherever its points
/// sit on the grid.
fn quantile_parameter_bound_bytes(input: &QuantileInput) -> usize {
    let rows: usize = input.series.iter().map(|series| series.time.len()).sum();
    let per_point = std::mem::size_of::<usize>() // its position, while the points are collected
        .saturating_add(std::mem::size_of::<f64>()) // the value it carries
        .saturating_add(2 * std::mem::size_of::<usize>()) // the span of a run of one point
        .saturating_add(std::mem::size_of::<f64>()); // the array's validity slot for it
    std::mem::size_of::<Series>()
        .saturating_add(std::mem::size_of::<TimeAxis>())
        .saturating_add(rows.saturating_mul(per_point))
}

/// The bytes the `quantile` kernel needs beside its retained input while it answers.
///
/// [`aggregate_quantile`] reports no workspace as it allocates, so this is an estimate reserved once
/// before it runs, in the same spirit as the whole-vector kernels'. What it covers: the state it
/// keeps per member (one position list, one heap entry and one cursor per field), its own copy of the
/// parameter, the temporaries of one grid point (its members' values, the copy the kernel sorts, and
/// one row of the width it folds), and the rows it builds -- the group being built and the groups it
/// has already handed over. The row count is the input's own: a group's rows are the points at least
/// one of its members holds a sample at, so the input's rows bound every group's rows and their sum.
///
/// The parameter copy is `parameter_points` pairs and not the input's rows: the kernel reads the
/// whole parameter into one `(position, φ)` per point it covers before it folds anything, so a dense
/// `time()` read against one row of input copies every point of the grid, and sizing that copy from
/// the input's one row would reserve a pair for a million points. What the kernel iterates are the
/// parameter's own positions -- the points a bound `φ` was built over and the points a held one
/// covers -- so the caller reports that count rather than this reading the input for it.
fn quantile_workspace_bytes(
    schema: &SeriesSchema,
    input: &QuantileInput,
    parameter_points: usize,
) -> usize {
    let width = schema.fields.len();
    let members = input.series.len();
    let rows: usize = input.series.iter().map(|series| series.time.len()).sum();
    let per_member = std::mem::size_of::<(usize, usize, usize)>()
        .saturating_add(std::mem::size_of::<usize>())
        .saturating_add(width.saturating_mul(4 * std::mem::size_of::<usize>()));
    let per_point = members
        .saturating_mul(2 * std::mem::size_of::<f64>())
        .saturating_add(width.saturating_mul(std::mem::size_of::<Option<f64>>()));
    // One row, charged twice: the builder holds one group at a time and the series it has already
    // handed over are held beside it.
    let per_row = 2 * width
        .saturating_mul(std::mem::size_of::<f64>())
        .saturating_add(2 * std::mem::size_of::<usize>())
        .saturating_add(1);
    // The parameter copy, charged twice for the capacity a growing vector keeps over its length.
    let parameter_copy = 2 * parameter_points.saturating_mul(std::mem::size_of::<(usize, f64)>());
    members
        .saturating_mul(per_member)
        .saturating_add(per_point)
        .saturating_add(rows.saturating_mul(per_row))
        .saturating_add(parameter_copy)
        .saturating_add(members.saturating_mul(std::mem::size_of::<Series>()))
}

/// Charges the series a finished aggregate hands over, plus the queue they enter.
///
/// The queue's own backing is counted by the caller that appends to it, so what is added here is
/// the series themselves and the vector that carries them; the two are charged together because
/// the vector outlives the call that built it.
fn charge_samples(
    buffer: &mut NodeBuffer,
    output: &[Series],
    output_vec_bytes: usize,
    queue_bytes: usize,
) -> Result<()> {
    let output_bytes = output.iter().fold(0usize, |bytes, series| {
        bytes.saturating_add(series_bytes(series))
    });
    buffer.resize(
        output_bytes
            .saturating_add(output_vec_bytes)
            .saturating_add(queue_bytes),
    )
}

/// The rejection of a chunk that is not the layout this node folds.
///
/// The fold indexes its states by absolute grid position and by field, so a chunk of another field
/// count or another grid would be folded into states that mean something else. `error` is what the
/// series check reported, which says which of the two disagreed.
/// Wraps the error of a chunk this node cannot fold.
///
/// Only a chunk that does not fit the schema is the node's own refusal; anything else is the
/// kernel saying it does not compute this shape at all -- exporting native histogram state, for
/// instance -- and a caller that falls back to another planner on `NotImplemented` still can.
/// Wrapping that one in an `Execution` would deny it.
fn folded_chunk(aggregate: &str, error: DataFusionError) -> DataFusionError {
    match error {
        DataFusionError::NotImplemented(_) => error,
        error => DataFusionError::Execution(format!(
            "the `{aggregate}` aggregate folds chunks bound to its own schema, and this \
             one is not: {error}"
        )),
    }
}

/// The error for a fold that has already been finished.
fn no_fold(aggregate: &str) -> DataFusionError {
    DataFusionError::Execution(format!(
        "the `{aggregate}` aggregate already finished its fold, so it takes no more input"
    ))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_query::promql_annotations::PromqlAnnotationCollector;
    use datatypes::series::samples::{FieldSpec, SeriesSchema};
    use datatypes::series::time::{EvalGrid, GridSpec};

    use super::*;

    fn schema() -> Arc<SeriesSchema> {
        Arc::new(SeriesSchema {
            fields: vec![FieldSpec {
                name: Arc::from("value"),
            }]
            .into(),
            eval_grid: Some(Arc::new(
                EvalGrid::try_new(GridSpec::try_new(0, 1, 4).unwrap()).unwrap(),
            )),
        })
    }

    fn runtime(stage: AggregateStage) -> AggregateRuntime {
        let pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool> =
            Arc::new(datafusion::execution::memory_pool::UnboundedMemoryPool::default());
        let mut buffer = NodeBuffer::new(
            "test aggregate",
            Arc::new(crate::native::metrics::NodeMetrics::default()),
            &pool,
        );
        AggregateRuntime::new(
            BoundAggregate {
                kind: AggregateKind::Sum,
                group: BoundGrouping::By(Arc::new([])),
                stage,
                field_kind: crate::extension_plan::FieldKind::Histogram,
            },
            schema(),
            &mut buffer,
        )
        .unwrap()
    }

    /// The φ this node folds is a series over `points` grid points, as `time()` or `scalar()`
    /// hands one over.
    fn parameter_series(points: usize) -> Series {
        let grid = Arc::new(EvalGrid::try_new(GridSpec::try_new(0, 1, 4).unwrap()).unwrap());
        let schema = Arc::new(SeriesSchema {
            fields: vec![FieldSpec {
                name: Arc::from("scalar"),
            }]
            .into(),
            eval_grid: Some(Arc::clone(&grid)),
        });
        constant_series(&schema, GridRange::try_new(grid, 0, points).unwrap(), 0.5).unwrap()
    }

    /// Adopting the parameter transfers the charge it arrived with, and bills it once.
    ///
    /// The series is grown onto this node's reservation before it is handed over, because it
    /// outlives the child that produced it; `hold_scalar` is what does that, and it does it on this
    /// node's buffer. Adopting it allocates nothing, so the node settles on what it holds -- the
    /// series once, its descriptor counted by the state that holds it inline -- and a pool of
    /// exactly that figure admits the handoff. Growing for the same series again would ask for the
    /// double of what arrived, and a pool that holds the handoff would refuse it.
    #[test]
    fn adopting_a_parameter_charges_it_once() {
        let parameter = parameter_series(4);
        let held = series_bytes(&parameter);
        // What the node holds once the series arrived and was grown onto it, and what it holds with
        // the same series adopted: the second is the first less the descriptor the state carries
        // inline, because a series held in a slot costs the slot and the heap and not the slot
        // twice. The pool holds exactly the handoff, so an admission that asks for the series a
        // second time has nowhere to put it.
        let handoff = std::mem::size_of::<QuantileInput>() + held;
        let adopted = handoff - std::mem::size_of::<Series>();
        let pool: Arc<dyn datafusion::execution::memory_pool::MemoryPool> = Arc::new(
            datafusion::execution::memory_pool::GreedyMemoryPool::new(handoff),
        );
        let mut buffer = NodeBuffer::new(
            "test quantile",
            Arc::new(crate::native::metrics::NodeMetrics::default()),
            &pool,
        );
        let mut runtime = AggregateRuntime::new_parameterized(
            BoundAggregate {
                kind: AggregateKind::Quantile { quantile: 0.0 },
                group: BoundGrouping::By(Arc::new([])),
                stage: AggregateStage::Complete,
                field_kind: crate::extension_plan::FieldKind::Float,
            },
            schema(),
            &mut buffer,
        )
        .unwrap();
        // The handoff the parameter child makes: the series is reserved on this node's buffer
        // before it is kept, exactly as `hold_scalar` does for every scalar operand.
        buffer.grow(held).unwrap();
        runtime
            .hold_parameter(parameter, &mut buffer)
            .expect("a pool that holds the handoff refused the adoption");
        assert_eq!(
            pool.reserved(),
            adopted,
            "the adopted parameter is billed once, descriptor included"
        );
    }

    fn collected(collector: &PromqlAnnotationCollector) -> (Vec<String>, Vec<String>) {
        let (mut warnings, mut infos) = (Vec::new(), Vec::new());
        collector.append_to(&mut warnings, &mut infos);
        (warnings, infos)
    }

    /// Only the node that finishes the answer reports what the fold recorded, and it says each thing
    /// once. A `Partial` node reports nothing: one partition's evidence is a claim about a point the
    /// merge has not decided yet, and a message cannot be taken back.
    #[test]
    fn only_the_finishing_node_reports_histogram_evidence() {
        let evidence = HistogramEvidence {
            // The summary a poisoned point produces keeps its reconciliation bit out, so this is
            // what a merge hands over when a point is removed for its layout.
            poisoned: true,
            incompatible_layout: true,
            custom_bounds_reconciled: false,
            counter_reset_seen: true,
            not_counter_reset_seen: true,
            counter_reset_collision: true,
        };
        let mut partial = runtime(AggregateStage::Partial);
        let collector = PromqlAnnotationCollector::default();
        partial.set_annotations(collector.clone());
        partial.report_evidence(evidence);
        assert_eq!(
            collected(&collector),
            (Vec::new(), Vec::new()),
            "a partial node reports nothing"
        );

        for stage in [AggregateStage::Complete, AggregateStage::Final] {
            let mut finishing = runtime(stage);
            let collector = PromqlAnnotationCollector::default();
            finishing.set_annotations(collector.clone());
            finishing.report_evidence(evidence);
            // A second report is not a second message: the node handed its collector on with the
            // first one.
            finishing.report_evidence(evidence);
            let (warnings, infos) = collected(&collector);
            assert!(
                warnings
                    .iter()
                    .any(|warning| warning.contains("incompatible native histogram layouts")),
                "{stage:?} reported {warnings:?}"
            );
            assert!(
                warnings
                    .iter()
                    .any(|warning| warning.contains("contradictory reset hints")),
                "{stage:?} reported {warnings:?}"
            );
            assert_eq!(warnings.len(), 2, "{stage:?} reported {warnings:?}");
            assert!(infos.is_empty(), "{stage:?} reported {infos:?}");
        }
    }

    /// The two reset-hint bits are OR-ed over every point the fold saw, so between them they say
    /// only what the inputs reported -- two groups can each hold one of them. Whether they
    /// contradict is a per-point decision, which the summary carries in `counter_reset_collision`,
    /// and that bit is what the reporter warns about.
    #[test]
    fn a_reset_hint_held_by_one_group_each_is_not_a_collision() {
        let mut quiet = runtime(AggregateStage::Complete);
        let collector = PromqlAnnotationCollector::default();
        quiet.set_annotations(collector.clone());
        quiet.report_evidence(HistogramEvidence {
            counter_reset_seen: true,
            not_counter_reset_seen: true,
            ..HistogramEvidence::default()
        });
        assert_eq!(
            collected(&collector),
            (Vec::new(), Vec::new()),
            "two points that each saw one hint contradict nothing"
        );

        // One point that saw both is the collision, and it is what the message is for.
        let mut colliding = runtime(AggregateStage::Complete);
        let collector = PromqlAnnotationCollector::default();
        colliding.set_annotations(collector.clone());
        colliding.report_evidence(HistogramEvidence {
            counter_reset_collision: true,
            ..HistogramEvidence::default()
        });
        let (warnings, infos) = collected(&collector);
        assert_eq!(warnings.len(), 1, "{warnings:?}");
        assert!(
            warnings[0].contains("contradictory reset hints"),
            "{warnings:?}"
        );
        assert!(infos.is_empty(), "{infos:?}");
    }

    /// A reconciled point that answered reports its reconciliation as information, once.
    #[test]
    fn a_live_point_reports_its_reconciliation() {
        let mut runtime = runtime(AggregateStage::Complete);
        let collector = PromqlAnnotationCollector::default();
        runtime.set_annotations(collector.clone());
        runtime.report_evidence(HistogramEvidence {
            custom_bounds_reconciled: true,
            ..HistogramEvidence::default()
        });
        let (warnings, infos) = collected(&collector);
        assert!(warnings.is_empty(), "{warnings:?}");
        assert_eq!(infos.len(), 1, "{infos:?}");
        assert!(infos[0].contains("custom buckets reconciled"), "{infos:?}");
    }
}
