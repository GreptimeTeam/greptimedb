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

use std::sync::Arc;

use common_query::promql_annotations::PromqlAnnotationCollector;
use datafusion::error::{DataFusionError, Result};
use datatypes::series::SeriesLabels;
use datatypes::series::samples::{FieldSpec, Series, SeriesSchema};
use datatypes::series::time::{EvalGrid, GridRange, TimeAxis, union_positions};

use crate::aggregate::{
    AggregateOp, FloatAggregator, FloatMerger, Grouping, HistogramSamples, aggregate_quantile,
    histogram_samples, validate_aggregate_series,
};
use crate::evaluated::EvaluatedChunk;
use crate::grid_series::{GridSeriesBuilder, series_bytes};
use crate::map::{constant_series, validate_grid_series};
use crate::native::execute::{NodeBuffer, evaluated_bytes};
use crate::native::plan::{
    AggregateKind, AggregateStage, AggregateStateSchema, BoundAggregate, Grouping as BoundGrouping,
    aggregate_state_binding,
};
use crate::native::state::{StateChunk, state_bytes, state_chunk_from_partial};

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
    /// The φ the query wrote. The kernel reads it as Prometheus does, so a value outside `[0, 1]`
    /// answers `-Infinity` or `+Infinity` rather than being refused here.
    quantile: f64,
    /// One entry per series this node was handed.
    series: Vec<Series>,
}

impl QuantileInput {
    /// The bytes this input holds: the series themselves and the vector that carries them.
    fn bytes(&self) -> usize {
        self.series.iter().fold(
            std::mem::size_of::<Self>().saturating_add(
                self.series
                    .capacity()
                    .saturating_mul(std::mem::size_of::<Series>()),
            ),
            |total, series| total.saturating_add(series_bytes(series)),
        )
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
            // own and is built by [`Self::new_quantile`].
            AggregateKind::Quantile { .. } => return Self::new_quantile(op, schema, buffer),
        };
        let (state_kind, state_components) = aggregate_state_binding(op.kind).ok_or_else(|| {
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

    /// The states of a `Complete` `quantile` aggregate over `schema`.
    ///
    /// A `quantile` is answered from its group's whole sample set, so a stage that saw only part of a
    /// group would answer the quantile of a subset of it: `Partial` and `Final` are refused here
    /// rather than approximated, because the quantile of two partitions' quantiles is not the
    /// quantile of their union. That is a property of the operation and not of this runtime, so a
    /// plan that has to fold by partition keeps the previous route -- [`crate::native::split`]
    /// declines a `quantile` for the same reason.
    ///
    /// The input is evaluated on a grid because the kernel reads its parameter per point from a
    /// scalar on that grid, so a node without one is refused before it starts.
    fn new_quantile(
        op: BoundAggregate,
        schema: Arc<SeriesSchema>,
        buffer: &mut NodeBuffer,
    ) -> Result<Self> {
        if op.stage != AggregateStage::Complete {
            return Err(DataFusionError::NotImplemented(format!(
                "the native runtime executes `quantile` only as `Complete`, and this node is `{:?}`: \
                 a quantile is answered from the whole of its group, and the quantile of a union of \
                 partial groups cannot be computed from their quantiles",
                op.stage
            )));
        }
        let AggregateKind::Quantile { quantile } = op.kind else {
            return Err(DataFusionError::Internal(
                "a quantile runtime was built for another aggregate kind".into(),
            ));
        };
        if schema.eval_grid.is_none() {
            return Err(DataFusionError::NotImplemented(
                "a `quantile` aggregate whose input is not evaluated on a grid".into(),
            ));
        }
        let retained = QuantileInput {
            quantile,
            series: Vec::new(),
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

    /// What consuming `chunk` reserves while it is consumed.
    ///
    /// A point-wise fold drops the chunk at the end of the call and keeps only its states, so the
    /// charge is the chunk's own bytes -- the figure [`evaluated_bytes`] reports, which is one pass
    /// over the chunk and no state between chunks -- and not the `operand_chunk_bytes` figure a
    /// binary operand keeps, which exists because such an operand holds a series for the whole
    /// execution and so must count the slices of one buffer instead of the buffer once per chunk.
    ///
    /// A `quantile` keeps the chunk instead, so this figure is only what admitting it holds while it
    /// is handed over; [`Self::accumulate`] settles the reservation on the input it now retains.
    pub(crate) fn pending(&self, chunk: &EvaluatedChunk) -> usize {
        evaluated_bytes(chunk)
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
    /// set, and no fixed-width state holds a sample set. The charge is therefore the chunk's own
    /// bytes as this node now holds them -- the series and, when the retained vector has to grow for
    /// it, the capacity it grows to -- reserved before the push, because the push is what makes the
    /// node hold it; the reservation then settles on the whole input, which is what this node holds
    /// between chunks.
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
        buffer.grow(series_bytes(&chunk.series))?;
        // The slot the chunk is about to take, and - when the vector has to grow for it - the
        // capacity it grows to. `Vec::push` doubles, so booking one slot would let the real
        // allocation outrun what the pool admitted; the target is booked instead, and the settle
        // below then reads back the exact capacity the vector holds.
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
        buffer.resize(retained.bytes())
    }

    /// Merges one state chunk into this final aggregate.
    pub(crate) fn accumulate_state(
        &mut self,
        chunk: StateChunk,
        buffer: &mut NodeBuffer,
        pending: usize,
    ) -> Result<()> {
        let expected = self.state_schema.as_ref().ok_or_else(|| {
            DataFusionError::NotImplemented("this runtime does not merge aggregate state".into())
        })?;
        let StateChunk::Row(row) = chunk else {
            return Err(DataFusionError::Internal(
                "Final aggregate received producer state series".into(),
            ));
        };
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
        let description = self.describe();
        let merger = self.merger.as_mut().ok_or_else(|| no_fold(&description))?;
        merger.merge_row(&row, |bytes| buffer.resize(pending.saturating_add(bytes)))?;
        drop(row);
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
                    let output = accumulator.finish_budgeted(|total| {
                        buffer.resize(total.saturating_add(queue_bytes))
                    })?;
                    let output_vec_bytes = output
                        .capacity()
                        .saturating_mul(std::mem::size_of::<Series>());
                    charge_samples(buffer, &output, output_vec_bytes, queue_bytes)?;
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
                    let partial = accumulator
                        .finish_partial_budgeted(|bytes| buffer.resize(bytes + queue_bytes))?;
                    let chunk = state_chunk_from_partial(partial, state_schema)?;
                    Ok(Some(AggregateOutput::State(chunk)))
                }
                AggregateStage::Final => Err(no_fold(&self.describe())),
            };
        }
        if let Some(merger) = self.merger.take() {
            let output =
                merger.finish_budgeted(|total| buffer.resize(total.saturating_add(queue_bytes)))?;
            let output_vec_bytes = output
                .capacity()
                .saturating_mul(std::mem::size_of::<Series>());
            charge_samples(buffer, &output, output_vec_bytes, queue_bytes)?;
            return Ok(Some(AggregateOutput::Samples(output)));
        }
        Err(no_fold(&self.describe()))
    }

    /// Answers a `quantile` from the input it kept, and hands the series over.
    ///
    /// The parameter is the number the query wrote, so every point it covers carries it: the kernel
    /// reads `q` per point from an unlabelled single-field scalar on the aggregate's own grid, and a
    /// point that scalar has no value at is a point with no result. That case cannot arise from a
    /// constant, but the kernel keeps the rule for the parameter it is written for.
    ///
    /// The parameter covers the points the input holds and not the window they sit in: the kernel
    /// reads `q` only where a member has a sample, so a sparse input's parameter is as wide as its
    /// rows. A parameter over the envelope would instead allocate every position between two
    /// far-apart points -- a span sized by the statement's range rather than by the data it reads,
    /// which the pool never admitted. The parameter is booked from the bound its rows give before it
    /// exists, and settles on its exact figure once it is built.
    ///
    /// The reservation covers what is held while the kernel runs -- the input, that parameter, and
    /// the kernel's workspace -- and then settles on the series the kernel answered, exactly as a
    /// point-wise fold's finish does.
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
        let workspace = quantile_workspace_bytes(&self.schema, &input);
        // Admitted before the parameter exists, because building it is what makes this node hold
        // it: the bound is what a parameter over the input's points can take, and the resize below
        // settles on what it did take.
        buffer.resize(
            input
                .bytes()
                .saturating_add(quantile_parameter_bound_bytes(&input))
                .saturating_add(workspace)
                .saturating_add(queue_bytes),
        )?;
        let positions = quantile_positions(&input);
        let parameter = quantile_parameter(&grid, input.quantile, &positions)?;
        // The collected positions are what the bound above covered and what this node no longer
        // holds, so they go before the figure settles.
        drop(positions);
        let held = input
            .bytes()
            .saturating_add(series_bytes(&parameter))
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
        let output = aggregate_quantile(&self.schema, &input.series, &parameter, grouping)
            .map_err(|error| folded_chunk(&self.describe(), error))?;
        drop(names);
        drop(parameter);
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
/// parameter (one `(position, φ)` pair per point the parameter covers, which the input's rows bound
/// because the parameter is only built over the points the input holds), the temporaries of one grid
/// point (its members' values, the copy the kernel sorts, and one row of the width it folds), and the
/// rows it builds -- the group being built and the groups it has already handed over. The row count is
/// the input's own: a group's rows are the points at least one of its members holds a sample at, so
/// the input's rows bound every group's rows and their sum.
fn quantile_workspace_bytes(schema: &SeriesSchema, input: &QuantileInput) -> usize {
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
    let parameter_copy = 2 * rows.saturating_mul(std::mem::size_of::<(usize, f64)>());
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
/// kernel saying it does not compute this shape at all -- a native histogram in a float aggregate,
/// for instance -- and a caller that falls back to another planner on `NotImplemented` still can.
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
