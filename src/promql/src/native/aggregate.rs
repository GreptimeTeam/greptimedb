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
//! An aggregate folds each chunk into per-group state as it arrives and produces once, when the
//! input ends. It cannot tell an incomplete group from a complete one: a
//! [`AggregateStage::Complete`] aggregate is correct only if the plan routes every contribution to
//! each group into this execution. Partitioned plans use [`AggregateStage::Partial`] and
//! [`AggregateStage::Final`], which carry kernel accumulator state between executions.
//!
//! `quantile` has no fixed-width accumulator and no mergeable state, so it exists only as
//! `Complete` and keeps its whole input until the input ends, when
//! [`crate::aggregate::aggregate_quantile`] answers from it. There is no spill: each retained chunk
//! is reserved before it is kept, so an input that does not fit the pool is refused.

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
#[derive(Clone, Copy)]
enum Fold {
    /// One fixed-width accumulator per point and field.
    Pointwise(AggregateOp),
    /// The whole input, kept until it ends.
    Quantile,
}

/// The input a `quantile` aggregate keeps until it ends, ungrouped and in arrival order; the
/// kernel groups when it answers.
struct QuantileInput {
    /// The literal φ, or `None` when φ is read per point from `parameter`. As in Prometheus, a
    /// value outside `[0, 1]` answers `±Inf` rather than being refused.
    quantile: Option<f64>,
    /// The per-point φ: the unlabelled scalar series handed over by the parameter child, on this
    /// node's grid. A point it has no sample at is unanswered.
    parameter: Option<Series>,
    /// One entry per series this node was handed.
    series: Vec<Series>,
    /// Addresses of the histogram tables already billed, so each table is billed once, by the first
    /// retained chunk that references it.
    tables: HashSet<usize>,
    /// Addresses of the Arrow sample buffers already billed. A retained slice keeps its whole
    /// allocation resident, so each allocation is billed in full, once.
    buffers: HashSet<usize>,
    /// Bytes of the retained chunks beside their `Series` structs: [`chunk_bytes_beside_samples`]
    /// plus newly referenced buffers and tables. The structs are counted by [`Self::bytes`] from
    /// the vector's capacity.
    held: usize,
}

impl QuantileInput {
    /// The bytes this input holds: itself, every slot of the series vector, [`Self::held`], and
    /// the held parameter's heap. The parameter's `Series` struct sits inline in `Self`, so it is
    /// subtracted from [`series_bytes`] to avoid billing it twice.
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
    op: BoundAggregate,
    /// The layout input and output share: an aggregate keeps its input's fields and grid.
    schema: Arc<SeriesSchema>,
    fold: Fold,
    /// Taken after the first annotation, so each node reports at most once.
    annotations: Option<PromqlAnnotationCollector>,
    /// `None` for `Final` and `quantile`, and once [`Self::compute`] has taken it.
    accumulator: Option<FloatAggregator>,
    /// The whole input of a `quantile`; `None` for other kinds and once the kernel has answered.
    retained: Option<QuantileInput>,
    merger: Option<FloatMerger>,
    state_schema: Option<Arc<AggregateStateSchema>>,
    /// Whether [`Self::compute`] has run. The driver reads it to decide whether the node settled.
    pub(crate) computed: bool,
}

impl AggregateRuntime {
    /// The PromQL name used in annotations.
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

    /// Installs the query's annotation collector.
    pub(crate) fn set_annotations(&mut self, collector: PromqlAnnotationCollector) {
        self.annotations = Some(collector);
    }

    /// The aggregate kind, for error messages.
    fn describe(&self) -> String {
        format!("{:?}", self.op.kind)
    }

    /// The state of an aggregate over `schema`, in any stage. A kind with no mergeable state is
    /// refused before any chunk is folded.
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

    /// The state of a `Complete` `quantile` over `schema`. The stage is not checked here:
    /// [`crate::native::plan::NativeNode::validate`] already rejects a `Partial` or `Final`
    /// `quantile`, which has no state binding.
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

    /// The state of a `quantile` whose φ arrives per point through [`Self::hold_parameter`]; the
    /// binding's constant is ignored. Any other kind is refused.
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

    /// Keeps the per-point φ series. The driver's `hold_scalar` has already grown this reservation
    /// by [`series_bytes`], so this only settles on [`QuantileInput::bytes`] instead of growing
    /// again.
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

    /// What consuming `chunk` reserves while it is consumed: its [`evaluated_bytes`] for a
    /// point-wise fold, which drops it after folding. A `quantile` returns 0 because
    /// [`Self::retain_quantile`] reserves the retained chunk itself; booking it here as well would
    /// bill it twice.
    pub(crate) fn pending(&self, chunk: &EvaluatedChunk) -> usize {
        match self.fold {
            Fold::Pointwise(_) => evaluated_bytes(chunk),
            Fold::Quantile => 0,
        }
    }

    /// Folds one chunk into its group, or retains it for a `quantile`. A chunk's `is_last` closes a
    /// member, not a group, so it is ignored.
    pub(crate) fn accumulate(
        &mut self,
        chunk: EvaluatedChunk,
        buffer: &mut NodeBuffer,
        pending: usize,
    ) -> Result<()> {
        let Fold::Pointwise(kind) = self.fold else {
            return self.retain_quantile(chunk, buffer);
        };
        // States are indexed by field and absolute grid position, so a chunk on another schema
        // would fold into the wrong slots.
        validate_aggregate_series(&self.schema, &chunk.series, kind)
            .map_err(|error| folded_chunk(&self.describe(), error))?;
        if histogram_samples(kind) == HistogramSamples::Ignored
            && !crate::map::is_float_series(&chunk.series)
            && let Some(collector) = &self.annotations
        {
            // Prometheus reports the skipped histogram samples once and folds the floats.
            collector.record_info(format!(
                "{}: ignored native histogram samples",
                self.aggregate_name()
            ));
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

    /// Retains one chunk of a `quantile`'s input. The chunk's descriptors plus the buffers and
    /// tables it is first to reference are reserved before the push, then the reservation settles
    /// on [`QuantileInput::bytes`].
    fn retain_quantile(&mut self, chunk: EvaluatedChunk, buffer: &mut NodeBuffer) -> Result<()> {
        // Same check as the kernel's: histogram samples are skipped, not rejected.
        validate_grid_series(&self.schema, &chunk.series)
            .map_err(|error| folded_chunk(&self.describe(), error))?;
        if !crate::map::is_float_series(&chunk.series)
            && let Some(collector) = &self.annotations
        {
            // Prometheus reports the skipped histogram samples once.
            collector.record_info(format!(
                "{}: ignored native histogram samples",
                self.aggregate_name()
            ));
            self.annotations = None;
        }
        let Some(retained) = self.retained.as_mut() else {
            return Err(no_fold(&self.describe()));
        };
        let (buffers, buffer_bytes) = unbilled_sample_buffers(&chunk.series, &retained.buffers);
        let (tables, table_bytes) = unbilled_tables(&chunk.series, &retained.tables);
        let bytes = chunk_bytes_beside_samples(&chunk.series)
            .saturating_add(buffer_bytes)
            .saturating_add(table_bytes);
        buffer.grow(bytes)?;
        // Grow the vector explicitly so the reservation covers the doubled capacity before the
        // allocation; `QuantileInput::bytes` counts slots from capacity, not from `held`.
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
        retained.tables.extend(tables);
        retained.buffers.extend(buffers);
        retained.held = retained.held.saturating_add(bytes);
        buffer.resize(retained.bytes())
    }

    /// Merges one state chunk into this `Final` aggregate: either groups a local `Partial` folded
    /// ([`StateChunk::Series`]) or a decoded exported row ([`StateChunk::Row`]). Both are converted
    /// to the rows the merge kernel takes.
    pub(crate) fn accumulate_state(
        &mut self,
        chunk: StateChunk,
        buffer: &mut NodeBuffer,
        pending: usize,
    ) -> Result<()> {
        let expected = self.state_schema.as_ref().ok_or_else(|| {
            DataFusionError::NotImplemented("this runtime does not merge aggregate state".into())
        })?;
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
                // Layout and grid depend on the chunk, not the group, so they are checked once.
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
                    // A `Partial` hands over its partition's complete contribution to the group.
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

    /// The number of groups, for sizing the output queue. A `quantile` has not grouped yet and
    /// reports its retained member count, an upper bound.
    pub(crate) fn groups(&self) -> usize {
        if let Some(input) = self.retained.as_ref() {
            return input.series.len();
        }
        self.accumulator.as_ref().map_or(0, FloatAggregator::groups)
    }

    /// Finishes the fold and returns one series per group (or the state chunk of a `Partial`), or
    /// `None` if it already ran.
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
                    // Evidence travels in the state; only the `Final` merge can report it.
                    let partial = accumulator
                        .finish_partial_budgeted(|bytes| buffer.resize(bytes + queue_bytes))?;
                    let chunk = state_chunk_from_partial(partial, state_schema, &self.op)?;
                    Ok(Some(AggregateOutput::State(chunk)))
                }
                AggregateStage::Final => Err(no_fold(&self.describe())),
            };
        }
        if let Some(merger) = self.merger.take() {
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

    /// Reports histogram evidence once. A `Partial` reports nothing: a point's outcome is decided
    /// only after the `Final` merge, and a collected message cannot be withdrawn.
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

    /// Answers a `quantile` from its retained input. φ is the held per-point series, or for a
    /// literal φ a series built over only the positions the input holds (not the span between
    /// them, which could be as wide as the grid). The reservation covers input, parameter and
    /// workspace while the kernel runs, then settles on the output after the input is dropped.
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
        // A held φ may cover far more points than the input has rows (e.g. `time()`).
        let parameter_points = match &input.parameter {
            Some(parameter) => parameter.time.len(),
            None => input.series.iter().map(|series| series.time.len()).sum(),
        };
        let workspace = quantile_workspace_bytes(&self.schema, &input, parameter_points);
        // Reserve an upper bound for a parameter not built yet; a held one is in `input.bytes()`.
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
        let mut built = None;
        if input.parameter.is_none() {
            let Some(quantile) = input.quantile else {
                return Err(DataFusionError::Internal(
                    "a per-point `quantile` parameter was never handed over".into(),
                ));
            };
            let positions = quantile_positions(&input);
            built = Some(quantile_parameter(&grid, quantile, &positions)?);
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
        // Free the input before settling on the output; the output copies what it keeps.
        drop(built);
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

/// The positions the retained input holds, deduplicated and ascending: every point the kernel can
/// ask φ for.
fn quantile_positions(input: &QuantileInput) -> Vec<usize> {
    union_positions(input.series.iter().map(|series| &series.time)).collect()
}

/// An unlabelled single-field series holding `quantile` at `positions` only, or an empty scalar
/// when there are none.
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

/// An upper bound on the bytes of the parameter [`quantile_parameter`] builds over `input`,
/// reserved before it is built. The input's row count bounds its points and runs.
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

/// An estimate of the workspace [`aggregate_quantile`] allocates beside its input, reserved once
/// because the kernel does not report allocations: per-member cursors, one point's temporaries,
/// the output rows (bounded by the input's rows), and the kernel's `(position, φ)` copy of all
/// `parameter_points`.
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
    // Twice: the group being built plus the groups already finished.
    let per_row = 2 * width
        .saturating_mul(std::mem::size_of::<f64>())
        .saturating_add(2 * std::mem::size_of::<usize>())
        .saturating_add(1);
    // Twice, for a growing vector's spare capacity.
    let parameter_copy = 2 * parameter_points.saturating_mul(std::mem::size_of::<(usize, f64)>());
    members
        .saturating_mul(per_member)
        .saturating_add(per_point)
        .saturating_add(rows.saturating_mul(per_row))
        .saturating_add(parameter_copy)
        .saturating_add(members.saturating_mul(std::mem::size_of::<Series>()))
}

/// Settles the reservation on the output series, their vector, and `queue_bytes`.
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

/// Wraps a chunk-validation or kernel error as an `Execution` error naming the aggregate.
/// `NotImplemented` passes through unchanged so callers can still recognise an unsupported shape.
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

    #[test]
    fn adopting_a_parameter_charges_it_once() {
        let parameter = parameter_series(4);
        let held = series_bytes(&parameter);
        // The pool fits the handoff exactly, so growing for the series a second time would fail.
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
        // What `hold_scalar` does before handing the series over.
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

    #[test]
    fn only_the_finishing_node_reports_histogram_evidence() {
        let evidence = HistogramEvidence {
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

    /// The two reset-hint bits are OR-ed across points, so both set means nothing on its own; only
    /// the per-point `counter_reset_collision` warns.
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
