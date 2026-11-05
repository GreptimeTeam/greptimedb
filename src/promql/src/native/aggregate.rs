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

use std::sync::Arc;

use common_query::promql_annotations::PromqlAnnotationCollector;
use datafusion::error::{DataFusionError, Result};
use datatypes::series::samples::{Series, SeriesSchema};

use crate::aggregate::{
    AggregateOp, FloatAggregator, FloatMerger, Grouping, HistogramEvidence, HistogramSamples,
    histogram_samples, validate_aggregate_series,
};
use crate::evaluated::EvaluatedChunk;
use crate::grid_series::series_bytes;
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

/// One aggregate node's states, from the first chunk it is handed to the series it finishes with.
pub(crate) struct AggregateRuntime {
    /// What the node computes, as the tree bound it: the kind it folds by and the stage it reports.
    op: BoundAggregate,
    /// The layout its input and output share: the fields every chunk has and the grid they are
    /// addressed on. A `Complete` aggregate neither adds nor drops a field, and it evaluates on its
    /// input's grid, so one schema describes both sides.
    schema: Arc<SeriesSchema>,
    /// The kind this node folds by, as the runtime's own enum, so the chunk check can ask what the
    /// operation accepts.
    fold_op: AggregateOp,
    /// The query's annotation collector, once the driver installed it. Only an operation that skips
    /// histogram samples has anything to say, and it says it once.
    annotations: Option<PromqlAnnotationCollector>,
    /// The fold. `None` once [`Self::compute`] has taken it, which is also what says the node has
    /// nothing left to fold into.
    accumulator: Option<FloatAggregator>,
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
        match self.fold_op {
            AggregateOp::Sum => "sum",
            AggregateOp::Count => "count",
            AggregateOp::Avg => "avg",
            AggregateOp::Min => "min",
            AggregateOp::Max => "max",
            AggregateOp::Group => "group",
            AggregateOp::Stddev => "stddev",
            AggregateOp::Stdvar => "stdvar",
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
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "the native runtime does not execute the `{other:?}` aggregate yet: only the supported `Complete` aggregate kinds are computed here"
                )));
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
                fold_op: kind,
                annotations: None,
                schema,
                accumulator: None,
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
            fold_op: kind,
            annotations: None,
            schema,
            accumulator: Some(initial),
            merger: None,
            state_schema,
            computed: false,
        })
    }

    /// What folding `chunk` reserves while it is folded.
    ///
    /// The chunk is not retained: it is folded into the states and dropped at the end of the call,
    /// so what this node holds afterwards is the fold and none of the input. The charge is therefore
    /// the chunk's own bytes -- the figure [`evaluated_bytes`] reports, which is one pass over the
    /// chunk and no state between chunks -- and not the `operand_chunk_bytes` figure a binary
    /// operand keeps, which exists because such an operand holds a series for the whole execution
    /// and so must count the slices of one buffer instead of the buffer once per chunk.
    pub(crate) fn pending(&self, chunk: &EvaluatedChunk) -> usize {
        evaluated_bytes(chunk)
    }

    /// Folds one chunk into its group.
    ///
    /// A chunk that closes its series closes nothing here: a group is not a series, so the last
    /// contribution of one member says nothing about the group. The fold keeps its states, which is
    /// what makes a group's answer its whole group's answer once the input ends.
    pub(crate) fn accumulate(
        &mut self,
        chunk: EvaluatedChunk,
        buffer: &mut NodeBuffer,
        pending: usize,
    ) -> Result<()> {
        // `Series` keeps its fields public, so a chunk can be built by struct literal against
        // another schema; the states are indexed by absolute position on this node's grid, so a
        // chunk of another field count or another grid would be folded into the wrong places.
        validate_aggregate_series(&self.schema, &chunk.series, self.fold_op)
            .map_err(|error| folded_chunk(&self.describe(), error))?;
        if histogram_samples(self.fold_op) == HistogramSamples::Ignored
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
    pub(crate) fn groups(&self) -> usize {
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
        if evidence.counter_reset_seen && evidence.not_counter_reset_seen {
            collector.record_warning(format!("{name}: contradictory reset hints"));
        }
    }
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
