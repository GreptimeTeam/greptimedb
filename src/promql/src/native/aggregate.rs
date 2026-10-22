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
//! [`AggregateStage::Partial`] and [`AggregateStage::Final`], which this runtime does not execute
//! yet.

use std::sync::Arc;

use datafusion::error::{DataFusionError, Result};
use datatypes::series::samples::{Series, SeriesSchema};

use crate::aggregate::{AggregateOp, FloatAggregator, Grouping};
use crate::evaluated::EvaluatedChunk;
use crate::grid_series::series_bytes;
use crate::map::validate_float_series;
use crate::native::execute::{NodeBuffer, evaluated_bytes};
use crate::native::metrics::NodePath;
use crate::native::plan::{
    AggregateKind, AggregateStage, BoundAggregate, Grouping as BoundGrouping,
};

/// One aggregate node's states, from the first chunk it is handed to the series it finishes with.
pub(crate) struct AggregateRuntime {
    /// What the node computes, as the tree bound it: the kind it folds by and the stage it reports.
    op: BoundAggregate,
    /// The layout its input and output share: the fields every chunk has and the grid they are
    /// addressed on. A `Complete` aggregate neither adds nor drops a field, and it evaluates on its
    /// input's grid, so one schema describes both sides.
    schema: Arc<SeriesSchema>,
    /// The fold. `None` once [`Self::compute`] has taken it, which is also what says the node has
    /// nothing left to fold into.
    accumulator: Option<FloatAggregator>,
    /// Whether the fold has been finished and its series handed over. A node computes once: a second
    /// pull finds the series it queued rather than a fold that ran again.
    ///
    /// The driver reads this to decide whether the node settled: an aggregate answers only once its
    /// fold has run, so a child with nothing left to hand over is not an answer.
    pub(crate) computed: bool,
}

impl AggregateRuntime {
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
        if op.stage != AggregateStage::Complete {
            return Err(DataFusionError::NotImplemented(format!(
                "the native runtime does not execute a `{:?}` aggregate yet: a `Partial` aggregate \
                 reports the state a later stage merges and a `Final` aggregate merges that state, \
                 and only `Complete` -- every contribution inside one execution -- is computed here",
                op.stage
            )));
        }
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
            schema,
            accumulator: Some(initial),
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
        path: &NodePath,
        chunk: EvaluatedChunk,
        buffer: &mut NodeBuffer,
        pending: usize,
    ) -> Result<()> {
        // `Series` keeps its fields public, so a chunk can be built by struct literal against
        // another schema; the states are indexed by absolute position on this node's grid, so a
        // chunk of another field count or another grid would be folded into the wrong places.
        validate_float_series(&self.schema, &chunk.series)
            .map_err(|error| folded_chunk(path, &self.describe(), error))?;
        let accumulator = self.accumulator.as_mut().ok_or_else(|| no_fold(path))?;
        // Keep the chunk covered while the fold decides whether it needs a new group or block.
        accumulator.update_budgeted(&chunk.series, |fold_bytes| {
            buffer.resize(pending.saturating_add(fold_bytes))
        })?;
        drop(chunk);
        buffer.resize(accumulator.state_bytes())
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
        path: &NodePath,
        buffer: &mut NodeBuffer,
        queue_bytes: usize,
    ) -> Result<Option<Vec<Series>>> {
        if self.computed {
            return Ok(None);
        }
        if self.accumulator.is_none() {
            return Err(no_fold(path));
        }
        self.computed = true;
        let output = self
            .accumulator
            .take()
            .ok_or_else(|| no_fold(path))?
            .finish_budgeted(|total| buffer.resize(total.saturating_add(queue_bytes)))?;
        let output_bytes = output.iter().fold(0usize, |bytes, series| {
            bytes.saturating_add(series_bytes(series))
        });
        let output_vec_bytes = output
            .capacity()
            .saturating_mul(std::mem::size_of::<Series>());
        buffer.resize(
            output_bytes
                .saturating_add(output_vec_bytes)
                .saturating_add(queue_bytes),
        )?;
        Ok(Some(output))
    }
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
fn folded_chunk(path: &NodePath, aggregate: &str, error: DataFusionError) -> DataFusionError {
    match error {
        DataFusionError::NotImplemented(_) => error,
        error => DataFusionError::Execution(format!(
            "the `{aggregate}` aggregate at {path} folds chunks bound to its own schema, and this \
             one is not: {error}"
        )),
    }
}

/// The error for a fold that has already been finished.
fn no_fold(path: &NodePath) -> DataFusionError {
    DataFusionError::Execution(format!(
        "the aggregate at {path} already finished its fold, so it takes no more input"
    ))
}
