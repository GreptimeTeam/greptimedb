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
use datatypes::series::samples::{Samples, Series, SeriesSchema};
use datatypes::series::time::TimeAxis;

use crate::aggregate::{AggregateOp, FloatAccumulator, Grouping};
use crate::evaluated::EvaluatedChunk;
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
    /// How its groups are identified, kept in this node's own storage because the kernel borrows a
    /// slice of `&str` that cannot outlive the strings it points at.
    grouping: GroupingLabels,
    /// The fold. `None` once [`Self::compute`] has taken it, which is also what says the node has
    /// nothing left to fold into.
    accumulator: Option<FloatAccumulator>,
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
    /// It names the kind and the grouping as this node holds them, which is what a reader needs to
    /// tell the aggregate a message is about from another one in the same tree.
    fn describe(&self) -> String {
        let labels: Vec<&str> = self
            .grouping
            .labels
            .iter()
            .map(|label| label.as_ref())
            .collect();
        let grouping = match (labels.is_empty(), self.grouping.by) {
            (true, _) => String::new(),
            (false, true) => format!(" by({})", labels.join(",")),
            (false, false) => format!(" without({})", labels.join(",")),
        };
        format!("{:?}{grouping}", self.op.kind)
    }

    /// The states of a `Complete` `sum` over `schema`.
    ///
    /// A stage other than `Complete` and a kind other than `Sum` are refused here, before a single
    /// chunk is folded: a node this runtime cannot compute must not start, because a partial answer
    /// would be one the caller never asked for. The rejection names what was found, so a reader can
    /// tell a tree this runtime cannot run from one it cannot run correctly.
    pub(crate) fn new(op: BoundAggregate, schema: Arc<SeriesSchema>) -> Result<Self> {
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
            other => {
                return Err(DataFusionError::NotImplemented(format!(
                    "the native runtime does not execute the `{other:?}` aggregate yet: only `Sum` \
                     is computed here"
                )));
            }
        };
        let grouping = GroupingLabels::new(&op.group);
        // The grouping is checked once, here, rather than at each fold: `by` on a value field needs
        // per-point labels and an adapter this node does not have, and a tree that asked for one is
        // refused before it reads a chunk.
        let names: Vec<&str> = grouping.labels.iter().map(|label| label.as_ref()).collect();
        FloatAccumulator::validate_grouping(&schema, grouping.borrow(&names))?;
        Ok(Self {
            grouping,
            op,
            schema,
            accumulator: Some(FloatAccumulator::new(kind)),
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

    /// What the states this node holds are charged with.
    ///
    /// It is the fold's blocks and nothing else: the chunks it read are gone, so unlike a binary
    /// operand's figure this one does not grow with the input but with the groups and the blocks
    /// that hold them.
    pub(crate) fn retained_bytes(&self) -> usize {
        self.accumulator
            .as_ref()
            .map_or(0, FloatAccumulator::state_bytes)
    }

    /// Folds one chunk into its group.
    ///
    /// A chunk that closes its series closes nothing here: a group is not a series, so the last
    /// contribution of one member says nothing about the group. The fold keeps its states, which is
    /// what makes a group's answer its whole group's answer once the input ends.
    pub(crate) fn accumulate(&mut self, path: &NodePath, chunk: EvaluatedChunk) -> Result<()> {
        // `Series` keeps its fields public, so a chunk can be built by struct literal against
        // another schema; the states are indexed by absolute position on this node's grid, so a
        // chunk of another field count or another grid would be folded into the wrong places.
        validate_float_series(&self.schema, &chunk.series)
            .map_err(|error| folded_chunk(path, &self.describe(), error))?;
        // The kernel takes a borrowed slice of names, so the names are lent to it for this fold and
        // the strings themselves stay in the grouping this node owns.
        let names: Vec<&str> = self
            .grouping
            .labels
            .iter()
            .map(|label| label.as_ref())
            .collect();
        let grouping = self.grouping.borrow(&names);
        let accumulator = self.accumulator.as_mut().ok_or_else(|| no_fold(path))?;
        // The chunk goes out of scope here, so this node holds none of what it read.
        accumulator.accumulate(&chunk.series, grouping)
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
    ) -> Result<Option<Vec<Series>>> {
        if self.computed {
            return Ok(None);
        }
        let Some(accumulator) = self.accumulator.take() else {
            return Err(no_fold(path));
        };
        let points = self
            .schema
            .eval_grid
            .as_ref()
            .map_or(0, |grid| grid.timestamps().len());
        let fields = self.schema.fields.len();
        let groups = accumulator.groups();
        // The finish allocates the result boxes of its states and the builder's rows, and both are
        // reserved before they exist: the figure is the states the fold holds -- one state per group
        // per point of every block it filled -- plus the rows one builder pushes for the groups it
        // produces. Like the other figures in this runtime it is a rough estimate rather than an
        // allocation bound: the map's buckets, the metric payloads and the slack of a vector's
        // capacity are not measured one by one.
        buffer.grow(
            accumulator
                .state_bytes()
                .saturating_add(aggregate_output_bytes(groups, points, fields)),
        )?;
        let output = accumulator.finish(&self.schema)?;
        self.computed = true;
        // The states and the one builder are gone with the finish, so what this node holds is the
        // series it returns; the caller queues each of them, and the reservation settles on the
        // queue as it does for a binary result.
        buffer.resize(aggregate_output_bytes(output.len(), points, fields))?;
        Ok(Some(output))
    }
}

/// The labels an aggregate's groups are identified by, in this module's own storage.
///
/// [`Grouping`] borrows a slice of `&str`, which cannot be kept beside the strings it points at in
/// one value, so the node keeps the labels and lends them to the kernel at each fold.
struct GroupingLabels {
    /// Whether the labels are the ones a group keeps (`by`) or the ones it drops (`without`).
    by: bool,
    /// The labels themselves: group keys under `by`, the dropped ones under `without`.
    labels: Vec<Arc<str>>,
}

impl GroupingLabels {
    fn new(group: &BoundGrouping) -> Self {
        match group {
            BoundGrouping::By(labels) => Self {
                by: true,
                labels: labels.to_vec(),
            },
            BoundGrouping::Without(labels) => Self {
                by: false,
                labels: labels.to_vec(),
            },
        }
    }

    /// The grouping the kernel takes, over names lent from `self.labels`.
    fn borrow<'a>(&self, names: &'a [&'a str]) -> Grouping<'a> {
        if self.by {
            Grouping::By(names)
        } else {
            Grouping::Without(names)
        }
    }
}

/// A rough figure for the bytes finishing `groups` groups over `points` grid points allocates.
///
/// It allows for one row of an `f64` per field per point the builder may push, and for the header,
/// the axis and the sample list of each result series. The builder's vectors keep slack and the
/// sample buffers behind a result are what the node's queue measures afterwards, so the figure is an
/// allowance rather than an allocation bound.
fn aggregate_output_bytes(groups: usize, points: usize, fields: usize) -> usize {
    let rows = groups
        .saturating_mul(points)
        .saturating_mul(fields)
        .saturating_mul(std::mem::size_of::<f64>());
    let series = groups.saturating_mul(
        std::mem::size_of::<Series>()
            .saturating_add(std::mem::size_of::<TimeAxis>())
            .saturating_add(std::mem::size_of::<Samples>()),
    );
    rows.saturating_add(series)
}

/// The rejection of a chunk that is not the layout this node folds.
///
/// The fold indexes its states by absolute grid position and by field, so a chunk of another field
/// count or another grid would be folded into states that mean something else. `error` is what the
/// series check reported, which says which of the two disagreed.
fn folded_chunk(path: &NodePath, aggregate: &str, error: DataFusionError) -> DataFusionError {
    DataFusionError::Execution(format!(
        "the `{aggregate}` aggregate at {path} folds chunks bound to its own schema, and this one \
         is not: {error}"
    ))
}

/// The error for a fold that has already been finished.
fn no_fold(path: &NodePath) -> DataFusionError {
    DataFusionError::Execution(format!(
        "the aggregate at {path} already finished its fold, so it takes no more input"
    ))
}
