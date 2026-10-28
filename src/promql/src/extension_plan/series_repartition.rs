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

//! Routes evaluated series chunks by canonical label set, so that every piece of one label set
//! reaches one output partition.
//!
//! A per-partition execution cannot see another partition's pieces of a label set, so a merge there
//! would answer with a partial result and present it as a canonical one. This node removes that
//! premise: it reads every partition of its one child and hands each chunk to the output the
//! canonical label set of its series hashes to, so one merge sees all pieces of a label set. It is
//! the Repartition stage between two merges; nothing inserts it yet.
//!
//! The frame is the child's own: an evaluated chunk is decoded only to read the labels that route
//! it, and every output publishes the layout its input declared.
//!
//! # Routing
//!
//! The key is [`label_set`] of the chunk's metric, hashed whole: coarsening it (a subset of the
//! labels, a metric name alone) would send two label sets to one output for sharing a prefix, which
//! is not the relation a downstream merge groups by. One [`Routing`] is built per execution and
//! every chunk is reduced with it, so the destination of a label set cannot depend on which chunk of
//! it was routed first.
//!
//! The node declares [`Partitioning::UnknownPartitioning`], never [`Partitioning::Hash`]: the key is
//! decoded from the rows, and a hash declaration carries an Arrow expression DataFusion could use to
//! prove two plans agree on a partitioning. There is no such expression here, so the claim would be
//! one no optimizer could check and a hash exchange inserted on top of it could silently disagree
//! with. No ordering is declared either: the node reorders, and it says so by claiming nothing.
//!
//! # Chain boundaries
//!
//! A consumer of these frames - the native merge - requires that a series' identity does not change
//! and its positions strictly increase while it is open, and that `is_last` closes it. Two chains of
//! one label set, each valid on its own, violate that if their chunks interleave on one output: the
//! merge would refuse the stream, or a coarser reader would sort two series into a plausible answer.
//!
//! So a chain is the unit of routing, not a chunk: an input task reads its child until the chunk that
//! closes the chain it is assembling - or until its stream ends - and hands the whole chain to the one
//! output its label set hashes to, in one step. Two sources' chains may reach one output in either
//! order, but never inside one another, so an output is still a run of whole chains. That is what the
//! node handed downstream when one routing task served every input and only switched input at a chain
//! boundary, and it is what a downstream merge relies on.
//!
//! What that costs is stated rather than hidden. Nothing of a chain is handed over before the chain
//! closes, so the first chunk of a series reaches its output later than a chunk-at-a-time design would
//! deliver it, and a long chain occupies memory for as long as it is being read. Those bytes are
//! charged to a [`MemoryConsumer`] as the chain grows, so a pool under pressure ends the read with an
//! error instead of letting a chain grow without bound: the node refuses, it does not spin and it does
//! not buffer past the pool's limit.
//!
//! The alternative - routing one chunk at a time - would need a channel per (input, output) pair to
//! keep two sources' partial chains from interleaving on one shared output queue, which is N x M
//! channels plus a reader for each of them. That complexity is deliberately not taken here: a whole
//! chain per handover gets every output the same order with one queue per output.
//!
//! # Where the reads run
//!
//! One task per child partition, and no task above them: each input's task reads its own child,
//! assembles that input's chains and sends them to their owners itself. No task makes a routing
//! decision for another input, and nothing rotates over the inputs.
//!
//! That is not an optimisation of the routing: an evaluated partition is one stream whose computation
//! happens inside its `poll_next`, so a task polling every partition in turn runs the child's native
//! kernels for all of them itself, one chunk at a time, and the partition count the child declared
//! buys no parallelism at all. It is also what makes progress local, which is what this node needs
//! when two of them are composed:
//!
//! - A partial chain never occupies a shared output queue, so no output waits for a continuation: a
//!   chain is either fully assembled in the input task that read it, or not sent at all.
//! - An input task can only be parked on (a) its own child, or (b) a full output queue. (b) is
//!   relieved by the DataFusion contract that every output partition is polled - see the caller
//!   requirements below - and (a) depends on that input's own upstream progress and nothing else.
//! - The tasks share no lock while routing and never wait on each other, so no task's progress is a
//!   precondition of another's.
//!
//! A single task routing every input broke all three: it parked on the queue of one output while
//! holding the reads of every input, so an input that could have answered was not read until that
//! queue moved, and a composed node whose first input's stream only closes when the reader finishes
//! stalled outright.
//!
//! # Caller requirements
//!
//! **Every output partition MUST be polled.**
//!
//! Each input task suspends on a full output queue rather than dropping what belongs there, so an
//! output nobody polls keeps the input tasks whose chains hash to it parked. Concretely:
//!
//! - **Poll every partition.** Once an unread output's queue fills, every input task with a chain for
//!   it stops there, and what those inputs owe the other outputs stops with them. This node does not
//!   spill and does not drop to make progress.
//! - **Do not drain one partition to its end before polling the rest, unless what is routed to the
//!   others fits their queues.** A consumer that finishes partition 0 before it looks at partition 1
//!   stalls as soon as partition 1's queue fills: the input tasks holding chains for partition 1 are
//!   parked on it, and partition 0's remaining chunks are behind them. What an output ends with
//!   before that point ends nothing else - an output nobody has claimed yet stays claimable and the
//!   read keeps running for it, since it has not been handed out yet. Polling the partitions in turn
//!   - which is what a DataFusion executor does - is enough; they need not be on separate tasks.
//! - **Dropping an output is not the same as leaving it unread.** An input task stops routing to a
//!   dropped output instead of suspending on it, because a queue nobody will drain is not worth
//!   waiting for, and a plan that dropped one of its own outputs is being torn down. If chains read
//!   for it were never delivered, the outputs that are still being read end with an error saying so
//!   rather than with an end: the read did not finish.
//!
//! # Memory, backpressure and end of stream
//!
//! Each output has a bounded channel of [`OUTPUT_CAPACITY`] **chains** - a bound on how far the read
//! may run ahead of a consumer, not a bound on bytes - and each input task holds at most the one chain
//! it is assembling. Every queued chain is charged to a [`MemoryConsumer`] on the task context's pool,
//! the charge travels with the chain and is released when a consumer takes it or drops it, so bytes
//! are bounded by the pool refusing to grow rather than by the queue length. A pool that refuses a
//! chain ends the read with that error, because waiting for memory that will not come is not progress.
//!
//! An output ends only when every input partition finished and every chain routed to it was handed
//! over; a chain that closed locally is not the end of anything. With one task per input there is no
//! longer a single place that sees every input finish, so the tasks count themselves down: the one
//! that counts the last of the others off records that the read finished, and returning is what drops
//! the last sender and closes the queues - so an output woken by the close reads the outcome instead
//! of finding nothing there. If the child errors, every output receives that error as its terminal
//! item rather than a clean end, because an end would report the series that never arrived as series
//! that do not exist; the failing task records the failure before it hands the error over, and the
//! remaining input tasks are aborted rather than read to an end nobody asked for.
//!
//! A queue closing is not by itself that end: it also closes when the read is stopped - aborted, or
//! panicked - and a stopped read is missing exactly what it did not route. The read therefore records
//! how it ended, and an output whose queue has closed reads that record, so only a read that ended on
//! its own terms ends an output and anything else is an error saying the producer stopped
//! unexpectedly.
//!
//! An output stream owns the read, not the plan it was executed from: DataFusion's
//! `execute_stream_partitioned` takes the plan by value and keeps only the streams, so a read tied to
//! the plan would be stopped while its streams are still being consumed. The read therefore lives as
//! long as any of its outputs does, and stops only when there is nothing left it could deliver to:
//! every output was handed out and every one of them is gone. An output that ends - normally, or by
//! being dropped - while another partition was never executed stops nothing by itself, because that
//! partition can still claim its queue; the execution going away as a whole, which no later `execute`
//! can reach, is the other point at which everything is released.

use std::fmt::{self, Formatter};
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll};

use common_error::ext::BoxedError;
use common_query::series_frame::{
    SendableEvaluatedSeriesStream, pack_evaluated_series, unpack_evaluated_series,
};
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryReservation};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::EmissionType;
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, InputDistributionRequirements,
    Partitioning, PlanProperties, SendableRecordBatchStream,
};
use datatypes::series::SeriesLabels;
use datatypes::series::frame::{FrameContent, SeriesFrameDeclaration};
use datatypes::series::samples::{Series, SeriesSchema};
use futures::{Stream, StreamExt};
use tokio::sync::mpsc;
use twox_hash::XxHash64;

use crate::grid_series::{label_set, series_bytes};

#[cfg(test)]
mod tests;

/// How many whole chains one output partition queues before an input task suspends on it.
///
/// This bounds read-ahead, NOT memory: the chains are already-decoded evaluated slices, so four of
/// them can still be large, and what actually bounds the bytes is the [`MemoryConsumer`] on the
/// task context's pool refusing to grow. Small and fixed because queue depth buys little here and
/// the sooner a slow consumer stops the read, the sooner the memory it would have taken is not
/// taken.
const OUTPUT_CAPACITY: usize = 4;

/// The seed behind every routing decision.
///
/// Fixed rather than drawn per execution, so which output a label set belongs on is a function of
/// the label set and the output count alone: an execution's own decisions have to agree with each
/// other, and a seeded router can be reasoned about - and tested - without one.
const ROUTING_SEED: u64 = 0x517c_c1b7_2722_0a95;

/// The Repartition stage: one child's partitions, redistributed by canonical label set.
///
/// The node holds one execution's shared state: its output partitions are separate `execute` calls
/// that must agree on one read of the child, so the first call starts the read and every call takes
/// its own output from it.
pub struct SeriesRepartitionExec {
    input: Arc<dyn ExecutionPlan>,
    /// The frame the child declares, re-declared by every output batch.
    series_schema: Arc<SeriesSchema>,
    /// The one hash configuration of this node's execution.
    routing: Routing,
    /// Started by the first `execute` call, since the output partitions share one read of the child.
    shared: Mutex<Option<Arc<SharedExecution>>>,
    properties: Arc<PlanProperties>,
}

impl SeriesRepartitionExec {
    /// The name this node is shown under.
    pub const fn name() -> &'static str {
        "SeriesRepartitionExec"
    }

    /// Creates the node over `input`, routing into `outputs` partitions.
    ///
    /// `outputs` is fixed here, by the planner, which knows the target partition count; the node
    /// reads no session configuration of its own. A child that does not declare an evaluated frame
    /// this node can re-declare is refused before the plan can run: the output schema is the child's,
    /// and a frame whose declaration does not survive the round trip would be published under a
    /// schema its batches do not match.
    pub fn try_new(input: Arc<dyn ExecutionPlan>, outputs: usize) -> DataFusionResult<Self> {
        if outputs == 0 {
            return Err(DataFusionError::Plan(
                "SeriesRepartition requires at least one output partition".to_string(),
            ));
        }
        let declaration =
            SeriesFrameDeclaration::parse(input.schema().as_ref()).map_err(|error| {
                DataFusionError::Plan(format!(
                    "SeriesRepartition input is not a series frame: {error}"
                ))
            })?;
        if declaration.content() != FrameContent::Evaluated {
            return Err(DataFusionError::Plan(
                "SeriesRepartition routes evaluated series fragments, not raw samples".to_string(),
            ));
        }
        let series_schema = Arc::new(declaration.series().clone());
        let declared = pack_evaluated_series(
            Box::pin(futures::stream::empty()),
            Arc::clone(&series_schema),
        )?
        .schema();
        if declared != input.schema() {
            return Err(DataFusionError::Plan(
                "SeriesRepartition input frame carries a schema this node cannot re-declare"
                    .to_string(),
            ));
        }
        let properties = Arc::new(PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            Partitioning::UnknownPartitioning(outputs),
            // Every chunk is handed on as it is read, so an output is written more than once - and
            // an input that only answers at its end is still answered as it arrives.
            EmissionType::Incremental,
            // The node ends when its child does, and spends its life waiting until then.
            input.properties().boundedness,
        ));
        Ok(Self {
            input,
            series_schema,
            routing: Routing::new(outputs),
            shared: Mutex::new(None),
            properties,
        })
    }

    /// How many output partitions this node has.
    fn outputs(&self) -> usize {
        self.properties.partitioning.partition_count()
    }
}

impl fmt::Debug for SeriesRepartitionExec {
    /// The node and what it routes: the child, the schema and the running execution are noise in a
    /// plan's debug form.
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("SeriesRepartitionExec")
            .field("outputs", &self.outputs())
            .finish_non_exhaustive()
    }
}

impl DisplayAs for SeriesRepartitionExec {
    fn fmt_as(&self, _t: DisplayFormatType, f: &mut Formatter) -> fmt::Result {
        write!(
            f,
            "SeriesRepartitionExec: outputs={}, input_partitions={}",
            self.outputs(),
            self.input.properties().partitioning.partition_count()
        )
    }
}

impl ExecutionPlan for SeriesRepartitionExec {
    fn name(&self) -> &str {
        Self::name()
    }

    fn properties(&self) -> &Arc<PlanProperties> {
        &self.properties
    }

    /// Nothing is asked of the input's distribution, and nothing may be inserted above it.
    ///
    /// The child's own partitioning is what this node reads: it reads every partition as it is, and
    /// an exchange inserted between the two would reorder chunks across the chain boundaries the node
    /// relies on, which no requirement it states could survive. A hash requirement would be worse
    /// still, since the routing key is decoded from the rows rather than computed by an expression.
    fn input_distribution_requirements(&self) -> InputDistributionRequirements {
        InputDistributionRequirements::new(vec![Distribution::UnspecifiedDistribution])
    }

    /// Splitting the child further would not help: this node reads every input partition itself, so
    /// a repartition below it only adds a hop that reorders what it must read in order.
    fn benefits_from_input_partitioning(&self) -> Vec<bool> {
        vec![false]
    }

    fn children(&self) -> Vec<&Arc<dyn ExecutionPlan>> {
        vec![&self.input]
    }

    fn apply_expressions(
        &self,
        _f: &mut dyn FnMut(&Arc<dyn PhysicalExpr>) -> DataFusionResult<TreeNodeRecursion>,
    ) -> DataFusionResult<TreeNodeRecursion> {
        // The routing key is decoded from the rows, so this node binds no expression of its own.
        Ok(TreeNodeRecursion::Continue)
    }

    fn with_new_children(
        self: Arc<Self>,
        mut children: Vec<Arc<dyn ExecutionPlan>>,
    ) -> DataFusionResult<Arc<dyn ExecutionPlan>> {
        if children.len() != 1 {
            return Err(DataFusionError::Plan(format!(
                "SeriesRepartitionExec requires one child, got {}",
                children.len()
            )));
        }
        // Rebuilt rather than patched, so a child whose schema changed is refused here instead of
        // routed under a frame declaration the new child does not make.
        Ok(Arc::new(Self::try_new(
            children.swap_remove(0),
            self.outputs(),
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        if partition >= self.outputs() {
            return Err(DataFusionError::Execution(format!(
                "SeriesRepartition output partition {partition} is out of range"
            )));
        }
        let shared = {
            let mut state = self
                .shared
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            if state.is_none() {
                // The whole execution is started here, once: the output partitions are separate
                // calls, and reading the child once for all of them is what keeps a chunk from being
                // emitted twice or read by a scan that was never meant to run twice.
                *state = Some(SharedExecution::start(
                    &self.input,
                    self.routing,
                    Arc::clone(&self.series_schema),
                    &context,
                )?);
            }
            Arc::clone(state.as_ref().expect("the execution was just started"))
        };
        let (receiver, lease) = SharedExecution::claim(&shared, partition).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "SeriesRepartition output partition {partition} was executed twice"
            ))
        })?;
        // The queue and the claim on the read are taken in one step, so an output that is refused -
        // or one that is handed out after every other output already finished - is counted for
        // exactly what it is.
        pack_evaluated_series(
            Box::pin(RepartitionStream {
                receiver,
                // Nothing of a chain is read from the queue yet.
                chain: Vec::new().into_iter(),
                lease,
                reported: false,
            }),
            Arc::clone(&self.series_schema),
        )
    }
}

/// One execution's state: the child read once, and the outputs it is being distributed into.
///
/// The plan that started it holds this, and so does every output stream it hands out. The read it
/// owns lives while any of them does - which is what makes it survive the plan - and what stops it is
/// [`Running::claimed`] and [`Running::remaining`] both reaching zero, or the state itself going away.
struct SharedExecution {
    /// The queues, the tasks filling them, and how many of the execution's outputs the read still has.
    running: Mutex<Running>,
    /// How the read ended, for an output whose queue has closed to read.
    ending: Arc<Mutex<Ending>>,
}

/// What one execution's state holds that has to be stopped together.
struct Running {
    /// One receiver per output partition, taken by the `execute` call of that partition. A receiver
    /// that is never taken stays here, which is what makes its queue fill and the input tasks whose
    /// chains belong there suspend.
    receivers: Vec<Option<mpsc::Receiver<Queued>>>,
    /// The tasks reading the child's partitions into those queues, and how many of them are left.
    inputs: Arc<Inputs>,
    /// How many output streams were handed out and are not yet released.
    ///
    /// Counted rather than derived from a reference count, because the plan holds this state too and
    /// is not an output: what has to reach zero for the read to be over is the number of streams, not
    /// the number of owners.
    claimed: usize,
    /// How many outputs were never handed out, and can still be.
    ///
    /// Kept apart from [`claimed`](Self::claimed) because the two say different things: an output that
    /// is claimed is one somebody is reading, and one that is only `remaining` is one nobody has asked
    /// for yet. A claimed count that reaches zero with outputs left in this count has not lost its
    /// consumers - it is waiting for them - and the queues of those outputs are exactly what a later
    /// `execute` comes for, so they must outlive the read of every output that was claimed.
    remaining: usize,
}

impl Running {
    /// Stops the read: every queue nobody took is dropped and every input task is aborted.
    ///
    /// Idempotent - the queues are already gone on a second call, and aborting a finished or already
    /// aborted task is a no-op. Dropping the queues of outputs that were never handed out is part of
    /// it: what was queued for them is read-ahead nobody will take, and holding it until the process
    /// ends would keep both the memory and the child's scans alive for nothing.
    fn stop(&mut self) {
        self.receivers.clear();
        // Aborting a read that has not recorded an outcome is what the outputs still open have to
        // see: nothing was recorded, so the end they read is "the producer stopped unexpectedly".
        self.inputs.stop();
    }
}

impl SharedExecution {
    /// Starts reading every partition of `input` into `routing`'s outputs on `context`'s pool.
    ///
    /// One task per child partition, started here: the child's computation happens inside its own
    /// `poll_next`, so a partition read on a task of another partition would run there, and the
    /// partition count the child declared would buy no parallelism at all.
    fn start(
        input: &Arc<dyn ExecutionPlan>,
        routing: Routing,
        series_schema: Arc<SeriesSchema>,
        context: &Arc<TaskContext>,
    ) -> DataFusionResult<Arc<Self>> {
        let partitions = input.properties().partitioning.partition_count();
        let mut streams = Vec::with_capacity(partitions);
        for child_partition in 0..partitions {
            let frames = input.execute(child_partition, Arc::clone(context))?;
            // The expected schema is a check and never an input: the decoder reads the frame's own
            // declaration, and a partition declaring another frame is refused here rather than
            // routed under labels it does not carry.
            streams.push(unpack_evaluated_series(
                frames,
                Some(Arc::clone(&series_schema)),
            )?);
        }
        let mut senders = Vec::with_capacity(routing.outputs);
        let mut receivers = Vec::with_capacity(routing.outputs);
        for _ in 0..routing.outputs {
            let (sender, receiver) = mpsc::channel(OUTPUT_CAPACITY);
            senders.push(sender);
            receivers.push(Some(receiver));
        }
        let ending = Arc::new(Mutex::new(Ending::Unrecorded));
        let inputs = Inputs::new(Arc::clone(&ending), partitions);
        // A child with no partitions has nothing to read, so the read finished before it started: the
        // count that the last input task would bring to zero is already there.
        if partitions == 0 {
            inputs.record(Ending::Finished);
        }
        let pool = Arc::clone(&context.runtime_env().memory_pool);
        for (partition, stream) in streams.into_iter().enumerate() {
            // Each task is handed its own copy of the senders rather than a lock over one copy: a
            // send is then a send of its own, and the queues close when the last task returns instead
            // of when some shared lock says so.
            let task = common_runtime::spawn_query(route_partition(
                stream,
                partition,
                routing,
                senders.clone(),
                // What this partition reads ahead is charged to its own consumer, so the read-ahead
                // of one partition is attributable to the task that did it.
                MemoryConsumer::new("SeriesRepartitionInput").register(&pool),
                Arc::clone(&inputs),
            ));
            inputs.attach(partition, task);
        }
        // This copy is dropped before any output exists, so it is no part of when the queues close.
        drop(senders);
        Ok(Arc::new(Self {
            running: Mutex::new(Running {
                receivers,
                inputs,
                // No output is handed out yet: every one of them is still to be claimed.
                claimed: 0,
                remaining: routing.outputs,
            }),
            ending,
        }))
    }

    /// The queue of `partition` and the lease on this read that reading it takes.
    ///
    /// The queue and the claim are taken under one lock, and that is what keeps the counts honest: an
    /// output stops being one nobody asked for and becomes one the read is running for in the same
    /// step, so a release on another task cannot see "nothing left" in between and stop a read that
    /// is being handed out. `None` means that output was claimed before - its queue is gone and this
    /// call does not count as one the read is running for.
    fn claim(
        shared: &Arc<Self>,
        partition: usize,
    ) -> Option<(mpsc::Receiver<Queued>, OutputLease)> {
        let mut running = shared
            .running
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        let receiver = running
            .receivers
            .get_mut(partition)
            .and_then(Option::take)?;
        running.remaining -= 1;
        running.claimed += 1;
        Some((
            receiver,
            OutputLease {
                shared: Arc::clone(shared),
            },
        ))
    }

    /// Gives up one claim, and stops the read once there is nothing left it could deliver to.
    ///
    /// This is the one decision the lifecycle turns on, and it is not "the last output went away": an
    /// output that is released is one that finished being read, and the outputs nobody has claimed
    /// yet are ones a later `execute` will still claim. The read may stop only when both are
    /// impossible - no output is being read ([`claimed`](Running::claimed) is zero) and none is left
    /// to hand out ([`remaining`](Running::remaining) is zero) - so an input task parked on the queue
    /// of an output that has not been claimed yet keeps waiting: that is backpressure on a consumer
    /// that is still coming, not a read with nowhere to go.
    fn release_output(&self) {
        let mut running = self
            .running
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        running.claimed -= 1;
        if running.claimed == 0 && running.remaining == 0 {
            running.stop();
        }
    }

    /// How the read ended, as an output whose queue has closed has to report it.
    fn ending(&self) -> Ending {
        *self
            .ending
            .lock()
            .unwrap_or_else(|error| error.into_inner())
    }

    /// Stops the read and releases everything it holds.
    ///
    /// Called when the execution itself goes away - the plan and every output are gone, so no later
    /// `execute` can claim an output that was never handed out - and by
    /// [`release_output`](Self::release_output) when the last claim is given up on an execution that
    /// has nothing left to hand out. Idempotent, because both can happen and because the state's own
    /// drop runs it once more.
    fn stop(&self) {
        let mut running = self
            .running
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        running.stop();
    }
}

impl Drop for SharedExecution {
    fn drop(&mut self) {
        // Nothing holds the read any more: every output is gone and so is the plan. A task parked on
        // a queue nobody will drain, or on a scan, must not outlive both of them.
        self.stop();
    }
}

/// The input tasks of one read: what is left of them, and what the last of them records.
///
/// There is one task per child partition and no task above them, so there is no longer one place that
/// sees every input finish and no producer whose return closes the queues. This is that place
/// instead: the tasks count themselves down, and the task that counts the last of the others off
/// records that the read finished and then returns - returning is what drops its senders, so the
/// record is made before the queues close, and an output woken by their closing reads the outcome
/// instead of finding nothing there.
struct Inputs {
    /// The tasks themselves, and the count of those that have not stopped.
    state: Mutex<InputState>,
    /// How the read ended, for an output whose queue has closed to read.
    ending: Arc<Mutex<Ending>>,
}

/// What [`Inputs`] guards.
struct InputState {
    /// The task of each input partition, by partition, `None` once it stopped.
    ///
    /// Kept rather than dropped so that the read can be stopped as a whole, and so that the first task
    /// to fail can abort the rest: a partition whose peer already failed has nothing left to route
    /// that an output would still take.
    tasks: Vec<Option<common_runtime::JoinHandle<()>>>,
    /// How many of those tasks have not stopped yet.
    live: usize,
    /// Whether the read was stopped from outside - every output is gone, or the plan's state went
    /// away. It is the one thing that keeps the last task from recording that the read finished: a
    /// stopped read never finished, and the outputs that wake on the close of their queues must say
    /// so rather than report a complete answer.
    stopped: bool,
}

impl Inputs {
    /// The bookkeeping of a read over `partitions` input partitions.
    fn new(ending: Arc<Mutex<Ending>>, partitions: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(InputState {
                tasks: (0..partitions).map(|_| None).collect(),
                live: partitions,
                stopped: false,
            }),
            ending,
        })
    }

    /// Gives `task` the slot of `partition`, so that the read can still abort it.
    ///
    /// Stored even if the task has already stopped - a partition that answers at once can return
    /// before its handle is stored - because the slot is only consulted to stop what is still running,
    /// and aborting a finished task is a no-op.
    fn attach(&self, partition: usize, task: common_runtime::JoinHandle<()>) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.tasks[partition] = Some(task);
    }

    /// Records that `partition` ended, and that the read finished if it was the last one running.
    ///
    /// Nothing is recorded when the read was stopped from outside: its tasks do not reach this point,
    /// and one that gets here anyway must not turn a stopped read into a finished one.
    fn ended(&self, partition: usize) {
        if self.count_out(partition) {
            self.record(Ending::Finished);
        }
    }

    /// Records that the chain this partition was routing had no output left to be delivered to.
    ///
    /// Nothing is recorded as finished here, whatever the other partitions go on to do: the read lost
    /// the chains that output would have carried, so it did not finish.
    fn abandoned(&self, partition: usize) {
        self.record(Ending::Abandoned);
        self.count_out(partition);
    }

    /// Records that `partition` failed, hands the failure to every output, and stops the rest.
    ///
    /// The failure is recorded first: the queues close when the last task returns, and an output
    /// woken by that has to read that the read failed rather than read nothing. Every output then
    /// receives the error as a terminal item, so an output that was still being read cannot mistake
    /// the failure for an end - and the other input tasks are aborted, because what they would still
    /// route belongs to a read that has already failed.
    async fn failed(
        &self,
        partition: usize,
        error: &Arc<DataFusionError>,
        senders: &[mpsc::Sender<Queued>],
    ) {
        self.record(Ending::Failed);
        self.abort_others(partition);
        for sender in senders {
            // Ignored: an output that is gone has nobody left to tell, and one whose queue is full
            // is drained by the consumer that owns it.
            let _ = sender.send(Err(Arc::clone(error))).await;
        }
        self.count_out(partition);
    }

    /// Counts `partition` off the read, and says whether it was the last task still running.
    fn count_out(&self, partition: usize) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        // Taken and dropped: this task is the one stopping, and holding its own handle would leave the
        // read waiting for a task that has already finished. Saturating because a task that was
        // stopped from outside may still reach this after [`stop`](Self::stop) counted it off.
        state.tasks[partition] = None;
        state.live = state.live.saturating_sub(1);
        state.live == 0 && !state.stopped
    }

    /// Aborts every input task but `partition`'s own.
    fn abort_others(&self, partition: usize) {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        for (index, task) in state.tasks.iter().enumerate() {
            // Aborting a task that already stopped is a no-op, so the finished ones cost nothing
            // here.
            if index != partition
                && let Some(task) = task
            {
                task.abort();
            }
        }
    }

    /// Stops every input task that has not stopped yet, and counts the read as stopped.
    ///
    /// The count is set rather than left to the tasks: an aborted task does not run its own
    /// bookkeeping, and a read whose tasks were aborted never reached an end of its own.
    fn stop(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.stopped = true;
        state.live = 0;
        for task in state.tasks.iter_mut() {
            if let Some(task) = task.take() {
                task.abort();
            }
        }
    }

    /// Records `outcome` as how the read ended, unless an outcome that outranks it is already there.
    ///
    /// A failure outranks everything: it is the only ending whose error is handed to every output, and
    /// nothing may replace it. Nothing outranks a recorded ending either, so the first task to record
    /// `Abandoned` or `Finished` is the one reported - a read that lost chains to a dropped output did
    /// not finish, whatever the other partitions went on to do.
    fn record(&self, outcome: Ending) {
        let mut ending = self
            .ending
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        match outcome {
            Ending::Failed => *ending = Ending::Failed,
            _ if matches!(*ending, Ending::Unrecorded) => *ending = outcome,
            _ => {}
        }
    }

    /// How many input tasks have not stopped, which is what a test sees the read stop by.
    #[cfg(test)]
    fn live(&self) -> usize {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .live
    }
}

/// One input partition's task: reads its own child, one whole chain at a time, and hands each chain
/// to the output its canonical label set hashes to.
///
/// This is the whole of that input's routing. Nothing here reads another partition, waits for another
/// task's decision, or holds a lock another task takes, so the two things it can be parked on are its
/// own child and a full output queue - and neither can be waiting on another input's progress.
///
/// What it saw is recorded before it returns, and returning is what drops its senders: the queues
/// close only once every task has done that, so the outcome is in place before anything can be woken
/// by a close.
async fn route_partition(
    mut stream: SendableEvaluatedSeriesStream,
    partition: usize,
    routing: Routing,
    senders: Vec<mpsc::Sender<Queued>>,
    read_ahead: MemoryReservation,
    inputs: Arc<Inputs>,
) {
    let stop = read_chains(&mut stream, &routing, &senders, &read_ahead).await;
    match &stop {
        Stop::Ended => inputs.ended(partition),
        Stop::Abandoned => inputs.abandoned(partition),
        Stop::Failed(error) => inputs.failed(partition, error, &senders).await,
    }
}

/// Why one input task stopped reading its partition.
enum Stop {
    /// The partition ended: every chunk it had was read and handed to the output it belongs on.
    Ended,
    /// The output that should have taken a chain was gone, so the chain was never delivered.
    Abandoned,
    /// The partition failed, or a chain could not be buffered: every output has to learn that.
    Failed(Arc<DataFusionError>),
}

/// Reads one input partition to its end, one whole chain at a time, routing each chain as it closes.
///
/// A chain is assembled here and handed over as one unit, which is what keeps a shared output queue
/// from holding half of one series while another source's chain is open on it: the queue carries whole
/// chains, so two inputs can order them differently but never interleave them. The price is paid on
/// this side of the queue - nothing of a chain is sent before the chain closes, and a long chain is
/// held for as long as it is being read - and it is charged to the pool as it grows.
///
/// What this function can be parked on is its own child or a full output queue. The queue is drained
/// by whoever polls that output, which is the DataFusion contract the module documents; the child is
/// driven by nothing but this task. It never waits on another input's task.
async fn read_chains(
    stream: &mut SendableEvaluatedSeriesStream,
    routing: &Routing,
    senders: &[mpsc::Sender<Queued>],
    read_ahead: &MemoryReservation,
) -> Stop {
    loop {
        // One whole chain, assembled here: the chunk that closes the series ends it, and so does a
        // stream that ends between two chains.
        let mut chain: Chain = Vec::new();
        let mut ended = false;
        loop {
            match stream.next().await {
                Some(Ok((series, is_last))) => {
                    // Charged as the chain grows, so a chain that does not fit the pool is refused
                    // here rather than after it has been held.
                    let charge = read_ahead.new_empty();
                    if let Err(error) = charge.try_grow(series_bytes(&series)) {
                        return Stop::Failed(Arc::new(error));
                    }
                    chain.push((series, is_last, charge));
                    if is_last {
                        break;
                    }
                }
                Some(Err(error)) => {
                    // The error is the last thing this partition says: it is handed to every output
                    // rather than ending this one task quietly.
                    return Stop::Failed(Arc::new(DataFusionError::External(Box::new(error))));
                }
                None => {
                    ended = true;
                    break;
                }
            }
        }
        if ended {
            // A partition that ends is a partition with nothing left to route - unless it ended
            // inside a chain, which no later chunk can close. The decoders refuse that, so this is a
            // producer that bypassed them, and its output would hold a series no chunk can close.
            return if chain.is_empty() {
                Stop::Ended
            } else {
                Stop::Failed(Arc::new(DataFusionError::Execution(
                    "series repartition input ended with its series open".to_string(),
                )))
            };
        }
        // One hash per chain, over the labels of its first chunk: the chunks of a chain are one
        // series, so the first of them names the output for all of them.
        let output = routing.output(&chain[0].0.metric);
        if senders[output].send(Ok(chain)).await.is_err() {
            // The output was dropped, so nothing it would have received is wanted: this task stops
            // instead of waiting on a queue nobody will drain.
            return Stop::Abandoned;
        }
    }
}

/// What one output stream holds so that the read belongs to the streams rather than to the plan.
///
/// DataFusion's `execute_stream_partitioned` takes the plan by value and keeps only the streams, so
/// the state that started the read is dropped before the first chunk is read: a read stopped there
/// would end every output early, and an end is not something a consumer can tell from a complete
/// answer. The lease is what keeps it running instead; and because only streams hold one, a lease
/// going away is the point at which that one output can no longer take a chunk - which
/// [`SharedExecution::release_output`] turns into the end of the read only when no output is left that
/// could.
struct OutputLease {
    shared: Arc<SharedExecution>,
}

impl OutputLease {
    /// What an output whose queue has closed reports.
    ///
    /// An end is this node's claim that every input partition finished and every chunk routed here
    /// was handed over, so only a read that ended that way may produce one. Everything else is the
    /// read stopping underneath its outputs, which is a failure of the answer and not the end of it.
    fn ending(&self) -> Option<Result<(Series, bool), BoxedError>> {
        match self.shared.ending() {
            Ending::Finished | Ending::Failed => None,
            Ending::Abandoned => Some(Err(boxed(DataFusionError::Execution(
                "series repartition stopped before the read finished: an output partition was dropped"
                    .to_string(),
            )))),
            Ending::Unrecorded => Some(Err(boxed(DataFusionError::Execution(
                "series repartition producer stopped unexpectedly before the read finished"
                    .to_string(),
            )))),
        }
    }
}

impl Drop for OutputLease {
    fn drop(&mut self) {
        // An output that is done is not by itself the end of the read: the outputs that were never
        // claimed are still to come, and what is queued for them is what they will read. Giving up
        // the claim therefore only says that this one output will take nothing more - whether the
        // read can go on is decided from every output, claimed and unclaimed alike.
        self.shared.release_output();
    }
}

/// How a read ended, as an output whose queue has closed has to report it.
#[derive(Clone, Copy, Debug)]
enum Ending {
    /// Nothing recorded an outcome - and a read that ends on its own terms records one before it
    /// drops its queues - so this is a read that was stopped, aborted or panicked, mid-flight.
    Unrecorded,
    /// Every input partition finished and every chunk routed to an output was handed over.
    Finished,
    /// The read stopped because an output went away, leaving the chunks read for it undelivered.
    Abandoned,
    /// The read failed; its error was already handed to every output as a terminal item.
    Failed,
}

/// One whole chain of one series: its chunks from the one that opened it to the one that closed it,
/// each with the bytes it is charged.
///
/// This is what an output queue carries, and that is deliberate. A downstream frame reader needs one
/// series' chunks contiguous, so a queue of chunks would let two sources' partial chains interleave on
/// one output; a queue of chains cannot, whatever order the sources happen to send in.
type Chain = Vec<(Series, bool, MemoryReservation)>;

/// What one output queue carries: a whole chain, charged for the bytes it holds, or the error that
/// ended one input partition's read.
///
/// The charge a partition's task took is handed on with the chain it is for, so what one output has
/// queued is exactly what the pool has reserved for it.
type Queued = Result<Chain, Arc<DataFusionError>>;

/// One output partition's stream: the chains routed here, handed over a chunk at a time.
struct RepartitionStream {
    receiver: mpsc::Receiver<Queued>,
    /// The chain being handed over, and how far into it this stream is.
    ///
    /// Taken off the queue as one unit and published one chunk per batch, so a consumer sees the same
    /// chunks it always saw while what the queue held was a whole chain: the interleaving a chunk-wise
    /// queue would allow has no way to happen after the fact.
    chain: std::vec::IntoIter<(Series, bool, MemoryReservation)>,
    /// The read this stream is an output of.
    lease: OutputLease,
    /// Whether the end of the queue was already reported, so a poll after it is the end and not the
    /// same failure a second time.
    reported: bool,
}

impl Stream for RepartitionStream {
    type Item = Result<(Series, bool), BoxedError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        loop {
            if let Some((series, is_last, charge)) = this.chain.next() {
                // The chunk is no longer the node's to hold: whoever reads it owns it from here.
                drop(charge);
                return Poll::Ready(Some(Ok((series, is_last))));
            }
            if this.reported {
                return Poll::Ready(None);
            }
            match this.receiver.poll_recv(cx) {
                Poll::Ready(Some(Ok(chain))) => this.chain = chain.into_iter(),
                // The error is shared rather than rebuilt, so every output reports the one failure the
                // child answered with instead of M copies of its text.
                Poll::Ready(Some(Err(error))) => {
                    return Poll::Ready(Some(Err(boxed(DataFusionError::from(&error)))));
                }
                // The queue closing is not the end of the read on its own: aborting the read closes it
                // too, and calling that an end would report every chunk it never routed as a chunk
                // that does not exist. What the read recorded says which of the two this is.
                Poll::Ready(None) => {
                    this.reported = true;
                    return Poll::Ready(this.lease.ending());
                }
                Poll::Pending => return Poll::Pending,
            }
        }
    }
}

/// The boxed error a frame stream reports.
fn boxed(error: DataFusionError) -> BoxedError {
    BoxedError::new(common_query::error::Error::from(error))
}

/// The hash configuration one execution routes with.
///
/// The seed is what an execution builds once; a [`XxHash64`] accumulates, so a fresh hasher starts
/// from this configuration for each chunk, and that is the only thing each chunk builds.
#[derive(Clone, Copy, Debug)]
struct Routing {
    /// The number of output partitions a hash is reduced modulo.
    outputs: usize,
    /// The seed every chunk of this execution is hashed with.
    seed: u64,
}

impl Routing {
    /// The routing of an execution with `outputs` output partitions.
    fn new(outputs: usize) -> Self {
        Self {
            outputs,
            seed: ROUTING_SEED,
        }
    }

    /// The output partition `metric`'s canonical label set belongs on.
    ///
    /// The whole label set is hashed, since that is what a merge groups by: pieces of two series
    /// whose label sets differ have to be able to land apart, or the split this node exists to make
    /// would not happen. [`label_set`] is what canonicalizes the labels - dropping empty values and
    /// keeping the metric name - so two pieces that merge hash the same.
    fn output(&self, metric: &SeriesLabels) -> usize {
        let mut hasher = XxHash64::with_seed(self.seed);
        label_set(metric).hash(&mut hasher);
        // `outputs` is at least one, which [`SeriesRepartitionExec::try_new`] is what guarantees.
        (hasher.finish() % self.outputs as u64) as usize
    }
}
