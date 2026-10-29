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

//! Routes evaluated series chains by canonical label set across DataFusion input partitions.
//!
//! This node routes chains by canonical label-set hash, then waits for all producers to finish and
//! merges exact metric-name/tag identities at each owner. This is owner-local grouping, not a claim
//! of global canonical completeness. It operates on DataFusion partitions only; arbitrary `MatchKey`
//! and `GroupKey` routing is unsupported.
//!
//! Chains are transported intact to owners; they are grouped there only after successful EOF. The
//! frame is the child's own: evaluated chunks are decoded only to read routing labels, and outputs
//! publish the child's declared layout.
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
//! A consumer of these frames requires a series' identity not to change and its positions to increase
//! strictly while open, with `is_last` closing it. An input task therefore assembles each child chain
//! before routing it as one atomic unit. This prevents chains from interleaving during transport; it
//! does not establish the output order or final series boundaries. After successful global EOF, each
//! owner merges exact identities and publishes one complete series per identity.
//!
//! What that costs is stated rather than hidden. Nothing of a chain is handed over before the chain
//! closes, so the first chunk of a series reaches its output later than a chunk-at-a-time design would
//! deliver it, and a long chain occupies memory for as long as it is being read. Those bytes are
//! charged to a [`MemoryConsumer`] as the chain grows, so a pool under pressure ends the read with an
//! error instead of letting a chain grow without bound: the node refuses, it does not spin and it does
//! not buffer past the pool's limit.
//!
//! Queues carry whole chains rather than individual chunks, so two sources' partial chains cannot
//! interleave on one shared output queue; each output needs only one queue.
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
//! buys no parallelism at all. It is also what keeps routing local, which is what this node needs
//! when two of them are composed:
//!
//! - A partial chain never occupies a shared output queue, so no output waits for a continuation: a
//!   chain is either fully assembled in the input task that read it, or not sent at all.
//! - A task never waits for another task's routing decision, and the tasks share no lock while
//!   routing, so where one input's chain goes is not something another input learns or needs.
//!
//! Producer queues are unbounded: unread owners do not stall routing, but their retained chains
//! continue to consume pool-accounted memory. Owners wait for successful EOF before publishing any
//! data, then merge complete exact identities and recompute finality as one output per identity.
//!
//! # Memory, backpressure and end of stream
//!
//! Output queues are unbounded; each input task holds at most the one chain it is assembling. Every
//! queued chain is admitted through the task context's pool and carries its charge. The pool controls
//! admission, but queue length is not a constant-byte bound. Owners retain all routed chains until all
//! producers finish successfully, then reserve merge workspace and merge exact identities locally;
//! this owner-side EOF buffer is an additional retention cost. No task waits for memory admission.
//!
//! An output ends only when every input partition finished and every chain routed to it was handed
//! over; a chain that closed locally is not the end of anything. With one task per input there is no
//! single place that sees every input finish, so the tasks count themselves down: the one
//! that counts the last of the others off records that the read finished, and returning is what drops
//! the last sender and closes the queues - so an output woken by the close reads the outcome instead
//! of finding nothing there. If the child errors, the failing task records it before cancelling its peers; sender closure makes
//! every output report that recorded error instead of a clean end.
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

use std::collections::BTreeMap;
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
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{EmissionType, EvaluationType, SchedulingType};
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

use crate::grid_series::{label_set, merge_same_labelset};
use crate::native::execute::merge_workspace_bytes;
use crate::native::execute::operand_chunk_bytes;
use datatypes::series::samples::DenseSamplesRef;

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
        if input.properties().boundedness.is_unbounded() {
            return Err(DataFusionError::Plan(
                "SeriesRepartition requires a finite input to finalize series identities".into(),
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
        let properties = PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            Partitioning::UnknownPartitioning(outputs),
            // An owner can finalize an identity only after all producers finish successfully.
            EmissionType::Final,
            // The node ends when its child does, and spends its life waiting until then.
            input.properties().boundedness,
        )
        .with_evaluation_type(EvaluationType::Eager)
        .with_scheduling_type(SchedulingType::Cooperative);
        let properties = Arc::new(properties);
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
        // Take the queue and its claim on the read together.
        let output = pack_evaluated_series(
            Box::pin(RepartitionStream::new(
                receiver,
                lease,
                Arc::clone(&self.series_schema),
                &context.runtime_env().memory_pool,
            )),
            Arc::clone(&self.series_schema),
        )?;
        Ok(datafusion::physical_plan::coop::make_cooperative(output))
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
    /// How the read ended, and the failure it owes its outputs if it failed.
    ending: Arc<Mutex<Ending>>,
}

/// What one execution's state holds that has to be stopped together.
struct Running {
    /// One receiver per output partition, taken by the `execute` call of that partition. A receiver
    /// that is never taken stays here with its pool-accounted queued chains.
    receivers: Vec<Option<mpsc::UnboundedReceiver<Queued>>>,
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
    /// for yet. A claimed count that reaches zero with outputs left here has no active consumers
    /// yet, but those queues survive so a later `execute` can claim them.
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
            let (sender, receiver) = mpsc::unbounded_channel();
            senders.push(sender);
            receivers.push(Some(receiver));
        }
        let ending = Arc::new(Mutex::new(Ending::default()));
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
            let report = ReportIfUnreported {
                inputs: Arc::clone(&inputs),
                partition,
                reported: false,
            };
            let task = common_runtime::spawn_query(route_partition(
                stream,
                partition,
                routing,
                senders.clone(),
                // What this partition reads ahead is charged to its own consumer, so the read-ahead
                // of one partition is attributable to the task that did it.
                MemoryConsumer::new("SeriesRepartitionInput").register(&pool),
                Arc::clone(&inputs),
                report,
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
    ) -> Option<(mpsc::UnboundedReceiver<Queued>, OutputLease)> {
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
    /// An output that is released is one that finished being read; outputs nobody has claimed yet can
    /// still be claimed later. Stop only when no output is being read and none remains to hand out.
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

    /// How the read ended, and the failure it owes its outputs if it failed.
    ///
    /// Both are read under the one lock that records them, so an output never sees a failure without
    /// the error that belongs to it.
    fn ending(&self) -> Ending {
        self.ending
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .clone()
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
        // Nothing holds the read any more: every output is gone and so is the plan. No input task
        // may outlive both of them.
        self.stop();
    }
}

/// The input tasks of one read: what is left of them, and what the last of them records.
///
/// One task reads each child partition. The tasks count themselves down; the last records completion
/// before returning and dropping its senders, so outputs read the recorded outcome when queues close.
struct Inputs {
    /// The tasks themselves, and the count of those that have not stopped.
    state: Mutex<InputState>,
    /// How the read ended, and the failure it owes its outputs if it failed.
    ending: Arc<Mutex<Ending>>,
}

/// What [`Inputs`] guards.
struct InputState {
    /// The task of each input partition, by partition, `None` once it stopped.
    ///
    /// Kept rather than dropped so that the read can be stopped as a whole, and so that the one task
    /// that records a failure can abort the rest: a partition whose peer already failed has
    /// nothing left to route that an output would still take.
    tasks: Vec<Option<common_runtime::JoinHandle<()>>>,
    /// How many of those tasks have not stopped yet.
    ///
    /// The last task to stop brings this to zero and records that the read finished. Cancelled peers
    /// are aborted and their report guards count them out; a task attached after cancellation is
    /// aborted by [`Inputs::attach`] and its guard does the same.
    live: usize,
    /// Whether the read has been cancelled: a failure was elected, or [`Inputs::stop`] was called.
    ///
    /// Recorded under the same lock that guards [`tasks`](Self::tasks), so a task attached after the
    /// cancellation is aborted by [`Inputs::attach`] rather than stored. The sweep that cancels the
    /// read can only abort the handles that exist when it runs, and a task spawned afterwards would
    /// otherwise continue polling a child for a read that is over while holding a sender to every
    /// output, keeping its queues open.
    cancelled: bool,
}

impl Inputs {
    /// The bookkeeping of a read over `partitions` input partitions.
    fn new(ending: Arc<Mutex<Ending>>, partitions: usize) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(InputState {
                tasks: (0..partitions).map(|_| None).collect(),
                live: partitions,
                cancelled: false,
            }),
            ending,
        })
    }

    /// Gives `task` the slot of `partition`, so that the read can still abort it.
    ///
    /// Stored even if the task has already stopped - a partition that answers at once can return
    /// before its handle is stored - because the slot is only consulted to stop what is still running,
    /// and aborting a finished task is a no-op.
    ///
    /// A task attached after cancellation is aborted rather than stored, so it cannot keep output
    /// queues open after the read is over. Its report guard counts it out if it is polled.
    fn attach(&self, partition: usize, task: common_runtime::JoinHandle<()>) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if state.cancelled {
            state.tasks[partition] = None;
            task.abort();
            return;
        }
        state.tasks[partition] = Some(task);
    }

    /// Records that `partition` ended, and that the read finished if it was the last one running.
    ///
    /// A stopped or cancelled read cannot be reported as finished.
    fn ended(&self, partition: usize) {
        if self.count_out(partition) {
            self.record(Ending::Finished);
        }
    }

    /// Records that `partition`'s task stopped without reporting an outcome, and stops the read with
    /// it.
    ///
    /// This runs where no arm of [`route_partition`]'s match can: in a `Drop`. A panic inside
    /// [`read_chains`] unwinds the task and drops the future that was polling it, and an abort drops
    /// that same future where it stands, so both land here - which is the point, because neither
    /// records anything otherwise. What that costs when nothing stops it is the whole read: the peers
    /// go on reading a child that may never answer, their sender clones stay alive, no queue closes,
    /// and every output waits for an ending that is not coming. So a task that died is a failure of
    /// the read, and it is recorded before peer tasks are aborted.
    ///
    /// A read that is already cancelled - stopped from outside or by a failure - only counts this task out. The outcome that cancelled it is the one
    /// the outputs report, and the aborts it performs are what killed this task: a task dying there is
    /// a consequence of that outcome, not a second one for the outputs to choose between.
    ///
    /// Nothing is sent to the outputs: a `Drop` cannot send asynchronously, and closing the senders
    /// lets every output observe the recorded failure.
    /// Delivery here is structural: a queue closes when the last of its senders is dropped, every input
    /// task holds a clone of every sender, and the peers cancelled here drop theirs as their futures
    /// are dropped - so the queues close with nobody sending anything. What an output whose queue has
    /// closed reads is [`RepartitionStream::poll_next`] at `Ready(None)`, which consults
    /// [`OutputLease::ending`] before calling the read over, and that turns the [`Ending::Failed`]
    /// recorded here into the error built from its terminal error. Every output therefore reports this
    /// failure out of the read's own record after all input senders close.
    fn task_died(&self, partition: usize) {
        if self.is_cancelled() {
            // The read has an outcome already, or another task's failure is the one being handed
            // over. Counting out only keeps the bookkeeping honest: the read is cancelled, so this
            // cannot make it look finished.
            self.count_out(partition);
            return;
        }
        // Record the error before cancellation drops this task's senders.
        self.fail(Arc::new(DataFusionError::Execution(format!(
            "series repartition input partition {partition} stopped without reporting an outcome"
        ))));
        self.cancel(partition);
        self.count_out(partition);
    }

    /// Records the first failure before cancelling peers. Outputs read it after sender closure.
    fn failed(&self, partition: usize, error: &Arc<DataFusionError>) {
        self.fail(Arc::clone(error));
        self.cancel(partition);
        self.count_out(partition);
    }

    fn is_cancelled(&self) -> bool {
        self.state
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .cancelled
    }

    /// Marks the read cancelled under the task lock, then aborts its peer tasks.
    ///
    /// Cancellation keeps peers from recording a successful completion after this read has failed.
    fn cancel(&self, partition: usize) {
        {
            let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
            state.cancelled = true;
        }
        self.abort_others(partition);
    }

    /// Counts `partition` off the read, and says whether it was the last task still running.
    fn count_out(&self, partition: usize) -> bool {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        // Taken and dropped: this task is the one stopping, and holding its own handle would leave the
        // read waiting for a task that has already finished.
        state.tasks[partition] = None;
        state.live -= 1;
        // A cancelled read never records that it finished: a failure is the outcome its outputs
        // report, and a stop is not an end.
        state.live == 0 && !state.cancelled
    }

    /// Aborts every input task but `partition`'s own.
    ///
    /// Stops peer tasks after a failure or an unexpected task death. `partition`'s own task is spared
    /// so it can finish its bookkeeping, or because it has already died.
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

    /// Stops every input task that has not stopped yet and marks the read cancelled, so a task
    /// attached after this sweep is aborted by [`attach`](Self::attach) rather than keeping queues
    /// open after the read is over.
    fn stop(&self) {
        let mut state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        state.cancelled = true;
        for task in state.tasks.iter_mut() {
            if let Some(task) = task.take() {
                task.abort();
            }
        }
    }

    /// Records `outcome` as how the read ended, unless an ending is already recorded.
    ///
    /// A normal ending is recorded once. A failure is recorded by [`fail`](Self::fail), which stores
    /// its error in the same write.
    fn record(&self, outcome: Ending) {
        debug_assert!(
            !matches!(outcome, Ending::Failed(_)),
            "a failure is recorded by `fail`, with its error"
        );
        let mut ending = self
            .ending
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if matches!(*ending, Ending::Pending) {
            *ending = outcome;
        }
    }

    /// Records that the read failed with `error`, keeping the error where its outputs can read it.
    ///
    /// The first failure and its error are recorded together so outputs all report the same error.
    fn fail(&self, error: Arc<DataFusionError>) {
        let mut ending = self
            .ending
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if !matches!(*ending, Ending::Failed(_)) {
            *ending = Ending::Failed(error);
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

    /// Aborts the task of one input partition, for a test that has to make exactly one producer stop.
    ///
    /// [`stop`](Self::stop) cannot state that: it stops every task and records the read as stopped,
    /// so what an output reports afterwards is the stop rather than one producer dying.
    #[cfg(test)]
    fn abort(&self, partition: usize) {
        let state = self.state.lock().unwrap_or_else(|error| error.into_inner());
        if let Some(task) = &state.tasks[partition] {
            task.abort();
        }
    }
}

/// One input partition's task: reads its own child, assembles chains, and sends each complete chain to
/// its canonical owner. Output queues are unbounded, so sending does not wait for an owner to poll;
/// queued chains remain charged to the pool until consumed or dropped. Each task records its outcome
/// before returning and dropping its senders, so output streams observe the final read outcome.
async fn route_partition(
    mut stream: SendableEvaluatedSeriesStream,
    partition: usize,
    routing: Routing,
    senders: Vec<mpsc::UnboundedSender<Queued>>,
    read_ahead: MemoryReservation,
    inputs: Arc<Inputs>,
    mut report: ReportIfUnreported,
) {
    let stop = read_chains(&mut stream, &routing, &senders, &read_ahead).await;
    match &stop {
        Stop::Ended => inputs.ended(partition),
        Stop::Failed(error) => inputs.failed(partition, error),
    }
    // This task said what became of its partition, so the guard has nothing left to report.
    report.reported = true;
    drop(report);
}

/// Reports an input task that stopped without recording an outcome, on the paths that would
/// otherwise report nothing.
///
/// [`route_partition`] records ordinary input completion and errors; this guard records when a task
/// stops without returning. A panic unwinds the task and takes the future polling it with it, and the task
/// being aborted drops that future where it stands; neither reaches an arm of that match, so nothing
/// is recorded, the count is untouched and the peers go on reading. A peer parked on a child that
/// never answers then holds a sender to every output for good, no queue ever closes, and every output
/// waits for an ending that is not coming - the `Pending` fallback is no help there, since it is
/// only read once a queue has closed.
///
/// A guard is what covers both without [`std::panic::catch_unwind`], which covers neither an abort nor
/// a panic the runtime catches elsewhere: unwinding and cancellation both drop this, so both report.
struct ReportIfUnreported {
    /// The bookkeeping of the read this task belongs to.
    inputs: Arc<Inputs>,
    /// The input partition whose task holds this guard.
    partition: usize,
    /// Whether that task recorded its own outcome, which is what makes this a no-op.
    reported: bool,
}

impl Drop for ReportIfUnreported {
    fn drop(&mut self) {
        // Also runs on the way out of a normal return, which is what the flag is for: a task that
        // reached its own ending reported it there and has nothing to add here.
        if !self.reported {
            self.inputs.task_died(self.partition);
        }
    }
}

/// Why one input task stopped reading its partition.
enum Stop {
    /// The partition ended: every chunk it had was read and handed to the output it belongs on.
    Ended,
    /// The partition failed, or a chain could not be buffered: every output has to learn that.
    Failed(Arc<DataFusionError>),
}

/// Accounts for the portion of a decoded histogram table retained by one series slice.
pub(crate) fn histogram_table_slice_bytes(
    batch: &datatypes::series::histogram::HistogramBatch,
) -> DataFusionResult<usize> {
    use datafusion::arrow::array::{Array, ListArray};

    let array = batch.as_arrow();
    let mut bytes = std::mem::size_of_val(batch);
    if array.nulls().is_some() {
        bytes = bytes.saturating_add(array.len().div_ceil(8));
    }
    for (index, column) in array.columns().iter().enumerate() {
        if (7..=13).contains(&index) {
            let list = column.as_any().downcast_ref::<ListArray>().ok_or_else(|| {
                DataFusionError::Execution(format!(
                    "histogram list column {index} has invalid type"
                ))
            })?;
            let offsets = list.value_offsets();
            let first = *offsets
                .first()
                .ok_or_else(|| DataFusionError::Execution("empty histogram offsets".into()))?
                as usize;
            let last = *offsets
                .last()
                .ok_or_else(|| DataFusionError::Execution("empty histogram offsets".into()))?
                as usize;
            let values = list.values().slice(first, last.saturating_sub(first));
            bytes =
                bytes.saturating_add((list.len() + 1).saturating_mul(std::mem::size_of::<i32>()));
            bytes = bytes.saturating_add(values.to_data().get_slice_memory_size()?);
            if list.nulls().is_some() {
                bytes = bytes.saturating_add(list.len().div_ceil(8));
            }
        } else {
            bytes = bytes.saturating_add(column.to_data().get_slice_memory_size()?);
        }
    }
    Ok(bytes)
}

/// Reads one input partition to its end, one whole chain at a time, routing each chain as it closes.
///
/// A chain is assembled here and handed over atomically after it closes; its growing memory is charged
/// to the pool. Output queues are unbounded, so routing does not wait for output polling. Each queued
/// chain retains its pool charge until the owner consumes it or the queue is dropped. This function
/// otherwise waits only on its own child.
async fn read_chains(
    stream: &mut SendableEvaluatedSeriesStream,
    routing: &Routing,
    senders: &[mpsc::UnboundedSender<Queued>],
    read_ahead: &MemoryReservation,
) -> Stop {
    loop {
        // One whole chain, assembled here: the chunk that closes the series ends it, and so does a
        // stream that ends between two chains.
        let mut chain: Chain = Vec::new();
        let mut chain_output: Option<usize> = None;
        let mut discarding = false;
        let mut ended = false;
        loop {
            // Input streams may return a long run of ready chunks without yielding to Tokio. Consume
            // the normal task budget per chunk so sibling producers can run and report failures.
            tokio::task::coop::consume_budget().await;
            let next = match (chain_output, discarding) {
                (Some(output), false) if !chain.is_empty() => {
                    tokio::select! {
                        biased;
                        _ = senders[output].closed() => {
                            chain.clear();
                            discarding = true;
                            continue;
                        }
                        next = stream.next() => next,
                    }
                }
                _ => stream.next().await,
            };
            match next {
                Some(Ok((series, is_last))) => {
                    let output = match chain_output {
                        Some(output) => output,
                        None => {
                            let output = routing.output(&series.metric);
                            chain_output = Some(output);
                            output
                        }
                    };
                    // A dropped output is no longer a destination. Still read and validate its
                    // entire chain, but do not retain or reserve its chunks.
                    if senders[output].is_closed() {
                        chain.clear();
                        discarding = true;
                        if is_last {
                            break;
                        }
                        continue;
                    }
                    // Charged as the chain grows, so a chain that does not fit the pool is refused
                    // here rather than after it has been held.
                    if discarding {
                        if is_last {
                            break;
                        }
                        continue;
                    }
                    let charge = read_ahead.new_empty();
                    let mut tables = 0usize;
                    for segment in series.fields.iter().flat_map(|field| field.segments()) {
                        let batch = match segment.dense() {
                            Some(DenseSamplesRef::Histogram(histograms)) => {
                                Some(histograms.batch())
                            }
                            Some(DenseSamplesRef::Mixed(mixed)) => Some(mixed.histograms().batch()),
                            _ => None,
                        };
                        if let Some(batch) = batch {
                            match histogram_table_slice_bytes(batch) {
                                Ok(bytes) => tables = tables.saturating_add(bytes),
                                Err(error) => return Stop::Failed(Arc::new(error)),
                            }
                        }
                    }
                    let bytes = operand_chunk_bytes(&series).saturating_add(tables);
                    if let Err(error) = charge.try_grow(bytes) {
                        if senders[output].is_closed() {
                            chain.clear();
                            discarding = true;
                            if is_last {
                                break;
                            }
                            continue;
                        }
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
            return if chain_output.is_none() {
                Stop::Ended
            } else {
                Stop::Failed(Arc::new(DataFusionError::Execution(
                    "series repartition input ended with its series open".to_string(),
                )))
            };
        }
        // One hash per chain, over the labels of its first chunk: the chunks of a chain are one
        // series, so the first of them names the output for all of them.
        let Some(output) = chain_output else {
            return Stop::Failed(Arc::new(DataFusionError::Internal(
                "series repartition completed a chain without a routing destination".into(),
            )));
        };
        if !discarding && senders[output].send(chain).is_err() {
            // A closed output is no longer a destination. Drop this chain and keep consuming.
            continue;
        }
    }
}

/// Merges exact metric-name and complete-tag identities, preserving first-seen group order.
pub(crate) fn merge_exact_series(
    schema: &Arc<SeriesSchema>,
    series: Vec<Series>,
) -> DataFusionResult<Vec<Series>> {
    let mut indices = BTreeMap::new();
    let mut groups: Vec<Vec<Series>> = Vec::new();
    for series in series {
        let key = (
            series.metric.name().map(Arc::<str>::from),
            series
                .metric
                .tags()
                .iter()
                .map(|tag| (Arc::clone(&tag.name), Arc::clone(&tag.value)))
                .collect::<Vec<_>>(),
        );
        let group = match indices.get(&key) {
            Some(index) => *index,
            None => {
                let index = groups.len();
                indices.insert(key, index);
                groups.push(Vec::new());
                index
            }
        };
        groups[group].push(series);
    }
    let mut output = Vec::with_capacity(groups.len());
    for group in groups {
        output.extend(
            merge_same_labelset(schema, group)
                .map_err(|error| DataFusionError::External(Box::new(error)))?,
        );
    }
    Ok(output)
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
    /// Records an owner-side failure and cancels producer tasks without replacing an earlier error.
    fn fail(&self, error: Arc<DataFusionError>) {
        let inputs = {
            let running = self
                .shared
                .running
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            Arc::clone(&running.inputs)
        };
        inputs.fail(Arc::clone(&error));
        inputs.stop();
    }

    /// Reports successful EOF only when every producer ended successfully.
    fn ending(&self) -> Option<Result<(Series, bool), BoxedError>> {
        match self.shared.ending() {
            Ending::Finished => None,
            Ending::Failed(error) => Some(Err(boxed(DataFusionError::from(&error)))),
            Ending::Pending => Some(Err(boxed(DataFusionError::Execution(
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
#[derive(Clone, Debug, Default)]
enum Ending {
    #[default]
    Pending,
    Finished,
    Failed(Arc<DataFusionError>),
}

/// One whole chain of one series: its chunks from the one that opened it to the one that closed it,
/// each with the bytes it is charged.
///
/// The transport unit is a chain so queued source contributions remain intact. Owners collect all
/// chains before locally merging exact identities and producing their final output.
type Chain = Vec<(Series, bool, MemoryReservation)>;

/// What one output queue carries: a whole chain and its memory charge.
type Queued = Chain;

/// One owner collects producer chains through successful EOF before merging exact identities.
struct RepartitionStream {
    receiver: mpsc::UnboundedReceiver<Queued>,
    lease: OutputLease,
    schema: Arc<SeriesSchema>,
    pool: Arc<dyn MemoryPool>,
    chains: Vec<Series>,
    charges: Vec<MemoryReservation>,
    output: std::vec::IntoIter<Series>,
    workspace: Option<MemoryReservation>,
    reported: bool,
}

impl RepartitionStream {
    fn new(
        receiver: mpsc::UnboundedReceiver<Queued>,
        lease: OutputLease,
        schema: Arc<SeriesSchema>,
        pool: &Arc<dyn MemoryPool>,
    ) -> Self {
        Self {
            receiver,
            lease,
            schema,
            pool: Arc::clone(pool),
            chains: Vec::new(),
            charges: Vec::new(),
            output: Vec::new().into_iter(),
            workspace: None,
            reported: false,
        }
    }

    fn finalize(&mut self) -> Result<(), BoxedError> {
        let rows = self.chains.iter().fold(0usize, |rows, series| {
            rows.saturating_add(series.time.len())
        });
        let fragments = self.chains.len();
        let bytes = merge_workspace_bytes(rows, fragments, self.schema.fields.len());
        let workspace = MemoryConsumer::new("SeriesRepartitionOwner").register(&self.pool);
        if let Err(error) = workspace.try_grow(bytes) {
            let error = Arc::new(error);
            self.lease.fail(Arc::clone(&error));
            self.chains.clear();
            self.charges.clear();
            return Err(boxed(DataFusionError::from(&error)));
        }
        self.workspace = Some(workspace);
        let series = std::mem::take(&mut self.chains);
        match merge_exact_series(&self.schema, series) {
            Ok(series) => {
                self.output = series.into_iter();
                Ok(())
            }
            Err(error) => {
                let error = Arc::new(error);
                self.lease.fail(Arc::clone(&error));
                self.charges.clear();
                self.workspace = None;
                Err(boxed(DataFusionError::from(&error)))
            }
        }
    }
}

impl Stream for RepartitionStream {
    type Item = Result<(Series, bool), BoxedError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let mut drained = 0;
        loop {
            if drained == 128 {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            if let Some(series) = this.output.next() {
                if let Ending::Failed(error) = this.lease.shared.ending() {
                    this.reported = true;
                    this.output = Vec::new().into_iter();
                    this.chains.clear();
                    this.charges.clear();
                    this.workspace = None;
                    return Poll::Ready(Some(Err(boxed(DataFusionError::from(&error)))));
                }
                if this.output.len() == 0 {
                    this.charges.clear();
                    this.workspace = None;
                }
                return Poll::Ready(Some(Ok((series, true))));
            }
            if this.reported {
                this.charges.clear();
                this.workspace = None;
                return Poll::Ready(None);
            }
            match this.receiver.poll_recv(cx) {
                Poll::Ready(Some(chain)) => {
                    drained += 1;
                    for (series, _, charge) in chain {
                        this.chains.push(series);
                        this.charges.push(charge);
                    }
                }
                Poll::Ready(None) => {
                    this.reported = true;
                    match this.lease.ending() {
                        Some(Err(error)) => {
                            this.chains.clear();
                            this.charges.clear();
                            return Poll::Ready(Some(Err(error)));
                        }
                        Some(Ok(_)) => return Poll::Ready(None),
                        None => match this.finalize() {
                            Ok(()) => continue,
                            Err(error) => return Poll::Ready(Some(Err(error))),
                        },
                    }
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

#[cfg(test)]
mod tests;
