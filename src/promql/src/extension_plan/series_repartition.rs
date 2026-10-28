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
//! So the node only changes which input partition it reads at a chain boundary: once a chain is open
//! the chunk that closes it is read from the same input first, and every chunk of that chain goes to
//! the one output its label set hashes to before any other chain's chunk is emitted there. Only the
//! routing makes that choice, and it makes it over chunks that are already computed.
//!
//! # Where the reads run
//!
//! Every child partition is read on a task of its own - see [`Forwarders`] - and the routing task
//! only moves what those tasks have already computed. That is not an optimisation of the routing: an
//! evaluated partition is one stream whose computation happens inside its `poll_next`, so a single
//! task polling every partition in turn runs the child's native kernels for all of them itself, one
//! chunk at a time, and the partition count the child declared buys no parallelism at all. One reader
//! per partition is what keeps that computation where DataFusion put it; the routing task waits on
//! one partition's queue at a time, and the partitions it is not reading compute ahead into their own
//! queues meanwhile.
//!
//! The chain rule above lives in the routing task and nowhere else: a partition's reader knows
//! nothing about chains, and the routing task never interleaves two open chains into one output.
//!
//! # Caller requirements
//!
//! **Every output partition MUST be polled, and they must be polled concurrently enough that one
//! slow output cannot hold all the others indefinitely.**
//!
//! One routing task serves all outputs. There is no scheduler behind it and it does not reorder work
//! around a slow output, so the read stops at the slowest one. Concretely:
//!
//! - **Poll every partition.** A plan that consumes some output partitions and never touches the
//!   others will not finish: once an unread output's queue fills, the read suspends and every other
//!   output stops receiving too. This node does not spill and does not drop to make progress.
//! - **Do not drain one partition to its end before polling the rest, unless what is routed to the
//!   others fits their queues.** A consumer that finishes partition 0 before it looks at partition 1
//!   deadlocks as soon as partition 1's queue fills: partition 0 cannot reach its end until the read
//!   finishes, and the read cannot finish while it is parked on a queue nobody drains. What an output
//!   ends with before that point ends nothing else - an output nobody has claimed yet stays claimable
//!   and the read keeps running for it, since it has not been handed out yet. Polling the partitions
//!   in turn - which is what a DataFusion executor does - is enough; they need not be on separate
//!   tasks.
//! - **Dropping an output is not the same as leaving it unread.** The read stops routing to a dropped
//!   output instead of suspending on it, because a queue nobody will drain is not worth waiting for,
//!   and a plan that dropped one of its own outputs is being torn down. If chunks read for it were
//!   never delivered, the outputs that are still being read end with an error saying so rather than
//!   with an end: the read did not finish.
//!
//! # Memory, backpressure and end of stream
//!
//! Each output has a bounded channel of [`OUTPUT_CAPACITY`] **chunks** - a bound on how far the read
//! may run ahead of a consumer, not a bound on bytes. Every queued chunk is charged to a
//! [`MemoryConsumer`] on the task context's pool, the charge travels with the chunk and is released
//! when a consumer takes it or drops it, so bytes are bounded by the pool refusing to grow rather
//! than by the queue length.
//!
//! An output ends only when every input partition finished and every chunk routed to it was handed
//! over; a chain that closed locally is not the end of anything. If the child errors, every output
//! receives that error as its terminal item rather than a clean end, because an end would report the
//! series that never arrived as series that do not exist.
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
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
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

/// How many chunks one output partition queues before the producer suspends on it.
///
/// This bounds read-ahead, NOT memory: the chunks are already-decoded evaluated slices, so four of
/// them can still be large, and what actually bounds the bytes is the [`MemoryConsumer`] on the
/// task context's pool refusing to grow. Small and fixed because queue depth buys little here and
/// the sooner a slow consumer stops the read, the sooner the memory it would have taken is not
/// taken.
const OUTPUT_CAPACITY: usize = 4;

/// How many chunks one input partition queues ahead of the routing task.
///
/// With the one its task holds while the queue is full, a partition reads at most this many plus one
/// chunks ahead: small, so that a chatty partition cannot pull a whole scan into memory while the
/// routing task is parked on an output nobody is draining.
const INPUT_CAPACITY: usize = 2;

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
    /// The queues, the task filling them, and how many of the execution's outputs the read still has.
    running: Mutex<Running>,
    /// How the read ended, for an output whose queue has closed to read.
    ending: Arc<Mutex<Ending>>,
}

/// What one execution holds that has to be stopped together.
struct Running {
    /// One receiver per output partition, taken by the `execute` call of that partition. A receiver
    /// that is never taken stays here, which is what makes its queue fill and the read suspend.
    receivers: Vec<Option<mpsc::Receiver<Queued>>>,
    /// The task routing the child's partitions into those queues.
    producer: common_runtime::JoinHandle<()>,
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
    /// Stops the read: every queue nobody took is dropped and the task filling them is aborted.
    ///
    /// Idempotent - the queues are already gone on a second call, and aborting a finished or already
    /// aborted task is a no-op. Dropping the queues of outputs that were never handed out is part of
    /// it: what was queued for them is read-ahead nobody will take, and holding it until the process
    /// ends would keep both the memory and the child's scans alive for nothing.
    fn stop(&mut self) {
        self.receivers.clear();
        // Aborting a read that has not recorded an outcome is what the outputs still open have to
        // see: nothing was recorded, so the end they read is "the producer stopped unexpectedly".
        self.producer.abort();
    }
}

impl SharedExecution {
    /// Starts reading every partition of `input` into `routing`'s outputs on `context`'s pool.
    fn start(
        input: &Arc<dyn ExecutionPlan>,
        routing: Routing,
        series_schema: Arc<SeriesSchema>,
        context: &Arc<TaskContext>,
    ) -> DataFusionResult<Arc<Self>> {
        let mut streams = Vec::with_capacity(input.properties().partitioning.partition_count());
        for child_partition in 0..input.properties().partitioning.partition_count() {
            let frames = input.execute(child_partition, Arc::clone(context))?;
            // The expected schema is a check and never an input: the decoder reads the frame's own
            // declaration, and a partition declaring another frame is refused here rather than
            // routed under labels it does not carry.
            streams.push(unpack_evaluated_series(
                frames,
                Some(Arc::clone(&series_schema)),
            )?);
        }
        // From here on each partition is read on its own task, so what the routing task moves has
        // already been computed where the child's own partitioning put it.
        let forwarders = Forwarders::start(streams, &context.runtime_env().memory_pool);
        let mut senders = Vec::with_capacity(routing.outputs);
        let mut receivers = Vec::with_capacity(routing.outputs);
        for _ in 0..routing.outputs {
            let (sender, receiver) = mpsc::channel(OUTPUT_CAPACITY);
            senders.push(sender);
            receivers.push(Some(receiver));
        }
        let ending = Arc::new(Mutex::new(Ending::Unrecorded));
        let producer =
            common_runtime::spawn_query(produce(forwarders, senders, routing, Arc::clone(&ending)));
        Ok(Arc::new(Self {
            running: Mutex::new(Running {
                receivers,
                producer,
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
    /// to hand out ([`remaining`](Running::remaining) is zero) - so a producer parked on the queue of
    /// an output that has not been claimed yet keeps waiting: that is backpressure on a consumer that
    /// is still coming, not a read with nowhere to go.
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

/// The tasks that read one child partition each, and the queues they fill.
///
/// A partition's computation happens inside its own `poll_next`, so polling two partitions from one
/// task runs their kernels one after another on that task, whatever the child declared about its
/// partition count. Each partition is therefore driven on a task of its own - the way
/// `series_gather::spawn_partitions` drives the streams of its input - and the routing task only
/// moves chunks that are already computed.
struct Forwarders {
    /// The queue of each partition, in partition order.
    readers: Vec<InputReader>,
    /// The task of each partition, taken when its queue has closed and the task was accounted for.
    tasks: Vec<Option<common_runtime::JoinHandle<()>>>,
}

impl Drop for Forwarders {
    fn drop(&mut self) {
        // A reader parked on a queue nobody will drain, or on a scan, must not outlive the read that
        // asked for it. Dropping this guard is what cancels the upstream child.
        for task in self.tasks.iter().flatten() {
            task.abort();
        }
    }
}

impl Forwarders {
    /// Starts one task per child partition.
    ///
    /// One partition gets a task of its own as well, unlike `series_gather::spawn_partitions`: the
    /// routing task must never run the child's computation, and a single partition read there would
    /// be exactly that.
    fn start(streams: Vec<SendableEvaluatedSeriesStream>, pool: &Arc<dyn MemoryPool>) -> Self {
        let mut readers = Vec::with_capacity(streams.len());
        let mut tasks = Vec::with_capacity(streams.len());
        for mut stream in streams {
            // What this partition read ahead is charged to this partition's own consumer, so the
            // read-ahead of one partition is attributable to the task that did it.
            let reservation = MemoryConsumer::new("SeriesRepartitionInput").register(pool);
            let (sender, receiver) = mpsc::channel(INPUT_CAPACITY);
            let task = common_runtime::spawn_query(async move {
                while let Some(item) = stream.next().await {
                    let item = item
                        .map_err(|error| Arc::new(DataFusionError::External(Box::new(error))))
                        .and_then(|(series, is_last)| {
                            let charge = reservation.new_empty();
                            charge
                                .try_grow(series_bytes(&series))
                                .map_err(Arc::new)
                                .map(|()| (series, is_last, charge))
                        });
                    // A partition that failed has said everything it will: the error is the last
                    // item it hands over.
                    let failed = item.is_err();
                    if sender.send(item).await.is_err() || failed {
                        return;
                    }
                }
            });
            readers.push(InputReader {
                receiver,
                ended: false,
            });
            tasks.push(Some(task));
        }
        Self { readers, tasks }
    }
}

/// One input partition's queue, as the routing task reads it.
struct InputReader {
    /// What the partition's task has read and not yet handed over.
    receiver: mpsc::Receiver<Queued>,
    /// Whether the partition ended, which its queue closing is the sign of.
    ended: bool,
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

/// Reads every child partition, one chain at a time, and routes each chunk it reads.
///
/// The input being read only changes at a chain boundary: once a chunk leaves a series open, the
/// same input is read until the chunk that closes it, and every chunk of that chain therefore
/// reaches one output before another chain's chunk reaches it. Which input a boundary hands the read
/// to is the next partition that has not ended, so a chatty input cannot starve a quiet one.
///
/// The chunks are taken from per-partition queues, never from the child itself: what this task does
/// is routing, and the child's computation stays on the tasks [`Forwarders`] started.
async fn produce(
    mut forwarders: Forwarders,
    senders: Vec<mpsc::Sender<Queued>>,
    routing: Routing,
    ending: Arc<Mutex<Ending>>,
) {
    let read = read_all(&mut forwarders, &senders, routing).await;
    let outcome = match &read {
        Ok(Read::Whole) => Ending::Finished,
        Ok(Read::Abandoned) => Ending::Abandoned,
        Err(error) => {
            // Every output has to learn that the child failed: an end of stream would report the
            // series that never arrived as series that do not exist, which is the one answer a merge
            // cannot tell from a real one.
            for sender in &senders {
                let _ = sender.send(Err(Arc::clone(error))).await;
            }
            Ending::Failed
        }
    };
    // The outcome is recorded before the queues are dropped, so an output woken by their closing
    // reads how the read ended instead of finding nothing there.
    *ending.lock().unwrap_or_else(|error| error.into_inner()) = outcome;
    drop(senders);
    // Whatever partition was still being read is not wanted either, and dropping this is what
    // cancels it.
    drop(forwarders);
}

/// Why [`read_all`] stopped, when it stopped without failing.
enum Read {
    /// Every input partition ended and every chunk it routed was handed over.
    Whole,
    /// An output went away, so chunks read for it were never delivered and the read stops instead of
    /// waiting on a queue nobody will drain.
    Abandoned,
}

/// The read [`produce`] runs, and how it reports a child that failed or produced an illegal frame.
///
/// This is the same rotation it always was - the input being read only changes at a chain boundary -
/// over chunks that the partitions' own tasks have already computed. Which partition is read next is
/// decided here and only here, so the order chunks reach an output is a function of the child's own
/// order and nothing else.
async fn read_all(
    forwarders: &mut Forwarders,
    senders: &[mpsc::Sender<Queued>],
    routing: Routing,
) -> Result<Read, Arc<DataFusionError>> {
    let Forwarders { readers, tasks } = forwarders;
    let mut live = readers.len();
    let mut index = 0;
    // Whether the input being read has a series open, which only an input that ends inside one can
    // make a problem.
    let mut open = false;
    while live > 0 {
        if readers[index].ended {
            index = next_live(index, readers);
            continue;
        }
        // Waiting here is waiting on one partition's queue: the partition itself is being read on
        // its own task, and the ones behind it keep computing into their queues while this read
        // waits.
        match readers[index].receiver.recv().await {
            Some(Ok((series, is_last, charge))) => {
                let output = routing.output(&series.metric);
                if senders[output]
                    .send(Ok((series, is_last, charge)))
                    .await
                    .is_err()
                {
                    // The output was dropped, so nothing it would have received is wanted: the read
                    // stops instead of waiting on a queue nobody will drain.
                    return Ok(Read::Abandoned);
                }
                if is_last {
                    open = false;
                    index = next_live(index, readers);
                } else {
                    open = true;
                }
            }
            Some(Err(error)) => {
                return Err(error);
            }
            None => {
                // A closed queue is also how a task that stopped - panicked, or was aborted - is
                // seen, and that must not read as a partition that ended: what it never handed over
                // would be missing from an answer that claims to be complete.
                if let Some(task) = tasks[index].take() {
                    let joined = task.await;
                    if let Err(error) = joined {
                        return Err(Arc::new(DataFusionError::ExecutionJoin(Box::new(error))));
                    }
                }
                // The decoders refuse a stream that ends with its series open, so this is a producer
                // that bypassed them; its output would hold a series no later chunk can close.
                if open {
                    return Err(Arc::new(DataFusionError::Execution(
                        "series repartition input ended with its series open".to_string(),
                    )));
                }
                readers[index].ended = true;
                live -= 1;
                index = next_live(index, readers);
            }
        }
    }
    Ok(Read::Whole)
}

/// The first input after `index` that has not ended.
fn next_live(index: usize, readers: &[InputReader]) -> usize {
    let count = readers.len();
    (1..=count)
        .map(|step| (index + step) % count)
        .find(|candidate| !readers[*candidate].ended)
        .unwrap_or(index)
}

/// What one queue carries: a chunk with the bytes it is charged, or the error that ended the child.
///
/// The same item type travels on both hops - a partition's task to the routing task, and the routing
/// task to an output - because the charge a partition took is handed on with the chunk it is for, so
/// what one partition has queued, or one output has queued, is exactly what the pool has reserved.
type Queued = Result<(Series, bool, MemoryReservation), Arc<DataFusionError>>;

/// One output partition's stream: what the producer routed here, in the order it was routed.
struct RepartitionStream {
    receiver: mpsc::Receiver<Queued>,
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
        if this.reported {
            return Poll::Ready(None);
        }
        match this.receiver.poll_recv(cx) {
            Poll::Ready(Some(Ok((series, is_last, charge)))) => {
                // The chunk is no longer the node's to hold: whoever reads it owns it from here.
                drop(charge);
                Poll::Ready(Some(Ok((series, is_last))))
            }
            // The error is shared rather than rebuilt, so every output reports the one failure the
            // child answered with instead of M copies of its text.
            Poll::Ready(Some(Err(error))) => {
                Poll::Ready(Some(Err(boxed(DataFusionError::from(&error)))))
            }
            // The queue closing is not the end of the read on its own: aborting the read closes it
            // too, and calling that an end would report every chunk it never routed as a chunk that
            // does not exist. What the read recorded says which of the two this is.
            Poll::Ready(None) => {
                this.reported = true;
                Poll::Ready(this.lease.ending())
            }
            Poll::Pending => Poll::Pending,
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
