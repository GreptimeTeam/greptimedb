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

//! Routes evaluated and raw series across DataFusion input partitions.
//!
//! Canonical routing hashes complete labels; matching routing hashes the evaluator's own PromQL
//! matching key. Routing groups are not merge identities: evaluated series merge only by exact name
//! and tags. Raw ownership is the complete primary key plus exact label agreement; each input's
//! complete chain or EOF establishes its contribution to a raw identity.
//!
//! Chains are transported intact to owners with their input-partition ID, followed by one charged
//! normal-EOF message per live output from each completed input. Owners group exact identities and
//! may publish an identity as soon as every input has contributed its final chain or normal EOF. The
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
//! does not establish the output order or final series boundaries. For a full exact identity (metric
//! name and every tag name/value), the input contract is that each identity closes at most once per
//! input. Each owner therefore waits only for each distinct input's final chain or normal EOF before
//! merging and publishing that identity.
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
//! continue to consume pool-accounted memory. Owners publish each exact identity after its per-input
//! close/EOF barrier, with finality recomputed for that output series.
//!
//! # Memory, backpressure and end of stream
//!
//! Output queues are unbounded; each input task holds at most the one chain it is assembling. Every
//! queued chain is admitted through the task context's pool and carries its charge. The public packed
//! stream releases a completed cursor's charges after its batch is flushed, while retaining the
//! current cursor's charges until the cursor is exhausted. The pool controls
//! admission, but queue length is not a constant-byte bound. Owners retain each pending exact identity
//! until its per-input close/EOF barrier is satisfied, then
//! reserve merge workspace for that identity alone. This bounds merge workspace to one pending
//! identity, while its inputs remain charged until handoff. No task waits for memory admission.
//!
//! An output ends only when every input partition finished and its queue was drained; a chain that
//! closed locally is not the end of anything. Each input sends its own EOF to each live output before
//! recording normal completion. With one task per input the tasks count themselves down: the last
//! records the read outcome, then sender closure lets outputs detect missing EOF or report failure. If the child errors, the failing task records it before cancelling its peers; sender closure makes
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

use std::collections::{BTreeMap, VecDeque};
use std::fmt::{self, Formatter};
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll};

use common_error::ext::BoxedError;
use common_query::series_frame::{
    SendableEvaluatedSeriesStream, pack_evaluated_series, pack_raw_series, unpack_evaluated_series,
    unpack_raw_series,
};
use datafusion::arrow::datatypes::SchemaRef;
use datafusion::arrow::record_batch::RecordBatch;
use datafusion::common::tree_node::TreeNodeRecursion;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::execution::context::TaskContext;
use datafusion::execution::memory_pool::{MemoryConsumer, MemoryPool, MemoryReservation};
use datafusion::physical_expr::{EquivalenceProperties, PhysicalExpr};
use datafusion::physical_plan::execution_plan::{EmissionType, EvaluationType, SchedulingType};
use datafusion::physical_plan::{
    DisplayAs, DisplayFormatType, Distribution, ExecutionPlan, InputDistributionRequirements,
    Partitioning, PlanProperties, RecordBatchStream, SendableRecordBatchStream,
};
use datatypes::series::frame::{
    FrameContent, FrameLabelMode, LabelDecodeSpec, SeriesFrameDeclaration,
};
use datatypes::series::raw::RawSeriesChunk;
use datatypes::series::samples::{Series, SeriesSchema};
use datatypes::series::{SeriesLabels, SeriesOrdering};
use futures::{Stream, StreamExt};
use store_api::region_engine::{NO_BOUND_LABEL_SOURCE, RawSeriesStream, SendableRawSeriesStream};
use tokio::sync::mpsc;
use twox_hash::XxHash64;

use crate::grid_series::{label_set, merge_same_labelset};
use crate::native::execute::{merge_workspace_bytes, operand_chunk_bytes};
use crate::source::merge_raw_identity;
use datatypes::series::samples::DenseSamplesRef;

/// Selects the key used to route a series to an output partition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SeriesRouting {
    /// Hash the canonical label set.
    CanonicalLabels,
    /// Hash the exact PromQL matching key, so both sides of a vector operation meet.
    Matching(crate::vector_match::MatchingLabels),
    /// Route raw frames by their complete stored primary key.
    RawPrimaryKey,
}

struct EmptyRawStream {
    schema: Arc<SeriesSchema>,
    ordering: SeriesOrdering,
}

impl Stream for EmptyRawStream {
    type Item = Result<RawSeriesChunk, BoxedError>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

impl RawSeriesStream for EmptyRawStream {
    fn schema(&self) -> &Arc<SeriesSchema> {
        &self.schema
    }

    fn label_source(&self) -> &store_api::region_engine::RawLabelSource {
        &NO_BOUND_LABEL_SOURCE
    }

    fn label_decode_spec(&self) -> Option<&LabelDecodeSpec> {
        None
    }

    fn ordering(&self) -> SeriesOrdering {
        self.ordering
    }
}

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
    routing_policy: SeriesRouting,
    content: FrameContent,
    ordering: SeriesOrdering,
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
        let declaration =
            SeriesFrameDeclaration::parse(input.schema().as_ref()).map_err(|error| {
                DataFusionError::Plan(format!(
                    "SeriesRepartition input is not a series frame: {error}"
                ))
            })?;
        let policy = match declaration.content() {
            FrameContent::Evaluated => SeriesRouting::CanonicalLabels,
            FrameContent::Raw => SeriesRouting::RawPrimaryKey,
        };
        Self::try_new_with_routing(input, outputs, policy)
    }

    /// Creates the node with an explicit content-compatible routing policy.
    pub fn try_new_with_routing(
        input: Arc<dyn ExecutionPlan>,
        outputs: usize,
        routing_policy: SeriesRouting,
    ) -> DataFusionResult<Self> {
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
        let content = declaration.content();
        match (content, &routing_policy) {
            (FrameContent::Evaluated, SeriesRouting::RawPrimaryKey) => {
                return Err(DataFusionError::Plan(
                    "SeriesRepartition evaluated frames cannot use RawPrimaryKey routing".into(),
                ));
            }
            (FrameContent::Raw, SeriesRouting::CanonicalLabels) => {
                return Err(DataFusionError::Plan(
                    "SeriesRepartition raw frames cannot use CanonicalLabels routing".into(),
                ));
            }
            _ => {}
        }
        if content == FrameContent::Raw && declaration.labels() != FrameLabelMode::Explicit {
            return Err(DataFusionError::Plan(
                "SeriesRepartition raw frames require explicit labels".into(),
            ));
        }
        let series_schema = Arc::new(declaration.series().clone());
        let ordering = if content == FrameContent::Raw {
            common_query::series_frame::series_frame_ordering(input.schema().as_ref())?
        } else {
            SeriesOrdering::Contiguous
        };
        let declared = match content {
            FrameContent::Evaluated => pack_evaluated_series(
                Box::pin(futures::stream::empty()),
                Arc::clone(&series_schema),
            )?
            .schema(),
            FrameContent::Raw => {
                let source = EmptyRawStream {
                    schema: Arc::clone(&series_schema),
                    ordering,
                };
                pack_raw_series(Box::pin(source))?.schema()
            }
        };
        if declared != input.schema() {
            return Err(DataFusionError::Plan(
                "SeriesRepartition input frame carries a schema this node cannot re-declare"
                    .to_string(),
            ));
        }
        let properties = PlanProperties::new(
            EquivalenceProperties::new(input.schema()),
            Partitioning::UnknownPartitioning(outputs),
            // Each exact identity becomes final independently at its per-input close/EOF barrier.
            EmissionType::Incremental,
            // The node ends when its child does, and spends its life waiting until then.
            input.properties().boundedness,
        )
        .with_evaluation_type(EvaluationType::Eager)
        .with_scheduling_type(SchedulingType::Cooperative);
        let properties = Arc::new(properties);
        Ok(Self {
            input,
            series_schema,
            routing: Routing::with_policy(outputs, routing_policy.clone()),
            routing_policy,
            content,
            ordering,
            shared: Mutex::new(None),
            properties,
        })
    }

    /// The routing policy used by this node.
    pub fn routing_policy(&self) -> &SeriesRouting {
        &self.routing_policy
    }

    /// How many output partitions this node has.
    pub fn outputs(&self) -> usize {
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
        Ok(Arc::new(Self::try_new_with_routing(
            children.swap_remove(0),
            self.outputs(),
            self.routing_policy.clone(),
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
                    self.routing.clone(),
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
        let inner = RepartitionStream::new(
            receiver,
            lease,
            Arc::clone(&self.series_schema),
            &context.runtime_env().memory_pool,
            self.input.properties().partitioning.partition_count(),
            self.ordering,
        );
        let handoff = Arc::clone(&inner.handoff);
        let output: SendableRecordBatchStream = match self.content {
            FrameContent::Evaluated => pack_evaluated_series(
                Box::pin(EvaluatedOutputStream {
                    inner: Some(Box::pin(inner)),
                }),
                Arc::clone(&self.series_schema),
            )?,
            FrameContent::Raw => {
                let raw = RawOutputStream {
                    inner: Some(Box::pin(inner)),
                    schema: Arc::clone(&self.series_schema),
                    ordering: self.ordering,
                };
                pack_raw_series(Box::pin(raw))?
            }
        };
        let schema = output.schema();
        let output = Box::pin(PackedHandoffStream {
            inner: Some(output),
            schema,
            handoff,
        });
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
        // What the child carries is what it declares, not what the policy suggests: a raw frame
        // can be routed by a match key too, so the policy no longer names the content.
        let content = SeriesFrameDeclaration::parse(input.schema().as_ref())
            .map_err(|error| DataFusionError::Plan(error.to_string()))?
            .content();
        let partitions = input.properties().partitioning.partition_count();
        let mut streams = Vec::with_capacity(partitions);
        for child_partition in 0..partitions {
            let frames = input.execute(child_partition, Arc::clone(context))?;
            // The expected schema is a check and never an input: the decoder reads the frame's own
            // declaration, and a partition declaring another frame is refused here rather than
            // routed under labels it does not carry.
            streams.push(match content {
                FrameContent::Evaluated => RoutedInput::Evaluated(unpack_evaluated_series(
                    frames,
                    Some(Arc::clone(&series_schema)),
                )?),
                FrameContent::Raw => {
                    RoutedInput::Raw(unpack_raw_series(frames, Some(Arc::clone(&series_schema)))?)
                }
            });
        }
        let mut senders = Vec::with_capacity(routing.outputs);
        let mut receivers = Vec::with_capacity(routing.outputs);
        for _ in 0..routing.outputs {
            let (sender, receiver) = mpsc::unbounded_channel();
            senders.push(sender);
            receivers.push(Some(receiver));
        }
        let ending = Arc::new(Mutex::new(Ending::default()));
        let shared = Arc::new_cyclic(|weak| SharedExecution {
            running: Mutex::new(Running {
                receivers,
                inputs: Inputs::new(Arc::clone(&ending), partitions, weak.clone()),
                claimed: 0,
                remaining: routing.outputs,
            }),
            ending: Arc::clone(&ending),
        });
        let inputs = {
            let running = shared
                .running
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            Arc::clone(&running.inputs)
        };
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
                routing.clone(),
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
        Ok(shared)
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
    shared: Weak<SharedExecution>,
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
    fn new(
        ending: Arc<Mutex<Ending>>,
        partitions: usize,
        shared: Weak<SharedExecution>,
    ) -> Arc<Self> {
        Arc::new(Self {
            state: Mutex::new(InputState {
                tasks: (0..partitions).map(|_| None).collect(),
                live: partitions,
                cancelled: false,
            }),
            ending,
            shared,
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
        let first_failure = !matches!(*ending, Ending::Failed(_));
        if first_failure {
            *ending = Ending::Failed(error);
        }
        drop(ending);
        if first_failure && let Some(shared) = self.shared.upgrade() {
            let mut running = shared
                .running
                .lock()
                .unwrap_or_else(|error| error.into_inner());
            for receiver in running.receivers.iter_mut().flatten() {
                receiver.close();
                while receiver.try_recv().is_ok() {}
            }
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
    mut stream: RoutedInput,
    partition: usize,
    routing: Routing,
    senders: Vec<mpsc::UnboundedSender<Queued>>,
    read_ahead: MemoryReservation,
    inputs: Arc<Inputs>,
    mut report: ReportIfUnreported,
) {
    // The variant is the content: decoding the input's declared frame is what produced it.
    let stop = match &mut stream {
        RoutedInput::Evaluated(stream) => {
            read_evaluated_chains(stream, partition, &routing, &senders, &read_ahead).await
        }
        RoutedInput::Raw(stream) => {
            read_raw_chains(stream, partition, &routing, &senders, &read_ahead).await
        }
    };
    if matches!(&stop, Stop::Ended) {
        for sender in &senders {
            if sender.is_closed() {
                continue;
            }
            let metadata = read_ahead.new_empty();
            if let Err(error) = metadata.try_grow(std::mem::size_of::<Queued>()) {
                inputs.failed(partition, &Arc::new(error));
                report.reported = true;
                return;
            }
            if let Err(error) = sender.send(Queued::Eof {
                input: partition,
                _metadata: metadata,
            }) {
                drop(error.0);
            }
        }
    }
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
async fn read_evaluated_chains(
    stream: &mut SendableEvaluatedSeriesStream,
    partition: usize,
    routing: &Routing,
    senders: &[mpsc::UnboundedSender<Queued>],
    read_ahead: &MemoryReservation,
) -> Stop {
    loop {
        // One whole chain, assembled here: the chunk that closes the series ends it, and so does a
        // stream that ends between two chains.
        let mut chain: Vec<(Series, bool, MemoryReservation)> = Vec::new();
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
                            let output = routing.output_labels(&series.metric);
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
        if !discarding && {
            let metadata = read_ahead.new_empty();
            if let Err(error) = metadata.try_grow(std::mem::size_of::<Queued>()) {
                return Stop::Failed(Arc::new(error));
            }
            senders[output]
                .send(Queued::Chain {
                    input: partition,
                    chain: RoutedChain::Evaluated(chain),
                    _metadata: metadata,
                })
                .is_err()
        } {
            // A closed output is no longer a destination. Drop this chain and keep consuming.
            continue;
        }
    }
}

async fn read_raw_chains(
    stream: &mut SendableRawSeriesStream,
    partition: usize,
    routing: &Routing,
    senders: &[mpsc::UnboundedSender<Queued>],
    read_ahead: &MemoryReservation,
) -> Stop {
    loop {
        let mut chain: Vec<(RawSeriesChunk, MemoryReservation)> = Vec::new();
        let mut destination: Option<usize> = None;
        let mut discarding = false;
        loop {
            tokio::task::coop::consume_budget().await;
            let next = match (destination, discarding) {
                (Some(output), false) if !chain.is_empty() => tokio::select! {
                    biased;
                    _ = senders[output].closed() => { chain.clear(); discarding = true; continue; }
                    item = stream.next() => item,
                },
                _ => stream.next().await,
            };
            match next {
                Some(Ok(chunk)) => {
                    let output = match destination {
                        Some(output) => output,
                        None => {
                            let output = match &routing.policy {
                                SeriesRouting::Matching(_) => {
                                    let Some(labels) = chunk.metric.as_ref() else {
                                        return Stop::Failed(Arc::new(DataFusionError::Execution(
                                            "a matching-routed raw chain carries no labels".into(),
                                        )));
                                    };
                                    routing.output_labels(labels)
                                }
                                _ => routing.output_key(chunk.primary_key.as_slice()),
                            };
                            *destination.insert(output)
                        }
                    };
                    let final_chunk = chunk.is_last;
                    if senders[output].is_closed() {
                        chain.clear();
                        discarding = true;
                        if final_chunk {
                            break;
                        }
                        continue;
                    }
                    if discarding {
                        if final_chunk {
                            break;
                        }
                        continue;
                    }
                    let charge = read_ahead.new_empty();
                    let bytes = match raw_chunk_slice_bytes(&chunk) {
                        Ok(bytes) => bytes,
                        Err(error) => return Stop::Failed(Arc::new(error)),
                    };
                    if let Err(error) = charge.try_grow(bytes) {
                        return Stop::Failed(Arc::new(error));
                    }
                    chain.push((chunk, charge));
                    if final_chunk {
                        break;
                    }
                }
                Some(Err(error)) => {
                    return Stop::Failed(Arc::new(DataFusionError::External(Box::new(error))));
                }
                None => {
                    return if destination.is_none() {
                        Stop::Ended
                    } else {
                        Stop::Failed(Arc::new(DataFusionError::Execution(
                            "raw series ended without is_last".into(),
                        )))
                    };
                }
            }
        }
        let Some(output) = destination else {
            return Stop::Failed(Arc::new(DataFusionError::Internal(
                "raw chain has no destination".into(),
            )));
        };
        if !discarding {
            let metadata = read_ahead.new_empty();
            if let Err(error) = metadata.try_grow(std::mem::size_of::<Queued>()) {
                return Stop::Failed(Arc::new(error));
            }
            if senders[output]
                .send(Queued::Chain {
                    input: partition,
                    chain: RoutedChain::Raw(chain),
                    _metadata: metadata,
                })
                .is_err()
            {
                continue;
            }
        }
    }
}

fn exact_identity(series: &Series) -> ExactIdentity {
    (
        series.metric.name().map(Arc::<str>::from),
        series
            .metric
            .tags()
            .iter()
            .map(|tag| (Arc::clone(&tag.name), Arc::clone(&tag.value)))
            .collect(),
    )
}

/// Merges exact metric-name and complete-tag identities, preserving first-seen group order.
///
/// The Fused series stack calls this helper for evaluated frame finalization; this branch does not
/// yet contain that caller, so the shared helper is not dead code in the integrated feature.
#[allow(dead_code)]
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
enum RoutedInput {
    Evaluated(SendableEvaluatedSeriesStream),
    Raw(SendableRawSeriesStream),
}

enum RoutedChain {
    Evaluated(Vec<(Series, bool, MemoryReservation)>),
    Raw(Vec<(RawSeriesChunk, MemoryReservation)>),
}

/// What one output queue carries: a whole chain and its memory charge.
enum Queued {
    Chain {
        input: usize,
        chain: RoutedChain,
        _metadata: MemoryReservation,
    },
    Eof {
        input: usize,
        _metadata: MemoryReservation,
    },
}

type ExactIdentity = (Option<Arc<str>>, Vec<(Arc<str>, Arc<str>)>);

/// Turns an owner's evaluated publications into the evaluated stream the packer consumes.
struct EvaluatedOutputStream {
    inner: Option<Pin<Box<RepartitionStream>>>,
}

impl Stream for EvaluatedOutputStream {
    type Item = Result<(Series, bool), BoxedError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let Some(inner) = this.inner.as_mut() else {
            return Poll::Ready(None);
        };
        match inner.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                this.inner = None;
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(error))) => {
                this.inner = None;
                Poll::Ready(Some(Err(error)))
            }
            // A merged identity is final: the owner publishes it once, already closed.
            Poll::Ready(Some(Ok(ReadyOutput::Evaluated(series)))) => {
                Poll::Ready(Some(Ok((series, true))))
            }
            Poll::Ready(Some(Ok(ReadyOutput::Raw(_)))) => {
                this.inner = None;
                Poll::Ready(Some(Err(boxed(DataFusionError::Internal(
                    "an evaluated repartition output carried a raw series".into(),
                )))))
            }
        }
    }
}

/// Turns an owner's raw publications into the raw frame stream the packer consumes.
struct RawOutputStream {
    inner: Option<Pin<Box<RepartitionStream>>>,
    schema: Arc<SeriesSchema>,
    ordering: SeriesOrdering,
}

impl Stream for RawOutputStream {
    type Item = Result<RawSeriesChunk, BoxedError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        let Some(inner) = this.inner.as_mut() else {
            return Poll::Ready(None);
        };
        match inner.as_mut().poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(None) => {
                this.inner = None;
                Poll::Ready(None)
            }
            Poll::Ready(Some(Err(error))) => {
                this.inner = None;
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(Some(Ok(ReadyOutput::Raw(chunk)))) => Poll::Ready(Some(Ok(chunk))),
            Poll::Ready(Some(Ok(ReadyOutput::Evaluated(_)))) => {
                this.inner = None;
                Poll::Ready(Some(Err(boxed(DataFusionError::Internal(
                    "a raw repartition output carried an evaluated series".into(),
                )))))
            }
        }
    }
}

impl RawSeriesStream for RawOutputStream {
    fn schema(&self) -> &Arc<SeriesSchema> {
        &self.schema
    }

    fn label_source(&self) -> &store_api::region_engine::RawLabelSource {
        &store_api::region_engine::NO_BOUND_LABEL_SOURCE
    }

    fn label_decode_spec(&self) -> Option<&LabelDecodeSpec> {
        None
    }

    fn ordering(&self) -> SeriesOrdering {
        self.ordering
    }
}

/// What one owner groups by: an evaluated exact identity, or a complete raw primary key.
///
/// The two are separate variants rather than one tuple, so a raw key can never be confused with an
/// evaluated identity that happens to look similar.
#[derive(Clone, Debug, PartialEq, Eq, PartialOrd, Ord)]
enum OwnerKey {
    Evaluated(ExactIdentity),
    Raw(Vec<u8>),
}

/// The raw identity of one chunk: its complete stored primary key and nothing else.
///
/// Labels are deliberately not part of the key. Two pieces of one physical key must always meet in
/// the same owner so a label disagreement can be rejected there, instead of being split across
/// owners that would each look consistent on its own.
fn raw_chunk_slice_bytes(chunk: &RawSeriesChunk) -> DataFusionResult<usize> {
    use datafusion::arrow::array::Array;

    let metric_bytes = chunk.metric.as_ref().map_or(0, |metric| {
        metric
            .name()
            .map_or(0, |name| name.len())
            .saturating_add(metric.tags().iter().fold(0usize, |bytes, tag| {
                bytes
                    .saturating_add(tag.name.len())
                    .saturating_add(tag.value.len())
            }))
    });
    // A histogram field is an index array naming rows of a shared table; both are held per sample.
    let mut fields_bytes = 0usize;
    for samples in chunk.fields.iter() {
        let field_bytes = match samples.dense() {
            Some(DenseSamplesRef::Float(values)) => values.to_data().get_slice_memory_size()?,
            Some(DenseSamplesRef::Histogram(histograms)) => histograms
                .indices()
                .to_data()
                .get_slice_memory_size()?
                .saturating_add(histogram_table_slice_bytes(histograms.batch())?),
            Some(DenseSamplesRef::Mixed(mixed)) => mixed
                .floats()
                .to_data()
                .get_slice_memory_size()?
                .saturating_add(
                    mixed
                        .histograms()
                        .indices()
                        .to_data()
                        .get_slice_memory_size()?,
                )
                .saturating_add(histogram_table_slice_bytes(mixed.histograms().batch())?),
            None => 0,
        };
        fields_bytes = fields_bytes.saturating_add(field_bytes);
    }
    Ok(std::mem::size_of::<RawSeriesChunk>()
        .saturating_add(chunk.primary_key.len())
        .saturating_add(metric_bytes)
        .saturating_add(
            chunk
                .time
                .as_buffer()
                .len()
                .saturating_mul(std::mem::size_of::<i64>()),
        )
        .saturating_add(fields_bytes))
}

fn raw_identity(chunk: &RawSeriesChunk) -> OwnerKey {
    OwnerKey::Raw(chunk.primary_key.as_slice().to_vec())
}

struct PendingGroup {
    fragments: Vec<Series>,
    raw_chunks: Vec<RawSeriesChunk>,
    charges: Vec<MemoryReservation>,
    received: Vec<bool>,
    metadata: MemoryReservation,
}

/// Whether this owner may publish `group` now.
///
/// A key-then-time raw frame promises its keys arrive in order, and this owner publishes in key
/// order only when it publishes them together: an identity that is complete while a smaller one is
/// still waiting would go out first and break that promise. Such a frame therefore waits for every
/// input to end, which is when the scan over the owned keys can hand them all out in order.
fn group_ready(
    group: &PendingGroup,
    eof: &[bool],
    last_key_from: &[Option<Vec<u8>>],
    ordering: SeriesOrdering,
    key: &OwnerKey,
) -> bool {
    if ordering == SeriesOrdering::KeyThenTime {
        let OwnerKey::Raw(key) = key else {
            return false;
        };
        return group.received.iter().zip(eof).zip(last_key_from).all(
            |((received, eof), last_key)| {
                *received || *eof || last_key.as_ref().is_some_and(|last| last > key)
            },
        );
    }
    group
        .received
        .iter()
        .zip(eof)
        .all(|(received, eof)| *received || *eof)
}

enum ReadyOutput {
    Evaluated(Series),
    Raw(RawSeriesChunk),
}

struct ReadySeries {
    series: ReadyOutput,
    charges: ReadyCharges,
}

struct ReadyCharges {
    _charges: Vec<MemoryReservation>,
    _metadata: MemoryReservation,
    _workspace: Option<MemoryReservation>,
}

/// One owner processes identities as soon as every input has closed or contributed its final chain.
struct RepartitionStream {
    receiver: mpsc::UnboundedReceiver<Queued>,
    lease: OutputLease,
    schema: Arc<SeriesSchema>,
    pool: Arc<dyn MemoryPool>,
    /// What the frame this stream emits declares about the order of its keys.
    ordering: SeriesOrdering,
    groups: BTreeMap<OwnerKey, PendingGroup>,
    eof: Vec<bool>,
    last_key_from: Vec<Option<Vec<u8>>>,
    eof_metadata: Option<MemoryReservation>,
    inputs: usize,
    output: VecDeque<ReadySeries>,
    handoff: Arc<Mutex<Handoff>>,
    /// Whether a new pass over the pending identities is owed after an input EOF.
    ///
    /// A chain can only complete the identity it carries, so the chain arm checks that group
    /// directly. An EOF can complete any waiting identity, so it opens a pass instead; the pass
    /// resumes after [`resume_after`](Self::resume_after) so the same prefix is not rescanned for
    /// every identity it releases.
    rescan: bool,
    /// The last identity a pass examined, exclusive. Retains at most one identity descriptor.
    resume_after: Option<OwnerKey>,
    reported: bool,
}

/// Charges move out of the current cursor only after it is exhausted; the public batch stream
/// releases completed cursors after the packer has flushed the frames they back.
#[derive(Default)]
struct Handoff {
    current: Option<ReadyCharges>,
    completed: Vec<ReadyCharges>,
}

struct PackedHandoffStream {
    inner: Option<SendableRecordBatchStream>,
    schema: SchemaRef,
    handoff: Arc<Mutex<Handoff>>,
}

impl RecordBatchStream for PackedHandoffStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Stream for PackedHandoffStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let result = match self.inner.as_mut() {
            Some(inner) => inner.as_mut().poll_next(cx),
            None => return Poll::Ready(None),
        };
        match &result {
            Poll::Ready(Some(Ok(_))) | Poll::Pending => {
                let mut handoff = self.handoff.lock().unwrap_or_else(|e| e.into_inner());
                // A pending packer has exhausted its cursor and its frame builder is empty, so
                // completed charges no longer retain any payload; `current` may still own the
                // whole merged series behind a live cursor.
                handoff.completed.clear();
            }
            Poll::Ready(None) | Poll::Ready(Some(Err(_))) => {
                drop(self.inner.take());
                let mut handoff = self.handoff.lock().unwrap_or_else(|e| e.into_inner());
                handoff.current = None;
                handoff.completed.clear();
            }
        }
        result
    }
}

impl RepartitionStream {
    fn new(
        receiver: mpsc::UnboundedReceiver<Queued>,
        lease: OutputLease,
        schema: Arc<SeriesSchema>,
        pool: &Arc<dyn MemoryPool>,
        inputs: usize,
        ordering: SeriesOrdering,
    ) -> Self {
        Self {
            receiver,
            lease,
            schema,
            ordering,
            pool: Arc::clone(pool),
            groups: BTreeMap::new(),
            eof: Vec::new(),
            last_key_from: vec![None; inputs],
            eof_metadata: None,
            inputs,
            output: VecDeque::new(),
            handoff: Arc::new(Mutex::new(Handoff::default())),
            rescan: false,
            resume_after: None,
            reported: false,
        }
    }

    fn finish_group(&mut self, key: &OwnerKey) -> DataFusionResult<()> {
        let Some(group) = self.groups.remove(key) else {
            return Ok(());
        };
        if matches!(key, OwnerKey::Raw(_)) {
            return self.finish_raw_group(group);
        }
        let workspace = if group.fragments.len() > 1 {
            let rows = group.fragments.iter().fold(0usize, |rows, series| {
                rows.saturating_add(series.time.len())
            });
            let reservation = MemoryConsumer::new("SeriesRepartitionOwner").register(&self.pool);
            reservation.try_grow(merge_workspace_bytes(
                rows,
                group.fragments.len(),
                self.schema.fields.len(),
            ))?;
            Some(reservation)
        } else {
            None
        };
        let merged = merge_same_labelset(&self.schema, group.fragments)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        if merged.len() != 1 {
            return Err(DataFusionError::Internal(
                "exact identity produced multiple merged series".into(),
            ));
        }
        let Some(series) = merged.into_iter().next() else {
            return Err(DataFusionError::Internal(
                "exact identity produced no merged series".into(),
            ));
        };
        self.output.push_back(ReadySeries {
            series: ReadyOutput::Evaluated(series),
            charges: ReadyCharges {
                _charges: group.charges,
                _metadata: group.metadata,
                _workspace: workspace,
            },
        });
        Ok(())
    }
}

impl RepartitionStream {
    /// Publishes one complete raw identity.
    ///
    /// Raw pieces are only published once every input reached its normal EOF, because a piece's
    /// `is_last` closes that producer's stream, not the series. The workspace this reconstruction
    /// needs is charged before it runs and rides with the output until the packer hands the batch
    /// off, like any other owner output.
    fn finish_raw_group(&mut self, group: PendingGroup) -> DataFusionResult<()> {
        let labels = group
            .raw_chunks
            .first()
            .and_then(|chunk| chunk.metric.clone())
            .ok_or_else(|| {
                DataFusionError::Execution("raw identity has no explicit labels".to_string())
            })?;
        if group
            .raw_chunks
            .iter()
            .any(|chunk| chunk.metric.as_ref() != Some(&labels))
        {
            return Err(DataFusionError::Execution(
                "raw identity pieces disagree on their explicit labels".to_string(),
            ));
        }
        if group.raw_chunks.len() == 1 {
            let mut chunk = group.raw_chunks.into_iter().next().expect("one raw chunk");
            chunk.is_last = true;
            self.output.push_back(ReadySeries {
                series: ReadyOutput::Raw(chunk),
                charges: ReadyCharges {
                    _charges: group.charges,
                    _metadata: group.metadata,
                    _workspace: None,
                },
            });
            return Ok(());
        }
        let rows = group
            .raw_chunks
            .iter()
            .fold(0usize, |rows, chunk| rows.saturating_add(chunk.time.len()));
        // Reconstruction sorts into two parallel row vectors before it builds the output, then
        // emits up to one sample slice per surviving run per field and collects those runs into a
        // field. Alternating contributions from several chunks can make that one slice per row per
        // field, so the bound below covers the row vectors, the timestamps and the worst case
        // rather than one descriptor per field. It charges one run descriptor plus the flattened
        // field it lands in, matching what `merge_workspace_bytes` charges the evaluated merge for
        // the same interleaving.
        let fields = self.schema.fields.len();
        let row = std::mem::size_of::<(i64, usize, usize)>();
        let per_row_field = 2 * std::mem::size_of::<datatypes::series::samples::Samples>()
            + 3 * std::mem::size_of::<usize>();
        let workspace = MemoryConsumer::new("SeriesRepartitionRawOwner").register(&self.pool);
        workspace.try_grow(
            2usize
                .saturating_mul(rows.saturating_mul(row))
                .saturating_add(rows.saturating_mul(std::mem::size_of::<i64>()))
                .saturating_add(rows.saturating_mul(fields).saturating_mul(per_row_field))
                .saturating_add(fields.saturating_mul(per_row_field)),
        )?;
        let chunk = merge_raw_identity(&self.schema, &group.raw_chunks)
            .map_err(|error| DataFusionError::External(Box::new(error)))?;
        self.output.push_back(ReadySeries {
            series: ReadyOutput::Raw(chunk),
            charges: ReadyCharges {
                _charges: group.charges,
                _metadata: group.metadata,
                _workspace: Some(workspace),
            },
        });
        Ok(())
    }
}

impl Stream for RepartitionStream {
    type Item = Result<ReadyOutput, BoxedError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        {
            let mut handoff = this.handoff.lock().unwrap_or_else(|e| e.into_inner());
            if let Some(current) = handoff.current.take() {
                handoff.completed.push(current);
            }
        }
        let mut drained = 0;
        loop {
            if let Ending::Failed(error) = this.lease.shared.ending() {
                if this.reported {
                    return Poll::Ready(None);
                }
                this.reported = true;
                this.receiver.close();
                while this.receiver.try_recv().is_ok() {}
                this.groups.clear();
                this.output.clear();
                this.eof.clear();
                this.eof_metadata = None;
                return Poll::Ready(Some(Err(boxed(DataFusionError::from(&error)))));
            }
            if this.eof_metadata.is_none() {
                let metadata = MemoryConsumer::new("SeriesRepartitionEof").register(&this.pool);
                let bytes = this.inputs.saturating_mul(std::mem::size_of::<bool>());
                if let Err(error) = metadata.try_grow(bytes) {
                    this.lease.fail(Arc::new(error));
                    continue;
                }
                this.eof = vec![false; this.inputs];
                this.eof_metadata = Some(metadata);
            }
            if let Ending::Failed(error) = this.lease.shared.ending() {
                if this.reported {
                    return Poll::Ready(None);
                }
                this.reported = true;
                this.receiver.close();
                while this.receiver.try_recv().is_ok() {}
                this.groups.clear();
                this.output.clear();
                this.eof.clear();
                this.eof_metadata = None;
                return Poll::Ready(Some(Err(boxed(DataFusionError::from(&error)))));
            }
            if this.rescan {
                let next = match &this.resume_after {
                    Some(after) => this
                        .groups
                        .range((
                            std::ops::Bound::Excluded(after.clone()),
                            std::ops::Bound::Unbounded,
                        ))
                        .find_map(|(key, group)| {
                            group_ready(group, &this.eof, &this.last_key_from, this.ordering, key)
                                .then(|| key.clone())
                        }),
                    None => this.groups.iter().find_map(|(key, group)| {
                        group_ready(group, &this.eof, &this.last_key_from, this.ordering, key)
                                .then(|| key.clone())
                    }),
                };
                match next {
                    Some(key) => {
                        this.resume_after = Some(key.clone());
                        if let Err(error) = this.finish_group(&key) {
                            this.rescan = false;
                            this.resume_after = None;
                            this.lease.fail(Arc::new(error));
                            continue;
                        }
                    }
                    None => {
                        // Every pending identity after the resume point was examined and none is
                        // ready; only a new chain or EOF can change that.
                        this.rescan = false;
                        this.resume_after = None;
                    }
                }
            }
            if let Some(ready) = this.output.pop_front() {
                this.handoff
                    .lock()
                    .unwrap_or_else(|e| e.into_inner())
                    .current = Some(ready.charges);
                return Poll::Ready(Some(Ok(ready.series)));
            }
            if this.reported {
                return Poll::Ready(None);
            }
            if drained == 128 {
                cx.waker().wake_by_ref();
                return Poll::Pending;
            }
            match this.receiver.poll_recv(cx) {
                Poll::Ready(Some(Queued::Chain {
                    input,
                    chain,
                    _metadata,
                })) => {
                    drained += 1;
                    let (key, pieces) = match chain {
                        RoutedChain::Evaluated(chain) => {
                            let Some(first) = chain.first() else {
                                continue;
                            };
                            let key = exact_identity(&first.0);
                            if chain
                                .iter()
                                .any(|(series, _, _)| exact_identity(series) != key)
                            {
                                this.lease.fail(Arc::new(DataFusionError::Execution(
                                    "one series chain contains multiple exact identities".into(),
                                )));
                                continue;
                            }
                            (OwnerKey::Evaluated(key), RoutedChain::Evaluated(chain))
                        }
                        RoutedChain::Raw(chain) => {
                            let Some(first) = chain.first() else {
                                continue;
                            };
                            let key = raw_identity(&first.0);
                            if chain.iter().any(|(chunk, _)| raw_identity(chunk) != key) {
                                this.lease.fail(Arc::new(DataFusionError::Execution(
                                    "one raw chain contains multiple primary keys".into(),
                                )));
                                continue;
                            }
                            if let OwnerKey::Raw(raw_key) = &key
                                && this.ordering == SeriesOrdering::KeyThenTime
                            {
                                let last = &mut this.last_key_from[input];
                                if last.as_ref().is_none_or(|last| raw_key > last) {
                                    *last = Some(raw_key.clone());
                                }
                            }
                            (key, RoutedChain::Raw(chain))
                        }
                    };
                    if !this.groups.contains_key(&key) {
                        let metadata =
                            MemoryConsumer::new("SeriesRepartitionIdentity").register(&this.pool);
                        let key_bytes =
                            match &key {
                                OwnerKey::Evaluated((name, tags)) => {
                                    std::mem::size_of::<ExactIdentity>()
                                        .saturating_add(name.as_ref().map_or(0, |name| name.len()))
                                        .saturating_add(tags.capacity().saturating_mul(
                                            std::mem::size_of::<(Arc<str>, Arc<str>)>(),
                                        ))
                                        .saturating_add(
                                            tags.iter()
                                                .map(|(n, v)| n.len().saturating_add(v.len()))
                                                .sum::<usize>(),
                                        )
                                }
                                OwnerKey::Raw(key) => key.capacity(),
                            };
                        let bytes = std::mem::size_of::<PendingGroup>()
                            .saturating_add(std::mem::size_of::<OwnerKey>())
                            .saturating_add(key_bytes)
                            .saturating_add(this.eof.len().saturating_mul(2));
                        if let Err(error) = metadata.try_grow(bytes) {
                            this.lease.fail(Arc::new(error));
                            continue;
                        }
                        this.groups.insert(
                            key.clone(),
                            PendingGroup {
                                fragments: Vec::new(),
                                raw_chunks: Vec::new(),
                                charges: Vec::new(),
                                received: vec![false; this.eof.len()],
                                metadata,
                            },
                        );
                    }
                    let Some(group) = this.groups.get_mut(&key) else {
                        this.lease.fail(Arc::new(DataFusionError::Internal(
                            "series identity group was not retained".into(),
                        )));
                        continue;
                    };
                    if input >= group.received.len() || group.received[input] {
                        this.lease.fail(Arc::new(DataFusionError::Execution(
                            "input repeated an exact series identity".into(),
                        )));
                        continue;
                    }
                    group.received[input] = true;
                    match pieces {
                        RoutedChain::Evaluated(chain) => {
                            for (series, _, charge) in chain {
                                group.fragments.push(series);
                                group.charges.push(charge);
                            }
                        }
                        RoutedChain::Raw(chain) => {
                            for (chunk, charge) in chain {
                                group.raw_chunks.push(chunk);
                                group.charges.push(charge);
                            }
                        }
                    }
                    if this.ordering == SeriesOrdering::KeyThenTime {
                        this.rescan = true;
                        this.resume_after = None;
                    } else if group_ready(group, &this.eof, &this.last_key_from, this.ordering, &key)
                        && let Err(error) = this.finish_group(&key)
                    {
                        this.lease.fail(Arc::new(error));
                    }
                }
                Poll::Ready(Some(Queued::Eof { input, _metadata })) => {
                    drained += 1;
                    if input >= this.eof.len() || this.eof[input] {
                        this.lease.fail(Arc::new(DataFusionError::Execution(
                            "duplicate series repartition input EOF".into(),
                        )));
                        continue;
                    }
                    this.eof[input] = true;
                    // An EOF can complete any identity waiting only for this input.
                    this.rescan = true;
                    this.resume_after = None;
                }
                Poll::Ready(None) => {
                    this.reported = true;
                    if let Some(Err(error)) = this.lease.ending() {
                        this.groups.clear();
                        this.output.clear();
                        return Poll::Ready(Some(Err(error)));
                    }
                    if this.eof.iter().any(|seen| !seen) || !this.groups.is_empty() {
                        let error = boxed(DataFusionError::Execution(
                            "series repartition queue closed before every input EOF".into(),
                        ));
                        this.lease.fail(Arc::new(DataFusionError::Execution(
                            "series repartition queue closed before every input EOF".into(),
                        )));
                        this.groups.clear();
                        return Poll::Ready(Some(Err(error)));
                    }
                    if let Some(ready) = this.output.pop_front() {
                        this.handoff
                            .lock()
                            .unwrap_or_else(|e| e.into_inner())
                            .current = Some(ready.charges);
                        return Poll::Ready(Some(Ok(ready.series)));
                    }
                    return Poll::Ready(None);
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
#[derive(Clone, Debug)]
struct Routing {
    outputs: usize,
    seed: u64,
    policy: SeriesRouting,
}

impl Routing {
    /// The canonical routing of an execution with `outputs` output partitions.
    #[cfg(test)]
    fn new(outputs: usize) -> Self {
        Self::with_policy(outputs, SeriesRouting::CanonicalLabels)
    }

    /// The routing of an execution with `outputs` output partitions and an explicit policy.
    fn with_policy(outputs: usize, policy: SeriesRouting) -> Self {
        Self {
            outputs,
            seed: ROUTING_SEED,
            policy,
        }
    }

    /// The output a label-carrying chunk belongs on.
    #[cfg(test)]
    fn output(&self, metric: &SeriesLabels) -> usize {
        self.output_labels(metric)
    }

    fn output_labels(&self, metric: &SeriesLabels) -> usize {
        let mut hasher = XxHash64::with_seed(self.seed);
        match &self.policy {
            SeriesRouting::CanonicalLabels => label_set(metric).hash(&mut hasher),
            SeriesRouting::Matching(policy) => {
                crate::vector_match::group_key(metric, policy).hash(&mut hasher)
            }
            // A raw frame routed by its stored key never reaches here: its destination is the
            // stored key itself, not a label hash. Evaluated frames never use this policy.
            SeriesRouting::RawPrimaryKey => {
                debug_assert!(
                    false,
                    "RawPrimaryKey routing hashes stored keys, not labels"
                );
            }
        }
        (hasher.finish() % self.outputs as u64) as usize
    }

    fn output_key(&self, key: &[u8]) -> usize {
        let mut hasher = XxHash64::with_seed(self.seed);
        key.hash(&mut hasher);
        (hasher.finish() % self.outputs as u64) as usize
    }
}

#[cfg(test)]
mod tests;
