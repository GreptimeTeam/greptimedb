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
//! the one output its label set hashes to before any other chain's chunk is emitted there. Reading
//! one input at a time is the price; it is a sequential read, not a scheduler, and the child's own
//! partitions are independent, so it costs concurrency rather than progress.
//!
//! # Caller requirements
//!
//! **Every output partition MUST be polled, and they must be polled concurrently enough that one
//! slow output cannot hold all the others indefinitely.**
//!
//! One read task serves all outputs. There is no scheduler behind it and it does not reorder work
//! around a slow output, so the read stops at the slowest one. Concretely:
//!
//! - **Poll every partition.** A plan that consumes some output partitions and never touches the
//!   others will not finish: once an unread output's queue fills, the read suspends and every other
//!   output stops receiving too. This node does not spill and does not drop to make progress.
//! - **Do not drain one partition to its end before polling the rest.** A consumer that finishes
//!   partition 0 before it looks at partition 1 deadlocks: partition 0 cannot reach its end until the
//!   read finishes, and the read cannot finish while partition 1's queue is full and unread. Polling
//!   the partitions in turn - which is what a DataFusion executor does - is enough; they need not be
//!   on separate tasks.
//! - **Dropping an output is not the same as leaving it unread.** A dropped output stops the read
//!   instead of suspending it, because a queue nobody will drain is not worth waiting for, and a plan
//!   that dropped one of its own outputs is being torn down.
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
//! series that never arrived as series that do not exist. Dropping the execution aborts the producer
//! and releases what it held.

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

/// How many chunks one output partition queues before the producer suspends on it.
///
/// This bounds read-ahead, NOT memory: the chunks are already-decoded evaluated slices, so four of
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
    shared: Mutex<Option<SharedExecution>>,
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
        let mut state = self
            .shared
            .lock()
            .unwrap_or_else(|error| error.into_inner());
        if state.is_none() {
            // The whole execution is started here, once: the output partitions are separate calls,
            // and reading the child once for all of them is what keeps a chunk from being emitted
            // twice or read by a scan that was never meant to run twice.
            *state = Some(SharedExecution::start(
                &self.input,
                self.routing,
                Arc::clone(&self.series_schema),
                &context,
            )?);
        }
        let shared = state.as_mut().expect("the execution was just started");
        let receiver = shared.take_output(partition).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "SeriesRepartition output partition {partition} was executed twice"
            ))
        })?;
        pack_evaluated_series(
            Box::pin(RepartitionStream { receiver }),
            Arc::clone(&self.series_schema),
        )
    }
}

/// One execution's state: the child read once, and the outputs it is being distributed into.
struct SharedExecution {
    /// One receiver per output partition, taken by the `execute` call of that partition. A receiver
    /// that is never taken stays here, which is what makes its queue fill and the producer suspend.
    receivers: Vec<Option<mpsc::Receiver<Queued>>>,
    /// The task reading the child and feeding the outputs.
    producer: common_runtime::JoinHandle<()>,
}

impl SharedExecution {
    /// Starts reading every partition of `input` into `routing`'s outputs on `context`'s pool.
    fn start(
        input: &Arc<dyn ExecutionPlan>,
        routing: Routing,
        series_schema: Arc<SeriesSchema>,
        context: &Arc<TaskContext>,
    ) -> DataFusionResult<Self> {
        let mut inputs = Vec::with_capacity(input.properties().partitioning.partition_count());
        for child_partition in 0..input.properties().partitioning.partition_count() {
            let frames = input.execute(child_partition, Arc::clone(context))?;
            // The expected schema is a check and never an input: the decoder reads the frame's own
            // declaration, and a partition declaring another frame is refused here rather than
            // routed under labels it does not carry.
            inputs.push(unpack_evaluated_series(
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
        // One charge for the whole execution: every queued chunk is a reservation of this consumer,
        // so what the node holds is attributable as one read's read-ahead rather than as M pools.
        let reservation =
            MemoryConsumer::new("SeriesRepartition").register(&context.runtime_env().memory_pool);
        let producer = common_runtime::spawn_query(produce(inputs, senders, routing, reservation));
        Ok(Self {
            receivers,
            producer,
        })
    }

    /// The receiver of `partition`, if that output was not executed before.
    fn take_output(&mut self, partition: usize) -> Option<mpsc::Receiver<Queued>> {
        self.receivers.get_mut(partition).and_then(Option::take)
    }
}

impl Drop for SharedExecution {
    fn drop(&mut self) {
        // The read must not outlive the plan that asked for it: a producer waiting on an output
        // whose stream was dropped would otherwise hold the child, and its scans, open forever.
        // Dropping the receivers as well closes the channels, so the task stops even if the abort
        // lands after its next poll.
        self.producer.abort();
    }
}

/// Reads every child partition, one chain at a time, and routes each chunk it reads.
///
/// The input being read only changes at a chain boundary: once a chunk leaves a series open, the
/// same input is read until the chunk that closes it, and every chunk of that chain therefore
/// reaches one output before another chain's chunk reaches it. The rotation is what makes the inputs
/// fair: each closing chunk hands the read to the next partition that has not ended, so a chatty
/// input cannot starve a quiet one.
async fn produce(
    inputs: Vec<SendableEvaluatedSeriesStream>,
    senders: Vec<mpsc::Sender<Queued>>,
    routing: Routing,
    reservation: MemoryReservation,
) {
    if let Err(error) = read_all(inputs, &senders, routing, &reservation).await {
        // Every output has to learn that the child failed: an end of stream would report the
        // series that never arrived as series that do not exist, which is the one answer a merge
        // cannot tell from a real one.
        for sender in &senders {
            let _ = sender.send(Err(Arc::clone(&error))).await;
        }
    }
}

/// The read [`produce`] runs, and how it reports a child that failed or produced an illegal frame.
async fn read_all(
    mut inputs: Vec<SendableEvaluatedSeriesStream>,
    senders: &[mpsc::Sender<Queued>],
    routing: Routing,
    reservation: &MemoryReservation,
) -> Result<(), Arc<DataFusionError>> {
    let count = inputs.len();
    let mut ended = vec![false; count];
    let mut live = count;
    let mut index = 0;
    // Whether the input being read has a series open, which only an input that ends inside one can
    // make a problem.
    let mut open = false;
    while live > 0 {
        if ended[index] {
            index = next_live(index, &ended);
            continue;
        }
        match inputs[index].next().await {
            Some(Ok((series, is_last))) => {
                let output = routing.output(&series.metric);
                let charge = reservation.new_empty();
                charge.try_grow(series_bytes(&series)).map_err(Arc::new)?;
                if senders[output]
                    .send(Ok((series, is_last, charge)))
                    .await
                    .is_err()
                {
                    // The output was dropped, so nothing it would have received is wanted: the read
                    // stops instead of waiting on a queue nobody will drain.
                    return Ok(());
                }
                if is_last {
                    open = false;
                    index = next_live(index, &ended);
                } else {
                    open = true;
                }
            }
            Some(Err(error)) => {
                return Err(Arc::new(DataFusionError::External(Box::new(error))));
            }
            None => {
                ended[index] = true;
                live -= 1;
                // The decoders refuse a stream that ends with its series open, so this is a producer
                // that bypassed them; its output would hold a series no later chunk can close.
                if open {
                    return Err(Arc::new(DataFusionError::Execution(
                        "series repartition input ended with its series open".to_string(),
                    )));
                }
                if live > 0 {
                    index = next_live(index, &ended);
                }
            }
        }
    }
    Ok(())
}

/// The first input after `index` that has not ended.
fn next_live(index: usize, ended: &[bool]) -> usize {
    let count = ended.len();
    (1..=count)
        .map(|step| (index + step) % count)
        .find(|candidate| !ended[*candidate])
        .unwrap_or(index)
}

/// What one output partition receives from the producer: a chunk with the bytes it is charged, or
/// the error that ended the child.
type Queued = Result<(Series, bool, MemoryReservation), Arc<DataFusionError>>;

/// One output partition's stream: what the producer routed here, in the order it was routed.
struct RepartitionStream {
    receiver: mpsc::Receiver<Queued>,
}

impl Stream for RepartitionStream {
    type Item = Result<(Series, bool), BoxedError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match self.get_mut().receiver.poll_recv(cx) {
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
            Poll::Ready(None) => Poll::Ready(None),
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
