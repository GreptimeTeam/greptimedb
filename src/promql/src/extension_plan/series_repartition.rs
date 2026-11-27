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
//! complete chain or EOF establishes its contribution to a raw identity for contiguous inputs.
//! An interleaved raw frame here consists of concatenated, independently closed producer chains;
//! identities can recur, but open chains cannot be multiplexed. The owner retains those raw
//! identities until every input reaches normal EOF, then reconstructs contiguous output.
//!
//! Chains are transported intact to owners with their input-partition ID, followed by one charged
//! normal-EOF message per live output from each completed input. Owners group exact identities and,
//! except for interleaved raw input, may publish an identity as soon as every input has contributed
//! its final chain or normal EOF. Evaluated chunks are decoded only to read routing labels.
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
//! input, except for the independently closed chains of an interleaved raw frame. Each owner waits
//! for each distinct input's final chain or normal EOF before merging a contiguous identity, and
//! for all input EOFs before merging an interleaved raw identity.
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
//! close/EOF barrier, or all input EOFs for interleaved raw frames, with finality recomputed for
//! that output series.
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
//! Shared execution records the read outcome before producer senders close, so queue closure together
//! with that shared outcome distinguishes normal completion from a stopped or failed read. A stopped
//! read is missing exactly what it did not route and is reported as an error rather than a clean end.
//!
//! An output stream owns the read, not the plan it was executed from: DataFusion's
//! `execute_stream_partitioned` takes the plan by value and keeps only the streams, so a read tied to
//! the plan would be stopped while its streams are still being consumed. The read therefore lives as
//! long as any of its outputs does, and stops only when there is nothing left it could deliver to:
//! every output was handed out and every one of them is gone. An output that ends - normally, or by
//! being dropped - while another partition was never executed stops nothing by itself, because that
//! partition can still claim its queue; the execution going away as a whole, which no later `execute`
//! can reach, is the other point at which everything is released.

use std::any::Any;
use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, VecDeque};
use std::fmt::{self, Formatter};
use std::hash::{Hash, Hasher};
use std::pin::Pin;
use std::sync::{Arc, Mutex, Weak};
use std::task::{Context, Poll};

use common_error::ext::BoxedError;
use common_query::series_frame::{
    PendingBatching, SendableEvaluatedSeriesStream, pack_evaluated_series, pack_raw_series_with,
    raw_series_frame_schema_with_ordering, series_frame_ordering, unpack_evaluated_series,
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
use datatypes::series::aggregate_state::AggregateStateSchema;
use datatypes::series::frame::{
    FrameContent, FrameLabelMode, LabelDecodeSpec, SeriesFrameDeclaration, SeriesFrameDecoder,
};
use datatypes::series::raw::RawSeriesChunk;
use datatypes::series::samples::{Series, SeriesSchema};
use datatypes::series::{SeriesLabels, SeriesOrdering};
use futures::{Stream, StreamExt};
use store_api::region_engine::{RawLabelSource, RawSeriesStream, SendableRawSeriesStream};
use tokio::sync::mpsc;
use twox_hash::XxHash64;

use crate::evaluated::EvaluatedChunk;
use crate::extension_plan::series_execution::{
    HandedSeriesChunk, SendableSeriesStream, SeriesCharge, SeriesChunk, SeriesExecutionPlan,
    checked_series_stream, series_execution_plan,
};
use crate::grid_series::{label_set, merge_same_labelset};
use crate::native::execute::{field_segments, merge_workspace_bytes, operand_chunk_bytes};
use crate::native::state::{StateChunk, state_bytes, state_row_of};
use crate::source::merge_raw_identity_charging;
use datatypes::series::samples::DenseSamplesRef;

/// Selects the key used to route a series to an output partition.
#[derive(Clone, Debug, PartialEq, Eq)]
pub enum SeriesRouting {
    /// Hash the canonical label set.
    CanonicalLabels,
    /// Hash the canonical label set without these labels, including an excluded metric name.
    CanonicalLabelsExcept(Arc<[Arc<str>]>),
    /// Hash the exact PromQL matching key, so both sides of a vector operation meet.
    Matching(crate::vector_match::MatchingLabels),
    /// Route raw frames by their complete stored primary key.
    RawPrimaryKey,
}

/// The seed behind every routing decision.
///
/// Fixed rather than drawn per execution, so which output a label set belongs on is a function of
/// the label set and the output count alone: an execution's own decisions have to agree with each
/// other, and a seeded router can be reasoned about - and tested - without one.
const ROUTING_SEED: u64 = 0x517c_c1b7_2722_0a95;

/// Whether a series identity is complete on a single chain, or has to be merged across inputs.
///
/// A router can publish an identity as soon as its chain closes only when that chain is the whole
/// identity for the execution. Whether that holds is a property of the producer, not of the router,
/// so it is declared explicitly and defaults to the conservative merge.
#[derive(Clone, Copy, Debug, Default, PartialEq, Eq)]
pub enum IdentityCompletion {
    /// An identity may be split across inputs: every input has to contribute it or end first.
    #[default]
    MergeInputs,
    /// The producer guarantees each identity appears as one complete chain on one input partition.
    /// The router may publish a closed chain immediately and does not retain history to validate
    /// that guarantee after publication.
    CompleteDisjoint,
}

/// How the claim of an output reads this node's child.
///
/// The child is read once, for whichever of the two ways its consumer asked for first - the frames
/// this node publishes, or the chunks it hands over - and the second claim is refused, so the two
/// cannot disagree about it.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
enum InputRead {
    /// The consumer reads frames: the child is executed and its frames are decoded here.
    Frames,
    /// The consumer reads chunks: the child is asked for its chunks when it can hand them over, and
    /// decoded here only when it cannot.
    Chunks,
}

impl InputRead {
    /// What this way of reading is, for a refusal that has to name both.
    fn as_str(&self) -> &'static str {
        match self {
            Self::Frames => "frames",
            Self::Chunks => "chunks",
        }
    }
}

/// One child partition, as this node reads it: its chunks when it hands them over, its frames when
/// it does not.
///
/// `hand_over` is the child as a native operator, decided once for the whole read: with it, the
/// partition is read in process and wrapped in the form this node's chain assembly expects; without
/// it, the partition is executed and its frames decoded here.
#[allow(clippy::too_many_arguments)]
fn routed_input(
    input: &Arc<dyn ExecutionPlan>,
    hand_over: Option<&dyn SeriesExecutionPlan>,
    content: FrameContent,
    series_schema: &Arc<SeriesSchema>,
    state: Option<&Arc<AggregateStateSchema>>,
    declared_ordering: SeriesOrdering,
    partition: usize,
    context: &Arc<TaskContext>,
) -> DataFusionResult<RoutedInput> {
    if let Some(plan) = hand_over {
        // A handed-over stream is checked against the order its own plan declares, exactly as a
        // decoded frame is: this node narrows what it publishes, but the child's promise is what its
        // chunks were produced under, and a chain that breaks it has to be refused here too.
        let chunks = checked_series_stream(
            plan.execute_series(partition, Arc::clone(context))?,
            declared_ordering,
        );
        return Ok(match content {
            FrameContent::Evaluated => {
                RoutedInput::Evaluated(Box::pin(HeldSeriesCharges::evaluated(chunks)))
            }
            FrameContent::Raw => {
                RoutedInput::Raw(HeldSeriesCharges::raw(chunks, Arc::clone(series_schema)))
            }
            FrameContent::AggregateState => {
                // A chunk carries its own declaration, so the state this node declared is what the
                // read is held to; without it there is nothing to hold the chunk to.
                let state = state.ok_or_else(|| {
                    DataFusionError::Plan("aggregate state declaration is missing".into())
                })?;
                RoutedInput::StateChunks(chunks, Arc::clone(state), Arc::clone(series_schema))
            }
        });
    }
    let frames = input.execute(partition, Arc::clone(context))?;
    // The expected schema is a check and never an input: the decoder reads the frame's own
    // declaration, and a partition declaring another frame is refused here rather than routed
    // under labels it does not carry.
    Ok(match content {
        FrameContent::Evaluated => RoutedInput::Evaluated(unpack_evaluated_series(
            frames,
            Some(Arc::clone(series_schema)),
        )?),
        FrameContent::Raw => {
            // The node publishes labels, so a child that names them by its key is decoded here,
            // where the frames are already unpacked. An already-labelled stream passes through
            // untouched.
            let stream = unpack_raw_series(frames, Some(Arc::clone(series_schema)))?;
            let stream = crate::source::materialize_raw_labels(stream)
                .map_err(|error| DataFusionError::External(Box::new(error)))?;
            RoutedInput::Raw(stream)
        }
        FrameContent::AggregateState => {
            let state = state.ok_or_else(|| {
                DataFusionError::Plan("aggregate state declaration is missing".into())
            })?;
            RoutedInput::State(frames, Arc::clone(state), Arc::clone(series_schema))
        }
    })
}

/// A child's chunks, read as the series this node's chain assembly expects.
///
/// The producer's charge for a chunk is held until the next poll instead of being dropped with the
/// conversion: by then this node has grown its own reservation for what it queued, so there is no
/// moment at which the bytes are charged to nobody. That is the same hand-off the frame consumers
/// make when they keep a batch's charge through the next poll.
struct HeldSeriesCharges {
    /// The child's chunks.
    chunks: SendableSeriesStream,
    /// What this stream is read as, so another content is refused rather than reinterpreted.
    content: FrameContent,
    /// The charge of the chunk handed out last, released when the next one is read.
    held: Option<SeriesCharge>,
}

impl HeldSeriesCharges {
    /// `chunks` read as evaluated series.
    fn evaluated(chunks: SendableSeriesStream) -> SendableEvaluatedSeriesStream {
        Box::pin(HeldEvaluated {
            inner: HeldSeriesCharges {
                chunks,
                content: FrameContent::Evaluated,
                held: None,
            },
        })
    }

    /// `chunks` read as raw series declared on `schema`.
    ///
    /// The chunks already carry complete labels - that is what a raw producer hands over - so the
    /// stream declares no decode description and no bound source, which is what a decoding consumer
    /// reads to decide it has nothing to decode.
    fn raw(chunks: SendableSeriesStream, schema: Arc<SeriesSchema>) -> SendableRawSeriesStream {
        Box::pin(HeldRaw {
            inner: HeldSeriesCharges {
                chunks,
                content: FrameContent::Raw,
                held: None,
            },
            schema,
            label_source: RawLabelSource::Explicit,
        })
    }

    /// The next chunk, with the charge of the previous one released first.
    fn next_chunk(
        &mut self,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<SeriesChunk, BoxedError>>> {
        self.held = None;
        match self.chunks.as_mut().poll_next(cx) {
            Poll::Ready(Some(Ok(handed))) => {
                let (chunk, charge) = handed.into_parts();
                self.held = charge;
                Poll::Ready(Some(Ok(chunk)))
            }
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(error))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    /// The error a child hands over a content this stream is not read as.
    fn refused(&self, chunk: &SeriesChunk) -> BoxedError {
        boxed(DataFusionError::Execution(format!(
            "a {} input handed over {chunk:?}",
            self.content.as_str()
        )))
    }
}

/// [`HeldSeriesCharges`] read as evaluated series.
struct HeldEvaluated {
    inner: HeldSeriesCharges,
}

impl Stream for HeldEvaluated {
    type Item = Result<(Series, bool), BoxedError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.inner.next_chunk(cx) {
            Poll::Ready(Some(Ok(SeriesChunk::Evaluated(chunk)))) => {
                Poll::Ready(Some(Ok((chunk.series, chunk.is_last))))
            }
            Poll::Ready(Some(Ok(other))) => {
                let error = this.inner.refused(&other);
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(error))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// [`HeldSeriesCharges`] read as raw series.
struct HeldRaw {
    inner: HeldSeriesCharges,
    schema: Arc<SeriesSchema>,
    label_source: RawLabelSource,
}

impl RawSeriesStream for HeldRaw {
    fn schema(&self) -> &Arc<SeriesSchema> {
        &self.schema
    }

    fn label_source(&self) -> &RawLabelSource {
        &self.label_source
    }

    fn ordering(&self) -> SeriesOrdering {
        SeriesOrdering::Contiguous
    }
}

impl Stream for HeldRaw {
    type Item = Result<RawSeriesChunk, BoxedError>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.get_mut();
        match this.inner.next_chunk(cx) {
            Poll::Ready(Some(Ok(SeriesChunk::Raw(chunk)))) => Poll::Ready(Some(Ok(chunk))),
            Poll::Ready(Some(Ok(other))) => {
                let error = this.inner.refused(&other);
                Poll::Ready(Some(Err(error)))
            }
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(error))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }
}

/// The Repartition stage: one child's partitions, redistributed by canonical label set.
///
/// The node holds one execution's shared state: its output partitions are separate `execute` calls
/// that must agree on one read of the child, so the first call starts the read and every call takes
/// its own output from it.
pub struct SeriesRepartitionExec {
    input: Arc<dyn ExecutionPlan>,
    /// The child frame's series schema, re-declared by every output batch.
    series_schema: Arc<SeriesSchema>,
    /// The one hash configuration of this node's execution.
    routing: Routing,
    routing_policy: SeriesRouting,
    identity_completion: IdentityCompletion,
    content: FrameContent,
    state: Option<Arc<AggregateStateSchema>>,
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
            FrameContent::AggregateState => SeriesRouting::CanonicalLabels,
        };
        Self::try_new_with_routing(input, outputs, policy)
    }

    /// Creates a node for an input whose producer guarantees each identity is one complete chain
    /// on one input partition. Closed identities are published immediately; later duplicate chains
    /// are not detected, so callers must uphold the producer guarantee.
    pub fn try_new_complete_disjoint(
        input: Arc<dyn ExecutionPlan>,
        outputs: usize,
    ) -> DataFusionResult<Self> {
        let declaration =
            SeriesFrameDeclaration::parse(input.schema().as_ref()).map_err(|error| {
                DataFusionError::Plan(format!(
                    "SeriesRepartition input is not a series frame: {error}"
                ))
            })?;
        let policy = match declaration.content() {
            FrameContent::Evaluated => SeriesRouting::CanonicalLabels,
            FrameContent::Raw => SeriesRouting::RawPrimaryKey,
            FrameContent::AggregateState => {
                return Err(DataFusionError::NotImplemented(
                    "a complete-disjoint router cannot prove aggregate state ownership; use the \
                     ordinary canonical-label routing for state frames"
                        .into(),
                ));
            }
        };
        Self::try_new_with_completion(input, outputs, policy, IdentityCompletion::CompleteDisjoint)
    }

    /// Creates the node with an explicit content-compatible routing policy.
    pub fn try_new_with_routing(
        input: Arc<dyn ExecutionPlan>,
        outputs: usize,
        routing_policy: SeriesRouting,
    ) -> DataFusionResult<Self> {
        Self::try_new_with_completion(
            input,
            outputs,
            routing_policy,
            IdentityCompletion::MergeInputs,
        )
    }

    fn try_new_with_completion(
        input: Arc<dyn ExecutionPlan>,
        outputs: usize,
        routing_policy: SeriesRouting,
        identity_completion: IdentityCompletion,
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
            (
                FrameContent::Raw,
                SeriesRouting::CanonicalLabels | SeriesRouting::CanonicalLabelsExcept(_),
            ) => {
                return Err(DataFusionError::Plan(
                    "SeriesRepartition raw frames cannot use CanonicalLabels routing".into(),
                ));
            }
            (
                FrameContent::AggregateState,
                SeriesRouting::RawPrimaryKey | SeriesRouting::Matching(_),
            ) => {
                return Err(DataFusionError::Plan(
                    "SeriesRepartition aggregate-state frames require canonical-label routing"
                        .into(),
                ));
            }
            _ => {}
        }
        let series_schema = Arc::new(declaration.series().clone());
        let ordering = if content == FrameContent::Raw {
            common_query::series_frame::series_frame_ordering(input.schema().as_ref())?
        } else {
            SeriesOrdering::Contiguous
        };
        if content == FrameContent::Raw
            && ordering == SeriesOrdering::Interleaved
            && identity_completion == IdentityCompletion::CompleteDisjoint
        {
            return Err(DataFusionError::Plan(
                "SeriesRepartition cannot use CompleteDisjoint with interleaved raw input".into(),
            ));
        }
        let output_ordering = if ordering == SeriesOrdering::Interleaved {
            SeriesOrdering::Contiguous
        } else {
            ordering
        };
        let declared = match content {
            FrameContent::Evaluated => common_query::series_frame::frame_schema_with(
                &series_schema,
                FrameLabelMode::Explicit,
                None,
                SeriesOrdering::Contiguous,
            )?,
            FrameContent::Raw => raw_series_frame_schema_with_ordering(
                &series_schema,
                &store_api::region_engine::NO_BOUND_LABEL_SOURCE,
                output_ordering,
            )?,
            FrameContent::AggregateState => input.schema(),
        };
        // The input is re-declared from what it says about itself, not from what this node
        // publishes: the two differ whenever the child carries its labels in its keys.
        let rebuilt_input = if content == FrameContent::AggregateState {
            None
        } else {
            Some(
                common_query::series_frame::frame_schema_with(
                    &series_schema,
                    declaration.labels(),
                    declaration.label_decode(),
                    ordering,
                )
                .map_err(|error| DataFusionError::Plan(error.to_string()))?,
            )
        };
        if rebuilt_input.is_some_and(|rebuilt| rebuilt != input.schema()) {
            return Err(DataFusionError::Plan(
                "SeriesRepartition input frame carries a schema this node cannot re-declare"
                    .to_string(),
            ));
        }
        let properties = PlanProperties::new(
            EquivalenceProperties::new(declared),
            Partitioning::UnknownPartitioning(outputs),
            // Interleaved raw identities can recur until every input has ended.
            if ordering == SeriesOrdering::Interleaved {
                EmissionType::Final
            } else {
                EmissionType::Incremental
            },
            // The node ends when its child does, and spends its life waiting until then.
            input.properties().boundedness,
        )
        .with_evaluation_type(EvaluationType::Eager)
        .with_scheduling_type(SchedulingType::Cooperative);
        let state = declaration.aggregate_state().cloned().map(Arc::new);
        let properties = Arc::new(properties);
        Ok(Self {
            input,
            series_schema,
            routing: Routing::with_policy(outputs, routing_policy.clone()),
            routing_policy,
            identity_completion,
            content,
            state,
            ordering,
            shared: Mutex::new(None),
            properties,
        })
    }

    /// The routing policy used by this node.
    pub fn routing_policy(&self) -> &SeriesRouting {
        &self.routing_policy
    }

    pub fn identity_completion(&self) -> IdentityCompletion {
        self.identity_completion
    }

    /// How many output partitions this node has.
    pub fn outputs(&self) -> usize {
        self.properties.partitioning.partition_count()
    }

    /// The queue of `partition` and the lease on this read that reading it takes.
    ///
    /// Both public entry points take an output through here - the frames it publishes and the chunks
    /// it hands over are the same publication, read two ways - so an output is claimed once no
    /// matter which of them is asked for, and asking for both is the error it has always been.
    fn claim_output(
        &self,
        partition: usize,
        context: &Arc<TaskContext>,
        read: InputRead,
    ) -> DataFusionResult<(mpsc::UnboundedReceiver<Queued>, OutputLease)> {
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
                    self.state.clone(),
                    context,
                    read,
                )?);
            }
            let shared = Arc::clone(state.as_ref().expect("the execution was just started"));
            // One read, one way of reading the child: an output that asks for the other form would
            // be answered from a stream it did not ask for, so it is refused here rather than
            // handed the wrong one.
            if shared.read != read {
                return Err(DataFusionError::Execution(format!(
                    "SeriesRepartition was already read as {} for an earlier output, so output \
                     partition {partition} cannot read it as {}",
                    shared.read.as_str(),
                    read.as_str()
                )));
            }
            shared
        };
        SharedExecution::claim(&shared, partition).ok_or_else(|| {
            DataFusionError::Execution(format!(
                "SeriesRepartition output partition {partition} was executed twice"
            ))
        })
    }

    /// One claimed output as the owner that merges it: the queue, its charge, and its lease.
    fn partition_stream(
        &self,
        receiver: mpsc::UnboundedReceiver<Queued>,
        lease: OutputLease,
        context: &Arc<TaskContext>,
    ) -> RepartitionStream {
        RepartitionStream::new(
            receiver,
            lease,
            Arc::clone(&self.series_schema),
            &context.runtime_env().memory_pool,
            self.input.properties().partitioning.partition_count(),
            self.ordering,
            self.identity_completion,
        )
    }

    /// One claimed output's exported state, as the rows the queue carries.
    ///
    /// The decoder is built from the schema this node publishes, which is the schema a frame
    /// consumer would parse, and it is run once on an empty batch before the read starts: a
    /// declaration this revision cannot read is refused here rather than on the first row, which is
    /// exactly what `unpack_aggregate_state` does for the frame path.
    fn state_chunk_stream(
        &self,
        receiver: mpsc::UnboundedReceiver<Queued>,
        lease: OutputLease,
    ) -> DataFusionResult<StateChunkOutputStream> {
        let frame_schema = self.input.schema();
        let declaration = SeriesFrameDeclaration::parse(frame_schema.as_ref())
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        let state = declaration.aggregate_state().ok_or_else(|| {
            DataFusionError::Execution(
                "SeriesRepartition aggregate-state output declares no aggregate state".to_string(),
            )
        })?;
        let decoder = SeriesFrameDecoder::try_new_aggregate_state(
            state,
            &self.series_schema,
            FrameLabelMode::Explicit,
        )
        .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        decoder
            .decode_aggregate_state(&RecordBatch::new_empty(Arc::clone(&frame_schema)))
            .map_err(|error| DataFusionError::Execution(error.to_string()))?;
        Ok(StateChunkOutputStream {
            decoder,
            frame_schema,
            receiver,
            lease,
            pending: VecDeque::new(),
            reported: false,
        })
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
        Ok(Arc::new(Self::try_new_with_completion(
            children.swap_remove(0),
            self.outputs(),
            self.routing_policy.clone(),
            self.identity_completion,
        )?))
    }

    fn execute(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableRecordBatchStream> {
        let (receiver, lease) = self.claim_output(partition, &context, InputRead::Frames)?;
        if self.content == FrameContent::AggregateState {
            return Ok(Box::pin(StateBatchOutputStream {
                schema: self.input.schema(),
                receiver,
                lease,
                retained: None,
                reported: false,
            }));
        }
        let inner = self.partition_stream(receiver, lease, &context);
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
                    ordering: if self.ordering == SeriesOrdering::Interleaved {
                        SeriesOrdering::Contiguous
                    } else {
                        self.ordering
                    },
                };
                // Repartition runs inside the query engine's DataFusion execution, whose runtimes
                // enable the time driver, so the packer may hold a partially filled batch across a
                // pending period.
                pack_raw_series_with(Box::pin(raw), PendingBatching::Deadline)?
            }
            FrameContent::AggregateState => unreachable!("aggregate state output handled above"),
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

impl SeriesExecutionPlan for SeriesRepartitionExec {
    fn as_any(&self) -> &dyn Any {
        self
    }

    /// One owner's publications, as the chunks they are.
    ///
    /// This is [`ExecutionPlan::execute`] without the encode: the same claim of the same output of
    /// the same read, the same owner merging the same identity, and the values that owner produced
    /// handed over instead of packed into a frame. An evaluated publication leaves as one closed
    /// chunk - the owner publishes an identity once, already merged, which is exactly the `is_last`
    /// the packer's evaluated stream declares - and a raw one as the chunk whose finality
    /// [`merge_raw_identity_charging`] recomputed for the whole identity.
    ///
    /// Everything the owner reserved for that publication rides along with the value
    /// ([`SeriesCharge`]), because the consumer holds the bytes those reservations account for: the
    /// frame path holds them in the packer's handoff until the batch is flushed, and there is no
    /// packer here to hold them.
    ///
    /// A state publication is the one content this node does not merge: it forwards the exported row
    /// of a group its child folded, on the same queue and the same lease as the frames it would have
    /// published ([`StateChunkOutputStream`]), so the two entry points stay one read of one output.
    fn execute_series(
        &self,
        partition: usize,
        context: Arc<TaskContext>,
    ) -> DataFusionResult<SendableSeriesStream> {
        let (receiver, lease) = self.claim_output(partition, &context, InputRead::Chunks)?;
        match self.content {
            FrameContent::Raw | FrameContent::Evaluated => {
                let inner = self.partition_stream(receiver, lease, &context);
                Ok(Box::pin(ChunkOutputStream {
                    inner: Some(Box::pin(inner)),
                    content: self.content,
                }))
            }
            FrameContent::AggregateState => Ok(Box::pin(self.state_chunk_stream(receiver, lease)?)),
        }
    }
}

/// One owner's exported state, as the rows it is.
///
/// The queue, the lease and the charge are [`StateBatchOutputStream`]'s: this node does not merge
/// state, it forwards the row a group was exported as. The only thing that changes is the form -
/// the batch the queue carries is decoded into the rows it holds, by the same
/// [`SeriesFrameDeclaration`] decoder a frame consumer would have used, and each row is handed over
/// with the charge of the batch it points into. So a consumer reads the same rows either way, and
/// the batch is never re-encoded.
struct StateChunkOutputStream {
    /// The decoder the exported batches are read with, which is the frame path's decoder.
    decoder: SeriesFrameDecoder,
    /// The schema every exported batch carries, checked before it is decoded.
    frame_schema: SchemaRef,
    /// The owner's queue, shared with the frames this output would otherwise publish.
    receiver: mpsc::UnboundedReceiver<Queued>,
    /// The claim on this read, released when this output ends.
    lease: OutputLease,
    /// Rows decoded from the batch last taken off the queue, in the order the frame carries them.
    pending: VecDeque<HandedSeriesChunk>,
    reported: bool,
}

/// How many empty exported batches one poll reads before yielding, mirroring the frame consumer's
/// own guard so a producer of empty batches cannot hold the task.
const MAX_EMPTY_STATE_BATCHES_PER_POLL: usize = 32;

impl StateChunkOutputStream {
    /// Ends this output with a failure, draining the queue and releasing the rows it still held.
    ///
    /// The frame output ends exactly this way: the read fails once, every queued item is dropped
    /// with its charge, and the output reports the first failure rather than a second one.
    fn failed(
        &mut self,
        reason: impl Into<String>,
    ) -> Poll<Option<Result<HandedSeriesChunk, BoxedError>>> {
        let reason = reason.into();
        self.lease
            .fail(Arc::new(DataFusionError::Execution(reason.clone())));
        self.reported = true;
        self.receiver.close();
        while self.receiver.try_recv().is_ok() {}
        self.pending.clear();
        Poll::Ready(Some(Err(boxed(DataFusionError::Execution(reason)))))
    }
}

impl Stream for StateChunkOutputStream {
    type Item = Result<HandedSeriesChunk, BoxedError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        if this.reported {
            return Poll::Ready(None);
        }
        let mut empty_batches = 0;
        loop {
            if let Some(chunk) = this.pending.pop_front() {
                return Poll::Ready(Some(Ok(chunk)));
            }
            if let Ending::Failed(error) = this.lease.shared.ending() {
                this.reported = true;
                this.receiver.close();
                while this.receiver.try_recv().is_ok() {}
                return Poll::Ready(Some(Err(boxed(DataFusionError::from(&error)))));
            }
            match this.receiver.poll_recv(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Queued::StateBatch {
                    batch,
                    row,
                    _charge,
                    _metadata,
                })) => {
                    // The queue publishes either the whole batch or the one row a multi-output
                    // routing selected, exactly as the frame output reads it.
                    let batch = match row {
                        Some(row) => batch.slice(row, 1),
                        None => batch.as_ref().clone(),
                    };
                    if batch.schema().as_ref() != this.frame_schema.as_ref() {
                        return this.failed("aggregate state frame schema mismatch");
                    }
                    let rows = match this.decoder.decode_aggregate_state(&batch) {
                        Ok(rows) => rows,
                        Err(error) => return this.failed(error.to_string()),
                    };
                    if rows.is_empty() {
                        empty_batches += 1;
                        if empty_batches == MAX_EMPTY_STATE_BATCHES_PER_POLL {
                            cx.waker().wake_by_ref();
                            return Poll::Pending;
                        }
                        continue;
                    }
                    // One batch, one reservation, shared by every row cut from it: the bytes are
                    // live while any of those rows is, and released when the last one is dropped.
                    let metadata = Arc::new(_metadata);
                    for row in rows {
                        let row = match state_row_of(row) {
                            Ok(row) => row,
                            Err(error) => return this.failed(error.to_string()),
                        };
                        this.pending.push_back(HandedSeriesChunk::charged(
                            SeriesChunk::State(StateChunk::Row(row)),
                            SeriesCharge::shared(vec![Arc::clone(&_charge), Arc::clone(&metadata)]),
                        ));
                    }
                }
                Poll::Ready(Some(Queued::StateChunk(queued))) => {
                    let QueuedStateChunk {
                        chunk,
                        _charge,
                        _metadata,
                        child,
                    } = *queued;
                    // A child that answered with its chunks has already done what this output's
                    // frame form would do, so the group is handed on as it is; only the charge this
                    // node queued it with moves onto the value, together with what the child charged
                    // for the publication the group was cut from. The group is a slice of that
                    // publication, so those bytes are still live while it is held.
                    let mut shared = child.map_or_else(Vec::new, |child| (*child).clone());
                    shared.push(Arc::new(_charge));
                    shared.push(Arc::new(_metadata));
                    this.pending.push_back(HandedSeriesChunk::charged(
                        SeriesChunk::State(chunk),
                        SeriesCharge::shared(shared),
                    ));
                }
                Poll::Ready(Some(Queued::Eof { .. })) => {
                    // The shared ending and the queue's closure establish completion; this message
                    // is only a queue-order barrier.
                }
                Poll::Ready(Some(Queued::Chain { .. })) => {
                    return this.failed("series router received a non-state message");
                }
                Poll::Ready(None) => {
                    this.reported = true;
                    this.pending.clear();
                    return match this.lease.ending() {
                        Some(Err(error)) => Poll::Ready(Some(Err(boxed(
                            DataFusionError::External(Box::new(error)),
                        )))),
                        Some(Ok(_)) => unreachable!(),
                        None => Poll::Ready(None),
                    };
                }
            }
        }
    }
}

/// One owner's publications as handed-over chunks.
///
/// The inner stream is [`RepartitionStream`], which decides what an owner publishes; this only
/// changes the form - a [`ReadyOutput`] becomes the [`SeriesChunk`] a native consumer reads - and
/// moves the charge of that publication out of the packer's handoff and onto the value.
struct ChunkOutputStream {
    /// The owner's stream, or `None` once this one ended.
    inner: Option<Pin<Box<RepartitionStream>>>,
    /// What this node publishes, so a raw publication cannot be handed over as an evaluated one.
    content: FrameContent,
}

impl Stream for ChunkOutputStream {
    type Item = Result<HandedSeriesChunk, BoxedError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        let Some(inner) = this.inner.as_mut() else {
            return Poll::Ready(None);
        };
        let ready = match inner.as_mut().poll_next(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Some(Err(error))) => {
                this.inner = None;
                return Poll::Ready(Some(Err(error)));
            }
            Poll::Ready(None) => {
                this.inner = None;
                return Poll::Ready(None);
            }
            Poll::Ready(Some(Ok(ready))) => ready,
        };
        // The owner charged this publication just before publishing it, and the charge is the whole
        // of what the consumer's copy of the values keeps alive.
        let charge = inner.take_charge();
        let chunk = match (this.content, ready) {
            (FrameContent::Evaluated, ReadyOutput::Evaluated(series)) => {
                SeriesChunk::Evaluated(EvaluatedChunk {
                    series,
                    // A merged identity is final: the owner publishes it once, already closed.
                    is_last: true,
                })
            }
            (FrameContent::Raw, ReadyOutput::Raw(chunk)) => SeriesChunk::Raw(chunk),
            (content, ready) => {
                this.inner = None;
                return Poll::Ready(Some(Err(boxed(DataFusionError::Internal(format!(
                    "a {} repartition output carried {}",
                    content.as_str(),
                    ready.what()
                ))))));
            }
        };
        match charge {
            Some(charge) => Poll::Ready(Some(Ok(HandedSeriesChunk::charged(chunk, charge)))),
            // The charge stays where the frame path keeps it, so it is released with the handoff
            // rather than silently dropped here.
            None => Poll::Ready(Some(Ok(HandedSeriesChunk::uncharged(chunk)))),
        }
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
    /// The way this read's child is being read, fixed by the first output that claimed it.
    read: InputRead,
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
    #[allow(clippy::too_many_arguments)]
    fn start(
        input: &Arc<dyn ExecutionPlan>,
        routing: Routing,
        series_schema: Arc<SeriesSchema>,
        state: Option<Arc<AggregateStateSchema>>,
        context: &Arc<TaskContext>,
        read: InputRead,
    ) -> DataFusionResult<Arc<Self>> {
        // What the child carries is what it declares, not what the policy suggests: a raw frame
        // can be routed by a match key too, so the policy no longer names the content.
        let content = SeriesFrameDeclaration::parse(input.schema().as_ref())
            .map_err(|error| DataFusionError::Plan(error.to_string()))?
            .content();
        // What the child promises about the order of what it publishes, which is what a chunk read
        // of it has to be held to.
        let declared_ordering = series_frame_ordering(input.schema().as_ref())?;
        let partitions = input.properties().partitioning.partition_count();
        // The child is asked for its chunks only when a reader of them claimed this read: the other
        // claim publishes frames, and a chunk read would have to be packed again.
        let hand_over = match read {
            InputRead::Chunks => series_execution_plan(input.as_ref()),
            InputRead::Frames => None,
        };
        let mut streams = Vec::with_capacity(partitions);
        for child_partition in 0..partitions {
            streams.push(routed_input(
                input,
                hand_over,
                content,
                &series_schema,
                state.as_ref(),
                declared_ordering,
                child_partition,
                context,
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
        let shared = Arc::new_cyclic(|weak| SharedExecution {
            read,
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

/// Upper bound for transient arrays allocated by slicing one aggregate-state row. It counts each
/// schema node's concrete array object, two `usize` wrapper words, and one reference per child.
fn state_slice_wrapper_bytes(
    schema: &datafusion::arrow::datatypes::Schema,
) -> DataFusionResult<usize> {
    use datafusion::arrow::array::{
        ArrayRef, BinaryArray, BooleanArray, ListArray, PrimitiveArray, StringArray, StructArray,
    };
    use datafusion::arrow::datatypes::{DataType, Float64Type, UInt64Type};
    fn node(data_type: &DataType) -> DataFusionResult<usize> {
        let children = match data_type {
            DataType::Struct(fields) => fields
                .iter()
                .map(|field| node(field.data_type()))
                .collect::<DataFusionResult<Vec<_>>>()?,
            DataType::List(_) => Vec::new(),
            _ => Vec::new(),
        };
        let object = match data_type {
            DataType::Struct(_) => std::mem::size_of::<StructArray>(),
            DataType::List(_) => std::mem::size_of::<ListArray>(),
            DataType::Utf8 | DataType::LargeUtf8 => std::mem::size_of::<StringArray>(),
            DataType::Binary | DataType::LargeBinary => std::mem::size_of::<BinaryArray>(),
            DataType::Boolean => std::mem::size_of::<BooleanArray>(),
            DataType::Float64 => std::mem::size_of::<PrimitiveArray<Float64Type>>(),
            DataType::UInt64 => std::mem::size_of::<PrimitiveArray<UInt64Type>>(),
            // Frame schema types outside this list are bounded by the largest enumerated object.
            _ => [
                std::mem::size_of::<StructArray>(),
                std::mem::size_of::<ListArray>(),
                std::mem::size_of::<StringArray>(),
                std::mem::size_of::<BinaryArray>(),
                std::mem::size_of::<BooleanArray>(),
                std::mem::size_of::<PrimitiveArray<Float64Type>>(),
                std::mem::size_of::<PrimitiveArray<UInt64Type>>(),
            ]
            .into_iter()
            .max()
            .unwrap(),
        };
        object
            .checked_add(2 * std::mem::size_of::<usize>())
            .and_then(|bytes| bytes.checked_add(children.len() * std::mem::size_of::<ArrayRef>()))
            .and_then(|bytes| children.into_iter().try_fold(bytes, usize::checked_add))
            .ok_or_else(|| {
                DataFusionError::Execution("aggregate-state slice wrapper size overflow".into())
            })
    }
    let arrays = schema.fields().iter().try_fold(0usize, |bytes, field| {
        bytes.checked_add(node(field.data_type())?).ok_or_else(|| {
            DataFusionError::Execution("aggregate-state slice wrapper size overflow".into())
        })
    })?;
    // `RecordBatch::slice` also allocates the vector it collects the sliced columns into.
    arrays
        .checked_add(
            schema
                .fields()
                .len()
                .checked_mul(std::mem::size_of::<ArrayRef>())
                .ok_or_else(|| {
                    DataFusionError::Execution("aggregate-state slice wrapper size overflow".into())
                })?,
        )
        .ok_or_else(|| {
            DataFusionError::Execution("aggregate-state slice wrapper size overflow".into())
        })
}

/// Routes state rows without materializing Rust accumulator objects or rebuilding Arrow columns.
async fn read_state_batches(
    stream: &mut SendableRecordBatchStream,
    _partition: usize,
    routing: &Routing,
    senders: &[mpsc::UnboundedSender<Queued>],
    read_ahead: &MemoryReservation,
    state: &AggregateStateSchema,
    series: &SeriesSchema,
) -> Stop {
    use datatypes::series::frame::FrameLabelMode;
    let decoder = match SeriesFrameDecoder::try_new_aggregate_state(
        state,
        series,
        FrameLabelMode::Explicit,
    ) {
        Ok(decoder) => decoder,
        Err(error) => return Stop::Failed(Arc::new(DataFusionError::Execution(error.to_string()))),
    };
    let expected_schema = match datatypes::series::frame::aggregate_state_frame_schema(
        state,
        series,
        FrameLabelMode::Explicit,
    ) {
        Ok(schema) => schema,
        Err(error) => return Stop::Failed(Arc::new(DataFusionError::Execution(error.to_string()))),
    };
    while let Some(next) = stream.next().await {
        let batch = match next {
            Ok(batch) => batch,
            Err(error) => {
                return Stop::Failed(Arc::new(DataFusionError::External(Box::new(error))));
            }
        };
        if batch.schema().as_ref() != expected_schema.as_ref() {
            return Stop::Failed(Arc::new(DataFusionError::Execution(
                "aggregate state frame schema mismatch".into(),
            )));
        }
        let charge = read_ahead.new_empty();
        // Charge the input batch's retained buffers once. List slices still reference their entire
        // values array, so a slice-size estimate cannot safely charge each queued row instead.
        let batch_bytes = std::mem::size_of::<RecordBatch>()
            .saturating_add(
                batch
                    .num_columns()
                    .saturating_mul(std::mem::size_of::<datafusion::arrow::array::ArrayRef>()),
            )
            .saturating_add(batch.get_array_memory_size());
        if let Err(error) = charge.try_grow(batch_bytes) {
            return Stop::Failed(Arc::new(error));
        }
        let labels = match decoder.decode_aggregate_state_labels(&batch) {
            Ok(labels) => labels,
            Err(error) => {
                return Stop::Failed(Arc::new(DataFusionError::Execution(error.to_string())));
            }
        };
        let charge = Arc::new(charge);
        let batch = Arc::new(batch);
        if routing.outputs == 1 {
            // Every row has the same owner. Keep the frame packed so a downstream router reads
            // its shared payload once rather than receiving one parent-retaining slice per row.
            if !labels.is_empty() && !senders[0].is_closed() {
                let metadata = read_ahead.new_empty();
                // Cloning the batch for the output allocates a new column-reference vector.
                let wrapper_bytes = std::mem::size_of::<Queued>()
                    .saturating_add(std::mem::size_of::<RecordBatch>())
                    .saturating_add(
                        batch.num_columns().saturating_mul(std::mem::size_of::<
                            datafusion::arrow::array::ArrayRef,
                        >()),
                    );
                if let Err(error) = metadata.try_grow(wrapper_bytes) {
                    return Stop::Failed(Arc::new(error));
                }
                if let Err(error) = senders[0].send(Queued::StateBatch {
                    batch,
                    row: None,
                    _charge: charge,
                    _metadata: metadata,
                }) {
                    drop(error.0);
                }
            }
            tokio::task::coop::consume_budget().await;
            continue;
        }
        // Each queued row receives a reservation for the schema-based slice-wrapper upper bound.
        let slice_charge = match state_slice_wrapper_bytes(batch.schema().as_ref()) {
            Ok(bytes) => bytes,
            Err(error) => return Stop::Failed(Arc::new(error)),
        };
        let slice_metadata_bytes = match std::mem::size_of::<Queued>().checked_add(slice_charge) {
            Some(bytes) => bytes,
            None => {
                return Stop::Failed(Arc::new(DataFusionError::Execution(
                    "aggregate-state slice wrapper size overflow".into(),
                )));
            }
        };
        for (index, metric) in labels.iter().enumerate() {
            let output = routing.output_labels(metric);
            if senders[output].is_closed() {
                continue;
            }
            // All slices retain the same parent buffers; a shared reservation charges them once.
            let metadata = read_ahead.new_empty();
            if let Err(error) = metadata.try_grow(slice_metadata_bytes) {
                return Stop::Failed(Arc::new(error));
            }
            if let Err(error) = senders[output].send(Queued::StateBatch {
                batch: Arc::clone(&batch),
                row: Some(index),
                _charge: Arc::clone(&charge),
                _metadata: metadata,
            }) {
                drop(error.0);
            }
        }
        tokio::task::coop::consume_budget().await;
    }
    Stop::Ended
}

/// Whether `chunk` is a publication of the state this node declared it routes.
///
/// A state frame says what it is in its schema: the aggregate kind, the ordered component layout and
/// the series - fields and evaluation grid - it is of are all part of the frame `read_state_batches`
/// decodes, so a batch under any other schema is refused there before a row of it is routed. A chunk
/// says the same three things on the value: the layout in the [`StateChunk::Series`] it carries (or
/// in the row's fields), and the grid each group's axis is on. This is what holds a handed-over
/// publication to the standard the frame path holds a batch to; anything less would let a child hand
/// over state this node never declared and have it merged as that state.
fn declared_state_of(
    declared: &AggregateStateSchema,
    series: &SeriesSchema,
    chunk: &StateChunk,
) -> bool {
    let declared_grid = series.eval_grid.as_ref().map(|grid| grid.spec());
    let same_grid = |grid: Option<&Arc<datatypes::series::time::EvalGrid>>| {
        declared_grid.is_some_and(|declared| grid.is_some_and(|grid| grid.spec() == declared))
    };
    match chunk {
        StateChunk::Series {
            series: groups,
            state_schema,
            ..
        } => {
            state_schema.kind == declared.kind
                && state_schema.components == declared.components
                && declared_grid.is_some_and(|declared| state_schema.grid.spec() == declared)
                && groups.iter().all(|group| {
                    group.states.len() == series.fields.len() && same_grid(group.time.grid())
                })
        }
        StateChunk::Row(row) => {
            row.kind == declared.kind
                && row.fields.components() == declared.components.as_ref()
                && row.fields.len() == series.fields.len()
                && same_grid(row.time.grid())
        }
    }
}

/// Reads a child's handed-over state, routing each group to the owner that folds it.
///
/// A state publication is not merged here: every group the child folded is one row of its export, and
/// which owner folds it is the hash of its labels, exactly as a state frame's row is routed. What
/// differs is only the form - the group arrives as the child's own chunk instead of as a batch - so
/// the charge this node queues it with is grown while the child's charge is still in hand, and the
/// child's own charge moves onto the groups rather than going with the publication it is for.
async fn read_state_chunks(
    stream: &mut SendableSeriesStream,
    // The queue item names the group, not the partition it came from: a state publication is routed
    // by its labels, and nothing waits for one partition's group to complete another's.
    _partition: usize,
    routing: &Routing,
    senders: &[mpsc::UnboundedSender<Queued>],
    read_ahead: &MemoryReservation,
    declared: &Arc<AggregateStateSchema>,
    series: &Arc<SeriesSchema>,
) -> Stop {
    loop {
        tokio::task::coop::consume_budget().await;
        let next = stream.next().await;
        match next {
            Some(Ok(handed)) => {
                let (chunk, child_charge) = handed.into_parts();
                let SeriesChunk::State(chunk) = chunk else {
                    return Stop::Failed(Arc::new(DataFusionError::Execution(format!(
                        "an aggregate-state input handed over {chunk:?}"
                    ))));
                };
                // The check the frame path makes of a batch's schema, made of the declaration the
                // chunk carries: a publication of another kind, layout or grid is refused here
                // rather than routed under a state this node never declared.
                if !declared_state_of(declared, series, &chunk) {
                    return Stop::Failed(Arc::new(DataFusionError::Execution(
                        "aggregate state chunk schema mismatch".into(),
                    )));
                }
                // One publication may hold several groups; each is routed on its own labels, and
                // the charge the child put on the whole of it rides on all of them - a group is a
                // slice of that publication, so its bytes stay live until the last group is dropped.
                let mut publication = child_charge.map_or_else(Vec::new, SeriesCharge::into_shared);
                let groups = match chunk {
                    StateChunk::Series {
                        series,
                        state_schema,
                        reservation,
                    } => {
                        publication.extend(reservation.map(Arc::new));
                        series
                            .into_iter()
                            .map(|series| StateChunk::Series {
                                series: vec![series],
                                state_schema: Arc::clone(&state_schema),
                                reservation: None,
                            })
                            .collect::<Vec<_>>()
                    }
                    StateChunk::Row(row) => vec![StateChunk::Row(row)],
                };
                // One reference to the publication's charge, shared by every group cut from it.
                let publication = (!publication.is_empty()).then(|| Arc::new(publication));
                for group in groups {
                    let metric = match &group {
                        StateChunk::Series { series, .. } => &series[0].metric,
                        StateChunk::Row(row) => &row.metric,
                    };
                    let output = routing.output_labels(metric);
                    if senders[output].is_closed() {
                        continue;
                    }
                    // Charged as it is queued, so what this node holds is accounted for before the
                    // child's own charge goes.
                    let charge = read_ahead.new_empty();
                    if let Err(error) = charge.try_grow(state_bytes(&group)) {
                        return Stop::Failed(Arc::new(error));
                    }
                    // The message and the boxed group it points at, which is this content's own
                    // share of what a queue holds for one item.
                    let metadata = read_ahead.new_empty();
                    if let Err(error) = metadata.try_grow(
                        std::mem::size_of::<Queued>()
                            .saturating_add(std::mem::size_of::<QueuedStateChunk>()),
                    ) {
                        return Stop::Failed(Arc::new(error));
                    }
                    if let Err(error) =
                        senders[output].send(Queued::StateChunk(Box::new(QueuedStateChunk {
                            chunk: group,
                            _charge: charge,
                            _metadata: metadata,
                            // One reference per queued group, so none of them is this task's to
                            // hold: the publication's charge goes when its last group does.
                            child: publication.clone(),
                        })))
                    {
                        drop(error.0);
                    }
                }
            }
            Some(Err(error)) => {
                return Stop::Failed(Arc::new(DataFusionError::External(Box::new(error))));
            }
            None => return Stop::Ended,
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
        RoutedInput::StateChunks(stream, state, series) => {
            read_state_chunks(
                stream,
                partition,
                &routing,
                &senders,
                &read_ahead,
                state,
                series,
            )
            .await
        }
        RoutedInput::State(stream, state, series) => {
            read_state_batches(
                stream,
                partition,
                &routing,
                &senders,
                &read_ahead,
                state,
                series,
            )
            .await
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
    /// Exported state a child handed over, one publication at a time, with the state and the series
    /// this node declared those publications to be.
    StateChunks(
        SendableSeriesStream,
        Arc<AggregateStateSchema>,
        Arc<SeriesSchema>,
    ),
    State(
        SendableRecordBatchStream,
        Arc<AggregateStateSchema>,
        Arc<SeriesSchema>,
    ),
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
    StateBatch {
        batch: Arc<RecordBatch>,
        // None forwards the entire parent; Some selects one row for multi-output routing.
        row: Option<usize>,
        _charge: Arc<MemoryReservation>,
        _metadata: MemoryReservation,
    },
    /// One group of exported state, as the child that folded it handed it over.
    ///
    /// Boxed because the queue message is charged for by its own size, and every chain and every row
    /// of every other content pays that charge too: a state publication carries more than a chain
    /// does, and it is the only content that does.
    StateChunk(Box<QueuedStateChunk>),
}

/// What one queued state group holds: the group, and the charges it keeps alive.
struct QueuedStateChunk {
    chunk: StateChunk,
    /// What this node queued it with: the payload the group holds.
    _charge: MemoryReservation,
    _metadata: MemoryReservation,
    /// What the child charged for the publication this group was cut from.
    ///
    /// A group is a slice of the publication it came from - a row's state columns still point into
    /// the batch they were decoded out of - so the bytes the child reserved for that publication are
    /// live for as long as any group cut from it is. They ride on the group for that reason: released
    /// here they would be released while the group is still held, and the pool would hand the
    /// parent's buffers to somebody else. The reference is taken once per publication and cloned per
    /// group, so the publication's charge goes when its last group does.
    child: Option<Arc<Vec<Arc<MemoryReservation>>>>,
}

type ExactIdentity = (Option<Arc<str>>, Vec<(Arc<str>, Arc<str>)>);

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

fn raw_identity(chunk: &RawSeriesChunk) -> OwnerKey {
    OwnerKey::Raw(chunk.primary_key.as_slice().to_vec())
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
    // A field is one or more dense segments - a merged chunk carries one per source piece - so this
    // walks them and measures each. Asking the field for a single dense view instead answered `None`
    // for a multi-segment field, which charged nothing for bytes the router then queued. A segment is
    // measured here by the buffers a slice of it keeps alive, where the evaluated figure
    // ([`sample_slice_bytes`]) bills its rows.
    let mut fields_bytes = 0usize;
    for segment in field_segments(&chunk.fields) {
        let field_bytes = match segment.dense() {
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
            // `segments` hands out dense segments only, so this is unreachable; a zero here is
            // what the whole-field view used to answer for a merged field.
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
/// still waiting would go out first and break that promise. Watermarks let it publish once every
/// input has advanced past the identity, without waiting for every input to end. An interleaved
/// raw identity can recur even after an input contributed, so only all normal EOFs complete it.
fn group_ready(
    group: &PendingGroup,
    eof: &[bool],
    last_key_from: &[Option<Vec<u8>>],
    ordering: SeriesOrdering,
    completion: IdentityCompletion,
    key: &OwnerKey,
) -> bool {
    if ordering == SeriesOrdering::Interleaved && matches!(key, OwnerKey::Raw(_)) {
        return eof.iter().all(|eof| *eof);
    }
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
    if completion == IdentityCompletion::CompleteDisjoint {
        return true;
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

impl ReadyOutput {
    /// What this publication is, for an error that has to name what arrived instead.
    fn what(&self) -> &'static str {
        match self {
            Self::Evaluated(_) => "an evaluated series",
            Self::Raw(_) => "a raw series",
        }
    }
}

struct ReadySeries {
    series: ReadyOutput,
    charge: SeriesCharge,
}

/// One owner processes identities as soon as every input has closed or contributed its final chain.
struct StateBatchOutputStream {
    schema: SchemaRef,
    receiver: mpsc::UnboundedReceiver<Queued>,
    lease: OutputLease,
    retained: Option<(Arc<MemoryReservation>, MemoryReservation)>,
    reported: bool,
}

impl RecordBatchStream for StateBatchOutputStream {
    fn schema(&self) -> SchemaRef {
        Arc::clone(&self.schema)
    }
}

impl Stream for StateBatchOutputStream {
    type Item = DataFusionResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.reported {
            return Poll::Ready(None);
        }
        self.retained = None;
        loop {
            if let Ending::Failed(error) = self.lease.shared.ending() {
                self.reported = true;
                self.receiver.close();
                while self.receiver.try_recv().is_ok() {}
                self.retained = None;
                return Poll::Ready(Some(Err(DataFusionError::from(&error))));
            }
            match self.receiver.poll_recv(cx) {
                Poll::Pending => return Poll::Pending,
                Poll::Ready(Some(Queued::StateBatch {
                    batch,
                    row,
                    _charge,
                    _metadata,
                })) => {
                    // Keep the router reservation through the next poll so Final can admit the
                    // returned batch's retained component arrays before releasing router charge.
                    self.retained = Some((_charge, _metadata));
                    let batch = match row {
                        Some(row) => batch.slice(row, 1),
                        None => batch.as_ref().clone(),
                    };
                    return Poll::Ready(Some(Ok(batch)));
                }
                Poll::Ready(Some(Queued::Eof { input, _metadata })) => {
                    // The shared Ending state and channel closure establish stream completion;
                    // this message is only a queue-order barrier for the routed state stream.
                    let _ = input;
                    drop(_metadata);
                }
                Poll::Ready(Some(Queued::StateChunk(_))) => {
                    let error = DataFusionError::Internal(
                        "a state frame output received a handed-over group".into(),
                    );
                    self.lease.fail(Arc::new(DataFusionError::Internal(
                        "a state frame output received a handed-over group".into(),
                    )));
                    self.reported = true;
                    self.receiver.close();
                    while self.receiver.try_recv().is_ok() {}
                    self.retained = None;
                    return Poll::Ready(Some(Err(error)));
                }
                Poll::Ready(Some(Queued::Chain { .. })) => {
                    let error = DataFusionError::Internal(
                        "series router received a non-state message".into(),
                    );
                    self.lease.fail(Arc::new(DataFusionError::Internal(
                        "series router received a non-state message".into(),
                    )));
                    self.reported = true;
                    self.receiver.close();
                    while self.receiver.try_recv().is_ok() {}
                    self.retained = None;
                    return Poll::Ready(Some(Err(error)));
                }
                Poll::Ready(None) => {
                    self.reported = true;
                    self.retained = None;
                    return match self.lease.ending() {
                        Some(Err(error)) => {
                            Poll::Ready(Some(Err(DataFusionError::External(Box::new(error)))))
                        }
                        Some(Ok(_)) => unreachable!(),
                        None => Poll::Ready(None),
                    };
                }
            }
        }
    }
}

struct RepartitionStream {
    receiver: mpsc::UnboundedReceiver<Queued>,
    lease: OutputLease,
    schema: Arc<SeriesSchema>,
    pool: Arc<dyn MemoryPool>,
    /// The input's ordering and identity completion guarantee.
    ordering: SeriesOrdering,
    identity_completion: IdentityCompletion,
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
    current: Option<SeriesCharge>,
    completed: Vec<SeriesCharge>,
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
        identity_completion: IdentityCompletion,
    ) -> Self {
        Self {
            receiver,
            lease,
            schema,
            ordering,
            identity_completion,
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

    /// Takes what the owner charged for the publication it just returned.
    ///
    /// [`poll_next`](Stream::poll_next) installs the charge of the publication it returns in the
    /// handoff, where [`PackedHandoffStream`] releases it once the batch it backs has been flushed.
    /// A consumer that is handed the value itself takes it here instead, so the reservation rides
    /// with the chunk and is released when the consumer drops it - neither before the bytes it
    /// accounts for are gone, nor after.
    fn take_charge(&mut self) -> Option<SeriesCharge> {
        self.handoff
            .lock()
            .unwrap_or_else(|error| error.into_inner())
            .current
            .take()
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
            charge: SeriesCharge::new(group.charges, group.metadata, workspace),
        });
        Ok(())
    }
}

impl RepartitionStream {
    /// Publishes one complete raw identity.
    ///
    /// A piece's `is_last` closes that producer's chain, not the reconstructed series. For
    /// interleaved input, pieces are retained until every input reaches normal EOF. The workspace
    /// reconstruction needs is charged as it runs and rides with the output until the packer
    /// hands the batch off, like any other owner output.
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
                charge: SeriesCharge::new(group.charges, group.metadata, None),
            });
            return Ok(());
        }
        // Reconstruction's working set depends on how the pieces interleave, which is only known
        // while it runs: charge each allocation as it is made rather than reserving a worst case
        // that a well-ordered identity never approaches.
        let workspace = MemoryConsumer::new("SeriesRepartitionRawOwner").register(&self.pool);
        let mut reserve = |bytes: usize| workspace.try_grow(bytes);
        let mut shrink = |bytes: usize| {
            let result = workspace.try_shrink(bytes);
            debug_assert!(
                result.is_ok(),
                "raw merge workspace shrink failed: {result:?}"
            );
            // The merged data is already correct; tolerate accounting inconsistency rather than
            // turning a valid query result into an error.
            let _ = result;
        };
        let chunk = merge_raw_identity_charging(
            &self.schema,
            &group.raw_chunks,
            &mut reserve,
            &mut shrink,
        )?;
        self.output.push_back(ReadySeries {
            series: ReadyOutput::Raw(chunk),
            charge: SeriesCharge::new(group.charges, group.metadata, Some(workspace)),
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
                let next = if this.ordering == SeriesOrdering::KeyThenTime {
                    this.groups.first_key_value().and_then(|(key, group)| {
                        group_ready(
                            group,
                            &this.eof,
                            &this.last_key_from,
                            this.ordering,
                            this.identity_completion,
                            key,
                        )
                        .then(|| key.clone())
                    })
                } else {
                    match &this.resume_after {
                        Some(after) => this
                            .groups
                            .range((
                                std::ops::Bound::Excluded(after.clone()),
                                std::ops::Bound::Unbounded,
                            ))
                            .find_map(|(key, group)| {
                                group_ready(
                                    group,
                                    &this.eof,
                                    &this.last_key_from,
                                    this.ordering,
                                    this.identity_completion,
                                    key,
                                )
                                .then(|| key.clone())
                            }),
                        None => this.groups.iter().find_map(|(key, group)| {
                            group_ready(
                                group,
                                &this.eof,
                                &this.last_key_from,
                                this.ordering,
                                this.identity_completion,
                                key,
                            )
                            .then(|| key.clone())
                        }),
                    }
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
                    .current = Some(ready.charge);
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
                Poll::Ready(Some(Queued::StateBatch { .. }))
                | Poll::Ready(Some(Queued::StateChunk(_))) => {
                    this.lease.fail(Arc::new(DataFusionError::Internal(
                        "a series output received aggregate state".into(),
                    )));
                }
                Poll::Ready(Some(Queued::Chain {
                    input,
                    chain,
                    _metadata,
                })) => {
                    drained += 1;
                    if input >= this.eof.len() {
                        this.lease.fail(Arc::new(DataFusionError::Execution(
                            "series repartition input index is out of range".into(),
                        )));
                        continue;
                    }
                    let (key, pieces) = match chain {
                        RoutedChain::Evaluated(chain) => {
                            let Some(first) = chain.first() else {
                                continue;
                            };
                            let key = exact_identity(&first.0);
                            (OwnerKey::Evaluated(key), RoutedChain::Evaluated(chain))
                        }
                        RoutedChain::Raw(chain) => {
                            let Some(first) = chain.first() else {
                                continue;
                            };
                            let key = raw_identity(&first.0);
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
                    // One tree search answers all three questions a message asks of the identity
                    // tree: whether the identity is present, creating it if it is not, and which
                    // group the pieces join. A vacant entry charges the identity before it inserts
                    // it, and the key moves in rather than being cloned in beside the search.
                    let admission = match this.groups.entry(key) {
                        Entry::Vacant(vacant) => {
                            let metadata = MemoryConsumer::new("SeriesRepartitionIdentity")
                                .register(&this.pool);
                            let key_bytes = match vacant.key() {
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
                            // The entry borrows the identity tree, so a refused charge reports
                            // itself after that borrow ends: the identity it could not pay for is
                            // never inserted.
                            match metadata.try_grow(bytes) {
                                Ok(()) => Ok(vacant.insert_entry(PendingGroup {
                                    fragments: Vec::new(),
                                    raw_chunks: Vec::new(),
                                    charges: Vec::new(),
                                    received: vec![false; this.eof.len()],
                                    metadata,
                                })),
                                Err(error) => Err(Arc::new(error)),
                            }
                        }
                        Entry::Occupied(occupied) => Ok(occupied),
                    };
                    // The entry borrows the identity tree, so it lives only inside this block:
                    // publishing takes the whole stream, and a refused charge reports itself with no
                    // entry left alive.
                    let ready = {
                        let mut entry = match admission {
                            Ok(entry) => entry,
                            Err(error) => {
                                this.lease.fail(error);
                                continue;
                            }
                        };
                        if input >= entry.get().received.len()
                            || (entry.get().received[input]
                                && !(this.ordering == SeriesOrdering::Interleaved
                                    && matches!(entry.key(), OwnerKey::Raw(_))))
                        {
                            this.lease.fail(Arc::new(DataFusionError::Execution(
                                "input repeated an exact series identity".into(),
                            )));
                            continue;
                        }
                        {
                            let group = entry.get_mut();
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
                        }
                        // Key-then-time publishes through the ordered rescan above; any other
                        // ordering publishes this identity as soon as it is ready. The identity
                        // `finish_group` removes is copied out only when this group is the one being
                        // published.
                        if this.ordering == SeriesOrdering::KeyThenTime {
                            this.rescan = true;
                            this.resume_after = None;
                            None
                        } else if group_ready(
                            entry.get(),
                            &this.eof,
                            &this.last_key_from,
                            this.ordering,
                            this.identity_completion,
                            entry.key(),
                        ) {
                            Some(entry.key().clone())
                        } else {
                            None
                        }
                    };
                    if let Some(key) = ready
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
                            .current = Some(ready.charge);
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
            SeriesRouting::CanonicalLabelsExcept(excluded) => {
                let mut labels = label_set(metric);
                labels.retain(|(name, _)| !excluded.contains(name));
                labels.hash(&mut hasher);
            }
            SeriesRouting::Matching(policy) => {
                crate::vector_match::group_key(metric, policy).hash(&mut hasher)
            }
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
