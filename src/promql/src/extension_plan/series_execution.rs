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

//! The in-process handover between two adjacent native PromQL operators.
//!
//! Two native operators pass each other a `SeriesFrame` record batch today: the producer packs its
//! chunks into one and the consumer unpacks them again, only because
//! [`ExecutionPlan::execute`] answers a `SendableRecordBatchStream`. No DataFusion operator stands
//! between them, so the frame is written and read back for nobody: a transport type used for a
//! function call.
//!
//! [`SeriesExecutionPlan`] is that call. A native operator that can answer its chunks as they are
//! implements the trait, and a native consumer that receives one calls
//! [`SeriesExecutionPlan::execute_series`] instead of decoding a frame. The chunk is the internal
//! value the operator already holds - a [`RawSeriesChunk`] read from a region, an
//! [`EvaluatedChunk`] a tree settled on its grid, or a [`StateFrameRow`] a `Partial` exported - so
//! the handover reuses the buffers instead of encoding them. Every content a native node publishes
//! travels this way; there is no second path for one of them to be left on.
//!
//! What keeps the shortcut structural, rather than a convention an operator has to remember, is
//! that the capability is a type: [`series_execution_plan`] answers for the operators this crate
//! implements it on and for nothing else. A plan that produces frames is not one of them - a
//! network exchange, a remote scan, or any node reading a frame it received answers `None` - so no
//! chunk reaches a consumer across a boundary this way. The question is asked once per input and
//! answered by the input alone: calling [`execute_series`](SeriesExecutionPlan::execute_series) is
//! how a consumer says it reads that input's chunks, and calling
//! [`ExecutionPlan::execute`] is how it says it reads its frames. There is no second decision to
//! keep in step with the first.
//!
//! Which edge this is, and which edge it is not:
//!
//! - In process: the two ends are nodes of one plan in one process, and the consumer called
//!   [`execute_series`](SeriesExecutionPlan::execute_series) on the operator it was handed as an
//!   input. That is the whole of where a chunk can travel.
//! - Still bytes: every edge that leaves the process, or reaches a node this crate does not own. A
//!   region's scan and the scan a merge sits on still publish `RecordBatch`es, a remote scan answers
//!   frames, and a root output still converts series back into a batch (`series_to_record_batch`).
//!   No chunk is serialized and none is sent anywhere, so nothing about the wire changes here.
//!
//! Two things the frame path does are not skipped, because they are not about the encoding:
//!
//! - The sequence is checked. [`SequenceValidator`] and [`EvaluatedSequenceValidator`] are what a
//!   frame consumer runs while it decodes; [`checked_series_stream`] runs the same validators over
//!   the same sequence of chunks, so a producer whose chains are malformed is refused here exactly
//!   as it is through a frame. An exported state row has no such chain - its frame consumer runs no
//!   cross-row validator either - so there is nothing for that content to skip.
//! - The memory is charged for as long as it is held. A producer that merges an identity reserves
//!   the bytes it merges into and hands them over with the value ([`SeriesCharge`]), so the pool
//!   sees them until the consumer drops the chunk that carries them - not until a packer flushed a
//!   batch, and not for longer either.
//!
//! What a consumer records does not change with the representation. The figures a native node keeps
//! are recorded per chunk it was fed, and both paths feed it the producer's chunks: a frame carries
//! a chunk as a row, and decoding it yields the chunk back. The one figure that can differ is the
//! count of those chunks, and only for a chunk whose field is itself split into segments - a frame
//! row never spans two segments of one field, so such a chunk is one chunk in process and several
//! rows in a frame. Nothing else, and in particular no sample, value or closed-series count, is
//! recorded on one path and not the other.

use std::any::Any;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use common_error::ext::BoxedError;
use common_query::series_frame::{EvaluatedSequenceValidator, SequenceValidator};
use datafusion::error::Result as DataFusionResult;
use datafusion::execution::context::TaskContext;
use datafusion::execution::memory_pool::MemoryReservation;
use datafusion::physical_plan::ExecutionPlan;
use std::fmt::{self, Formatter};

use datatypes::series::SeriesOrdering;
use datatypes::series::frame::StateFrameRow;
use datatypes::series::raw::RawSeriesChunk;
use datatypes::series::samples::SeriesSchema;
use futures::Stream;

use crate::evaluated::EvaluatedChunk;
use crate::extension_plan::RawSeriesSourceExec;
use crate::extension_plan::series_repartition::SeriesRepartitionExec;

/// One piece of one series, handed from one native operator to the next.
///
/// The variants are the three contents the native chain passes between its nodes: raw samples read
/// from a region, values a tree evaluated on the grid its output declares, and one group's exported
/// aggregate state. [`EvaluatedChunk`] is [`Series`](datatypes::series::samples::Series) plus the
/// finality of the chunk, so it is the evaluated half of this enum rather than a `Series` alone: a
/// chunk that cannot say whether it closes its series would make the consumer guess, and "no value
/// here" and "nothing more here" would become the same thing.
///
/// [`SeriesChunk::State`] is the content a state frame row carries, and deliberately not a second
/// encoding of it: [`AggregateStateChunk`] holds the [`StateFrameRow`] a decoding consumer would
/// have built, so the producer's decoder is the frame path's decoder and the consumer's conversion
/// is the frame path's conversion. The variant exists because that publication is a value this
/// crate holds, exactly like the other two, and an in-process consumer reads it as that value.
#[derive(Debug)]
pub enum SeriesChunk {
    /// One scan series, with its own timestamps and its finality.
    Raw(RawSeriesChunk),
    /// Values a producer already evaluated on the grid its frame declares.
    Evaluated(EvaluatedChunk),
    /// One group's exported aggregate state, as the row a `Final` aggregate folds.
    State(AggregateStateChunk),
}

/// One row of an exported aggregate state, handed over in process.
///
/// Nothing is re-encoded: the row is what [`StateFrameRow`] is - the group, the grid positions it
/// has contributions at, whether it closes its chain, and the state arrays themselves - and the
/// series layout it was exported on travels with it, because that layout is what the frame's
/// declaration would have told a decoding consumer, and a consumer has to be able to check that it
/// still binds the same series.
pub struct AggregateStateChunk {
    /// The exported row.
    row: StateFrameRow,
    /// The series layout the export was declared on.
    series: Arc<SeriesSchema>,
}

impl AggregateStateChunk {
    /// One exported row, and the series layout it was declared on.
    pub(crate) fn new(row: StateFrameRow, series: Arc<SeriesSchema>) -> Self {
        Self { row, series }
    }

    /// The exported row, and the series layout it was declared on, to be read together.
    pub fn into_parts(self) -> (StateFrameRow, Arc<SeriesSchema>) {
        (self.row, self.series)
    }
}

impl fmt::Debug for AggregateStateChunk {
    /// Which group closes where, not the payload: a `StateFrameRow` keeps its arrays and is not
    /// `Debug` for the same reason a chunk is not printed sample by sample.
    fn fmt(&self, f: &mut Formatter<'_>) -> fmt::Result {
        f.debug_struct("AggregateStateChunk")
            .field("metric", self.row.metric())
            .field("is_last", &self.row.is_last())
            .field("fields", &self.series.fields)
            .finish_non_exhaustive()
    }
}

/// What one handed chunk is charged to, released when the chunk is.
///
/// A producer that holds a value on behalf of a consumer has to hold the pool's reservation for it
/// too: the bytes are live, so the pool must not hand them to anybody else. The frame path keeps
/// them in the packer's handoff until the batch they back is flushed; a chunk handed over in
/// process keeps them here, so they are released exactly when the consumer drops the value they
/// account for - never earlier, which would let the pool over-commit, and never for longer, which
/// would hold memory for a chunk nobody has.
///
/// Two shapes of reservation make up that total. An owner reserves the charges its inputs carried,
/// the descriptor naming the identity, and the workspace it merged or reconstructed into - all of
/// them its own. A state export reserves one batch that several rows are cut from, so those rows
/// share it instead of each holding a copy: that is what [`SeriesCharge::shared`] is, and it is
/// released when the last chunk holding it is dropped. Dropping this value is the whole of what
/// makes either release correct - there is no call to forget - which is why the fields are named as
/// fields nothing reads: holding them is the entire job. The packer's handoff named its own
/// reservations the same way, for the same reason.
#[derive(Debug)]
pub struct SeriesCharge {
    /// The reservations this publication holds outright.
    _owned: Vec<MemoryReservation>,
    /// The reservations it shares with the other chunks cut from the same publication.
    _shared: Vec<Arc<MemoryReservation>>,
}

impl SeriesCharge {
    /// The charge of one owner publication.
    pub(crate) fn new(
        fragments: Vec<MemoryReservation>,
        descriptor: MemoryReservation,
        workspace: Option<MemoryReservation>,
    ) -> Self {
        let mut owned = fragments;
        owned.push(descriptor);
        owned.extend(workspace);
        Self {
            _owned: owned,
            _shared: Vec::new(),
        }
    }

    /// The charge of a publication that several chunks are cut from.
    ///
    /// Each of those chunks holds a clone of every reservation here, so the bytes stay charged for
    /// as long as any of them is alive and are released when the last one is dropped.
    pub(crate) fn shared(shared: Vec<Arc<MemoryReservation>>) -> Self {
        Self {
            _owned: Vec::new(),
            _shared: shared,
        }
    }
}

/// One chunk handed over in process, and what it is charged to.
///
/// The charge travels with the value rather than beside the stream, so a consumer cannot release a
/// chunk's reservation while it still holds the chunk, and cannot drop the chunk while keeping the
/// reservation either. A consumer that only reads the chunk and forgets it leaks nothing.
#[derive(Debug)]
pub struct HandedSeriesChunk {
    chunk: SeriesChunk,
    charge: Option<SeriesCharge>,
}

impl HandedSeriesChunk {
    /// A chunk nothing is charged to.
    ///
    /// A producer that holds no reservation of its own - a decode of scan output, whose read is
    /// charged where it happens - hands its values over like this.
    pub fn uncharged(chunk: SeriesChunk) -> Self {
        Self {
            chunk,
            charge: None,
        }
    }

    /// A chunk the producer reserved `charge` for, held until this value is dropped.
    pub fn charged(chunk: SeriesChunk, charge: SeriesCharge) -> Self {
        Self {
            chunk,
            charge: Some(charge),
        }
    }

    /// The chunk itself, borrowed for a check that keeps the charge in place.
    pub fn chunk(&self) -> &SeriesChunk {
        &self.chunk
    }

    /// The chunk and the charge it carries, both to be held as long as the consumer reads it.
    pub fn into_parts(self) -> (SeriesChunk, Option<SeriesCharge>) {
        (self.chunk, self.charge)
    }
}

/// One partition's output as internal series, not as an encoded frame.
///
/// The error is the boxed error the internal series streams already report, so a chunk stream
/// composes with [`SendableRawSeriesStream`] and
/// [`SendableEvaluatedSeriesStream`](common_query::series_frame::SendableEvaluatedSeriesStream)
/// without translating it.
///
/// [`SendableRawSeriesStream`]: store_api::region_engine::SendableRawSeriesStream
pub type SendableSeriesStream =
    Pin<Box<dyn Stream<Item = Result<HandedSeriesChunk, BoxedError>> + Send>>;

/// A native operator that can hand its output over in-process without encoding a frame.
///
/// Implementing this is a statement about what the operator *is*: it runs in the same process as
/// its consumer, and what it produces is an internal value rather than a message. A node that reads
/// or writes a frame as its contract - a scan answering another process, an output boundary - must
/// not implement it, and [`series_execution_plan`] is the one place a consumer asks.
///
/// [`ExecutionPlan::execute`] stays the whole answer for every consumer that is not native: an
/// implementor still publishes its frames, packed from what it hands over here, so a plan with a
/// foreign operator in the middle keeps working and is merely slower.
pub trait SeriesExecutionPlan: ExecutionPlan {
    /// This plan as [`Any`], so a plan reached through this trait can still be asked which
    /// concrete operator it is.
    ///
    /// The obvious spelling of that question - `downcast_ref::<dyn SeriesExecutionPlan>()` on a
    /// `&dyn Any` - cannot be written, and not because of anything this trait does:
    /// [`Any::downcast_ref`] requires `T: Sized` and compares the type id of a concrete type, so a
    /// trait object is not a target it accepts. The standard answer is the one here: the trait
    /// exposes its object as `dyn Any` and the caller names the concrete type it wants, which is
    /// sized by construction. This is the same shape [`ExecutionPlan`] uses for its own downcasts,
    /// and it is a method on the trait rather than a `where Self: Sized` helper because it has to
    /// be callable on the trait object - the caller is a consumer holding
    /// `&dyn SeriesExecutionPlan`, which is exactly the value that is not sized.
    fn as_any(&self) -> &dyn Any;

    /// One partition's output as internal series, not as a `RecordBatch`.
    ///
    /// The stream carries each chunk with what it is charged to
    /// ([`HandedSeriesChunk`]), so nothing a producer reserved is released behind the consumer's
    /// back: a consumer holds the reservation exactly as long as it holds the chunk.
    fn execute_series(
        &self,
        partition: usize,
        ctx: Arc<TaskContext>,
    ) -> DataFusionResult<SendableSeriesStream>;
}

/// `plan` as a native series operator, when it is one.
///
/// This is the guard a native consumer asks before it decides how to read its input, and it is
/// deliberately the list of operators this crate implements the trait on: a plan that is not in
/// the list is read as frames, so a consumer's fast path can never outrun the operators that
/// support it. It is the *only* place that decision is made - a consumer that asks this per input
/// has nothing to keep in step with it, and no plan is read in process because something else said
/// so.
///
/// The obvious spelling - erasing the plan and asking it for `dyn SeriesExecutionPlan` - cannot be
/// written, and not because of the DataFusion this crate builds against:
/// [`Any::downcast_ref`] wants `T: Sized` and compares the type id of a concrete type, so a trait
/// object can never be its target and the call would not compile anywhere. The check therefore
/// names the concrete implementors it accepts, one `downcast_ref` per operator, which is why the
/// trait exposes [`SeriesExecutionPlan::as_any`] and why the question below starts from `dyn Any`
/// rather than from `dyn ExecutionPlan`: this DataFusion's [`ExecutionPlan`] declares no `as_any`,
/// but it is [`Any`], so the view is one upcast away. The decision stays in this one function: a
/// new implementor is added here, and until it is, a consumer reads that plan as frames.
pub fn series_execution_plan(plan: &dyn ExecutionPlan) -> Option<&dyn SeriesExecutionPlan> {
    let plan: &dyn Any = plan;
    if let Some(source) = plan.downcast_ref::<RawSeriesSourceExec>() {
        return Some(source as &dyn SeriesExecutionPlan);
    }
    // A router is asked for every content it publishes: an owner's merged raw identity, the series
    // an owner settled on its grid, and the row of a group an owner exported are all values this
    // crate holds, so all three are read in process and none of them is left on a frame path.
    plan.downcast_ref::<SeriesRepartitionExec>()
        .filter(|router| router.hands_over_series())
        .map(|router| router as &dyn SeriesExecutionPlan)
}

/// `chunks` with the sequence check the frame path runs, applied to each chunk as it arrives.
///
/// A consumer that decodes a frame validates the sequence it receives - a chain that never closes,
/// a key that recurs after its final chunk, timestamps that go back across chunks, keys that go back
/// under key-then-time order - because the frame may have been written by another process.
/// [`pack_raw_series`](common_query::series_frame::pack_raw_series) and
/// [`unpack_raw_series`](common_query::series_frame::unpack_raw_series) run that check on both ends,
/// and the evaluated packer and unpacker run its evaluated counterpart. A consumer handed the
/// chunks themselves has no frame to decode and no other process to distrust, but the producer can
/// still be wrong: a router publishes what its owners merged, and a second publication of one
/// identity would be as wrong in process as it is across a frame. So the same validators run here,
/// over the same sequence, and the shortcut refuses what the frame would have refused.
///
/// A state publication is the one content with no such check to run, on either path: the frame
/// consumer's state stream decodes each exported row and hands it on, so there is nothing it would
/// refuse that the handover must refuse too. What both paths do enforce is enforced where the row is
/// decoded - in the producer's handover stream - and this stream passes state rows through.
///
/// `raw_ordering` is the ordering the producer declares for its raw frames - the one a decoding
/// consumer would read out of the frame schema. It is not inferred from the chunks.
pub fn checked_series_stream(
    chunks: SendableSeriesStream,
    raw_ordering: SeriesOrdering,
) -> SendableSeriesStream {
    Box::pin(CheckedSeries {
        chunks: Some(chunks),
        raw: SequenceValidator::new(raw_ordering),
        evaluated: EvaluatedSequenceValidator::new(),
    })
}

/// The handover stream, with the frame path's sequence check in front of the consumer.
struct CheckedSeries {
    /// The producer's stream, or `None` once this one ended.
    chunks: Option<SendableSeriesStream>,
    /// The check a raw consumer of this input would run, at this input's declared ordering.
    raw: SequenceValidator,
    /// The check an evaluated consumer of this input would run.
    evaluated: EvaluatedSequenceValidator,
}

impl Stream for CheckedSeries {
    type Item = Result<HandedSeriesChunk, BoxedError>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        let Some(chunks) = this.chunks.as_mut() else {
            return Poll::Ready(None);
        };
        let handed = match chunks.as_mut().poll_next(cx) {
            Poll::Pending => return Poll::Pending,
            Poll::Ready(Some(Err(error))) => {
                this.chunks = None;
                return Poll::Ready(Some(Err(error)));
            }
            // The producer ended, so the check that only an end can make is made here: a chain
            // still open when the stream stopped is the failure the frame path reports as well.
            Poll::Ready(None) => {
                this.chunks = None;
                let checked = this.raw.finish().and_then(|()| this.evaluated.finish());
                return Poll::Ready(checked.err().map(|error| Err(checked_error(error))));
            }
            Poll::Ready(Some(Ok(handed))) => handed,
        };
        let checked = match handed.chunk() {
            SeriesChunk::Raw(chunk) => this.raw.validate(chunk),
            SeriesChunk::Evaluated(chunk) => this.evaluated.validate(&chunk.series, chunk.is_last),
            // A state publication has no chain of chunks to check, on either path: the frame
            // consumer's state stream decodes each exported row and hands it on, running no
            // cross-row validator at all, so there is nothing it would refuse that this would
            // accept. What both paths do enforce - the declaration is this revision's, the batch
            // carries the declared schema, and the declared ordering is contiguous - is enforced
            // where the row is decoded, in the producer's handover stream.
            SeriesChunk::State(_) => Ok(()),
        };
        match checked {
            Ok(()) => Poll::Ready(Some(Ok(handed))),
            Err(error) => {
                // Nothing more is read from a producer whose sequence is broken: the frame path
                // drops its decoder at the same point.
                this.chunks = None;
                Poll::Ready(Some(Err(checked_error(error))))
            }
        }
    }
}

/// The error a broken chunk sequence is reported as, in the handover's own error type.
fn checked_error(error: datafusion::error::DataFusionError) -> BoxedError {
    BoxedError::new(common_query::error::Error::from(error))
}

#[cfg(test)]
mod tests;
