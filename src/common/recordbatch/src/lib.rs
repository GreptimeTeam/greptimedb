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

#![feature(never_type)]

pub mod adapter;
pub mod cursor;
pub mod error;
pub mod filter;
pub mod recordbatch;
pub mod util;

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use adapter::RecordBatchMetrics;
use arc_swap::ArcSwapOption;
use common_base::readable_size::ReadableSize;
use common_error::ext::BoxedError;
use common_memory_manager::{
    MemoryGuard, MemoryManager, MemoryMetrics, OnExhaustedPolicy, PermitGranularity,
};
use common_telemetry::tracing::Span;
pub use datafusion::physical_plan::SendableRecordBatchStream as DfSendableRecordBatchStream;
use datatypes::arrow::array::{ArrayRef, AsArray, StringBuilder};
use datatypes::arrow::compute::SortOptions;
pub use datatypes::arrow::record_batch::RecordBatch as DfRecordBatch;
use datatypes::arrow::util::pretty;
use datatypes::prelude::{ConcreteDataType, VectorRef};
use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
use datatypes::types::{JsonFormat, jsonb_to_string};
use error::Result;
use futures::task::{Context, Poll};
use futures::{Stream, TryStreamExt};
pub use recordbatch::RecordBatch;
use snafu::{IntoError, ResultExt, ensure};

use crate::error::NewDfRecordBatchSnafu;

pub trait RecordBatchStream: Stream<Item = Result<RecordBatch>> {
    fn name(&self) -> &str {
        "RecordBatchStream"
    }

    fn schema(&self) -> SchemaRef;

    fn output_ordering(&self) -> Option<&[OrderOption]>;

    fn metrics(&self) -> Option<RecordBatchMetrics>;
}

pub type SendableRecordBatchStream = Pin<Box<dyn RecordBatchStream + Send>>;

#[derive(Debug, Clone, PartialEq, Eq)]
pub struct OrderOption {
    pub name: String,
    pub options: SortOptions,
}

/// A wrapper that maps a [RecordBatchStream] to a new [RecordBatchStream] by applying a function to each [RecordBatch].
///
/// The mapper function is applied to each [RecordBatch] in the stream.
/// The schema of the new [RecordBatchStream] is the same as the schema of the inner [RecordBatchStream] after applying the schema mapper function.
/// The output ordering of the new [RecordBatchStream] is the same as the output ordering of the inner [RecordBatchStream].
/// The metrics of the new [RecordBatchStream] is the same as the metrics of the inner [RecordBatchStream] if it is not `None`.
pub struct SendableRecordBatchMapper {
    inner: SendableRecordBatchStream,
    /// The mapper function is applied to each [RecordBatch] in the stream.
    /// The original schema and the mapped schema are passed to the mapper function.
    mapper: fn(RecordBatch, &SchemaRef, &SchemaRef) -> Result<RecordBatch>,
    /// The schema of the new [RecordBatchStream] is the same as the schema of the inner [RecordBatchStream] after applying the schema mapper function.
    schema: SchemaRef,
    /// Whether the mapper function is applied to each [RecordBatch] in the stream.
    apply_mapper: bool,
}

/// Maps the json type to string in the batch.
///
/// The json type is mapped to string by converting the json value to string.
/// The batch is updated to have the same number of columns as the original batch,
/// but with the json type mapped to string.
pub fn map_json_type_to_string(
    batch: RecordBatch,
    original_schema: &SchemaRef,
    mapped_schema: &SchemaRef,
) -> Result<RecordBatch> {
    let mut vectors = Vec::with_capacity(original_schema.column_schemas().len());
    for (vector, schema) in batch.columns().iter().zip(original_schema.column_schemas()) {
        if let ConcreteDataType::Json(j) = &schema.data_type {
            if matches!(&j.format, JsonFormat::Jsonb) {
                let mut string_vector_builder = StringBuilder::new();
                let binary_vector = vector.as_binary::<i32>();
                for value in binary_vector.iter() {
                    let Some(value) = value else {
                        string_vector_builder.append_null();
                        continue;
                    };
                    let string_value =
                        jsonb_to_string(value).with_context(|_| error::CastVectorSnafu {
                            from_type: schema.data_type.clone(),
                            to_type: ConcreteDataType::string_datatype(),
                        })?;
                    string_vector_builder.append_value(string_value);
                }

                let string_vector = string_vector_builder.finish();
                vectors.push(Arc::new(string_vector) as ArrayRef);
            } else {
                vectors.push(vector.clone());
            }
        } else {
            vectors.push(vector.clone());
        }
    }

    let record_batch = datatypes::arrow::record_batch::RecordBatch::try_new(
        mapped_schema.arrow_schema().clone(),
        vectors,
    )
    .context(NewDfRecordBatchSnafu)?;
    Ok(RecordBatch::from_df_record_batch(
        mapped_schema.clone(),
        record_batch,
    ))
}

/// Maps the json type to string in the schema.
///
/// The json type is mapped to string by converting the json value to string.
/// The schema is updated to have the same number of columns as the original schema,
/// but with the json type mapped to string.
///
/// Returns the new schema and whether the schema needs to be mapped to string.
pub fn map_json_type_to_string_schema(schema: SchemaRef) -> (SchemaRef, bool) {
    let mut new_columns = Vec::with_capacity(schema.column_schemas().len());
    let mut apply_mapper = false;
    for column in schema.column_schemas() {
        if matches!(column.data_type, ConcreteDataType::Json(_)) {
            new_columns.push(ColumnSchema::new(
                column.name.clone(),
                ConcreteDataType::string_datatype(),
                column.is_nullable(),
            ));
            apply_mapper = true;
        } else {
            new_columns.push(column.clone());
        }
    }
    (Arc::new(Schema::new(new_columns)), apply_mapper)
}

impl SendableRecordBatchMapper {
    /// Creates a new [SendableRecordBatchMapper] with the given inner [RecordBatchStream], mapper function, and schema mapper function.
    pub fn new(
        inner: SendableRecordBatchStream,
        mapper: fn(RecordBatch, &SchemaRef, &SchemaRef) -> Result<RecordBatch>,
        schema_mapper: fn(SchemaRef) -> (SchemaRef, bool),
    ) -> Self {
        let (mapped_schema, apply_mapper) = schema_mapper(inner.schema());
        Self {
            inner,
            mapper,
            schema: mapped_schema,
            apply_mapper,
        }
    }
}

impl RecordBatchStream for SendableRecordBatchMapper {
    fn name(&self) -> &str {
        "SendableRecordBatchMapper"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_ordering(&self) -> Option<&[OrderOption]> {
        self.inner.output_ordering()
    }

    fn metrics(&self) -> Option<RecordBatchMetrics> {
        self.inner.metrics()
    }
}

impl Stream for SendableRecordBatchMapper {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.apply_mapper {
            Pin::new(&mut self.inner).poll_next(cx).map(|opt| {
                opt.map(|result| {
                    result
                        .and_then(|batch| (self.mapper)(batch, &self.inner.schema(), &self.schema))
                })
            })
        } else {
            Pin::new(&mut self.inner).poll_next(cx)
        }
    }
}

/// EmptyRecordBatchStream can be used to create a RecordBatchStream
/// that will produce no results
pub struct EmptyRecordBatchStream {
    /// Schema wrapped by Arc
    schema: SchemaRef,
}

impl EmptyRecordBatchStream {
    /// Create an empty RecordBatchStream
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

impl RecordBatchStream for EmptyRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_ordering(&self) -> Option<&[OrderOption]> {
        None
    }

    fn metrics(&self) -> Option<RecordBatchMetrics> {
        None
    }
}

impl Stream for EmptyRecordBatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

#[derive(Debug, PartialEq)]
pub struct RecordBatches {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl RecordBatches {
    pub fn try_from_columns<I: IntoIterator<Item = VectorRef>>(
        schema: SchemaRef,
        columns: I,
    ) -> Result<Self> {
        let batches = vec![RecordBatch::new(schema.clone(), columns)?];
        Ok(Self { schema, batches })
    }

    pub async fn try_collect(stream: SendableRecordBatchStream) -> Result<Self> {
        let schema = stream.schema();
        let batches = stream.try_collect::<Vec<_>>().await?;
        Ok(Self { schema, batches })
    }

    #[inline]
    pub fn empty() -> Self {
        Self {
            schema: Arc::new(Schema::new(vec![])),
            batches: vec![],
        }
    }

    pub fn iter(&self) -> impl Iterator<Item = &RecordBatch> {
        self.batches.iter()
    }

    pub fn pretty_print(&self) -> Result<String> {
        let df_batches = &self
            .iter()
            .map(|x| x.df_record_batch().clone())
            .collect::<Vec<_>>();
        let result = pretty::pretty_format_batches(df_batches).context(error::FormatSnafu)?;

        Ok(result.to_string())
    }

    pub fn try_new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Result<Self> {
        for batch in &batches {
            ensure!(
                batch.schema == schema,
                error::CreateRecordBatchesSnafu {
                    reason: format!(
                        "expect RecordBatch schema equals {:?}, actual: {:?}",
                        schema, batch.schema
                    )
                }
            )
        }
        Ok(Self { schema, batches })
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn take(self) -> Vec<RecordBatch> {
        self.batches
    }

    pub fn as_stream(&self) -> SendableRecordBatchStream {
        Box::pin(SimpleRecordBatchStream {
            inner: RecordBatches {
                schema: self.schema(),
                batches: self.batches.clone(),
            },
            index: 0,
        })
    }
}

impl IntoIterator for RecordBatches {
    type Item = RecordBatch;
    type IntoIter = std::vec::IntoIter<Self::Item>;

    fn into_iter(self) -> Self::IntoIter {
        self.batches.into_iter()
    }
}

pub struct SimpleRecordBatchStream {
    inner: RecordBatches,
    index: usize,
}

impl RecordBatchStream for SimpleRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn output_ordering(&self) -> Option<&[OrderOption]> {
        None
    }

    fn metrics(&self) -> Option<RecordBatchMetrics> {
        None
    }
}

impl Stream for SimpleRecordBatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(if self.index < self.inner.batches.len() {
            let batch = self.inner.batches[self.index].clone();
            self.index += 1;
            Some(Ok(batch))
        } else {
            None
        })
    }
}

/// Adapt a [Stream] of [RecordBatch] to a [RecordBatchStream].
pub struct RecordBatchStreamWrapper<S> {
    pub schema: SchemaRef,
    pub stream: S,
    pub output_ordering: Option<Vec<OrderOption>>,
    pub metrics: Arc<ArcSwapOption<RecordBatchMetrics>>,
    pub span: Span,
}

impl<S> RecordBatchStreamWrapper<S> {
    /// Creates a [RecordBatchStreamWrapper] without output ordering requirement.
    pub fn new(schema: SchemaRef, stream: S) -> RecordBatchStreamWrapper<S> {
        RecordBatchStreamWrapper {
            schema,
            stream,
            output_ordering: None,
            metrics: Default::default(),
            span: Span::current(),
        }
    }
}

impl<S: Stream<Item = Result<RecordBatch>> + Unpin> RecordBatchStream
    for RecordBatchStreamWrapper<S>
{
    fn name(&self) -> &str {
        "RecordBatchStreamWrapper"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_ordering(&self) -> Option<&[OrderOption]> {
        self.output_ordering.as_deref()
    }

    fn metrics(&self) -> Option<RecordBatchMetrics> {
        self.metrics.load().as_ref().map(|s| s.as_ref().clone())
    }
}

impl<S: Stream<Item = Result<RecordBatch>> + Unpin> Stream for RecordBatchStreamWrapper<S> {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let _entered = self.span.clone().entered();
        Pin::new(&mut self.stream).poll_next(ctx)
    }
}

/// Memory tracker for RecordBatch streams. Clone to share the same limit across queries.
///
/// Each stream acquires quota independently from this tracker.
#[derive(Clone)]
pub struct QueryMemoryTracker {
    manager: MemoryManager<CallbackMemoryMetrics>,
    metrics: CallbackMemoryMetrics,
    on_exhausted_policy: OnExhaustedPolicy,
}

impl fmt::Debug for QueryMemoryTracker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryMemoryTracker")
            .field("current", &self.current())
            .field("limit", &self.limit())
            .field("on_exhausted_policy", &self.on_exhausted_policy)
            .field("on_update", &self.metrics.has_on_update())
            .field("on_exhausted", &self.metrics.has_on_exhausted())
            .field("on_rejected", &self.metrics.has_on_rejected())
            .finish()
    }
}

impl QueryMemoryTracker {
    /// Create a builder for a query memory tracker.
    pub fn builder(
        limit: usize,
        on_exhausted_policy: OnExhaustedPolicy,
    ) -> QueryMemoryTrackerBuilder {
        QueryMemoryTrackerBuilder {
            limit,
            on_exhausted_policy,
            on_update: None,
            on_exhausted: None,
            on_reject: None,
        }
    }

    fn new_stream_tracker(&self) -> StreamMemoryTracker {
        StreamMemoryTracker {
            tracker: self.clone(),
            guard: self.manager.try_acquire(0).unwrap(),
            tracked_bytes: 0,
        }
    }
    /// Get the current memory usage in bytes.
    pub fn current(&self) -> usize {
        self.manager.used_bytes() as usize
    }

    fn limit(&self) -> usize {
        self.manager.limit_bytes() as usize
    }

    fn reject_error(
        &self,
        current: usize,
        additional: usize,
        stream_tracked: usize,
    ) -> error::Error {
        let limit = self.limit();
        let msg = format!(
            "{} requested, {} used globally ({}%), {} used by this stream, hard limit: {}",
            ReadableSize(additional as u64),
            ReadableSize(current as u64),
            (current * 100).checked_div(limit).unwrap_or(0),
            ReadableSize(stream_tracked as u64),
            ReadableSize(limit as u64)
        );
        error::ExceedMemoryLimitSnafu { msg }.build()
    }

    fn inc_rejected(&self) {
        self.metrics.inc_rejected();
    }
}

/// Builder for constructing a [`QueryMemoryTracker`] with optional callbacks.
pub struct QueryMemoryTrackerBuilder {
    limit: usize,
    on_exhausted_policy: OnExhaustedPolicy,
    on_update: Option<UpdateCallback>,
    on_exhausted: Option<UnitCallback>,
    on_reject: Option<RejectCallback>,
}

impl QueryMemoryTrackerBuilder {
    /// Set a callback to be called whenever the usage changes successfully.
    /// The callback receives the new total usage in bytes.
    ///
    /// # Note
    /// The callback is called after both successful `track()` and stream drop.
    /// Usage is exact in unlimited mode and 1KB-aligned in limited mode.
    pub fn on_update<F>(mut self, on_update: F) -> Self
    where
        F: Fn(usize) + Send + Sync + 'static,
    {
        self.on_update = Some(Arc::new(on_update));
        self
    }

    /// Set a callback to be called when memory is unavailable for immediate acquisition.
    ///
    /// # Note
    /// This is called when the non-blocking allocation fast path fails.
    /// Requests using `OnExhaustedPolicy::Wait` may still succeed after waiting.
    /// It is never called when `limit == 0` (unlimited mode).
    pub fn on_exhausted<F>(mut self, on_exhausted: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.on_exhausted = Some(Arc::new(on_exhausted));
        self
    }

    /// Set a callback to be called when the request ultimately fails due to memory pressure.
    pub fn on_reject<F>(mut self, on_reject: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.on_reject = Some(Arc::new(on_reject));
        self
    }

    /// Build a [`QueryMemoryTracker`] from this builder.
    pub fn build(self) -> QueryMemoryTracker {
        let metrics = CallbackMemoryMetrics::new(self.on_update, self.on_exhausted, self.on_reject);
        let manager = MemoryManager::with_granularity(
            self.limit as u64,
            PermitGranularity::Kilobyte,
            metrics.clone(),
        );

        QueryMemoryTracker {
            manager,
            metrics,
            on_exhausted_policy: self.on_exhausted_policy,
        }
    }
}

struct StreamMemoryTracker {
    tracker: QueryMemoryTracker,
    guard: MemoryGuard<CallbackMemoryMetrics>,
    tracked_bytes: usize,
}

type MemoryAcquireResult = std::result::Result<(), common_memory_manager::Error>;

impl StreamMemoryTracker {
    fn inc_rejected(&self) {
        self.tracker.inc_rejected();
    }

    fn try_track(&mut self, additional: usize) -> Result<()> {
        if self.guard.try_acquire_additional(additional as u64) {
            self.tracked_bytes = self.tracked_bytes.saturating_add(additional);
            Ok(())
        } else {
            Err(self.reject_error(additional))
        }
    }

    async fn track_with_policy(mut self, additional: usize) -> (Self, MemoryAcquireResult) {
        let result = self
            .guard
            .acquire_additional_with_policy(additional as u64, self.tracker.on_exhausted_policy)
            .await;
        if result.is_ok() {
            self.tracked_bytes = self.tracked_bytes.saturating_add(additional);
        }
        (self, result)
    }

    fn reject_error(&self, additional: usize) -> error::Error {
        let current = self.tracker.current();
        self.tracker
            .reject_error(current, additional, self.tracked_bytes)
    }

    fn wait_error(&self, additional: usize, source: common_memory_manager::Error) -> error::Error {
        match source {
            common_memory_manager::Error::MemoryLimitExceeded { .. } => {
                self.reject_error(additional)
            }
            common_memory_manager::Error::MemoryAcquireTimeout { waited, .. } => {
                let current = self.tracker.current();
                let limit = self.tracker.limit();
                let msg = format!(
                    "timed out waiting {:?} for {}, {} used globally ({}%), {} used by this stream, hard limit: {}",
                    waited,
                    ReadableSize(additional as u64),
                    ReadableSize(current as u64),
                    (current * 100).checked_div(limit).unwrap_or(0),
                    ReadableSize(self.tracked_bytes as u64),
                    ReadableSize(limit as u64)
                );
                error::ExceedMemoryLimitSnafu { msg }.build()
            }
            error => error::ExternalSnafu.into_error(BoxedError::new(error)),
        }
    }
}

type PendingTrackFuture = Pin<
    Box<dyn Future<Output = (StreamMemoryTracker, RecordBatch, usize, MemoryAcquireResult)> + Send>,
>;

#[derive(Clone)]
struct CallbackMemoryMetrics {
    inner: Arc<CallbackMemoryMetricsInner>,
}

type UpdateCallback = Arc<dyn Fn(usize) + Send + Sync>;
type UnitCallback = Arc<dyn Fn() + Send + Sync>;
type RejectCallback = UnitCallback;

struct CallbackMemoryMetricsInner {
    on_update: Option<UpdateCallback>,
    on_exhausted: Option<UnitCallback>,
    on_reject: Option<RejectCallback>,
}

impl CallbackMemoryMetrics {
    fn new(
        on_update: Option<UpdateCallback>,
        on_exhausted: Option<UnitCallback>,
        on_reject: Option<RejectCallback>,
    ) -> Self {
        Self {
            inner: Arc::new(CallbackMemoryMetricsInner {
                on_update,
                on_exhausted,
                on_reject,
            }),
        }
    }

    fn has_on_update(&self) -> bool {
        self.inner.on_update.is_some()
    }

    fn has_on_exhausted(&self) -> bool {
        self.inner.on_exhausted.is_some()
    }

    fn has_on_rejected(&self) -> bool {
        self.inner.on_reject.is_some()
    }

    fn inc_rejected(&self) {
        if let Some(callback) = &self.inner.on_reject {
            callback();
        }
    }
}

impl MemoryMetrics for CallbackMemoryMetrics {
    fn set_limit(&self, _: i64) {}

    fn set_in_use(&self, bytes: i64) {
        if let Some(callback) = &self.inner.on_update {
            callback(bytes.max(0) as usize);
        }
    }

    fn inc_exhausted(&self, _: &str) {
        if let Some(callback) = &self.inner.on_exhausted {
            callback();
        }
    }
}

/// A wrapper stream that tracks memory usage of RecordBatches.
pub struct MemoryTrackedStream {
    inner: SendableRecordBatchStream,
    tracker: Option<StreamMemoryTracker>,
    // Waiting stores a batch that has already been pulled from the inner stream but has not yet
    // acquired additional quota. This keeps `poll_next()` non-blocking and allows bounded waits,
    // at the cost of temporarily holding one untracked batch per blocked stream in memory.
    waiting: Option<PendingTrackFuture>,
}

impl MemoryTrackedStream {
    pub fn new(inner: SendableRecordBatchStream, tracker: QueryMemoryTracker) -> Self {
        Self {
            inner,
            tracker: Some(tracker.new_stream_tracker()),
            waiting: None,
        }
    }

    fn ready_tracker_mut(&mut self) -> &mut StreamMemoryTracker {
        debug_assert!(
            self.waiting.is_none(),
            "a ready tracker must not coexist with a waiting future"
        );
        self.tracker.as_mut().unwrap()
    }

    fn enter_waiting(&mut self, batch: RecordBatch, additional: usize) {
        debug_assert!(
            self.waiting.is_none(),
            "enter_waiting should only be called from the ready state"
        );
        debug_assert!(
            self.tracker.is_some(),
            "enter_waiting requires a tracker in the ready state"
        );
        let tracker = self.tracker.take().unwrap();
        self.waiting = Some(Self::start_waiting(tracker, batch, additional));
    }

    fn start_waiting(
        tracker: StreamMemoryTracker,
        batch: RecordBatch,
        additional: usize,
    ) -> PendingTrackFuture {
        Box::pin(async move {
            let (tracker, result) = tracker.track_with_policy(additional).await;
            (tracker, batch, additional, result)
        })
    }

    fn poll_waiting(&mut self, cx: &mut Context<'_>) -> Poll<Option<Result<RecordBatch>>> {
        let future = self.waiting.as_mut().unwrap();
        match future.as_mut().poll(cx) {
            Poll::Ready((tracker, batch, additional, result)) => {
                let output = match result {
                    Ok(()) => Ok(batch),
                    Err(error) => {
                        tracker.inc_rejected();
                        Err(tracker.wait_error(additional, error))
                    }
                };
                self.waiting = None;
                self.tracker = Some(tracker);
                Poll::Ready(Some(output))
            }
            Poll::Pending => Poll::Pending,
        }
    }

    fn poll_batch(
        &mut self,
        batch: RecordBatch,
        cx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        let additional = batch.buffer_memory_size();
        let tracker = self.ready_tracker_mut();

        if let Err(error) = tracker.try_track(additional) {
            match tracker.tracker.on_exhausted_policy {
                OnExhaustedPolicy::Fail => {
                    tracker.inc_rejected();
                    return Poll::Ready(Some(Err(error)));
                }
                // `Wait` is a deliberate tradeoff: the batch has already been materialized, so we
                // keep it in memory while waiting for quota instead of failing immediately. Under
                // contention, real memory usage can therefore exceed `scan_memory_limit` by up to
                // one buffered batch per blocked stream.
                OnExhaustedPolicy::Wait { .. } => {
                    self.enter_waiting(batch, additional);
                    return self.poll_waiting(cx);
                }
            }
        }

        Poll::Ready(Some(Ok(batch)))
    }
}

impl Stream for MemoryTrackedStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.waiting.is_some() {
            return self.poll_waiting(cx);
        }

        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => self.poll_batch(batch, cx),
            Poll::Ready(Some(Err(error))) => Poll::Ready(Some(Err(error))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl RecordBatchStream for MemoryTrackedStream {
    fn schema(&self) -> SchemaRef {
        self.inner.schema()
    }

    fn output_ordering(&self) -> Option<&[OrderOption]> {
        self.inner.output_ordering()
    }

    fn metrics(&self) -> Option<RecordBatchMetrics> {
        self.inner.metrics()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;
    use std::sync::atomic::{AtomicUsize, Ordering};
    use std::time::Duration;

    use common_memory_manager::{OnExhaustedPolicy, PermitGranularity};
    use datatypes::prelude::{ConcreteDataType, VectorRef};
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::{BooleanVector, Int32Vector, StringVector};
    use futures::StreamExt;
    use tokio::time::{sleep, timeout};

    use super::*;

    fn large_string_batch(bytes: usize) -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "payload",
            ConcreteDataType::string_datatype(),
            false,
        )]));
        let payload = "x".repeat(bytes);
        let vector: VectorRef = Arc::new(StringVector::from(vec![payload]));
        RecordBatch::new(schema, vec![vector]).unwrap()
    }

    fn aligned_tracked_bytes(bytes: usize) -> usize {
        PermitGranularity::Kilobyte
            .permits_to_bytes(PermitGranularity::Kilobyte.bytes_to_permits(bytes as u64))
            as usize
    }

    #[test]
    fn test_recordbatches_try_from_columns() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "a",
            ConcreteDataType::int32_datatype(),
            false,
        )]));
        let result = RecordBatches::try_from_columns(
            schema.clone(),
            vec![Arc::new(StringVector::from(vec!["hello", "world"])) as _],
        );
        assert!(result.is_err());

        let v: VectorRef = Arc::new(Int32Vector::from_slice([1, 2]));
        let expected = vec![RecordBatch::new(schema.clone(), vec![v.clone()]).unwrap()];
        let r = RecordBatches::try_from_columns(schema, vec![v]).unwrap();
        assert_eq!(r.take(), expected);
    }

    #[test]
    fn test_recordbatches_try_new() {
        let column_a = ColumnSchema::new("a", ConcreteDataType::int32_datatype(), false);
        let column_b = ColumnSchema::new("b", ConcreteDataType::string_datatype(), false);
        let column_c = ColumnSchema::new("c", ConcreteDataType::boolean_datatype(), false);

        let va: VectorRef = Arc::new(Int32Vector::from_slice([1, 2]));
        let vb: VectorRef = Arc::new(StringVector::from(vec!["hello", "world"]));
        let vc: VectorRef = Arc::new(BooleanVector::from(vec![true, false]));

        let schema1 = Arc::new(Schema::new(vec![column_a.clone(), column_b]));
        let batch1 = RecordBatch::new(schema1.clone(), vec![va.clone(), vb]).unwrap();

        let schema2 = Arc::new(Schema::new(vec![column_a, column_c]));
        let batch2 = RecordBatch::new(schema2.clone(), vec![va, vc]).unwrap();

        let result = RecordBatches::try_new(schema1.clone(), vec![batch1.clone(), batch2]);
        assert!(result.is_err());
        assert_eq!(
            result.unwrap_err().to_string(),
            format!(
                "Failed to create RecordBatches, reason: expect RecordBatch schema equals {schema1:?}, actual: {schema2:?}",
            )
        );

        let batches = RecordBatches::try_new(schema1.clone(), vec![batch1.clone()]).unwrap();
        let expected = "\
+---+-------+
| a | b     |
+---+-------+
| 1 | hello |
| 2 | world |
+---+-------+";
        assert_eq!(batches.pretty_print().unwrap(), expected);

        assert_eq!(schema1, batches.schema());
        assert_eq!(vec![batch1], batches.take());
    }

    #[tokio::test]
    async fn test_simple_recordbatch_stream() {
        let column_a = ColumnSchema::new("a", ConcreteDataType::int32_datatype(), false);
        let column_b = ColumnSchema::new("b", ConcreteDataType::string_datatype(), false);
        let schema = Arc::new(Schema::new(vec![column_a, column_b]));

        let va1: VectorRef = Arc::new(Int32Vector::from_slice([1, 2]));
        let vb1: VectorRef = Arc::new(StringVector::from(vec!["a", "b"]));
        let batch1 = RecordBatch::new(schema.clone(), vec![va1, vb1]).unwrap();

        let va2: VectorRef = Arc::new(Int32Vector::from_slice([3, 4, 5]));
        let vb2: VectorRef = Arc::new(StringVector::from(vec!["c", "d", "e"]));
        let batch2 = RecordBatch::new(schema.clone(), vec![va2, vb2]).unwrap();

        let recordbatches =
            RecordBatches::try_new(schema.clone(), vec![batch1.clone(), batch2.clone()]).unwrap();
        let stream = recordbatches.as_stream();
        let collected = util::collect(stream).await.unwrap();
        assert_eq!(collected.len(), 2);
        assert_eq!(collected[0], batch1);
        assert_eq!(collected[1], batch2);
    }

    const MB: usize = 1024 * 1024;

    #[test]
    fn test_query_memory_tracker_basic() {
        let tracker =
            Arc::new(QueryMemoryTracker::builder(10 * MB, OnExhaustedPolicy::Fail).build());

        let mut stream1 = tracker.new_stream_tracker();
        assert!(stream1.try_track(5 * MB).is_ok());
        assert_eq!(tracker.current(), 5 * MB);

        let mut stream2 = tracker.new_stream_tracker();
        assert!(stream2.try_track(4 * MB).is_ok());
        assert_eq!(tracker.current(), 9 * MB);

        drop(stream1);
        drop(stream2);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn test_query_memory_tracker_shared_global_limit() {
        let tracker =
            Arc::new(QueryMemoryTracker::builder(10 * MB, OnExhaustedPolicy::Fail).build());
        let mut stream1 = tracker.new_stream_tracker();
        let mut stream2 = tracker.new_stream_tracker();

        assert!(stream1.try_track(3 * MB).is_ok());
        assert_eq!(tracker.current(), 3 * MB);
        assert!(stream2.try_track(6 * MB).is_ok());
        assert_eq!(tracker.current(), 9 * MB);

        let err = stream2.try_track(2 * MB).unwrap_err();
        let err_msg = err.to_string();
        assert!(err_msg.contains("6.0MiB used by this stream"));
        assert!(err_msg.contains("9.0MiB used globally (90%)"));
        assert!(err_msg.contains("hard limit: 10.0MiB"));
        assert_eq!(tracker.current(), 9 * MB);

        drop(stream1);
        assert_eq!(tracker.current(), 6 * MB);
        drop(stream2);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn test_query_memory_tracker_hard_limit() {
        let tracker =
            Arc::new(QueryMemoryTracker::builder(10 * MB, OnExhaustedPolicy::Fail).build());
        let mut stream = tracker.new_stream_tracker();

        assert!(stream.try_track(9 * MB).is_ok());
        assert_eq!(tracker.current(), 9 * MB);

        assert!(stream.try_track(2 * MB).is_err());
        assert_eq!(tracker.current(), 9 * MB);

        assert!(stream.try_track(MB).is_ok());
        assert_eq!(tracker.current(), 10 * MB);

        assert!(stream.try_track(MB).is_err());
        assert_eq!(tracker.current(), 10 * MB);

        drop(stream);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn test_query_memory_tracker_unlimited() {
        let tracker = Arc::new(QueryMemoryTracker::builder(0, OnExhaustedPolicy::Fail).build());
        let mut stream = tracker.new_stream_tracker();

        assert!(stream.try_track(10 * MB).is_ok());
        assert_eq!(tracker.current(), 10 * MB);
        drop(stream);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn test_query_memory_tracker_rounds_to_kilobytes() {
        let tracker =
            Arc::new(QueryMemoryTracker::builder(10 * MB, OnExhaustedPolicy::Fail).build());
        let mut stream = tracker.new_stream_tracker();

        assert!(stream.try_track(1_537).is_ok());
        assert_eq!(tracker.current(), 2 * 1024);

        drop(stream);
        assert_eq!(tracker.current(), 0);
    }

    #[tokio::test]
    async fn test_memory_tracked_stream_waits_for_capacity() {
        let exhausted = Arc::new(AtomicUsize::new(0));
        let rejected = Arc::new(AtomicUsize::new(0));
        let exhausted_counter = exhausted.clone();
        let rejected_counter = rejected.clone();
        let tracker = QueryMemoryTracker::builder(
            MB,
            OnExhaustedPolicy::Wait {
                timeout: Duration::from_millis(200),
            },
        )
        .on_exhausted(move || {
            exhausted_counter.fetch_add(1, Ordering::Relaxed);
        })
        .on_reject(move || {
            rejected_counter.fetch_add(1, Ordering::Relaxed);
        })
        .build();
        let batch = large_string_batch(700 * 1024);
        let expected_bytes = aligned_tracked_bytes(batch.buffer_memory_size());

        let mut stream1 = MemoryTrackedStream::new(
            RecordBatches::try_new(batch.schema.clone(), vec![batch.clone()])
                .unwrap()
                .as_stream(),
            tracker.clone(),
        );
        let first = stream1.next().await.unwrap().unwrap();
        assert_eq!(first.num_rows(), 1);
        assert_eq!(tracker.current(), expected_bytes);

        let stream2 = MemoryTrackedStream::new(
            RecordBatches::try_new(batch.schema.clone(), vec![batch])
                .unwrap()
                .as_stream(),
            tracker.clone(),
        );
        let waiter = tokio::spawn(async move {
            let mut stream2 = stream2;
            stream2.next().await.unwrap()
        });

        sleep(Duration::from_millis(50)).await;
        assert!(!waiter.is_finished());

        drop(stream1);
        let second = waiter.await.unwrap().unwrap();
        assert_eq!(second.num_rows(), 1);
        assert_eq!(exhausted.load(Ordering::Relaxed), 1);
        assert_eq!(rejected.load(Ordering::Relaxed), 0);
    }

    #[tokio::test]
    async fn test_memory_tracked_stream_wait_times_out() {
        let exhausted = Arc::new(AtomicUsize::new(0));
        let rejected = Arc::new(AtomicUsize::new(0));
        let exhausted_counter = exhausted.clone();
        let rejected_counter = rejected.clone();
        let tracker = QueryMemoryTracker::builder(
            MB,
            OnExhaustedPolicy::Wait {
                timeout: Duration::from_millis(50),
            },
        )
        .on_exhausted(move || {
            exhausted_counter.fetch_add(1, Ordering::Relaxed);
        })
        .on_reject(move || {
            rejected_counter.fetch_add(1, Ordering::Relaxed);
        })
        .build();
        let batch = large_string_batch(700 * 1024);

        let mut stream1 = MemoryTrackedStream::new(
            RecordBatches::try_new(batch.schema.clone(), vec![batch.clone()])
                .unwrap()
                .as_stream(),
            tracker.clone(),
        );
        let first = stream1.next().await.unwrap().unwrap();
        assert_eq!(first.num_rows(), 1);

        let mut stream2 = MemoryTrackedStream::new(
            RecordBatches::try_new(batch.schema.clone(), vec![batch])
                .unwrap()
                .as_stream(),
            tracker,
        );
        let result = timeout(Duration::from_secs(1), stream2.next())
            .await
            .unwrap();
        let error = result.unwrap().unwrap_err();
        assert!(error.to_string().contains("timed out waiting"));
        assert_eq!(exhausted.load(Ordering::Relaxed), 1);
        assert_eq!(rejected.load(Ordering::Relaxed), 1);
    }

    #[tokio::test]
    async fn test_memory_tracked_stream_fail_policy_rejects_immediately() {
        let exhausted = Arc::new(AtomicUsize::new(0));
        let rejected = Arc::new(AtomicUsize::new(0));
        let exhausted_counter = exhausted.clone();
        let rejected_counter = rejected.clone();
        let tracker = QueryMemoryTracker::builder(MB, OnExhaustedPolicy::Fail)
            .on_exhausted(move || {
                exhausted_counter.fetch_add(1, Ordering::Relaxed);
            })
            .on_reject(move || {
                rejected_counter.fetch_add(1, Ordering::Relaxed);
            })
            .build();
        let batch = large_string_batch(700 * 1024);

        let mut stream1 = MemoryTrackedStream::new(
            RecordBatches::try_new(batch.schema.clone(), vec![batch.clone()])
                .unwrap()
                .as_stream(),
            tracker.clone(),
        );
        let first = stream1.next().await.unwrap().unwrap();
        assert_eq!(first.num_rows(), 1);

        let mut stream2 = MemoryTrackedStream::new(
            RecordBatches::try_new(batch.schema.clone(), vec![batch])
                .unwrap()
                .as_stream(),
            tracker,
        );
        let result = stream2.next().await.unwrap();
        assert!(result.is_err());
        assert_eq!(exhausted.load(Ordering::Relaxed), 1);
        assert_eq!(rejected.load(Ordering::Relaxed), 1);
    }
}
