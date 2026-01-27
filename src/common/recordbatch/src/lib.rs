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
pub mod ext;
pub mod filter;
pub mod recordbatch;
pub mod util;

use std::fmt;
use std::pin::Pin;
use std::sync::Arc;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};

use adapter::RecordBatchMetrics;
use arc_swap::ArcSwapOption;
use common_base::readable_size::ReadableSize;
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
use snafu::{ResultExt, ensure};

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

/// Memory permit for a stream, providing privileged access or rate limiting.
///
/// The permit tracks whether this stream has privileged Top-K status.
/// When dropped, it automatically releases any privileged slot it holds.
pub struct MemoryPermit {
    tracker: QueryMemoryTracker,
    is_privileged: AtomicBool,
}

impl MemoryPermit {
    /// Check if this permit currently has privileged status.
    pub fn is_privileged(&self) -> bool {
        self.is_privileged.load(Ordering::Acquire)
    }

    /// Ensure this permit has privileged status by acquiring a slot if available.
    /// Returns true if privileged (either already privileged or just acquired privilege).
    fn ensure_privileged(&self) -> bool {
        if self.is_privileged.load(Ordering::Acquire) {
            return true;
        }

        // Try to claim a privileged slot
        self.tracker
            .privileged_count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                if count < self.tracker.privileged_slots {
                    Some(count + 1)
                } else {
                    None
                }
            })
            .map(|_| {
                self.is_privileged.store(true, Ordering::Release);
                true
            })
            .unwrap_or(false)
    }

    /// Track additional memory usage with this permit.
    /// Returns error if limit is exceeded.
    ///
    /// # Arguments
    /// * `additional` - Additional memory size to track in bytes
    /// * `stream_tracked` - Total memory already tracked by this stream
    ///
    /// # Behavior
    /// - Privileged streams: Can push global memory usage up to full limit
    /// - Standard-tier streams: Can push global memory usage up to limit * standard_tier_memory_fraction (default: 0.7)
    /// - Standard-tier streams automatically attempt to acquire privilege if slots become available
    /// - The configured limit is absolute hard limit - no stream can exceed it
    pub fn track(&self, additional: usize, stream_tracked: usize) -> Result<()> {
        // Ensure privileged status if possible
        let is_privileged = self.ensure_privileged();

        self.tracker
            .track_internal(additional, is_privileged, stream_tracked)
    }

    /// Release tracked memory.
    ///
    /// # Arguments
    /// * `amount` - Amount of memory to release in bytes
    pub fn release(&self, amount: usize) {
        self.tracker.release(amount);
    }
}

impl Drop for MemoryPermit {
    fn drop(&mut self) {
        // Release privileged slot if we had one
        if self.is_privileged.load(Ordering::Acquire) {
            self.tracker
                .privileged_count
                .fetch_sub(1, Ordering::Release);
        }
    }
}

/// Memory tracker for RecordBatch streams. Clone to share the same limit across queries.
///
/// Implements a two-tier memory allocation strategy:
/// - **Privileged tier**: First N streams (default: 20) can use up to the full memory limit
/// - **Standard tier**: Remaining streams are restricted to a fraction of the limit (default: 70%)
/// - Privilege is granted on a first-come-first-served basis
/// - The configured limit is an absolute hard cap - no stream can exceed it
#[derive(Clone)]
pub struct QueryMemoryTracker {
    current: Arc<AtomicUsize>,
    limit: usize,
    standard_tier_memory_fraction: f64,
    privileged_count: Arc<AtomicUsize>,
    privileged_slots: usize,
    on_update: Option<Arc<dyn Fn(usize) + Send + Sync>>,
    on_reject: Option<Arc<dyn Fn() + Send + Sync>>,
}

impl fmt::Debug for QueryMemoryTracker {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("QueryMemoryTracker")
            .field("current", &self.current.load(Ordering::Acquire))
            .field("limit", &self.limit)
            .field(
                "standard_tier_memory_fraction",
                &self.standard_tier_memory_fraction,
            )
            .field(
                "privileged_count",
                &self.privileged_count.load(Ordering::Acquire),
            )
            .field("privileged_slots", &self.privileged_slots)
            .field("on_update", &self.on_update.is_some())
            .field("on_reject", &self.on_reject.is_some())
            .finish()
    }
}

impl QueryMemoryTracker {
    // Default privileged slots when max_concurrent_queries is 0.
    const DEFAULT_PRIVILEGED_SLOTS: usize = 20;
    // Ratio for privileged tier: 70% queries get privileged access, standard tier uses 70% memory.
    const DEFAULT_PRIVILEGED_TIER_RATIO: f64 = 0.7;

    /// Create a new memory tracker with the given limit and max_concurrent_queries.
    /// Calculates privileged slots as 70% of max_concurrent_queries (or 20 if max_concurrent_queries is 0).
    ///
    /// # Arguments
    /// * `limit` - Maximum memory usage in bytes (hard limit for all streams). 0 means unlimited.
    /// * `max_concurrent_queries` - Maximum number of concurrent queries (0 = unlimited).
    pub fn new(limit: usize, max_concurrent_queries: usize) -> Self {
        let privileged_slots = Self::calculate_privileged_slots(max_concurrent_queries);
        Self::with_privileged_slots(limit, privileged_slots)
    }

    /// Create a new memory tracker with custom privileged slots limit.
    pub fn with_privileged_slots(limit: usize, privileged_slots: usize) -> Self {
        Self::with_config(limit, privileged_slots, Self::DEFAULT_PRIVILEGED_TIER_RATIO)
    }

    /// Create a new memory tracker with full configuration.
    ///
    /// # Arguments
    /// * `limit` - Maximum memory usage in bytes (hard limit for all streams). 0 means unlimited.
    /// * `privileged_slots` - Maximum number of streams that can get privileged status.
    /// * `standard_tier_memory_fraction` - Memory fraction for standard-tier streams (range: [0.0, 1.0]).
    ///
    /// # Panics
    /// Panics if `standard_tier_memory_fraction` is not in the range [0.0, 1.0].
    pub fn with_config(
        limit: usize,
        privileged_slots: usize,
        standard_tier_memory_fraction: f64,
    ) -> Self {
        assert!(
            (0.0..=1.0).contains(&standard_tier_memory_fraction),
            "standard_tier_memory_fraction must be in [0.0, 1.0], got {}",
            standard_tier_memory_fraction
        );

        Self {
            current: Arc::new(AtomicUsize::new(0)),
            limit,
            standard_tier_memory_fraction,
            privileged_count: Arc::new(AtomicUsize::new(0)),
            privileged_slots,
            on_update: None,
            on_reject: None,
        }
    }

    /// Register a new permit for memory tracking.
    /// The first `privileged_slots` permits get privileged status automatically.
    /// The returned permit can be shared across multiple streams of the same query.
    pub fn register_permit(&self) -> MemoryPermit {
        // Try to claim a privileged slot
        let is_privileged = self
            .privileged_count
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |count| {
                if count < self.privileged_slots {
                    Some(count + 1)
                } else {
                    None
                }
            })
            .is_ok();

        MemoryPermit {
            tracker: self.clone(),
            is_privileged: AtomicBool::new(is_privileged),
        }
    }

    /// Set a callback to be called whenever the usage changes successfully.
    /// The callback receives the new total usage in bytes.
    ///
    /// # Note
    /// The callback is called after both successful `track()` and `release()` operations.
    /// It is called even when `limit == 0` (unlimited mode) to track actual usage.
    pub fn with_on_update<F>(mut self, on_update: F) -> Self
    where
        F: Fn(usize) + Send + Sync + 'static,
    {
        self.on_update = Some(Arc::new(on_update));
        self
    }

    /// Set a callback to be called when memory allocation is rejected.
    ///
    /// # Note
    /// This is only called when `track()` fails due to exceeding the limit.
    /// It is never called when `limit == 0` (unlimited mode).
    pub fn with_on_reject<F>(mut self, on_reject: F) -> Self
    where
        F: Fn() + Send + Sync + 'static,
    {
        self.on_reject = Some(Arc::new(on_reject));
        self
    }

    /// Get the current memory usage in bytes.
    pub fn current(&self) -> usize {
        self.current.load(Ordering::Acquire)
    }

    fn calculate_privileged_slots(max_concurrent_queries: usize) -> usize {
        if max_concurrent_queries == 0 {
            Self::DEFAULT_PRIVILEGED_SLOTS
        } else {
            ((max_concurrent_queries as f64 * Self::DEFAULT_PRIVILEGED_TIER_RATIO) as usize).max(1)
        }
    }

    /// Internal method to track additional memory usage.
    ///
    /// Called by `MemoryPermit::track()`. Use `MemoryPermit::track()` instead of calling this directly.
    fn track_internal(
        &self,
        additional: usize,
        is_privileged: bool,
        stream_tracked: usize,
    ) -> Result<()> {
        // Calculate effective global limit based on stream privilege
        // Privileged streams: can push global usage up to full limit
        // Standard-tier streams: can only push global usage up to fraction of limit
        let effective_limit = if is_privileged {
            self.limit
        } else {
            (self.limit as f64 * self.standard_tier_memory_fraction) as usize
        };

        let mut new_total = 0;
        let result = self
            .current
            .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                new_total = current.saturating_add(additional);

                if self.limit == 0 {
                    // Unlimited mode
                    return Some(new_total);
                }

                // Check if new global total exceeds effective limit
                // The configured limit is absolute hard limit - no stream can exceed it
                if new_total <= effective_limit {
                    Some(new_total)
                } else {
                    None
                }
            });

        match result {
            Ok(_) => {
                if let Some(callback) = &self.on_update {
                    callback(new_total);
                }
                Ok(())
            }
            Err(current) => {
                if let Some(callback) = &self.on_reject {
                    callback();
                }
                let msg = format!(
                    "{} requested, {} used globally ({}%), {} used by this stream (privileged: {}), effective limit: {} ({}%), hard limit: {}",
                    ReadableSize(additional as u64),
                    ReadableSize(current as u64),
                    if self.limit > 0 {
                        current * 100 / self.limit
                    } else {
                        0
                    },
                    ReadableSize(stream_tracked as u64),
                    is_privileged,
                    ReadableSize(effective_limit as u64),
                    if self.limit > 0 {
                        effective_limit * 100 / self.limit
                    } else {
                        0
                    },
                    ReadableSize(self.limit as u64)
                );
                error::ExceedMemoryLimitSnafu { msg }.fail()
            }
        }
    }

    /// Release tracked memory.
    ///
    /// # Arguments
    /// * `amount` - Amount of memory to release in bytes
    pub fn release(&self, amount: usize) {
        if let Ok(old_value) =
            self.current
                .fetch_update(Ordering::AcqRel, Ordering::Acquire, |current| {
                    Some(current.saturating_sub(amount))
                })
            && let Some(callback) = &self.on_update
        {
            callback(old_value.saturating_sub(amount));
        }
    }
}

/// A wrapper stream that tracks memory usage of RecordBatches.
pub struct MemoryTrackedStream {
    inner: SendableRecordBatchStream,
    permit: Arc<MemoryPermit>,
    // Total tracked size, released when stream drops.
    total_tracked: usize,
}

impl MemoryTrackedStream {
    pub fn new(inner: SendableRecordBatchStream, permit: Arc<MemoryPermit>) -> Self {
        Self {
            inner,
            permit,
            total_tracked: 0,
        }
    }
}

impl Stream for MemoryTrackedStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.inner).poll_next(cx) {
            Poll::Ready(Some(Ok(batch))) => {
                let additional = batch.buffer_memory_size();

                if let Err(e) = self.permit.track(additional, self.total_tracked) {
                    return Poll::Ready(Some(Err(e)));
                }

                self.total_tracked += additional;

                Poll::Ready(Some(Ok(batch)))
            }
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => Poll::Ready(None),
            Poll::Pending => Poll::Pending,
        }
    }

    fn size_hint(&self) -> (usize, Option<usize>) {
        self.inner.size_hint()
    }
}

impl Drop for MemoryTrackedStream {
    fn drop(&mut self) {
        if self.total_tracked > 0 {
            self.permit.release(self.total_tracked);
        }
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

    use datatypes::prelude::{ConcreteDataType, VectorRef};
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::{BooleanVector, Int32Vector, StringVector};

    use super::*;

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

    #[test]
    fn test_query_memory_tracker_basic() {
        let tracker = Arc::new(QueryMemoryTracker::new(1000, 0));

        // Register first stream - should get privileged status
        let permit1 = tracker.register_permit();
        assert!(permit1.is_privileged());

        // Privileged stream can use up to limit
        assert!(permit1.track(500, 0).is_ok());
        assert_eq!(tracker.current(), 500);

        // Register second stream - also privileged
        let permit2 = tracker.register_permit();
        assert!(permit2.is_privileged());
        // Can add more but cannot exceed hard limit (1000)
        assert!(permit2.track(400, 0).is_ok());
        assert_eq!(tracker.current(), 900);

        permit1.release(500);
        permit2.release(400);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn test_query_memory_tracker_privileged_limit() {
        // Privileged slots = 2 for easy testing
        // Limit: 1000, standard-tier fraction: 0.7 (default)
        // Privileged can push global to 1000, standard-tier can push global to 700
        let tracker = Arc::new(QueryMemoryTracker::with_privileged_slots(1000, 2));

        // First 2 streams are privileged
        let permit1 = tracker.register_permit();
        let permit2 = tracker.register_permit();
        assert!(permit1.is_privileged());
        assert!(permit2.is_privileged());

        // Third stream is standard-tier (not privileged)
        let permit3 = tracker.register_permit();
        assert!(!permit3.is_privileged());

        // Privileged stream uses some memory
        assert!(permit1.track(300, 0).is_ok());
        assert_eq!(tracker.current(), 300);

        // Standard-tier can add up to 400 (total becomes 700, its effective limit)
        assert!(permit3.track(400, 0).is_ok());
        assert_eq!(tracker.current(), 700);

        // Standard-tier stream cannot push global beyond 700
        let err = permit3.track(100, 400).unwrap_err();
        let err_msg = err.to_string();
        assert!(err_msg.contains("400B used by this stream"));
        assert!(err_msg.contains("effective limit: 700B (70%)"));
        assert!(err_msg.contains("700B used globally (70%)"));
        assert_eq!(tracker.current(), 700);

        permit1.release(300);
        permit3.release(400);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn test_query_memory_tracker_promotion() {
        // Privileged slots = 1 for easy testing
        let tracker = Arc::new(QueryMemoryTracker::with_privileged_slots(1000, 1));

        // First stream is privileged
        let permit1 = tracker.register_permit();
        assert!(permit1.is_privileged());

        // Second stream is standard-tier (can only use 500)
        let permit2 = tracker.register_permit();
        assert!(!permit2.is_privileged());

        // Standard-tier can only track 500
        assert!(permit2.track(400, 0).is_ok());
        assert_eq!(tracker.current(), 400);

        // Drop first permit to release privileged slot
        drop(permit1);

        // Second stream can now be promoted and use more memory
        assert!(permit2.track(500, 400).is_ok());
        assert!(permit2.is_privileged());
        assert_eq!(tracker.current(), 900);

        permit2.release(900);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn test_query_memory_tracker_privileged_hard_limit() {
        // Test that the configured limit is absolute hard limit for all streams
        // Privileged: can use full limit (1000)
        // Standard-tier: can use 0.7x limit (700 with defaults)
        let tracker = Arc::new(QueryMemoryTracker::new(1000, 0));

        let permit1 = tracker.register_permit();
        assert!(permit1.is_privileged());

        // Privileged can use up to full limit (1000)
        assert!(permit1.track(900, 0).is_ok());
        assert_eq!(tracker.current(), 900);

        // Privileged cannot exceed hard limit (1000)
        assert!(permit1.track(200, 900).is_err());
        assert_eq!(tracker.current(), 900);

        // Can add within hard limit
        assert!(permit1.track(100, 900).is_ok());
        assert_eq!(tracker.current(), 1000);

        // Cannot exceed even by 1 byte
        assert!(permit1.track(1, 1000).is_err());
        assert_eq!(tracker.current(), 1000);

        permit1.release(1000);
        assert_eq!(tracker.current(), 0);
    }

    #[test]
    fn test_query_memory_tracker_standard_tier_fraction() {
        // Test standard-tier streams use fraction of limit
        // Limit: 1000, default fraction: 0.7, so standard-tier can use 700
        let tracker = Arc::new(QueryMemoryTracker::with_privileged_slots(1000, 1));

        let permit1 = tracker.register_permit();
        assert!(permit1.is_privileged());

        let permit2 = tracker.register_permit();
        assert!(!permit2.is_privileged());

        // Standard-tier can use up to 700 (1000 * 0.7 default)
        assert!(permit2.track(600, 0).is_ok());
        assert_eq!(tracker.current(), 600);

        // Cannot exceed standard-tier limit (700)
        assert!(permit2.track(200, 600).is_err());
        assert_eq!(tracker.current(), 600);

        // Can add within standard-tier limit
        assert!(permit2.track(100, 600).is_ok());
        assert_eq!(tracker.current(), 700);

        // Cannot exceed standard-tier limit
        assert!(permit2.track(1, 700).is_err());
        assert_eq!(tracker.current(), 700);

        permit2.release(700);
        assert_eq!(tracker.current(), 0);
    }
}
