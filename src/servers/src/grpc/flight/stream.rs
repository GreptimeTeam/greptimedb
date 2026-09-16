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

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use arrow_flight::FlightData;
use common_error::ext::ErrorExt;
use common_grpc::flight::{FlightEncoder, FlightMessage};
use common_recordbatch::recordbatch::merge_record_batches;
use common_recordbatch::{RecordBatch, SendableRecordBatchStream};
use common_telemetry::tracing::{Instrument, info_span};
use common_telemetry::tracing_context::{FutureExt, TracingContext};
use common_telemetry::{error, info, warn};
use datatypes::schema::SchemaRef;
use futures::channel::mpsc;
use futures::channel::mpsc::Sender;
use futures::future::poll_fn;
use futures::{SinkExt, Stream, StreamExt};
use pin_project::{pin_project, pinned_drop};
use session::context::{
    FLIGHT_METRICS_HEARTBEAT_INTERVAL, QueryContextRef,
    SUPPORT_FLIGHT_METRICS_BEFORE_BATCH_EXTENSION_KEY,
};
use snafu::ResultExt;
use tokio::task::JoinHandle;
use tokio::time;

use crate::error;
use crate::grpc::FlightCompression;
use crate::grpc::flight::TonicResult;

pub enum FlightRecordBatchSource {
    RecordBatches(SendableRecordBatchStream),
    AffectedRows {
        rows: usize,
        metrics: Option<String>,
    },
}

/// Determines whether a Flight result is ready now or initialized asynchronously.
pub enum FlightRecordBatchStreamInput<F = std::future::Ready<TonicResult<FlightRecordBatchSource>>>
{
    Ready(FlightRecordBatchSource),
    Initializer(F),
}

impl FlightRecordBatchStreamInput {
    /// Creates an input from a source that is already available.
    pub fn ready(source: FlightRecordBatchSource) -> Self {
        Self::Ready(source)
    }
}

impl<F> FlightRecordBatchStreamInput<F> {
    /// Creates an input that obtains its source asynchronously.
    ///
    /// Errors from the initializer are returned through the Flight response stream.
    pub fn initializer(initializer: F) -> Self {
        Self::Initializer(initializer)
    }
}

/// Metrics collector for Flight stream with RAII logging pattern
struct StreamMetrics {
    send_schema_duration: Duration,
    send_record_batch_duration: Duration,
    send_metrics_duration: Duration,
    fetch_content_duration: Duration,
    record_batch_count: usize,
    metrics_count: usize,
    total_rows: usize,
    total_bytes: usize,
    should_log: bool,
}

impl StreamMetrics {
    fn new(should_log: bool) -> Self {
        Self {
            send_schema_duration: Duration::ZERO,
            send_record_batch_duration: Duration::ZERO,
            send_metrics_duration: Duration::ZERO,
            fetch_content_duration: Duration::ZERO,
            record_batch_count: 0,
            metrics_count: 0,
            total_rows: 0,
            total_bytes: 0,
            should_log,
        }
    }
}

impl Drop for StreamMetrics {
    fn drop(&mut self) {
        if self.should_log {
            info!(
                "flight_data_stream finished: \
                send_schema_duration={:?}, \
                send_record_batch_duration={:?}, \
                send_metrics_duration={:?}, \
                fetch_content_duration={:?}, \
                record_batch_count={}, \
                metrics_count={}, \
                total_rows={}, \
                total_bytes={}",
                self.send_schema_duration,
                self.send_record_batch_duration,
                self.send_metrics_duration,
                self.fetch_content_duration,
                self.record_batch_count,
                self.metrics_count,
                self.total_rows,
                self.total_bytes
            );
        }
    }
}

/// Coalesces consecutive ready record batches into one outgoing group.
///
/// The budgets are soft limits (flush thresholds) rather than strict admission
/// caps: `push` appends a batch before reporting whether to flush, so a group may
/// exceed a budget by up to one batch.
struct BatchAccumulator {
    batches: Vec<RecordBatch>,
    rows: usize,
    bytes: usize,
}

impl BatchAccumulator {
    const MAX_ROWS: usize = 1024;
    const MAX_BYTES: usize = 256 * 1024;
    const MAX_BATCHES: usize = 16;

    fn new() -> Self {
        Self {
            batches: Vec::new(),
            rows: 0,
            bytes: 0,
        }
    }

    /// Whether a batch fetched to start a new group already reaches a budget, in
    /// which case the stream forwards it as a singleton instead of coalescing it.
    /// A batch appended inside a group is coalesced even if it exceeds a budget.
    fn reaches_budget(rows: usize, bytes: usize) -> bool {
        rows >= Self::MAX_ROWS || bytes >= Self::MAX_BYTES
    }

    fn is_empty(&self) -> bool {
        self.batches.is_empty()
    }

    /// Appends `batch` and reports whether the group should be flushed now.
    fn push(&mut self, batch: RecordBatch) -> bool {
        self.rows += batch.num_rows();
        self.bytes += batch.df_record_batch().get_array_memory_size();
        self.batches.push(batch);
        // Append-then-check: the group may exceed a budget by this batch.
        self.batches.len() >= Self::MAX_BATCHES || Self::reaches_budget(self.rows, self.bytes)
    }

    /// Removes and returns the accumulated batches in arrival order.
    ///
    /// Merging is left to the caller, which owns the stream schema.
    fn drain(&mut self) -> Vec<RecordBatch> {
        self.rows = 0;
        self.bytes = 0;
        std::mem::take(&mut self.batches)
    }
}

/// Forwards ready record batches on the non-verbose path, coalescing
/// consecutive small batches into one outgoing group before sending.
struct CoalescingBatcher {
    acc: BatchAccumulator,
    sent_first_batch: bool,
    recordbatch_schema: SchemaRef,
}

impl CoalescingBatcher {
    fn new(recordbatch_schema: SchemaRef) -> Self {
        Self {
            acc: BatchAccumulator::new(),
            sent_first_batch: false,
            recordbatch_schema,
        }
    }

    /// Runs the coalescing loop until the source stream ends or fails. Returns
    /// `true` on normal EOF, `false` when it stopped early on an error or a failed
    /// send.
    async fn run(
        &mut self,
        recordbatches: &mut SendableRecordBatchStream,
        tx: &mut Sender<TonicResult<FlightMessage>>,
        metrics: &mut StreamMetrics,
    ) -> bool {
        loop {
            let start = Instant::now();
            let batch_or_err = recordbatches.next().in_current_span().await;
            metrics.fetch_content_duration += start.elapsed();
            let Some(batch_or_err) = batch_or_err else {
                break;
            };
            let recordbatch = match batch_or_err {
                Ok(recordbatch) => recordbatch,
                Err(e) => {
                    if e.status_code().should_log_error() {
                        error!("{e:?}");
                    }
                    let e = Err(e).context(error::CollectRecordbatchSnafu);
                    if let Err(e) = tx.send(e.map_err(|x| x.into())).await {
                        warn!(e; "stop sending Flight data");
                    }
                    return false;
                }
            };
            let batch_rows = recordbatch.num_rows();
            let batch_bytes = recordbatch.df_record_batch().get_array_memory_size();
            metrics.total_rows += batch_rows;
            metrics.record_batch_count += 1;
            metrics.total_bytes += batch_bytes;

            // The first batch is forwarded immediately, and a batch starting a
            // new group that is already at or over a budget passes through as a
            // singleton. A batch appended inside a group is coalesced instead.
            if !self.sent_first_batch || BatchAccumulator::reaches_budget(batch_rows, batch_bytes) {
                let start = Instant::now();
                if let Err(e) = tx
                    .send(Ok(FlightMessage::RecordBatch(
                        recordbatch.into_df_record_batch(),
                    )))
                    .await
                {
                    warn!(e; "stop sending Flight data");
                    return false;
                }
                metrics.send_record_batch_duration += start.elapsed();
                self.sent_first_batch = true;
                continue;
            }

            // Coalesce ready batches until a budget is reached: every ready
            // batch is appended first, then the budgets are checked.
            debug_assert!(self.acc.is_empty(), "the previous group must be flushed");
            let mut should_flush = self.acc.push(recordbatch);
            let mut eof = false;
            let mut stream_error = None;
            while !should_flush {
                let start = Instant::now();
                let next = poll_fn(|cx| Poll::Ready(recordbatches.as_mut().poll_next(cx))).await;
                metrics.fetch_content_duration += start.elapsed();
                match next {
                    Poll::Ready(Some(Ok(recordbatch))) => {
                        metrics.total_rows += recordbatch.num_rows();
                        metrics.record_batch_count += 1;
                        metrics.total_bytes +=
                            recordbatch.df_record_batch().get_array_memory_size();
                        should_flush = self.acc.push(recordbatch);
                    }
                    Poll::Ready(Some(Err(e))) => {
                        stream_error = Some(e);
                        break;
                    }
                    Poll::Ready(None) => {
                        eof = true;
                        break;
                    }
                    Poll::Pending => break,
                }
            }

            let batches = self.acc.drain();
            let batches = if batches.len() >= 2 {
                match merge_record_batches(self.recordbatch_schema.clone(), &batches) {
                    Ok(merged) => vec![merged],
                    Err(_) => batches,
                }
            } else {
                batches
            };
            for recordbatch in batches {
                let start = Instant::now();
                if let Err(e) = tx
                    .send(Ok(FlightMessage::RecordBatch(
                        recordbatch.into_df_record_batch(),
                    )))
                    .await
                {
                    warn!(e; "stop sending Flight data");
                    return false;
                }
                metrics.send_record_batch_duration += start.elapsed();
            }
            if let Some(e) = stream_error {
                if e.status_code().should_log_error() {
                    error!("{e:?}");
                }
                let e = Err(e).context(error::CollectRecordbatchSnafu);
                if let Err(e) = tx.send(e.map_err(|x| x.into())).await {
                    warn!(e; "stop sending Flight data");
                }
                return false;
            }
            if eof {
                break;
            }
        }
        true
    }
}

#[pin_project(PinnedDrop)]
pub struct FlightRecordBatchStream {
    #[pin]
    rx: mpsc::Receiver<Result<FlightMessage, tonic::Status>>,
    join_handle: JoinHandle<()>,
    done: bool,
    encoder: FlightEncoder,
    buffer: VecDeque<FlightData>,
}

impl FlightRecordBatchStream {
    async fn send_metrics(
        tx: &mut Sender<TonicResult<FlightMessage>>,
        metrics: &mut StreamMetrics,
        metrics_str: String,
    ) -> bool {
        metrics.metrics_count += 1;
        let start = Instant::now();
        if let Err(e) = tx.send(Ok(FlightMessage::Metrics(metrics_str))).await {
            warn!(e; "stop sending Flight data");
            return false;
        }
        metrics.send_metrics_duration += start.elapsed();
        true
    }

    async fn send_metrics_if_changed(
        tx: &mut Sender<TonicResult<FlightMessage>>,
        metrics: &mut StreamMetrics,
        last_metrics_str: &mut Option<String>,
        metrics_str: String,
    ) -> bool {
        if last_metrics_str.as_deref() == Some(metrics_str.as_str()) {
            return true;
        }

        *last_metrics_str = Some(metrics_str.clone());
        Self::send_metrics(tx, metrics, metrics_str).await
    }

    pub fn new<F>(
        input: FlightRecordBatchStreamInput<F>,
        tracing_context: TracingContext,
        compression: FlightCompression,
        query_ctx: QueryContextRef,
    ) -> Self
    where
        F: Future<Output = TonicResult<FlightRecordBatchSource>> + Send + 'static,
    {
        let (mut tx, rx) = mpsc::channel::<TonicResult<FlightMessage>>(1);
        let source_type = match &input {
            FlightRecordBatchStreamInput::Ready(FlightRecordBatchSource::RecordBatches(_)) => {
                "record_batches"
            }
            FlightRecordBatchStreamInput::Ready(FlightRecordBatchSource::AffectedRows {
                ..
            }) => "affected_rows",
            FlightRecordBatchStreamInput::Initializer(_) => "initializer",
        };
        let initializer_tracing_context = tracing_context.clone();
        let join_handle = common_runtime::spawn_global(
            async move {
                let source = async move {
                    match input {
                        FlightRecordBatchStreamInput::Ready(source) => Ok(source),
                        FlightRecordBatchStreamInput::Initializer(initializer) => initializer.await,
                    }
                }
                .trace(
                    initializer_tracing_context
                        .attach(info_span!("flight_data_stream_init", source_type)),
                )
                .await;

                match source {
                    Ok(FlightRecordBatchSource::RecordBatches(recordbatches)) => {
                        // Verbose responses preserve their existing per-batch metrics behavior.
                        let should_send_partial_metrics = query_ctx.explain_verbose();
                        let can_send_metrics_before_batch =
                            query_ctx.explain_verbose()
                                && query_ctx.live_analyze_metrics_enabled()
                                && query_ctx
                                    .remote_query_id()
                                    .zip(query_ctx.extension(
                                        SUPPORT_FLIGHT_METRICS_BEFORE_BATCH_EXTENSION_KEY,
                                    ))
                                    .is_some_and(|(remote_query_id, capability)| {
                                        capability == remote_query_id
                                    });
                        Self::flight_data_stream(
                            recordbatches,
                            tx,
                            should_send_partial_metrics,
                            can_send_metrics_before_batch,
                        )
                        .await;
                    }
                    Ok(FlightRecordBatchSource::AffectedRows { rows, metrics }) => {
                        let _ = tx
                            .send(Ok(FlightMessage::AffectedRows { rows, metrics }))
                            .await;
                    }
                    Err(status) => {
                        let _ = tx.send(Err(status)).await;
                    }
                }
            }
            .trace(tracing_context.attach(info_span!("flight_data_stream"))),
        );
        let encoder = if compression.arrow_compression() {
            FlightEncoder::default()
        } else {
            FlightEncoder::with_compression_disabled()
        };
        Self {
            rx,
            join_handle,
            done: false,
            encoder,
            buffer: VecDeque::new(),
        }
    }

    async fn flight_data_stream(
        mut recordbatches: SendableRecordBatchStream,
        mut tx: Sender<TonicResult<FlightMessage>>,
        should_send_partial_metrics: bool,
        can_send_metrics_before_batch: bool,
    ) {
        let mut metrics = StreamMetrics::new(should_send_partial_metrics);
        let mut last_metrics_str = None;
        let recordbatch_schema = recordbatches.schema();
        let schema = recordbatch_schema.arrow_schema().clone();
        let start = Instant::now();
        if let Err(e) = tx.send(Ok(FlightMessage::Schema(schema))).await {
            warn!(e; "stop sending Flight data");
            return;
        }
        metrics.send_schema_duration += start.elapsed();

        // Each path reports whether it reached normal EOF. On any error or failed
        // send the path stops early and the final-metrics tail must be skipped:
        // final metrics are only sent after a cleanly completed stream.
        let reached_eof = if should_send_partial_metrics {
            Self::verbose_metrics_stream(
                &mut recordbatches,
                &mut tx,
                &mut metrics,
                &mut last_metrics_str,
                can_send_metrics_before_batch,
            )
            .await
        } else {
            CoalescingBatcher::new(recordbatch_schema.clone())
                .run(&mut recordbatches, &mut tx, &mut metrics)
                .await
        };
        if !reached_eof {
            return;
        }

        // Make the last package pass metrics exactly once at EOF.
        if let Some(metrics_str) = recordbatches
            .metrics()
            .and_then(|m| serde_json::to_string(&m).ok())
        {
            let _ = Self::send_metrics(&mut tx, &mut metrics, metrics_str).await;
        }
    }

    /// On the verbose path, sends every record batch individually and forwards
    /// partial metrics whenever they change.
    /// Returns `true` when the source stream reached normal EOF, `false` when it
    /// stopped early on an error or a failed send.
    async fn verbose_metrics_stream(
        recordbatches: &mut SendableRecordBatchStream,
        tx: &mut Sender<TonicResult<FlightMessage>>,
        metrics: &mut StreamMetrics,
        last_metrics_str: &mut Option<String>,
        can_send_metrics_before_batch: bool,
    ) -> bool {
        loop {
            let start = Instant::now();
            let batch_or_err = if can_send_metrics_before_batch {
                match time::timeout(
                    FLIGHT_METRICS_HEARTBEAT_INTERVAL,
                    recordbatches.next().in_current_span(),
                )
                .await
                {
                    Ok(result) => result,
                    Err(_) => {
                        if let Some(metrics_str) = recordbatches
                            .metrics()
                            .and_then(|m| serde_json::to_string(&m).ok())
                            && !Self::send_metrics_if_changed(
                                tx,
                                metrics,
                                last_metrics_str,
                                metrics_str,
                            )
                            .await
                        {
                            return false;
                        }
                        metrics.fetch_content_duration += start.elapsed();
                        continue;
                    }
                }
            } else {
                recordbatches.next().in_current_span().await
            };
            metrics.fetch_content_duration += start.elapsed();
            let Some(batch_or_err) = batch_or_err else {
                break;
            };
            match batch_or_err {
                Ok(recordbatch) => {
                    metrics.total_rows += recordbatch.num_rows();
                    metrics.record_batch_count += 1;
                    metrics.total_bytes += recordbatch.df_record_batch().get_array_memory_size();
                    let start = Instant::now();
                    if let Err(e) = tx
                        .send(Ok(FlightMessage::RecordBatch(
                            recordbatch.into_df_record_batch(),
                        )))
                        .await
                    {
                        warn!(e; "stop sending Flight data");
                        return false;
                    }
                    metrics.send_record_batch_duration += start.elapsed();
                    if let Some(metrics_str) = recordbatches
                        .metrics()
                        .and_then(|m| serde_json::to_string(&m).ok())
                        && {
                            *last_metrics_str = Some(metrics_str.clone());
                            !Self::send_metrics(tx, metrics, metrics_str).await
                        }
                    {
                        return false;
                    }
                }
                Err(e) => {
                    if e.status_code().should_log_error() {
                        error!("{e:?}");
                    }
                    let e = Err(e).context(error::CollectRecordbatchSnafu);
                    if let Err(e) = tx.send(e.map_err(|x| x.into())).await {
                        warn!(e; "stop sending Flight data");
                    }
                    return false;
                }
            }
        }
        true
    }
}

#[pinned_drop]
impl PinnedDrop for FlightRecordBatchStream {
    fn drop(self: Pin<&mut Self>) {
        self.join_handle.abort();
    }
}

impl Stream for FlightRecordBatchStream {
    type Item = TonicResult<FlightData>;

    fn poll_next(self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = self.project();
        if *this.done {
            Poll::Ready(None)
        } else {
            if let Some(x) = this.buffer.pop_front() {
                return Poll::Ready(Some(Ok(x)));
            }
            match this.rx.poll_next(cx) {
                Poll::Ready(None) => {
                    *this.done = true;
                    Poll::Ready(None)
                }
                Poll::Ready(Some(result)) => match result {
                    Ok(flight_message) => {
                        let mut iter = this.encoder.encode(flight_message).into_iter();
                        let Some(first) = iter.next() else {
                            // Safety: `iter` on a type of `Vec1`, which is guaranteed to have
                            // at least one element.
                            unreachable!()
                        };
                        this.buffer.extend(iter);
                        Poll::Ready(Some(Ok(first)))
                    }
                    Err(e) => {
                        *this.done = true;
                        Poll::Ready(Some(Err(e)))
                    }
                },
                Poll::Pending => Poll::Pending,
            }
        }
    }
}

#[cfg(test)]
mod test {
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};
    use std::time::Duration;

    use common_grpc::flight::{FlightDecoder, FlightMessage};
    use common_recordbatch::adapter::RecordBatchMetrics;
    use common_recordbatch::error::CreateRecordBatchesSnafu;
    use common_recordbatch::{OrderOption, RecordBatch, RecordBatchStream, RecordBatches};
    use datatypes::arrow::array::{DictionaryArray, Int32Array, StringArray};
    use datatypes::arrow::datatypes::Int32Type;
    use datatypes::prelude::*;
    use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
    use datatypes::vectors::{DictionaryVector, Int32Vector};
    use futures::StreamExt;
    use session::context::{
        LIVE_ANALYZE_METRICS_EXTENSION_KEY, QueryContext,
        SUPPORT_FLIGHT_METRICS_BEFORE_BATCH_EXTENSION_KEY,
    };

    use super::*;

    struct PendingMetricsStream {
        schema: SchemaRef,
        metrics: RecordBatchMetrics,
    }

    struct MetricsThenBatchStream {
        schema: SchemaRef,
        metrics: RecordBatchMetrics,
        rx: tokio::sync::mpsc::UnboundedReceiver<common_recordbatch::error::Result<RecordBatch>>,
    }

    enum ScriptedItem {
        Batch(common_recordbatch::error::Result<RecordBatch>),
        Pending,
        PersistentPending,
    }

    struct ScriptedBatchStream {
        schema: SchemaRef,
        items: VecDeque<ScriptedItem>,
        poll_count: Arc<std::sync::atomic::AtomicUsize>,
    }

    struct DropFlagStream {
        schema: SchemaRef,
        dropped: Arc<std::sync::atomic::AtomicBool>,
    }

    fn query_context_with_matching_capability() -> Arc<QueryContext> {
        let query_ctx = QueryContext::arc();
        let remote_query_id = query_ctx
            .remote_query_id()
            .expect("query context must have remote query id")
            .to_string();
        let mut query_ctx = (*query_ctx).clone();
        query_ctx.set_extension(
            SUPPORT_FLIGHT_METRICS_BEFORE_BATCH_EXTENSION_KEY,
            remote_query_id,
        );
        Arc::new(query_ctx)
    }

    fn query_context_with_live_metrics_and_matching_capability() -> Arc<QueryContext> {
        let mut query_ctx = (*query_context_with_matching_capability()).clone();
        query_ctx.enable_live_analyze_metrics();
        Arc::new(query_ctx)
    }

    impl RecordBatchStream for PendingMetricsStream {
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }

        fn output_ordering(&self) -> Option<&[OrderOption]> {
            None
        }

        fn metrics(&self) -> Option<RecordBatchMetrics> {
            Some(self.metrics.clone())
        }
    }

    impl Stream for PendingMetricsStream {
        type Item = common_recordbatch::error::Result<RecordBatch>;

        fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Pending
        }
    }

    impl RecordBatchStream for MetricsThenBatchStream {
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }

        fn output_ordering(&self) -> Option<&[OrderOption]> {
            None
        }

        fn metrics(&self) -> Option<RecordBatchMetrics> {
            Some(self.metrics.clone())
        }
    }

    impl Stream for MetricsThenBatchStream {
        type Item = common_recordbatch::error::Result<RecordBatch>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.rx.poll_recv(cx)
        }
    }

    impl RecordBatchStream for ScriptedBatchStream {
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }

        fn output_ordering(&self) -> Option<&[OrderOption]> {
            None
        }

        fn metrics(&self) -> Option<RecordBatchMetrics> {
            Some(RecordBatchMetrics::default())
        }
    }

    impl Stream for ScriptedBatchStream {
        type Item = common_recordbatch::error::Result<RecordBatch>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            self.poll_count
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            match self.items.pop_front() {
                Some(ScriptedItem::Batch(item)) => Poll::Ready(Some(item)),
                Some(ScriptedItem::Pending) => {
                    cx.waker().wake_by_ref();
                    Poll::Pending
                }
                Some(ScriptedItem::PersistentPending) => {
                    self.items.push_front(ScriptedItem::PersistentPending);
                    Poll::Pending
                }
                None => Poll::Ready(None),
            }
        }
    }

    impl RecordBatchStream for DropFlagStream {
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

    impl Stream for DropFlagStream {
        type Item = common_recordbatch::error::Result<RecordBatch>;

        fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            Poll::Pending
        }
    }

    impl Drop for DropFlagStream {
        fn drop(&mut self) {
            self.dropped
                .store(true, std::sync::atomic::Ordering::Relaxed);
        }
    }

    fn int_batch(schema: SchemaRef, values: impl IntoIterator<Item = i32>) -> RecordBatch {
        RecordBatch::new(
            schema,
            vec![Arc::new(Int32Vector::from_iter_values(values)) as VectorRef],
        )
        .unwrap()
    }

    async fn flight_messages_with_context(
        recordbatches: SendableRecordBatchStream,
        query_ctx: Arc<QueryContext>,
    ) -> Vec<TonicResult<FlightMessage>> {
        let mut stream = FlightRecordBatchStream::new(
            FlightRecordBatchStreamInput::ready(FlightRecordBatchSource::RecordBatches(
                recordbatches,
            )),
            TracingContext::default(),
            FlightCompression::default(),
            query_ctx,
        );
        let decoder = &mut FlightDecoder::default();
        let mut messages = Vec::new();
        while let Some(data) = stream.next().await {
            match data {
                Ok(data) => {
                    if let Some(message) = decoder.try_decode(&data).unwrap() {
                        messages.push(Ok(message));
                    }
                }
                Err(status) => messages.push(Err(status)),
            }
        }
        messages
    }

    async fn flight_messages(
        recordbatches: SendableRecordBatchStream,
    ) -> Vec<TonicResult<FlightMessage>> {
        flight_messages_with_context(recordbatches, QueryContext::arc()).await
    }

    #[tokio::test]
    async fn test_drop_cancels_and_releases_upstream_stream() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "a",
            ConcreteDataType::int32_datatype(),
            false,
        )]));
        let dropped = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let stream = FlightRecordBatchStream::new(
            FlightRecordBatchStreamInput::ready(FlightRecordBatchSource::RecordBatches(Box::pin(
                DropFlagStream {
                    schema,
                    dropped: dropped.clone(),
                },
            ))),
            TracingContext::default(),
            FlightCompression::default(),
            QueryContext::arc(),
        );
        drop(stream);
        tokio::time::timeout(Duration::from_secs(1), async {
            while !dropped.load(std::sync::atomic::Ordering::Relaxed) {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("dropping Flight stream must release upstream");
    }

    #[tokio::test]
    async fn test_first_batch_is_sent_before_any_additional_poll() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "a",
            ConcreteDataType::int32_datatype(),
            false,
        )]));
        let poll_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let recordbatches = Box::pin(ScriptedBatchStream {
            schema: schema.clone(),
            items: VecDeque::from([
                ScriptedItem::Batch(Ok(int_batch(schema.clone(), [1]))),
                ScriptedItem::PersistentPending,
            ]),
            poll_count: poll_count.clone(),
        });
        let mut stream = FlightRecordBatchStream::new(
            FlightRecordBatchStreamInput::ready(FlightRecordBatchSource::RecordBatches(
                recordbatches,
            )),
            TracingContext::default(),
            FlightCompression::default(),
            QueryContext::arc(),
        );
        let decoder = &mut FlightDecoder::default();
        let schema_data = stream.next().await.unwrap().unwrap();
        assert!(matches!(
            decoder.try_decode(&schema_data).unwrap().unwrap(),
            FlightMessage::Schema(_)
        ));
        let first = stream.next().await.unwrap().unwrap();
        assert!(matches!(
            decoder.try_decode(&first).unwrap().unwrap(),
            FlightMessage::RecordBatch(_)
        ));
    }

    #[tokio::test]
    async fn test_ready_only_coalesces_later_batches_without_polling_after_first() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "a",
            ConcreteDataType::int32_datatype(),
            false,
        )]));
        let poll_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let recordbatches = Box::pin(ScriptedBatchStream {
            schema: schema.clone(),
            items: VecDeque::from([
                ScriptedItem::Batch(Ok(int_batch(schema.clone(), [1]))),
                ScriptedItem::Batch(Ok(int_batch(schema.clone(), [2]))),
                ScriptedItem::Batch(Ok(int_batch(schema.clone(), [3]))),
            ]),
            poll_count: poll_count.clone(),
        });

        let messages = flight_messages(recordbatches).await;
        assert_eq!(poll_count.load(std::sync::atomic::Ordering::Relaxed), 4);
        assert!(matches!(messages[0], Ok(FlightMessage::Schema(_))));
        let FlightMessage::RecordBatch(first) = messages[1].as_ref().unwrap() else {
            panic!("expected the first record batch");
        };
        assert_eq!(first.num_rows(), 1);
        let FlightMessage::RecordBatch(merged) = messages[2].as_ref().unwrap() else {
            panic!("expected the coalesced record batch");
        };
        assert_eq!(merged.num_rows(), 2);
    }

    #[tokio::test]
    async fn test_ready_only_exact_row_cap_flushes() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "a",
            ConcreteDataType::int32_datatype(),
            false,
        )]));
        let poll_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let recordbatches = Box::pin(ScriptedBatchStream {
            schema: schema.clone(),
            items: VecDeque::from([
                ScriptedItem::Batch(Ok(int_batch(schema.clone(), [1]))),
                ScriptedItem::Batch(Ok(int_batch(schema.clone(), [1]))),
                ScriptedItem::Batch(Ok(int_batch(schema.clone(), 0..1023))),
                ScriptedItem::PersistentPending,
            ]),
            poll_count: poll_count.clone(),
        });
        let (tx, mut rx) = mpsc::channel::<TonicResult<FlightMessage>>(1);
        let handle = tokio::spawn(FlightRecordBatchStream::flight_data_stream(
            recordbatches,
            tx,
            false,
            false,
        ));

        assert!(matches!(
            rx.next().await.unwrap().unwrap(),
            FlightMessage::Schema(_)
        ));
        tokio::time::timeout(Duration::from_secs(1), async {
            while poll_count.load(std::sync::atomic::Ordering::Relaxed) != 3 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("exact-cap group must be formed");
        assert!(matches!(
            rx.next().await.unwrap().unwrap(),
            FlightMessage::RecordBatch(_)
        ));
        let FlightMessage::RecordBatch(batch) = rx.next().await.unwrap().unwrap() else {
            panic!("expected the exact-cap group");
        };
        assert_eq!(batch.num_rows(), 1024);
        handle.abort();
    }

    #[tokio::test]
    async fn test_over_cap_batch_merges_into_current_aggregate() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "a",
            ConcreteDataType::int32_datatype(),
            false,
        )]));
        let poll_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let recordbatches = Box::pin(ScriptedBatchStream {
            schema: schema.clone(),
            items: VecDeque::from([
                ScriptedItem::Batch(Ok(int_batch(schema.clone(), [1]))),
                ScriptedItem::Batch(Ok(int_batch(schema.clone(), [2]))),
                ScriptedItem::Batch(Ok(int_batch(schema.clone(), 0..1024))),
                ScriptedItem::PersistentPending,
            ]),
            poll_count: poll_count.clone(),
        });
        let mut stream = FlightRecordBatchStream::new(
            FlightRecordBatchStreamInput::ready(FlightRecordBatchSource::RecordBatches(
                recordbatches,
            )),
            TracingContext::default(),
            FlightCompression::default(),
            QueryContext::arc(),
        );
        let decoder = &mut FlightDecoder::default();
        let schema_data = stream.next().await.unwrap().unwrap();
        assert!(matches!(
            decoder.try_decode(&schema_data).unwrap().unwrap(),
            FlightMessage::Schema(_)
        ));
        let first = stream.next().await.unwrap().unwrap();
        assert!(matches!(
            decoder.try_decode(&first).unwrap().unwrap(),
            FlightMessage::RecordBatch(_)
        ));
        tokio::time::timeout(Duration::from_secs(1), async {
            while poll_count.load(std::sync::atomic::Ordering::Relaxed) < 3 {
                tokio::task::yield_now().await;
            }
        })
        .await
        .expect("the second group must fetch the at-cap batch");
        let aggregated = stream.next().await.unwrap().unwrap();
        let FlightMessage::RecordBatch(batch) = decoder.try_decode(&aggregated).unwrap().unwrap()
        else {
            panic!("expected the oversized aggregate");
        };
        // The at-cap batch is appended before the budget is checked, so the aggregate
        // exceeds the row budget by one batch instead of being held back as a
        // separate singleton.
        assert_eq!(batch.num_rows(), 1025);
    }

    #[tokio::test]
    async fn test_ready_only_soft_row_budget_flushes_oversized_group() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "a",
            ConcreteDataType::int32_datatype(),
            false,
        )]));
        let poll_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let messages = flight_messages(Box::pin(ScriptedBatchStream {
            schema: schema.clone(),
            items: VecDeque::from([
                ScriptedItem::Batch(Ok(int_batch(schema.clone(), [1]))),
                ScriptedItem::Batch(Ok(int_batch(schema.clone(), 0..600))),
                ScriptedItem::Batch(Ok(int_batch(schema.clone(), 0..600))),
            ]),
            poll_count: poll_count.clone(),
        }))
        .await;
        let batches = messages
            .iter()
            .filter_map(|message| match message.as_ref().unwrap() {
                FlightMessage::RecordBatch(batch) => Some(batch.num_rows()),
                _ => None,
            })
            .collect::<Vec<_>>();
        // 600 + 600 exceeds the 1024-row budget, but the second batch is appended
        // before the budget is checked, so the group flushes as one 1200-row batch.
        assert_eq!(batches, vec![1, 1200]);
        // Three fetches, plus the poll that reports end of stream.
        assert_eq!(poll_count.load(std::sync::atomic::Ordering::Relaxed), 4);
    }

    #[tokio::test]
    async fn test_ready_only_current_at_cap_batch_is_sent_as_singleton() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "a",
            ConcreteDataType::int32_datatype(),
            false,
        )]));
        let poll_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let messages = flight_messages(Box::pin(ScriptedBatchStream {
            schema: schema.clone(),
            items: VecDeque::from([
                ScriptedItem::Batch(Ok(int_batch(schema.clone(), [1]))),
                ScriptedItem::Batch(Ok(int_batch(schema.clone(), 0..1024))),
                ScriptedItem::Batch(Ok(int_batch(schema.clone(), [2]))),
            ]),
            poll_count: poll_count.clone(),
        }))
        .await;
        let batches = messages
            .iter()
            .filter_map(|message| match message.as_ref().unwrap() {
                FlightMessage::RecordBatch(batch) => Some(batch.num_rows()),
                _ => None,
            })
            .collect::<Vec<_>>();
        // The at-cap batch is current when it is fetched, so it passes through as a
        // singleton instead of joining an aggregate.
        assert_eq!(batches, vec![1, 1024, 1]);
        assert_eq!(poll_count.load(std::sync::atomic::Ordering::Relaxed), 4);
    }

    #[tokio::test]
    async fn test_ready_only_flushes_before_pending_and_before_error() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "a",
            ConcreteDataType::int32_datatype(),
            false,
        )]));
        let poll_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let recordbatches = Box::pin(ScriptedBatchStream {
            schema: schema.clone(),
            items: VecDeque::from([
                ScriptedItem::Batch(Ok(int_batch(schema.clone(), [1]))),
                ScriptedItem::Batch(Ok(int_batch(schema.clone(), [2]))),
                ScriptedItem::Pending,
                ScriptedItem::Batch(Ok(int_batch(schema.clone(), [3]))),
                ScriptedItem::Batch(Err(CreateRecordBatchesSnafu {
                    reason: "expected failure".to_string(),
                }
                .build())),
            ]),
            poll_count,
        });

        let messages = flight_messages(recordbatches).await;
        assert!(matches!(messages[1], Ok(FlightMessage::RecordBatch(_))));
        assert!(matches!(messages[2], Ok(FlightMessage::RecordBatch(_))));
        assert!(matches!(messages[3], Ok(FlightMessage::RecordBatch(_))));
        assert!(messages[4].is_err());
        assert_eq!(messages.len(), 5);
    }

    /// A stream that yields one batch then an error, and counts how many times
    /// `metrics()` is called. Used to prove the verbose error path does NOT invoke
    /// the shared EOF final-metrics tail (which would call `metrics()` one extra
    /// time). The public message stream hides the tail after an error, so this
    /// producer-side counter is what actually detects the regression.
    struct MetricsCountingErrorStream {
        schema: SchemaRef,
        yielded_batch: bool,
        metrics_calls: Arc<std::sync::atomic::AtomicUsize>,
    }

    impl RecordBatchStream for MetricsCountingErrorStream {
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }

        fn output_ordering(&self) -> Option<&[OrderOption]> {
            None
        }

        fn metrics(&self) -> Option<RecordBatchMetrics> {
            self.metrics_calls
                .fetch_add(1, std::sync::atomic::Ordering::Relaxed);
            Some(RecordBatchMetrics::default())
        }
    }

    impl Stream for MetricsCountingErrorStream {
        type Item = common_recordbatch::error::Result<RecordBatch>;

        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            if self.yielded_batch {
                return Poll::Ready(Some(Err(CreateRecordBatchesSnafu {
                    reason: "expected failure".to_string(),
                }
                .build())));
            }
            self.yielded_batch = true;
            Poll::Ready(Some(Ok(int_batch(self.schema.clone(), [1]))))
        }
    }

    /// On an upstream error the verbose path stops early and skips the shared
    /// EOF final-metrics tail: `metrics()` must not be called again after the
    /// error. This drives `flight_data_stream` directly and awaits its return,
    /// so the producer has fully finished before the counter is read (the public
    /// message stream alone would hide the tail and could race with it).
    #[tokio::test]
    async fn test_verbose_error_skips_final_metrics_tail() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "a",
            ConcreteDataType::int32_datatype(),
            false,
        )]));
        let metrics_calls = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let recordbatches: SendableRecordBatchStream = Box::pin(MetricsCountingErrorStream {
            schema: schema.clone(),
            yielded_batch: false,
            metrics_calls: metrics_calls.clone(),
        });
        let (tx, mut rx) = mpsc::channel::<TonicResult<FlightMessage>>(8);
        // should_send_partial_metrics = true selects the verbose path;
        // can_send_metrics_before_batch = false disables the heartbeat arm.
        FlightRecordBatchStream::flight_data_stream(recordbatches, tx, true, false).await;
        // Producer fully returned. Exactly one metrics() call (the per-batch one).
        // If the error path fell through to the EOF final-metrics tail, this
        // would be 2.
        assert_eq!(metrics_calls.load(std::sync::atomic::Ordering::Relaxed), 1);
        // The error is the last message; no trailing final-metrics package.
        let mut messages = Vec::new();
        while let Some(msg) = rx.next().await {
            messages.push(msg);
        }
        assert!(matches!(messages[0], Ok(FlightMessage::Schema(_))));
        assert!(matches!(messages[1], Ok(FlightMessage::RecordBatch(_))));
        assert!(matches!(messages[2], Ok(FlightMessage::Metrics(_))));
        assert!(messages[3].is_err());
        assert_eq!(messages.len(), 4);
    }

    #[tokio::test]
    async fn test_verbose_ready_batches_preserve_batch_metrics_order() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "a",
            ConcreteDataType::int32_datatype(),
            false,
        )]));
        let query_ctx = QueryContext::arc();
        query_ctx.set_explain_verbose(true);
        let messages = flight_messages_with_context(
            Box::pin(ScriptedBatchStream {
                schema: schema.clone(),
                items: VecDeque::from([
                    ScriptedItem::Batch(Ok(int_batch(schema.clone(), [1]))),
                    ScriptedItem::Batch(Ok(int_batch(schema.clone(), [2]))),
                ]),
                poll_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
            }),
            query_ctx,
        )
        .await;
        assert!(matches!(messages[1], Ok(FlightMessage::RecordBatch(_))));
        assert!(matches!(messages[2], Ok(FlightMessage::Metrics(_))));
        assert!(matches!(messages[3], Ok(FlightMessage::RecordBatch(_))));
        assert!(matches!(messages[4], Ok(FlightMessage::Metrics(_))));
    }

    #[tokio::test]
    async fn test_ready_only_merges_at_cap_batch_and_coalesces_empty_batches() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "a",
            ConcreteDataType::int32_datatype(),
            false,
        )]));
        let oversized = int_batch(schema.clone(), 0..1024);
        let mut items = vec![
            ScriptedItem::Batch(Ok(int_batch(schema.clone(), [1]))),
            ScriptedItem::Batch(Ok(int_batch(schema.clone(), [2]))),
            ScriptedItem::Batch(Ok(oversized)),
        ];
        // 17 trailing empty batches: the 16-batch budget flushes the first 16 as
        // one group, leaving one empty batch for a second group. This exercises the
        // batch-count bound rather than relying on EOF to flush.
        items.extend(
            (0..17).map(|_| ScriptedItem::Batch(Ok(RecordBatch::new_empty(schema.clone())))),
        );
        let poll_count = Arc::new(std::sync::atomic::AtomicUsize::new(0));
        let messages = flight_messages(Box::pin(ScriptedBatchStream {
            schema,
            items: items.into(),
            poll_count: poll_count.clone(),
        }))
        .await;
        let batches = messages
            .iter()
            .filter_map(|message| match message.as_ref().unwrap() {
                FlightMessage::RecordBatch(batch) => Some(batch.num_rows()),
                _ => None,
            })
            .collect::<Vec<_>>();
        // The at-cap batch is appended to the current aggregate, so the flushed
        // aggregate exceeds the row budget by one batch. The 17 trailing empty
        // batches split into two empty groups at the 16-batch budget.
        assert_eq!(batches, vec![1, 1025, 0, 0]);
        assert_eq!(poll_count.load(std::sync::atomic::Ordering::Relaxed), 21);
    }

    #[tokio::test]
    async fn test_ready_only_merges_byte_oversized_batch_into_aggregate() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "a",
            ConcreteDataType::string_datatype(),
            false,
        )]));
        let oversized = RecordBatch::new(
            schema.clone(),
            vec![Arc::new(datatypes::vectors::StringVector::from_slice(
                &vec!["x".repeat(300); 1023],
            )) as VectorRef],
        )
        .unwrap();
        let messages = flight_messages(Box::pin(ScriptedBatchStream {
            schema: schema.clone(),
            items: VecDeque::from([
                ScriptedItem::Batch(Ok(RecordBatch::new_empty(schema.clone()))),
                ScriptedItem::Batch(Ok(RecordBatch::new_empty(schema.clone()))),
                ScriptedItem::Batch(Ok(oversized)),
            ]),
            poll_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        }))
        .await;
        let batches = messages
            .iter()
            .filter_map(|message| match message.as_ref().unwrap() {
                FlightMessage::RecordBatch(batch) => Some(batch.num_rows()),
                _ => None,
            })
            .collect::<Vec<_>>();
        // The byte-oversized batch is appended to the leading empty batch, so the
        // aggregate exceeds the byte budget by one batch.
        assert_eq!(batches, vec![0, 1023]);
    }

    #[tokio::test]
    async fn test_ready_only_coalesces_dictionary_batches_with_different_mappings() {
        let arrow_schema = Arc::new(datatypes::arrow::datatypes::Schema::new(vec![
            datatypes::arrow::datatypes::Field::new_dictionary(
                "a",
                datatypes::arrow::datatypes::DataType::Int32,
                datatypes::arrow::datatypes::DataType::Utf8,
                false,
            ),
        ]));
        let schema = Arc::new(Schema::try_from(arrow_schema).unwrap());
        let dictionary_batch = |keys, values| {
            let array = DictionaryArray::<Int32Type>::new(
                Int32Array::from(keys),
                Arc::new(StringArray::from(values)),
            );
            RecordBatch::new(
                schema.clone(),
                vec![Arc::new(
                    DictionaryVector::new(array, ConcreteDataType::string_datatype()).unwrap(),
                ) as VectorRef],
            )
            .unwrap()
        };
        let recordbatches = Box::pin(ScriptedBatchStream {
            schema: schema.clone(),
            items: VecDeque::from([
                ScriptedItem::Batch(Ok(dictionary_batch(vec![0], vec!["zero"]))),
                ScriptedItem::Batch(Ok(dictionary_batch(vec![0], vec!["first"]))),
                ScriptedItem::Batch(Ok(dictionary_batch(vec![0], vec!["second"]))),
            ]),
            poll_count: Arc::new(std::sync::atomic::AtomicUsize::new(0)),
        });
        let messages = flight_messages(recordbatches).await;
        let FlightMessage::RecordBatch(merged) = messages[2].as_ref().unwrap() else {
            panic!("expected the coalesced dictionary batch");
        };
        assert_eq!(merged.num_rows(), 2);
        let dictionary = merged
            .column(0)
            .as_any()
            .downcast_ref::<DictionaryArray<Int32Type>>();
        let values = dictionary
            .unwrap()
            .values()
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert_eq!(values.value(0), "first");
        assert_eq!(values.value(1), "second");
    }

    #[tokio::test]
    async fn test_flight_record_batch_stream() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "a",
            ConcreteDataType::int32_datatype(),
            false,
        )]));

        let v: VectorRef = Arc::new(Int32Vector::from_slice([1, 2]));
        let recordbatch = RecordBatch::new(schema.clone(), vec![v]).unwrap();

        let recordbatches = RecordBatches::try_new(schema.clone(), vec![recordbatch.clone()])
            .unwrap()
            .as_stream();
        let mut stream = FlightRecordBatchStream::new(
            FlightRecordBatchStreamInput::ready(FlightRecordBatchSource::RecordBatches(
                recordbatches,
            )),
            TracingContext::default(),
            FlightCompression::default(),
            QueryContext::arc(),
        );

        let mut raw_data = Vec::with_capacity(2);
        raw_data.push(stream.next().await.unwrap().unwrap());
        raw_data.push(stream.next().await.unwrap().unwrap());
        assert!(stream.next().await.is_none());
        assert!(stream.done);

        let decoder = &mut FlightDecoder::default();
        let mut flight_messages = raw_data
            .into_iter()
            .map(|x| decoder.try_decode(&x).unwrap().unwrap())
            .collect::<Vec<FlightMessage>>();
        assert_eq!(flight_messages.len(), 2);

        match flight_messages.remove(0) {
            FlightMessage::Schema(actual_schema) => {
                assert_eq!(&actual_schema, schema.arrow_schema());
            }
            _ => unreachable!(),
        }

        match flight_messages.remove(0) {
            FlightMessage::RecordBatch(actual_recordbatch) => {
                assert_eq!(&actual_recordbatch, recordbatch.df_record_batch());
            }
            _ => unreachable!(),
        }
    }

    #[tokio::test]
    async fn test_flight_record_batch_stream_encodes_affected_rows() {
        let mut stream = FlightRecordBatchStream::new(
            FlightRecordBatchStreamInput::ready(FlightRecordBatchSource::AffectedRows {
                rows: 42,
                metrics: Some(r#"{"region_watermarks":[]}"#.to_string()),
            }),
            TracingContext::default(),
            FlightCompression::default(),
            QueryContext::arc(),
        );

        let data = stream.next().await.unwrap().unwrap();
        let message = FlightDecoder::default().try_decode(&data).unwrap().unwrap();
        assert!(matches!(
            message,
            FlightMessage::AffectedRows {
                rows: 42,
                metrics: Some(_),
            }
        ));
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_flight_record_batch_stream_forwards_initializer_error() {
        let mut stream = FlightRecordBatchStream::new(
            FlightRecordBatchStreamInput::initializer(async {
                Err(tonic::Status::unavailable(
                    "remote read initialization failed",
                ))
            }),
            TracingContext::default(),
            FlightCompression::default(),
            QueryContext::arc(),
        );

        let error = stream.next().await.unwrap().unwrap_err();
        assert_eq!(tonic::Code::Unavailable, error.code());
        assert!(stream.next().await.is_none());
    }
    #[tokio::test]
    async fn test_flight_record_batch_stream_emits_metrics_while_pending() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "a",
            ConcreteDataType::int32_datatype(),
            false,
        )]));
        let metrics = RecordBatchMetrics {
            elapsed_compute: 42,
            ..Default::default()
        };
        let recordbatches = Box::pin(PendingMetricsStream {
            schema: schema.clone(),
            metrics,
        });
        let query_ctx = query_context_with_live_metrics_and_matching_capability();
        let initializer_query_ctx = query_ctx.clone();
        let mut stream = FlightRecordBatchStream::new(
            FlightRecordBatchStreamInput::initializer(async move {
                initializer_query_ctx.set_explain_verbose(true);
                Ok(FlightRecordBatchSource::RecordBatches(recordbatches))
            }),
            TracingContext::default(),
            FlightCompression::default(),
            query_ctx,
        );

        let decoder = &mut FlightDecoder::default();
        let schema_data = stream.next().await.unwrap().unwrap();
        match decoder.try_decode(&schema_data).unwrap().unwrap() {
            FlightMessage::Schema(actual_schema) => {
                assert_eq!(&actual_schema, schema.arrow_schema());
            }
            _ => unreachable!(),
        }

        let metrics_data = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        match decoder.try_decode(&metrics_data).unwrap().unwrap() {
            FlightMessage::Metrics(metrics) => {
                let metrics: RecordBatchMetrics = serde_json::from_str(&metrics).unwrap();
                assert_eq!(metrics.elapsed_compute, 42);
            }
            other => panic!("expected metrics message, got {other:?}"),
        }
    }

    #[tokio::test]
    async fn test_flight_record_batch_stream_continues_after_pending_metrics() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "a",
            ConcreteDataType::int32_datatype(),
            false,
        )]));
        let metrics = RecordBatchMetrics {
            elapsed_compute: 42,
            ..Default::default()
        };
        let recordbatch = RecordBatch::new(
            schema.clone(),
            vec![Arc::new(Int32Vector::from_slice([1])) as VectorRef],
        )
        .unwrap();
        let expected_recordbatch = recordbatch.df_record_batch().clone();
        let (tx, rx) = tokio::sync::mpsc::unbounded_channel();
        let recordbatches = Box::pin(MetricsThenBatchStream {
            schema: schema.clone(),
            metrics,
            rx,
        });
        let query_ctx = query_context_with_live_metrics_and_matching_capability();
        query_ctx.set_explain_verbose(true);
        let mut stream = FlightRecordBatchStream::new(
            FlightRecordBatchStreamInput::ready(FlightRecordBatchSource::RecordBatches(
                recordbatches,
            )),
            TracingContext::default(),
            FlightCompression::default(),
            query_ctx,
        );

        let decoder = &mut FlightDecoder::default();
        let schema_data = stream.next().await.unwrap().unwrap();
        assert!(matches!(
            decoder.try_decode(&schema_data).unwrap().unwrap(),
            FlightMessage::Schema(_)
        ));

        let metrics_data = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(
            decoder.try_decode(&metrics_data).unwrap().unwrap(),
            FlightMessage::Metrics(_)
        ));

        tx.send(Ok(recordbatch)).unwrap();
        let batch_data = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        match decoder.try_decode(&batch_data).unwrap().unwrap() {
            FlightMessage::RecordBatch(actual_recordbatch) => {
                assert_eq!(&actual_recordbatch, &expected_recordbatch);
            }
            other => panic!("expected record batch after pending metrics, got {other:?}"),
        }

        drop(tx);
        let final_metrics_data = tokio::time::timeout(Duration::from_secs(2), stream.next())
            .await
            .unwrap()
            .unwrap()
            .unwrap();
        assert!(matches!(
            decoder.try_decode(&final_metrics_data).unwrap().unwrap(),
            FlightMessage::Metrics(_)
        ));
    }

    #[tokio::test]
    async fn test_flight_record_batch_stream_requires_live_metrics_for_pre_batch_metrics() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "a",
            ConcreteDataType::int32_datatype(),
            false,
        )]));
        let recordbatches = Box::pin(PendingMetricsStream {
            schema: schema.clone(),
            metrics: RecordBatchMetrics {
                elapsed_compute: 42,
                ..Default::default()
            },
        });
        let query_ctx = query_context_with_matching_capability();
        query_ctx.set_explain_verbose(true);
        let mut stream = FlightRecordBatchStream::new(
            FlightRecordBatchStreamInput::ready(FlightRecordBatchSource::RecordBatches(
                recordbatches,
            )),
            TracingContext::default(),
            FlightCompression::default(),
            query_ctx,
        );

        let decoder = &mut FlightDecoder::default();
        let schema_data = stream.next().await.unwrap().unwrap();
        assert!(matches!(
            decoder.try_decode(&schema_data).unwrap().unwrap(),
            FlightMessage::Schema(_)
        ));
        assert!(
            tokio::time::timeout(
                FLIGHT_METRICS_HEARTBEAT_INTERVAL + Duration::from_millis(200),
                stream.next()
            )
            .await
            .is_err(),
            "pre-batch Metrics must be gated by live analyze metrics"
        );
    }

    #[tokio::test]
    async fn test_flight_record_batch_stream_rejects_spoofed_live_metrics_for_pre_batch_metrics() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "a",
            ConcreteDataType::int32_datatype(),
            false,
        )]));
        let recordbatches = Box::pin(PendingMetricsStream {
            schema: schema.clone(),
            metrics: RecordBatchMetrics {
                elapsed_compute: 42,
                ..Default::default()
            },
        });
        let query_ctx = query_context_with_live_metrics_and_matching_capability();
        let mut query_ctx = (*query_ctx).clone();
        query_ctx.set_extension(LIVE_ANALYZE_METRICS_EXTENSION_KEY, "true");
        let query_ctx = Arc::new(query_ctx);
        query_ctx.set_explain_verbose(true);
        let mut stream = FlightRecordBatchStream::new(
            FlightRecordBatchStreamInput::ready(FlightRecordBatchSource::RecordBatches(
                recordbatches,
            )),
            TracingContext::default(),
            FlightCompression::default(),
            query_ctx,
        );

        let decoder = &mut FlightDecoder::default();
        let schema_data = stream.next().await.unwrap().unwrap();
        assert!(matches!(
            decoder.try_decode(&schema_data).unwrap().unwrap(),
            FlightMessage::Schema(_)
        ));
        assert!(
            tokio::time::timeout(
                FLIGHT_METRICS_HEARTBEAT_INTERVAL + Duration::from_millis(200),
                stream.next()
            )
            .await
            .is_err(),
            "pre-batch Metrics must reject spoofed live analyze metrics"
        );
    }

    #[tokio::test]
    async fn test_flight_record_batch_stream_requires_explain_verbose_for_pre_batch_metrics() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "a",
            ConcreteDataType::int32_datatype(),
            false,
        )]));
        let recordbatches = Box::pin(PendingMetricsStream {
            schema: schema.clone(),
            metrics: RecordBatchMetrics {
                elapsed_compute: 42,
                ..Default::default()
            },
        });
        let query_ctx = query_context_with_matching_capability();
        let mut stream = FlightRecordBatchStream::new(
            FlightRecordBatchStreamInput::ready(FlightRecordBatchSource::RecordBatches(
                recordbatches,
            )),
            TracingContext::default(),
            FlightCompression::default(),
            query_ctx,
        );

        let decoder = &mut FlightDecoder::default();
        let schema_data = stream.next().await.unwrap().unwrap();
        assert!(matches!(
            decoder.try_decode(&schema_data).unwrap().unwrap(),
            FlightMessage::Schema(_)
        ));
        assert!(
            tokio::time::timeout(
                FLIGHT_METRICS_HEARTBEAT_INTERVAL + Duration::from_millis(200),
                stream.next()
            )
            .await
            .is_err(),
            "pre-batch Metrics must be gated by explain verbose even when capability is set"
        );
    }
}
