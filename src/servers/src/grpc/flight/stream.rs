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
use std::pin::Pin;
use std::task::{Context, Poll};
use std::time::{Duration, Instant};

use arrow_flight::FlightData;
use common_error::ext::ErrorExt;
use common_grpc::flight::{FlightEncoder, FlightMessage};
use common_recordbatch::SendableRecordBatchStream;
use common_telemetry::tracing::{Instrument, info_span};
use common_telemetry::tracing_context::{FutureExt, TracingContext};
use common_telemetry::{error, info, warn};
use futures::channel::mpsc;
use futures::channel::mpsc::Sender;
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

    pub fn new(
        recordbatches: SendableRecordBatchStream,
        tracing_context: TracingContext,
        compression: FlightCompression,
        query_ctx: QueryContextRef,
    ) -> Self {
        let should_send_partial_metrics = query_ctx.explain_verbose();
        let can_send_metrics_before_batch = query_ctx
            .extension(SUPPORT_FLIGHT_METRICS_BEFORE_BATCH_EXTENSION_KEY)
            .is_some_and(|value| value.eq_ignore_ascii_case("true"));
        let (tx, rx) = mpsc::channel::<TonicResult<FlightMessage>>(1);
        let join_handle = common_runtime::spawn_global(async move {
            Self::flight_data_stream(
                recordbatches,
                tx,
                should_send_partial_metrics,
                can_send_metrics_before_batch,
            )
            .trace(tracing_context.attach(info_span!("flight_data_stream")))
            .await
        });
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

        let schema = recordbatches.schema().arrow_schema().clone();
        let start = Instant::now();
        if let Err(e) = tx.send(Ok(FlightMessage::Schema(schema))).await {
            warn!(e; "stop sending Flight data");
            return;
        }
        metrics.send_schema_duration += start.elapsed();

        loop {
            let start = Instant::now();
            let batch_or_err = if should_send_partial_metrics && can_send_metrics_before_batch {
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
                                &mut tx,
                                &mut metrics,
                                &mut last_metrics_str,
                                metrics_str,
                            )
                            .await
                        {
                            return;
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
                        return;
                    }
                    metrics.send_record_batch_duration += start.elapsed();

                    if should_send_partial_metrics
                        && let Some(metrics_str) = recordbatches
                            .metrics()
                            .and_then(|m| serde_json::to_string(&m).ok())
                        && {
                            last_metrics_str = Some(metrics_str.clone());
                            !Self::send_metrics(&mut tx, &mut metrics, metrics_str).await
                        }
                    {
                        return;
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
                    return;
                }
            }
        }
        // make last package to pass metrics
        if let Some(metrics_str) = recordbatches
            .metrics()
            .and_then(|m| serde_json::to_string(&m).ok())
        {
            let _ = Self::send_metrics(&mut tx, &mut metrics, metrics_str).await;
        }
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
    use common_recordbatch::{OrderOption, RecordBatch, RecordBatchStream, RecordBatches};
    use datatypes::prelude::*;
    use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
    use datatypes::vectors::Int32Vector;
    use futures::StreamExt;
    use session::context::{
        QueryContext, QueryContextBuilder, SUPPORT_FLIGHT_METRICS_BEFORE_BATCH_EXTENSION_KEY,
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
            recordbatches,
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
        let query_ctx = Arc::new(
            QueryContextBuilder::default()
                .set_extension(
                    SUPPORT_FLIGHT_METRICS_BEFORE_BATCH_EXTENSION_KEY.to_string(),
                    "true".to_string(),
                )
                .build(),
        );
        query_ctx.set_explain_verbose(true);
        let mut stream = FlightRecordBatchStream::new(
            recordbatches,
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
        let query_ctx = Arc::new(
            QueryContextBuilder::default()
                .set_extension(
                    SUPPORT_FLIGHT_METRICS_BEFORE_BATCH_EXTENSION_KEY.to_string(),
                    "true".to_string(),
                )
                .build(),
        );
        query_ctx.set_explain_verbose(true);
        let mut stream = FlightRecordBatchStream::new(
            recordbatches,
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
    }

    #[tokio::test]
    async fn test_flight_record_batch_stream_requires_capability_for_pre_batch_metrics() {
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
        let query_ctx = QueryContext::arc();
        query_ctx.set_explain_verbose(true);
        let mut stream = FlightRecordBatchStream::new(
            recordbatches,
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
            "pre-batch Metrics must be gated by client capability"
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
        let query_ctx = Arc::new(
            QueryContextBuilder::default()
                .set_extension(
                    SUPPORT_FLIGHT_METRICS_BEFORE_BATCH_EXTENSION_KEY.to_string(),
                    "true".to_string(),
                )
                .build(),
        );
        let mut stream = FlightRecordBatchStream::new(
            recordbatches,
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
