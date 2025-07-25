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

use arrow_flight::FlightData;
use common_error::ext::ErrorExt;
use common_grpc::flight::{FlightEncoder, FlightMessage};
use common_recordbatch::SendableRecordBatchStream;
use common_telemetry::tracing::{info_span, Instrument};
use common_telemetry::tracing_context::{FutureExt, TracingContext};
use common_telemetry::{error, warn};
use futures::channel::mpsc;
use futures::channel::mpsc::Sender;
use futures::{SinkExt, Stream, StreamExt};
use pin_project::{pin_project, pinned_drop};
use session::context::QueryContextRef;
use snafu::ResultExt;
use tokio::task::JoinHandle;

use crate::error;
use crate::grpc::flight::TonicResult;
use crate::grpc::FlightCompression;

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
    pub fn new(
        recordbatches: SendableRecordBatchStream,
        tracing_context: TracingContext,
        compression: FlightCompression,
        query_ctx: QueryContextRef,
    ) -> Self {
        let should_send_partial_metrics = query_ctx.explain_verbose();
        let (tx, rx) = mpsc::channel::<TonicResult<FlightMessage>>(1);
        let join_handle = common_runtime::spawn_global(async move {
            Self::flight_data_stream(recordbatches, tx, should_send_partial_metrics)
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
    ) {
        let schema = recordbatches.schema().arrow_schema().clone();
        if let Err(e) = tx.send(Ok(FlightMessage::Schema(schema))).await {
            warn!(e; "stop sending Flight data");
            return;
        }

        while let Some(batch_or_err) = recordbatches.next().in_current_span().await {
            match batch_or_err {
                Ok(recordbatch) => {
                    if let Err(e) = tx
                        .send(Ok(FlightMessage::RecordBatch(
                            recordbatch.into_df_record_batch(),
                        )))
                        .await
                    {
                        warn!(e; "stop sending Flight data");
                        return;
                    }
                    if should_send_partial_metrics {
                        if let Some(metrics) = recordbatches
                            .metrics()
                            .and_then(|m| serde_json::to_string(&m).ok())
                        {
                            if let Err(e) = tx.send(Ok(FlightMessage::Metrics(metrics))).await {
                                warn!(e; "stop sending Flight data");
                                return;
                            }
                        }
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
            let _ = tx.send(Ok(FlightMessage::Metrics(metrics_str))).await;
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
    use std::sync::Arc;

    use common_grpc::flight::{FlightDecoder, FlightMessage};
    use common_recordbatch::{RecordBatch, RecordBatches};
    use datatypes::prelude::*;
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::Int32Vector;
    use futures::StreamExt;
    use session::context::QueryContext;

    use super::*;

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
}
