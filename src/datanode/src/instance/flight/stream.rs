// Copyright 2022 Greptime Team
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

use std::pin::Pin;
use std::task::{Context, Poll};

use arrow_flight::utils::flight_data_from_arrow_batch;
use arrow_flight::{FlightData, SchemaAsIpc};
use common_recordbatch::SendableRecordBatchStream;
use common_telemetry::warn;
use datatypes::arrow;
use futures::channel::mpsc;
use futures::channel::mpsc::Sender;
use futures::{SinkExt, Stream, StreamExt};
use pin_project::{pin_project, pinned_drop};
use snafu::ResultExt;
use tokio::task::JoinHandle;

use crate::error;
use crate::instance::flight::TonicResult;

#[pin_project(PinnedDrop)]
pub(super) struct FlightRecordBatchStream {
    #[pin]
    rx: mpsc::Receiver<Result<FlightData, tonic::Status>>,
    join_handle: JoinHandle<()>,
    done: bool,
}

impl FlightRecordBatchStream {
    pub(super) fn new(recordbatches: SendableRecordBatchStream) -> Self {
        let (tx, rx) = mpsc::channel::<TonicResult<FlightData>>(1);
        let join_handle =
            common_runtime::spawn_read(
                async move { Self::flight_data_stream(recordbatches, tx).await },
            );
        Self {
            rx,
            join_handle,
            done: false,
        }
    }

    async fn flight_data_stream(
        mut recordbatches: SendableRecordBatchStream,
        mut tx: Sender<TonicResult<FlightData>>,
    ) {
        let schema = recordbatches.schema();
        let options = arrow::ipc::writer::IpcWriteOptions::default();
        let schema_flight_data: FlightData =
            SchemaAsIpc::new(schema.arrow_schema(), &options).into();
        if let Err(e) = tx.send(Ok(schema_flight_data)).await {
            warn!("stop sending Flight data, err: {e}");
            return;
        }

        while let Some(batch_or_err) = recordbatches.next().await {
            match batch_or_err {
                Ok(batch) => {
                    let (flight_dictionaries, flight_batch) =
                        flight_data_from_arrow_batch(batch.df_record_batch(), &options);

                    // TODO(LFC): Handle dictionary as FlightData here, when we supported Arrow's Dictionary DataType.
                    // Currently we don't have a datatype corresponding to Arrow's Dictionary DataType,
                    // so there won't be any "dictionaries" here. Assert to be sure about it, and
                    // perform a "testing guard" in case we forgot to handle the possible "dictionaries"
                    // here in the future.
                    debug_assert_eq!(flight_dictionaries.len(), 0);

                    if let Err(e) = tx.send(Ok(flight_batch)).await {
                        warn!("stop sending Flight data, err: {e}");
                        return;
                    }
                }
                Err(e) => {
                    let e = Err(e).context(error::PollRecordbatchStreamSnafu);
                    if let Err(e) = tx.send(e.map_err(|x| x.into())).await {
                        warn!("stop sending Flight data, err: {e}");
                    }
                    return;
                }
            }
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
            match this.rx.poll_next(cx) {
                Poll::Ready(None) => {
                    *this.done = true;
                    Poll::Ready(None)
                }
                e @ Poll::Ready(Some(Err(_))) => {
                    *this.done = true;
                    e
                }
                other => other,
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

    use super::*;

    #[tokio::test]
    async fn test_flight_record_batch_stream() {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "a",
            ConcreteDataType::int32_datatype(),
            false,
        )]));

        let v: VectorRef = Arc::new(Int32Vector::from_slice(&[1, 2]));
        let recordbatch = RecordBatch::new(schema.clone(), vec![v]).unwrap();

        let recordbatches = RecordBatches::try_new(schema.clone(), vec![recordbatch.clone()])
            .unwrap()
            .as_stream();
        let mut stream = FlightRecordBatchStream::new(recordbatches);

        let mut raw_data = Vec::with_capacity(2);
        raw_data.push(stream.next().await.unwrap().unwrap());
        raw_data.push(stream.next().await.unwrap().unwrap());
        assert!(stream.next().await.is_none());
        assert!(stream.done);

        let decoder = &mut FlightDecoder::default();
        let mut flight_messages = raw_data
            .into_iter()
            .map(|x| decoder.try_decode(x).unwrap())
            .collect::<Vec<FlightMessage>>();
        assert_eq!(flight_messages.len(), 2);

        match flight_messages.remove(0) {
            FlightMessage::Schema(actual_schema) => {
                assert_eq!(actual_schema, schema);
            }
            _ => unreachable!(),
        }

        match flight_messages.remove(0) {
            FlightMessage::Recordbatch(actual_recordbatch) => {
                assert_eq!(actual_recordbatch, recordbatch);
            }
            _ => unreachable!(),
        }
    }
}
