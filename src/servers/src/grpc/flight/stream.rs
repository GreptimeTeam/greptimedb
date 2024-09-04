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

use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use arrow_flight::FlightData;
use common_grpc::flight::{FlightEncoder, FlightMessage};
use common_recordbatch::{RecordBatch, SendableRecordBatchStream};
use common_telemetry::tracing::{info_span, Instrument};
use common_telemetry::tracing_context::{FutureExt, TracingContext};
use common_telemetry::warn;
use datatypes::prelude::ConcreteDataType;
use datatypes::schema::{ColumnSchema, Schema};
use datatypes::vectors::{BinaryVector, Vector, VectorRef};
use futures::channel::mpsc;
use futures::channel::mpsc::Sender;
use futures::{SinkExt, Stream, StreamExt};
use pin_project::{pin_project, pinned_drop};
use snafu::ResultExt;
use tokio::task::JoinHandle;

use super::TonicResult;
use crate::error;

#[pin_project(PinnedDrop)]
pub struct FlightRecordBatchStream {
    #[pin]
    rx: mpsc::Receiver<Result<FlightMessage, tonic::Status>>,
    join_handle: JoinHandle<()>,
    done: bool,
    encoder: FlightEncoder,
}

impl FlightRecordBatchStream {
    pub fn new(
        recordbatches: SendableRecordBatchStream,
        tracing_context: TracingContext,
        // If its needed to serialize jsons to strings
        serialize_json: bool,
    ) -> Self {
        let (tx, rx) = mpsc::channel::<TonicResult<FlightMessage>>(1);
        let join_handle = common_runtime::spawn_global(async move {
            Self::flight_data_stream(recordbatches, tx, serialize_json)
                .trace(tracing_context.attach(info_span!("flight_data_stream")))
                .await
        });
        Self {
            rx,
            join_handle,
            done: false,
            encoder: FlightEncoder::default(),
        }
    }

    async fn flight_data_stream(
        mut recordbatches: SendableRecordBatchStream,
        mut tx: Sender<TonicResult<FlightMessage>>,
        change_json: bool,
    ) {
        let schema = recordbatches.schema();
        let schema_contains_json = schema
            .column_schemas()
            .iter()
            .any(|x| x.data_type.is_json());
        let schema_without_json = if schema_contains_json && change_json {
            Arc::new(change_json_column_to_string_in_schema(schema.clone()))
        } else {
            schema.clone()
        };
        if let Err(e) = tx
            .send(Ok(FlightMessage::Schema(schema_without_json.clone())))
            .await
        {
            warn!(e; "stop sending Flight data");
            return;
        }

        while let Some(batch_or_err) = recordbatches.next().in_current_span().await {
            match batch_or_err {
                Ok(recordbatch) => {
                    // If the schema contains json, we need to serialize the data before sending it
                    let recordbatch = if schema_contains_json && change_json {
                        serialize_json_column(schema.clone(), &recordbatch)
                    } else {
                        recordbatch
                    };
                    if let Err(e) = tx.send(Ok(FlightMessage::Recordbatch(recordbatch))).await {
                        warn!(e; "stop sending Flight data");
                        return;
                    }
                }
                Err(e) => {
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
            match this.rx.poll_next(cx) {
                Poll::Ready(None) => {
                    *this.done = true;
                    Poll::Ready(None)
                }
                Poll::Ready(Some(result)) => match result {
                    Ok(flight_message) => {
                        let flight_data = this.encoder.encode(flight_message);
                        Poll::Ready(Some(Ok(flight_data)))
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

/// Column of json type in record batches needs to be serialized before sending
/// to the client. This function changes the json column in schema to string column.
fn change_json_column_to_string_in_schema(schema: Arc<Schema>) -> Schema {
    let column_schemas = schema
        .column_schemas()
        .iter()
        .map(|column| {
            if column.data_type.is_json() {
                ColumnSchema::new(
                    column.name.clone(),
                    ConcreteDataType::string_datatype(),
                    column.is_nullable(),
                )
            } else {
                column.clone()
            }
        })
        .collect();
    Schema::new(column_schemas)
}

/// Column of json type in record batches needs to be serialized before sending
/// to the client. This function serializes the binary data of json type to string.
fn serialize_json_column(schema: Arc<Schema>, recordbatch: &RecordBatch) -> RecordBatch {
    let mut column_schemas = Vec::with_capacity(schema.column_schemas().len());
    let mut vectors: Vec<Arc<dyn Vector>> = Vec::with_capacity(schema.column_schemas().len());
    for (i, column) in schema.column_schemas().iter().enumerate() {
        let vector = recordbatch.column(i);
        if column.data_type.is_json() {
            // Safety: json column with binary vector
            let vector = vector.as_any().downcast_ref::<BinaryVector>().unwrap();
            let vector = vector.from_json_vector_to_string_vector();
            vectors.push(Arc::new(vector) as VectorRef);
            column_schemas.push(ColumnSchema::new(
                column.name.clone(),
                ConcreteDataType::string_datatype(),
                column.is_nullable(),
            ));
        } else {
            vectors.push(vector.clone());
            column_schemas.push(column.clone());
        }
    }

    // Safety: only changed a column type from binary to string
    RecordBatch::new(Arc::new(Schema::new(column_schemas)), vectors).unwrap()
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

        let v: VectorRef = Arc::new(Int32Vector::from_slice([1, 2]));
        let recordbatch = RecordBatch::new(schema.clone(), vec![v]).unwrap();

        let recordbatches = RecordBatches::try_new(schema.clone(), vec![recordbatch.clone()])
            .unwrap()
            .as_stream();
        let mut stream =
            FlightRecordBatchStream::new(recordbatches, TracingContext::default(), true);

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
