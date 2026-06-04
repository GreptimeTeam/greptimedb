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
use std::task::{Context, Poll};

use common_recordbatch::adapter::RecordBatchMetrics;
use common_recordbatch::error::Result as RecordBatchResult;
use common_recordbatch::{OrderOption, RecordBatch, RecordBatchStream};
use datatypes::schema::SchemaRef;
use futures_util::Stream;
use tokio::sync::mpsc;

pub type QueryRuntimeSender = mpsc::Sender<RecordBatchResult<RecordBatch>>;

/// A record batch stream backed by batches produced on the datanode query runtime.
pub struct QueryRuntimeStream {
    schema: SchemaRef,
    receiver: mpsc::Receiver<RecordBatchResult<RecordBatch>>,
    output_ordering: Option<Vec<OrderOption>>,
    metrics: Option<RecordBatchMetrics>,
}

impl QueryRuntimeStream {
    pub fn new(schema: SchemaRef, receiver: mpsc::Receiver<RecordBatchResult<RecordBatch>>) -> Self {
        Self {
            schema,
            receiver,
            output_ordering: None,
            metrics: None,
        }
    }

    pub fn with_output_ordering(mut self, output_ordering: Option<Vec<OrderOption>>) -> Self {
        self.output_ordering = output_ordering;
        self
    }

    pub fn with_metrics(mut self, metrics: Option<RecordBatchMetrics>) -> Self {
        self.metrics = metrics;
        self
    }
}

impl Stream for QueryRuntimeStream {
    type Item = RecordBatchResult<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.receiver.poll_recv(cx)
    }
}

impl RecordBatchStream for QueryRuntimeStream {
    fn name(&self) -> &str {
        "QueryRuntimeStream"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_ordering(&self) -> Option<&[OrderOption]> {
        self.output_ordering.as_deref()
    }

    fn metrics(&self) -> Option<RecordBatchMetrics> {
        self.metrics.clone()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_recordbatch::error::CreateRecordBatchesSnafu;
    use datatypes::prelude::{ConcreteDataType, VectorRef};
    use datatypes::schema::{ColumnSchema, Schema};
    use datatypes::vectors::Int32Vector;
    use futures_util::StreamExt;

    use super::*;

    fn test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "v",
            ConcreteDataType::int32_datatype(),
            false,
        )]));
        let values: VectorRef = Arc::new(Int32Vector::from_slice([1]));
        RecordBatch::new(schema, vec![values]).unwrap()
    }

    #[tokio::test]
    async fn test_query_runtime_stream_receives_batches() {
        let batch = test_batch();
        let schema = batch.schema.clone();
        let (tx, rx) = mpsc::channel(1);
        tx.send(Ok(batch)).await.unwrap();
        drop(tx);

        let mut stream = QueryRuntimeStream::new(schema, rx);
        let batch = stream.next().await.unwrap().unwrap();
        assert_eq!(1, batch.num_rows());
        assert!(stream.next().await.is_none());
    }

    #[tokio::test]
    async fn test_query_runtime_stream_forwards_errors() {
        let schema = test_batch().schema.clone();
        let (tx, rx) = mpsc::channel(1);
        tx.send(Err(CreateRecordBatchesSnafu { reason: "test error" }.build()))
            .await
            .unwrap();
        drop(tx);

        let mut stream = QueryRuntimeStream::new(schema, rx);
        assert!(stream.next().await.unwrap().is_err());
    }

    #[tokio::test]
    async fn test_query_runtime_stream_close_stops_sender() {
        let schema = test_batch().schema.clone();
        let (tx, rx) = mpsc::channel(1);
        let stream = QueryRuntimeStream::new(schema, rx);
        drop(stream);

        assert!(tx.send(Ok(test_batch())).await.is_err());
    }
}
