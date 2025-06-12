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

use catalog::process_manager::Ticket;
use common_recordbatch::adapter::RecordBatchMetrics;
use common_recordbatch::{OrderOption, RecordBatch, RecordBatchStream, SendableRecordBatchStream};
use datatypes::schema::SchemaRef;
use futures::Stream;

pub struct CancellableStreamWrapper {
    inner: SendableRecordBatchStream,
    ticket: Ticket,
}

impl Unpin for CancellableStreamWrapper {}

impl CancellableStreamWrapper {
    pub fn new(stream: SendableRecordBatchStream, ticket: Ticket) -> Self {
        Self {
            inner: stream,
            ticket,
        }
    }
}

impl Stream for CancellableStreamWrapper {
    type Item = common_recordbatch::error::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        if this.ticket.cancellation_handler.is_cancelled() {
            return Poll::Ready(Some(common_recordbatch::error::StreamCancelledSnafu.fail()));
        }

        if let Poll::Ready(res) = Pin::new(&mut this.inner).poll_next(cx) {
            return Poll::Ready(res);
        }

        // on pending, register cancellation waker.
        this.ticket
            .cancellation_handler
            .waker()
            .register(cx.waker());
        // check if canceled again.
        if this.ticket.cancellation_handler.is_cancelled() {
            return Poll::Ready(Some(common_recordbatch::error::StreamCancelledSnafu.fail()));
        }
        Poll::Pending
    }
}

impl RecordBatchStream for CancellableStreamWrapper {
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
    use std::pin::Pin;
    use std::sync::Arc;
    use std::task::{Context, Poll};
    use std::time::Duration;

    use catalog::process_manager::ProcessManager;
    use common_recordbatch::adapter::RecordBatchMetrics;
    use common_recordbatch::{OrderOption, RecordBatch, RecordBatchStream};
    use datatypes::data_type::ConcreteDataType;
    use datatypes::prelude::VectorRef;
    use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
    use datatypes::vectors::Int32Vector;
    use futures::{Stream, StreamExt};
    use tokio::time::{sleep, timeout};

    use super::CancellableStreamWrapper;

    // Mock stream for testing
    struct MockRecordBatchStream {
        schema: SchemaRef,
        batches: Vec<common_recordbatch::error::Result<RecordBatch>>,
        current: usize,
        delay: Option<Duration>,
    }

    impl MockRecordBatchStream {
        fn new(batches: Vec<common_recordbatch::error::Result<RecordBatch>>) -> Self {
            let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
                "test_col",
                ConcreteDataType::int32_datatype(),
                false,
            )]));

            Self {
                schema,
                batches,
                current: 0,
                delay: None,
            }
        }

        fn with_delay(mut self, delay: Duration) -> Self {
            self.delay = Some(delay);
            self
        }
    }

    impl Stream for MockRecordBatchStream {
        type Item = common_recordbatch::error::Result<RecordBatch>;

        fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            if let Some(delay) = self.delay {
                // Simulate async delay
                let waker = cx.waker().clone();
                let delay_clone = delay;
                tokio::spawn(async move {
                    sleep(delay_clone).await;
                    waker.wake();
                });
                self.delay = None; // Only delay once
                return Poll::Pending;
            }

            if self.current >= self.batches.len() {
                return Poll::Ready(None);
            }

            let batch = self.batches[self.current].as_ref().unwrap().clone();
            self.current += 1;
            Poll::Ready(Some(Ok(batch)))
        }
    }

    impl RecordBatchStream for MockRecordBatchStream {
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

    fn create_test_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "test_col",
            ConcreteDataType::int32_datatype(),
            false,
        )]));
        RecordBatch::new(
            schema,
            vec![Arc::new(Int32Vector::from_values(0..3)) as VectorRef],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_stream_completes_normally() {
        let batch = create_test_batch();
        let mock_stream = MockRecordBatchStream::new(vec![Ok(batch.clone())]);
        let process_manager = Arc::new(ProcessManager::new("".to_string(), None));
        let ticket = process_manager.register_query(
            "catalog".to_string(),
            vec![],
            "query".to_string(),
            "client".to_string(),
            None,
        );

        let mut cancellable_stream = CancellableStreamWrapper::new(Box::pin(mock_stream), ticket);

        let result = cancellable_stream.next().await;
        assert!(result.is_some());
        assert!(result.unwrap().is_ok());

        let end_result = cancellable_stream.next().await;
        assert!(end_result.is_none());
    }

    #[tokio::test]
    async fn test_stream_cancelled_before_start() {
        let batch = create_test_batch();
        let mock_stream = MockRecordBatchStream::new(vec![Ok(batch)]);
        let process_manager = Arc::new(ProcessManager::new("".to_string(), None));
        let ticket = process_manager.register_query(
            "catalog".to_string(),
            vec![],
            "query".to_string(),
            "client".to_string(),
            None,
        );

        // Cancel before creating the wrapper
        ticket.cancellation_handler.cancel();

        let mut cancellable_stream = CancellableStreamWrapper::new(Box::pin(mock_stream), ticket);

        let result = cancellable_stream.next().await;
        assert!(result.is_some());
        assert!(result.unwrap().is_err());
    }

    #[tokio::test]
    async fn test_stream_cancelled_during_execution() {
        let batch = create_test_batch();
        let mock_stream =
            MockRecordBatchStream::new(vec![Ok(batch)]).with_delay(Duration::from_millis(100));
        let process_manager = Arc::new(ProcessManager::new("".to_string(), None));
        let ticket = process_manager.register_query(
            "catalog".to_string(),
            vec![],
            "query".to_string(),
            "client".to_string(),
            None,
        );
        let cancellation_handle = ticket.cancellation_handler.clone();

        let mut cancellable_stream = CancellableStreamWrapper::new(Box::pin(mock_stream), ticket);

        // Cancel after a short delay
        tokio::spawn(async move {
            sleep(Duration::from_millis(50)).await;
            cancellation_handle.cancel();
        });

        let result = cancellable_stream.next().await;
        assert!(result.is_some());
        assert!(result.unwrap().is_err());
    }

    #[tokio::test]
    async fn test_stream_completes_before_cancellation() {
        let batch = create_test_batch();
        let mock_stream = MockRecordBatchStream::new(vec![Ok(batch.clone())]);
        let process_manager = Arc::new(ProcessManager::new("".to_string(), None));
        let ticket = process_manager.register_query(
            "catalog".to_string(),
            vec![],
            "query".to_string(),
            "client".to_string(),
            None,
        );
        let cancellation_handle = ticket.cancellation_handler.clone();

        let mut cancellable_stream = CancellableStreamWrapper::new(Box::pin(mock_stream), ticket);

        // Try to cancel after the stream should have completed
        tokio::spawn(async move {
            sleep(Duration::from_millis(100)).await;
            cancellation_handle.cancel();
        });

        let result = cancellable_stream.next().await;
        assert!(result.is_some());
        assert!(result.unwrap().is_ok());
    }

    #[tokio::test]
    async fn test_multiple_batches() {
        let batch1 = create_test_batch();
        let batch2 = create_test_batch();
        let mock_stream = MockRecordBatchStream::new(vec![Ok(batch1), Ok(batch2)]);
        let process_manager = Arc::new(ProcessManager::new("".to_string(), None));
        let ticket = process_manager.register_query(
            "catalog".to_string(),
            vec![],
            "query".to_string(),
            "client".to_string(),
            None,
        );

        let mut cancellable_stream = CancellableStreamWrapper::new(Box::pin(mock_stream), ticket);

        // First batch
        let result1 = cancellable_stream.next().await;
        assert!(result1.is_some());
        assert!(result1.unwrap().is_ok());

        // Second batch
        let result2 = cancellable_stream.next().await;
        assert!(result2.is_some());
        assert!(result2.unwrap().is_ok());

        // End of stream
        let end_result = cancellable_stream.next().await;
        assert!(end_result.is_none());
    }

    #[tokio::test]
    async fn test_record_batch_stream_methods() {
        let batch = create_test_batch();
        let mock_stream = MockRecordBatchStream::new(vec![Ok(batch)]);
        let process_manager = Arc::new(ProcessManager::new("".to_string(), None));
        let ticket = process_manager.register_query(
            "catalog".to_string(),
            vec![],
            "query".to_string(),
            "client".to_string(),
            None,
        );

        let cancellable_stream = CancellableStreamWrapper::new(Box::pin(mock_stream), ticket);

        // Test schema method
        let schema = cancellable_stream.schema();
        assert_eq!(schema.column_schemas().len(), 1);
        assert_eq!(schema.column_schemas()[0].name, "test_col");

        // Test output_ordering method
        assert!(cancellable_stream.output_ordering().is_none());

        // Test metrics method
        assert!(cancellable_stream.metrics().is_none());
    }

    #[tokio::test]
    async fn test_cancellation_during_pending_poll() {
        let batch = create_test_batch();
        let mock_stream =
            MockRecordBatchStream::new(vec![Ok(batch)]).with_delay(Duration::from_millis(200));
        let process_manager = Arc::new(ProcessManager::new("".to_string(), None));
        let ticket = process_manager.register_query(
            "catalog".to_string(),
            vec![],
            "query".to_string(),
            "client".to_string(),
            None,
        );
        let cancellation_handle = ticket.cancellation_handler.clone();

        let mut cancellable_stream = CancellableStreamWrapper::new(Box::pin(mock_stream), ticket);

        // Cancel while the stream is pending
        tokio::spawn(async move {
            sleep(Duration::from_millis(50)).await;
            cancellation_handle.cancel();
        });

        let result = timeout(Duration::from_millis(300), cancellable_stream.next()).await;
        assert!(result.is_ok());
        let stream_result = result.unwrap();
        assert!(stream_result.is_some());
        assert!(stream_result.unwrap().is_err());
    }
}
