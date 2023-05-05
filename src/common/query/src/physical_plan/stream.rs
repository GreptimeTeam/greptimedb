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
use std::task::Context;

use common_recordbatch::error::Result;
use common_recordbatch::{RecordBatch, RecordBatchStream, SendableRecordBatchStream};
use datatypes::schema::SchemaRef;
use futures::{Stream, StreamExt};
use tokio::sync::mpsc::Receiver;
use tokio::task::JoinHandle;
use tokio_stream::wrappers::ReceiverStream;

// TODO(fys): add some docs
pub struct RecordBatchReceiverStream {
    schema: SchemaRef,

    inner: ReceiverStream<Result<RecordBatch>>,

    join_handle: JoinHandle<()>,
}

impl RecordBatchReceiverStream {
    pub fn create(
        schema: &SchemaRef,
        rx: Receiver<Result<RecordBatch>>,
        join_handle: JoinHandle<()>,
    ) -> SendableRecordBatchStream {
        let schema = schema.clone();
        let inner = ReceiverStream::new(rx);

        let stream = RecordBatchReceiverStream {
            schema,
            inner,
            join_handle,
        };

        Box::pin(stream)
    }
}

impl Stream for RecordBatchReceiverStream {
    type Item = Result<RecordBatch>;

    fn poll_next(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> std::task::Poll<Option<Self::Item>> {
        self.inner.poll_next_unpin(cx)
    }
}

impl RecordBatchStream for RecordBatchReceiverStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Drop for RecordBatchReceiverStream {
    fn drop(&mut self) {
        self.join_handle.abort();
    }
}
