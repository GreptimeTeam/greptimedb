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
