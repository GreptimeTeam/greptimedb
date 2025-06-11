use std::pin::Pin;
use std::task::{Context, Poll};

use common_recordbatch::adapter::RecordBatchMetrics;
use common_recordbatch::{OrderOption, RecordBatch, RecordBatchStream, SendableRecordBatchStream};
use datatypes::schema::SchemaRef;
use futures::Stream;

pub struct StreamWrapper<T> {
    inner: SendableRecordBatchStream,
    _attachment: T,
}

impl<T> Unpin for StreamWrapper<T> {}

impl<T> StreamWrapper<T> {
    pub fn new(stream: SendableRecordBatchStream, attachment: T) -> Self {
        Self {
            inner: stream,
            _attachment: attachment,
        }
    }
}

impl<T> Stream for StreamWrapper<T> {
    type Item = common_recordbatch::error::Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let this = &mut *self;
        Pin::new(&mut this.inner).poll_next(cx)
    }
}

impl<T> RecordBatchStream for StreamWrapper<T> {
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
