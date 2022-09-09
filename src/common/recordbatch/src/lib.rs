pub mod error;
mod recordbatch;
pub mod util;

use std::pin::Pin;

use datatypes::schema::SchemaRef;
use error::Result;
use futures::task::{Context, Poll};
use futures::Stream;
pub use recordbatch::RecordBatch;
use snafu::ensure;

pub trait RecordBatchStream: Stream<Item = Result<RecordBatch>> {
    fn schema(&self) -> SchemaRef;
}

pub type SendableRecordBatchStream = Pin<Box<dyn RecordBatchStream + Send>>;

/// EmptyRecordBatchStream can be used to create a RecordBatchStream
/// that will produce no results
pub struct EmptyRecordBatchStream {
    /// Schema wrapped by Arc
    schema: SchemaRef,
}

impl EmptyRecordBatchStream {
    /// Create an empty RecordBatchStream
    pub fn new(schema: SchemaRef) -> Self {
        Self { schema }
    }
}

impl RecordBatchStream for EmptyRecordBatchStream {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for EmptyRecordBatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        Poll::Ready(None)
    }
}

pub struct RecordBatches {
    schema: SchemaRef,
    batches: Vec<RecordBatch>,
}

impl RecordBatches {
    pub fn try_new(schema: SchemaRef, batches: Vec<RecordBatch>) -> Result<Self> {
        for batch in batches.iter() {
            ensure!(
                batch.schema == schema,
                error::CreateRecordBatchesSnafu {
                    reason: format!(
                        "expect RecordBatch schema equals {:?}, actual: {:?}",
                        schema, batch.schema
                    )
                }
            )
        }
        Ok(Self { schema, batches })
    }

    pub fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    pub fn to_vec(self) -> Vec<RecordBatch> {
        self.batches
    }
}
