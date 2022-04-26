pub mod error;
mod recordbatch;

use std::pin::Pin;

use datatypes::schema::SchemaRef;
use error::Result;
use futures::Stream;
pub use recordbatch::RecordBatch;

pub trait RecordBatchStream: Stream<Item = Result<RecordBatch>> {
    fn schema(&self) -> SchemaRef;
}

pub type SendableRecordBatchStream = Pin<Box<dyn RecordBatchStream + Send>>;
