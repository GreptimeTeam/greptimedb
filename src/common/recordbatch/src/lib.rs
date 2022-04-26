pub mod error;
mod recordbatch;

use core::pin::Pin;

use datatypes::schema::SchemaRef;
use error::Result;
use futures::Stream;
pub use recordbatch::RecordBatch;

pub trait RecordBatchStream: Stream<Item = Result<RecordBatch>> {
    fn schema(&self) -> SchemaRef;
}

pub type BoxedRecordBatchStream = Box<dyn RecordBatchStream>;
pub type SendableRecordBatchStream = Pin<BoxedRecordBatchStream>;
