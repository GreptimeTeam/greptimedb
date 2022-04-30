use futures::TryStreamExt;

use crate::{error::Result, RecordBatch, SendableRecordBatchStream};

pub async fn collect(stream: SendableRecordBatchStream) -> Result<Vec<RecordBatch>> {
    stream.try_collect::<Vec<_>>().await
}
