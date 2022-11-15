use futures::TryStreamExt;

use crate::error::Result;
use crate::{RecordBatch, SendableRecordBatchStream};

pub async fn collect(stream: SendableRecordBatchStream) -> Result<Vec<RecordBatch>> {
    stream.try_collect::<Vec<_>>().await
}

#[cfg(test)]
mod tests {
    use std::mem;
    use std::pin::Pin;
    use std::sync::Arc;

    use datafusion_common::field_util::SchemaExt;
    use datafusion_common::record_batch::RecordBatch as DfRecordBatch;
    use datatypes::arrow::array::UInt32Array;
    use datatypes::arrow::datatypes::{DataType, Field, Schema as ArrowSchema};
    use datatypes::schema::{Schema, SchemaRef};
    use futures::task::{Context, Poll};
    use futures::Stream;

    use super::*;
    use crate::RecordBatchStream;

    struct MockRecordBatchStream {
        batch: Option<RecordBatch>,
        schema: SchemaRef,
    }

    impl RecordBatchStream for MockRecordBatchStream {
        fn schema(&self) -> SchemaRef {
            self.schema.clone()
        }
    }

    impl Stream for MockRecordBatchStream {
        type Item = Result<RecordBatch>;

        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let batch = mem::replace(&mut self.batch, None);

            if let Some(batch) = batch {
                Poll::Ready(Some(Ok(batch)))
            } else {
                Poll::Ready(None)
            }
        }
    }

    #[tokio::test]
    async fn test_collect() {
        let arrow_schema = Arc::new(ArrowSchema::new(vec![Field::new(
            "number",
            DataType::UInt32,
            false,
        )]));
        let schema = Arc::new(Schema::try_from(arrow_schema.clone()).unwrap());

        let stream = MockRecordBatchStream {
            schema: schema.clone(),
            batch: None,
        };

        let batches = collect(Box::pin(stream)).await.unwrap();
        assert_eq!(0, batches.len());

        let numbers: Vec<u32> = (0..10).collect();
        let df_batch = DfRecordBatch::try_new(
            arrow_schema.clone(),
            vec![Arc::new(UInt32Array::from_slice(&numbers))],
        )
        .unwrap();

        let batch = RecordBatch {
            schema: schema.clone(),
            df_recordbatch: df_batch,
        };

        let stream = MockRecordBatchStream {
            schema: Arc::new(Schema::try_from(arrow_schema).unwrap()),
            batch: Some(batch.clone()),
        };
        let batches = collect(Box::pin(stream)).await.unwrap();
        assert_eq!(1, batches.len());

        assert_eq!(batch, batches[0]);
    }
}
