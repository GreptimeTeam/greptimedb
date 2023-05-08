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

use futures::TryStreamExt;

use crate::error::Result;
use crate::{RecordBatch, RecordBatches, SendableRecordBatchStream};

/// Collect all the items from the stream into a vector of [`RecordBatch`].
pub async fn collect(stream: SendableRecordBatchStream) -> Result<Vec<RecordBatch>> {
    stream.try_collect::<Vec<_>>().await
}

/// Collect all the items from the stream into [RecordBatches].
pub async fn collect_batches(stream: SendableRecordBatchStream) -> Result<RecordBatches> {
    let schema = stream.schema();
    let batches = stream.try_collect::<Vec<_>>().await?;
    RecordBatches::try_new(schema, batches)
}

#[cfg(test)]
mod tests {
    use std::pin::Pin;
    use std::sync::Arc;

    use datatypes::prelude::*;
    use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
    use datatypes::vectors::UInt32Vector;
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
            let batch = self.batch.take();

            if let Some(batch) = batch {
                Poll::Ready(Some(Ok(batch)))
            } else {
                Poll::Ready(None)
            }
        }
    }

    #[tokio::test]
    async fn test_collect() {
        let column_schemas = vec![ColumnSchema::new(
            "number",
            ConcreteDataType::uint32_datatype(),
            false,
        )];

        let schema = Arc::new(Schema::try_new(column_schemas).unwrap());

        let stream = MockRecordBatchStream {
            schema: schema.clone(),
            batch: None,
        };

        let batches = collect(Box::pin(stream)).await.unwrap();
        assert_eq!(0, batches.len());

        let numbers: Vec<u32> = (0..10).collect();
        let columns = [Arc::new(UInt32Vector::from_vec(numbers)) as _];
        let batch = RecordBatch::new(schema.clone(), columns).unwrap();

        let stream = MockRecordBatchStream {
            schema: schema.clone(),
            batch: Some(batch.clone()),
        };
        let batches = collect(Box::pin(stream)).await.unwrap();
        assert_eq!(1, batches.len());
        assert_eq!(batch, batches[0]);

        let stream = MockRecordBatchStream {
            schema: schema.clone(),
            batch: Some(batch.clone()),
        };
        let batches = collect_batches(Box::pin(stream)).await.unwrap();
        let expect_batches = RecordBatches::try_new(schema.clone(), vec![batch]).unwrap();
        assert_eq!(expect_batches, batches);
    }
}
