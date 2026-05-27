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
use std::sync::Arc;
use std::task::{Context, Poll};

use arc_swap::ArcSwapOption;
use datatypes::schema::SchemaRef;
use futures::{Stream, StreamExt, TryStreamExt};
use snafu::ensure;

use crate::adapter::RecordBatchMetrics;
use crate::error::{EmptyStreamSnafu, Result, SchemaNotMatchSnafu};
use crate::{
    OrderOption, RecordBatch, RecordBatchStream, RecordBatches, SendableRecordBatchStream,
};

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

/// A stream that chains multiple streams into a single stream.
pub struct ChainedRecordBatchStream {
    inputs: Vec<SendableRecordBatchStream>,
    curr_index: usize,
    schema: SchemaRef,
    metrics: Arc<ArcSwapOption<RecordBatchMetrics>>,
}

/// A stream that returns at most `limit` rows from the input stream.
pub struct LimitRecordBatchStream {
    input: Option<SendableRecordBatchStream>,
    schema: SchemaRef,
    output_ordering: Option<Vec<OrderOption>>,
    remaining: usize,
}

impl LimitRecordBatchStream {
    pub fn new(input: SendableRecordBatchStream, limit: usize) -> Self {
        let schema = input.schema();
        let output_ordering = input.output_ordering().map(|ordering| ordering.to_vec());
        Self {
            input: Some(input),
            schema,
            output_ordering,
            remaining: limit,
        }
    }

    fn finish(&mut self) {
        self.remaining = 0;
        self.input = None;
    }
}

impl RecordBatchStream for LimitRecordBatchStream {
    fn name(&self) -> &str {
        "LimitRecordBatchStream"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_ordering(&self) -> Option<&[OrderOption]> {
        self.output_ordering.as_deref()
    }

    fn metrics(&self) -> Option<RecordBatchMetrics> {
        self.input.as_ref().and_then(|input| input.metrics())
    }
}

impl Stream for LimitRecordBatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        if self.remaining == 0 {
            self.input = None;
            return Poll::Ready(None);
        }

        let Some(input) = self.input.as_mut() else {
            return Poll::Ready(None);
        };

        match input.poll_next_unpin(ctx) {
            Poll::Ready(Some(Ok(batch))) => {
                let remaining = self.remaining;
                if batch.num_rows() <= remaining {
                    self.remaining -= batch.num_rows();
                    if self.remaining == 0 {
                        self.input = None;
                    }
                    Poll::Ready(Some(Ok(batch)))
                } else {
                    self.finish();
                    Poll::Ready(Some(batch.slice(0, remaining)))
                }
            }
            other => other,
        }
    }
}

impl ChainedRecordBatchStream {
    pub fn new(inputs: Vec<SendableRecordBatchStream>) -> Result<Self> {
        // check length
        ensure!(!inputs.is_empty(), EmptyStreamSnafu);

        // check schema
        let first_schema = inputs[0].schema();
        for input in inputs.iter().skip(1) {
            let schema = input.schema();
            ensure!(
                first_schema == schema,
                SchemaNotMatchSnafu {
                    left: first_schema,
                    right: schema
                }
            );
        }

        Ok(Self {
            inputs,
            curr_index: 0,
            schema: first_schema,
            metrics: Default::default(),
        })
    }

    fn sequence_poll(
        mut self: Pin<&mut Self>,
        ctx: &mut Context<'_>,
    ) -> Poll<Option<Result<RecordBatch>>> {
        if self.curr_index >= self.inputs.len() {
            return Poll::Ready(None);
        }

        let curr_index = self.curr_index;
        match self.inputs[curr_index].poll_next_unpin(ctx) {
            Poll::Ready(Some(Ok(batch))) => Poll::Ready(Some(Ok(batch))),
            Poll::Ready(Some(Err(e))) => Poll::Ready(Some(Err(e))),
            Poll::Ready(None) => {
                self.curr_index += 1;
                if self.curr_index < self.inputs.len() {
                    self.sequence_poll(ctx)
                } else {
                    Poll::Ready(None)
                }
            }
            Poll::Pending => Poll::Pending,
        }
    }
}

impl RecordBatchStream for ChainedRecordBatchStream {
    fn name(&self) -> &str {
        "ChainedRecordBatchStream"
    }

    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }

    fn output_ordering(&self) -> Option<&[OrderOption]> {
        None
    }

    fn metrics(&self) -> Option<RecordBatchMetrics> {
        self.metrics.load().as_ref().map(|m| m.as_ref().clone())
    }
}

impl Stream for ChainedRecordBatchStream {
    type Item = Result<RecordBatch>;

    fn poll_next(self: Pin<&mut Self>, ctx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        self.sequence_poll(ctx)
    }
}

#[cfg(test)]
mod tests {
    use std::collections::VecDeque;
    use std::pin::Pin;
    use std::sync::Arc;

    use datatypes::prelude::*;
    use datatypes::schema::{ColumnSchema, Schema, SchemaRef};
    use datatypes::vectors::UInt32Vector;
    use futures::Stream;
    use futures::task::{Context, Poll};

    use super::*;
    use crate::adapter::RecordBatchMetrics;
    use crate::{OrderOption, RecordBatchStream};

    struct MockRecordBatchStream {
        batches: VecDeque<RecordBatch>,
        schema: SchemaRef,
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

    impl Stream for MockRecordBatchStream {
        type Item = Result<RecordBatch>;

        fn poll_next(mut self: Pin<&mut Self>, _cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
            let batch = self.batches.pop_front();

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
            batches: VecDeque::new(),
        };

        let batches = collect(Box::pin(stream)).await.unwrap();
        assert_eq!(0, batches.len());

        let numbers: Vec<u32> = (0..10).collect();
        let columns = [Arc::new(UInt32Vector::from_vec(numbers)) as _];
        let batch = RecordBatch::new(schema.clone(), columns).unwrap();

        let stream = MockRecordBatchStream {
            schema: schema.clone(),
            batches: VecDeque::from([batch.clone()]),
        };
        let batches = collect(Box::pin(stream)).await.unwrap();
        assert_eq!(1, batches.len());
        assert_eq!(batch, batches[0]);

        let stream = MockRecordBatchStream {
            schema: schema.clone(),
            batches: VecDeque::from([batch.clone()]),
        };
        let batches = collect_batches(Box::pin(stream)).await.unwrap();
        let expect_batches = RecordBatches::try_new(schema.clone(), vec![batch]).unwrap();
        assert_eq!(expect_batches, batches);
    }

    #[tokio::test]
    async fn test_limit_record_batch_stream() {
        let schema = Arc::new(
            Schema::try_new(vec![ColumnSchema::new(
                "number",
                ConcreteDataType::uint32_datatype(),
                false,
            )])
            .unwrap(),
        );
        let batch1 = RecordBatch::new(
            schema.clone(),
            [Arc::new(UInt32Vector::from_vec(vec![0, 1, 2])) as _],
        )
        .unwrap();
        let batch2 = RecordBatch::new(
            schema.clone(),
            [Arc::new(UInt32Vector::from_vec(vec![3, 4, 5])) as _],
        )
        .unwrap();
        let input = MockRecordBatchStream {
            schema: schema.clone(),
            batches: VecDeque::from([batch1, batch2]),
        };

        let batches = collect(Box::pin(LimitRecordBatchStream::new(Box::pin(input), 4)))
            .await
            .unwrap();
        assert_eq!(2, batches.len());
        assert_eq!(3, batches[0].num_rows());
        assert_eq!(1, batches[1].num_rows());
    }

    #[tokio::test]
    async fn test_limit_record_batch_stream_zero_limit() {
        let schema = Arc::new(
            Schema::try_new(vec![ColumnSchema::new(
                "number",
                ConcreteDataType::uint32_datatype(),
                false,
            )])
            .unwrap(),
        );
        let batch = RecordBatch::new(
            schema.clone(),
            [Arc::new(UInt32Vector::from_vec(vec![0, 1, 2])) as _],
        )
        .unwrap();
        let input = MockRecordBatchStream {
            schema,
            batches: VecDeque::from([batch]),
        };

        let batches = collect(Box::pin(LimitRecordBatchStream::new(Box::pin(input), 0)))
            .await
            .unwrap();
        assert!(batches.is_empty());
    }
}
