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

use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use datafusion::arrow::compute::cast;
use datafusion::arrow::datatypes::SchemaRef as DfSchemaRef;
use datafusion::error::Result as DfResult;
use datafusion::parquet::arrow::async_reader::{AsyncFileReader, ParquetRecordBatchStream};
use datafusion::physical_plan::metrics::BaselineMetrics;
use datafusion::physical_plan::RecordBatchStream as DfRecordBatchStream;
use datafusion_common::DataFusionError;
use datatypes::schema::{Schema, SchemaRef};
use futures::ready;
use snafu::ResultExt;

use crate::error::{self, Result};
use crate::{
    DfRecordBatch, DfSendableRecordBatchStream, RecordBatch, RecordBatchStream,
    SendableRecordBatchStream, Stream,
};

type FutureStream =
    Pin<Box<dyn std::future::Future<Output = Result<SendableRecordBatchStream>> + Send>>;

/// ParquetRecordBatchStream -> DataFusion RecordBatchStream
pub struct ParquetRecordBatchStreamAdapter<T> {
    stream: ParquetRecordBatchStream<T>,
    output_schema: DfSchemaRef,
    projection: Vec<usize>,
}

impl<T: Unpin + AsyncFileReader + Send + 'static> ParquetRecordBatchStreamAdapter<T> {
    pub fn new(
        output_schema: DfSchemaRef,
        stream: ParquetRecordBatchStream<T>,
        projection: Option<Vec<usize>>,
    ) -> Self {
        let projection = if let Some(projection) = projection {
            projection
        } else {
            (0..output_schema.fields().len()).collect()
        };

        Self {
            stream,
            output_schema,
            projection,
        }
    }
}

impl<T: Unpin + AsyncFileReader + Send + 'static> DfRecordBatchStream
    for ParquetRecordBatchStreamAdapter<T>
{
    fn schema(&self) -> DfSchemaRef {
        self.stream.schema().clone()
    }
}

impl<T: Unpin + AsyncFileReader + Send + 'static> Stream for ParquetRecordBatchStreamAdapter<T> {
    type Item = DfResult<DfRecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let batch = futures::ready!(Pin::new(&mut self.stream).poll_next(cx))
            .map(|r| r.map_err(|e| DataFusionError::External(Box::new(e))));

        let projected_schema = self.output_schema.project(&self.projection)?;
        let batch = batch.map(|b| {
            b.and_then(|b| {
                let mut columns = Vec::with_capacity(self.projection.len());
                for idx in self.projection.iter() {
                    let column = b.column(*idx);
                    let field = self.output_schema.field(*idx);

                    if column.data_type() != field.data_type() {
                        let output = cast(&column, field.data_type())?;
                        columns.push(output)
                    } else {
                        columns.push(column.clone())
                    }
                }

                let record_batch = DfRecordBatch::try_new(projected_schema.into(), columns)?;

                Ok(record_batch)
            })
        });

        Poll::Ready(batch)
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

/// Greptime SendableRecordBatchStream -> DataFusion RecordBatchStream.
/// The reverse one is [RecordBatchStreamAdapter].
pub struct DfRecordBatchStreamAdapter {
    stream: SendableRecordBatchStream,
}

impl DfRecordBatchStreamAdapter {
    pub fn new(stream: SendableRecordBatchStream) -> Self {
        Self { stream }
    }
}

impl DfRecordBatchStream for DfRecordBatchStreamAdapter {
    fn schema(&self) -> DfSchemaRef {
        self.stream.schema().arrow_schema().clone()
    }
}

impl Stream for DfRecordBatchStreamAdapter {
    type Item = DfResult<DfRecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        match Pin::new(&mut self.stream).poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(recordbatch)) => match recordbatch {
                Ok(recordbatch) => Poll::Ready(Some(Ok(recordbatch.into_df_record_batch()))),
                Err(e) => Poll::Ready(Some(Err(DataFusionError::External(Box::new(e))))),
            },
            Poll::Ready(None) => Poll::Ready(None),
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

/// DataFusion [SendableRecordBatchStream](DfSendableRecordBatchStream) -> Greptime [RecordBatchStream].
/// The reverse one is [DfRecordBatchStreamAdapter]
pub struct RecordBatchStreamAdapter {
    schema: SchemaRef,
    stream: DfSendableRecordBatchStream,
    metrics: Option<BaselineMetrics>,
}

impl RecordBatchStreamAdapter {
    pub fn try_new(stream: DfSendableRecordBatchStream) -> Result<Self> {
        let schema =
            Arc::new(Schema::try_from(stream.schema()).context(error::SchemaConversionSnafu)?);
        Ok(Self {
            schema,
            stream,
            metrics: None,
        })
    }

    pub fn try_new_with_metrics(
        stream: DfSendableRecordBatchStream,
        metrics: BaselineMetrics,
    ) -> Result<Self> {
        let schema =
            Arc::new(Schema::try_from(stream.schema()).context(error::SchemaConversionSnafu)?);
        Ok(Self {
            schema,
            stream,
            metrics: Some(metrics),
        })
    }
}

impl RecordBatchStream for RecordBatchStreamAdapter {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for RecordBatchStreamAdapter {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let timer = self
            .metrics
            .as_ref()
            .map(|m| m.elapsed_compute().clone())
            .unwrap_or_default();
        let _guard = timer.timer();
        match Pin::new(&mut self.stream).poll_next(cx) {
            Poll::Pending => Poll::Pending,
            Poll::Ready(Some(df_record_batch)) => {
                let df_record_batch = df_record_batch.context(error::PollStreamSnafu)?;
                Poll::Ready(Some(RecordBatch::try_from_df_record_batch(
                    self.schema(),
                    df_record_batch,
                )))
            }
            Poll::Ready(None) => Poll::Ready(None),
        }
    }

    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        self.stream.size_hint()
    }
}

enum AsyncRecordBatchStreamAdapterState {
    Uninit(FutureStream),
    Ready(SendableRecordBatchStream),
    Failed,
}

pub struct AsyncRecordBatchStreamAdapter {
    schema: SchemaRef,
    state: AsyncRecordBatchStreamAdapterState,
}

impl AsyncRecordBatchStreamAdapter {
    pub fn new(schema: SchemaRef, stream: FutureStream) -> Self {
        Self {
            schema,
            state: AsyncRecordBatchStreamAdapterState::Uninit(stream),
        }
    }
}

impl RecordBatchStream for AsyncRecordBatchStreamAdapter {
    fn schema(&self) -> SchemaRef {
        self.schema.clone()
    }
}

impl Stream for AsyncRecordBatchStreamAdapter {
    type Item = Result<RecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        loop {
            match &mut self.state {
                AsyncRecordBatchStreamAdapterState::Uninit(stream_future) => {
                    match ready!(Pin::new(stream_future).poll(cx)) {
                        Ok(stream) => {
                            self.state = AsyncRecordBatchStreamAdapterState::Ready(stream);
                            continue;
                        }
                        Err(e) => {
                            self.state = AsyncRecordBatchStreamAdapterState::Failed;
                            return Poll::Ready(Some(Err(e)));
                        }
                    };
                }
                AsyncRecordBatchStreamAdapterState::Ready(stream) => {
                    return Poll::Ready(ready!(Pin::new(stream).poll_next(cx)))
                }
                AsyncRecordBatchStreamAdapterState::Failed => return Poll::Ready(None),
            }
        }
    }

    // This is not supported for lazy stream.
    #[inline]
    fn size_hint(&self) -> (usize, Option<usize>) {
        (0, None)
    }
}

#[cfg(test)]
mod test {
    use common_error::ext::BoxedError;
    use common_error::mock::MockError;
    use common_error::status_code::StatusCode;
    use datatypes::prelude::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use datatypes::vectors::Int32Vector;
    use snafu::IntoError;

    use super::*;
    use crate::error::Error;
    use crate::RecordBatches;

    #[tokio::test]
    async fn test_async_recordbatch_stream_adaptor() {
        struct MaybeErrorRecordBatchStream {
            items: Vec<Result<RecordBatch>>,
        }

        impl RecordBatchStream for MaybeErrorRecordBatchStream {
            fn schema(&self) -> SchemaRef {
                unimplemented!()
            }
        }

        impl Stream for MaybeErrorRecordBatchStream {
            type Item = Result<RecordBatch>;

            fn poll_next(
                mut self: Pin<&mut Self>,
                _: &mut Context<'_>,
            ) -> Poll<Option<Self::Item>> {
                if let Some(batch) = self.items.pop() {
                    Poll::Ready(Some(Ok(batch?)))
                } else {
                    Poll::Ready(None)
                }
            }
        }

        fn new_future_stream(
            maybe_recordbatches: Result<Vec<Result<RecordBatch>>>,
        ) -> FutureStream {
            Box::pin(async move {
                maybe_recordbatches
                    .map(|items| Box::pin(MaybeErrorRecordBatchStream { items }) as _)
            })
        }

        let schema = Arc::new(Schema::new(vec![ColumnSchema::new(
            "a",
            ConcreteDataType::int32_datatype(),
            false,
        )]));
        let batch1 = RecordBatch::new(
            schema.clone(),
            vec![Arc::new(Int32Vector::from_slice([1])) as _],
        )
        .unwrap();
        let batch2 = RecordBatch::new(
            schema.clone(),
            vec![Arc::new(Int32Vector::from_slice([2])) as _],
        )
        .unwrap();

        let success_stream = new_future_stream(Ok(vec![Ok(batch1.clone()), Ok(batch2.clone())]));
        let adapter = AsyncRecordBatchStreamAdapter::new(schema.clone(), success_stream);
        let collected = RecordBatches::try_collect(Box::pin(adapter)).await.unwrap();
        assert_eq!(
            collected,
            RecordBatches::try_new(schema.clone(), vec![batch2.clone(), batch1.clone()]).unwrap()
        );

        let poll_err_stream = new_future_stream(Ok(vec![
            Ok(batch1.clone()),
            Err(error::ExternalSnafu
                .into_error(BoxedError::new(MockError::new(StatusCode::Unknown)))),
        ]));
        let adapter = AsyncRecordBatchStreamAdapter::new(schema.clone(), poll_err_stream);
        let err = RecordBatches::try_collect(Box::pin(adapter))
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::External { .. }),
            "unexpected err {err}"
        );

        let failed_to_init_stream =
            new_future_stream(Err(error::ExternalSnafu
                .into_error(BoxedError::new(MockError::new(StatusCode::Internal)))));
        let adapter = AsyncRecordBatchStreamAdapter::new(schema.clone(), failed_to_init_stream);
        let err = RecordBatches::try_collect(Box::pin(adapter))
            .await
            .unwrap_err();
        assert!(
            matches!(err, Error::External { .. }),
            "unexpected err {err}"
        );
    }
}
