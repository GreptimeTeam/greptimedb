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

use arrow::compute::cast;
use arrow_schema::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::arrow::record_batch::RecordBatch as DfRecordBatch;
use datafusion::error::{DataFusionError, Result as DfResult};
use datafusion::physical_plan::RecordBatchStream;
use futures::Stream;
use object_store::ObjectStore;
use orc_rust::arrow_reader::{create_arrow_schema, Cursor};
use orc_rust::async_arrow_reader::ArrowStreamReader;
pub use orc_rust::error::Error as OrcError;
use orc_rust::reader::Reader;
use snafu::ResultExt;
use tokio::io::{AsyncRead, AsyncSeek};

use crate::error::{self, Result};
use crate::file_format::FileFormat;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct OrcFormat;

pub async fn new_orc_cursor<R: AsyncRead + AsyncSeek + Unpin + Send + 'static>(
    reader: R,
) -> Result<Cursor<R>> {
    let reader = Reader::new_async(reader)
        .await
        .context(error::OrcReaderSnafu)?;
    let cursor = Cursor::root(reader).context(error::OrcReaderSnafu)?;
    Ok(cursor)
}

pub async fn new_orc_stream_reader<R: AsyncRead + AsyncSeek + Unpin + Send + 'static>(
    reader: R,
) -> Result<ArrowStreamReader<R>> {
    let cursor = new_orc_cursor(reader).await?;
    Ok(ArrowStreamReader::new(cursor, None))
}

pub async fn infer_orc_schema<R: AsyncRead + AsyncSeek + Unpin + Send + 'static>(
    reader: R,
) -> Result<Schema> {
    let cursor = new_orc_cursor(reader).await?;
    Ok(create_arrow_schema(&cursor))
}

pub struct OrcArrowStreamReaderAdapter<T: AsyncRead + AsyncSeek + Unpin + Send + 'static> {
    output_schema: SchemaRef,
    stream: ArrowStreamReader<T>,
}

impl<T: AsyncRead + AsyncSeek + Unpin + Send + 'static> OrcArrowStreamReaderAdapter<T> {
    pub fn new(output_schema: SchemaRef, stream: ArrowStreamReader<T>) -> Self {
        Self {
            stream,
            output_schema,
        }
    }
}

impl<T: AsyncRead + AsyncSeek + Unpin + Send + 'static> RecordBatchStream
    for OrcArrowStreamReaderAdapter<T>
{
    fn schema(&self) -> SchemaRef {
        self.output_schema.clone()
    }
}

impl<T: AsyncRead + AsyncSeek + Unpin + Send + 'static> Stream for OrcArrowStreamReaderAdapter<T> {
    type Item = DfResult<DfRecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let batch = futures::ready!(Pin::new(&mut self.stream).poll_next(cx))
            .map(|r| r.map_err(|e| DataFusionError::External(Box::new(e))));

        let batch = batch.map(|b| {
            b.and_then(|b| {
                let mut columns = Vec::with_capacity(b.num_columns());
                for (idx, column) in b.columns().iter().enumerate() {
                    if column.data_type() != self.output_schema.field(idx).data_type() {
                        let output = cast(&column, self.output_schema.field(idx).data_type())?;
                        columns.push(output)
                    } else {
                        columns.push(column.clone())
                    }
                }
                let record_batch = DfRecordBatch::try_new(self.output_schema.clone(), columns)?;

                Ok(record_batch)
            })
        });

        Poll::Ready(batch)
    }
}

#[async_trait]
impl FileFormat for OrcFormat {
    async fn infer_schema(&self, store: &ObjectStore, path: &str) -> Result<Schema> {
        let reader = store
            .reader(path)
            .await
            .context(error::ReadObjectSnafu { path })?;

        let schema = infer_orc_schema(reader).await?;

        Ok(schema)
    }
}
