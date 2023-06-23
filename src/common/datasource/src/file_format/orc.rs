use std::pin::Pin;
use std::task::{Context, Poll};

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

pub async fn create_orc_schema<R: AsyncRead + AsyncSeek + Unpin + Send + 'static>(
    reader: R,
) -> Result<Schema> {
    let cursor = new_orc_cursor(reader).await?;
    Ok(create_arrow_schema(&cursor))
}

pub async fn create_orc_stream_reader<R: AsyncRead + AsyncSeek + Unpin + Send + 'static>(
    reader: R,
) -> Result<ArrowStreamReader<R>> {
    let cursor = new_orc_cursor(reader).await?;
    Ok(ArrowStreamReader::new(cursor, None))
}

pub struct OrcArrowStreamReaderAdapter<T: AsyncRead + AsyncSeek + Unpin + Send + 'static> {
    stream: Box<ArrowStreamReader<T>>,
}

impl<T: AsyncRead + AsyncSeek + Unpin + Send + 'static> OrcArrowStreamReaderAdapter<T> {
    pub fn new(stream: ArrowStreamReader<T>) -> Self {
        Self {
            stream: Box::new(stream),
        }
    }
}

impl<T: AsyncRead + AsyncSeek + Unpin + Send + 'static> RecordBatchStream
    for OrcArrowStreamReaderAdapter<T>
{
    fn schema(&self) -> SchemaRef {
        self.stream.schema()
    }
}

impl<T: AsyncRead + AsyncSeek + Unpin + Send + 'static> Stream for OrcArrowStreamReaderAdapter<T> {
    type Item = DfResult<DfRecordBatch>;

    fn poll_next(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Option<Self::Item>> {
        let batch = futures::ready!(Pin::new(&mut self.stream).poll_next(cx))
            .map(|r| r.map_err(|e| DataFusionError::External(Box::new(e))));
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

        let schema = create_orc_schema(reader).await?;

        Ok(schema)
    }
}
