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

use std::result;
use std::sync::Arc;

use arrow::record_batch::RecordBatch;
use arrow_schema::Schema;
use async_trait::async_trait;
use datafusion::datasource::physical_plan::{FileMeta, ParquetFileReaderFactory};
use datafusion::error::Result as DatafusionResult;
use datafusion::parquet::arrow::async_reader::AsyncFileReader;
use datafusion::parquet::arrow::{parquet_to_arrow_schema, ArrowWriter};
use datafusion::parquet::errors::{ParquetError, Result as ParquetResult};
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::parquet::format::FileMetaData;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::SendableRecordBatchStream;
use datatypes::schema::SchemaRef;
use futures::future::BoxFuture;
use futures::StreamExt;
use object_store::{FuturesAsyncReader, ObjectStore};
use parquet::arrow::arrow_reader::ArrowReaderOptions;
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use parquet::schema::types::ColumnPath;
use snafu::ResultExt;
use tokio_util::compat::{Compat, FuturesAsyncReadCompatExt, FuturesAsyncWriteCompatExt};

use crate::buffered_writer::{ArrowWriterCloser, DfRecordBatchEncoder};
use crate::error::{self, Result, WriteObjectSnafu, WriteParquetSnafu};
use crate::file_format::FileFormat;
use crate::share_buffer::SharedBuffer;
use crate::DEFAULT_WRITE_BUFFER_SIZE;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ParquetFormat {}

#[async_trait]
impl FileFormat for ParquetFormat {
    async fn infer_schema(&self, store: &ObjectStore, path: &str) -> Result<Schema> {
        let meta = store
            .stat(path)
            .await
            .context(error::ReadObjectSnafu { path })?;

        let mut reader = store
            .reader(path)
            .await
            .context(error::ReadObjectSnafu { path })?
            .into_futures_async_read(0..meta.content_length())
            .await
            .context(error::ReadObjectSnafu { path })?
            .compat();

        let metadata = reader
            .get_metadata(None)
            .await
            .context(error::ReadParquetSnafuSnafu)?;

        let file_metadata = metadata.file_metadata();
        let schema = parquet_to_arrow_schema(
            file_metadata.schema_descr(),
            file_metadata.key_value_metadata(),
        )
        .context(error::ParquetToSchemaSnafu)?;

        Ok(schema)
    }
}

#[derive(Debug, Clone)]
pub struct DefaultParquetFileReaderFactory {
    object_store: ObjectStore,
}

/// Returns a AsyncFileReader factory
impl DefaultParquetFileReaderFactory {
    pub fn new(object_store: ObjectStore) -> Self {
        Self { object_store }
    }
}

impl ParquetFileReaderFactory for DefaultParquetFileReaderFactory {
    // TODO(weny): Supports [`metadata_size_hint`].
    // The upstream has a implementation supports [`metadata_size_hint`],
    // however it coupled with Box<dyn ObjectStore>.
    fn create_reader(
        &self,
        _partition_index: usize,
        file_meta: FileMeta,
        _metadata_size_hint: Option<usize>,
        _metrics: &ExecutionPlanMetricsSet,
    ) -> DatafusionResult<Box<dyn AsyncFileReader + Send>> {
        let path = file_meta.location().to_string();
        let object_store = self.object_store.clone();

        Ok(Box::new(LazyParquetFileReader::new(object_store, path)))
    }
}

pub struct LazyParquetFileReader {
    object_store: ObjectStore,
    reader: Option<Compat<FuturesAsyncReader>>,
    path: String,
}

impl LazyParquetFileReader {
    pub fn new(object_store: ObjectStore, path: String) -> Self {
        LazyParquetFileReader {
            object_store,
            path,
            reader: None,
        }
    }

    /// Must initialize the reader, or throw an error from the future.
    async fn maybe_initialize(&mut self) -> result::Result<(), object_store::Error> {
        if self.reader.is_none() {
            let meta = self.object_store.stat(&self.path).await?;
            let reader = self
                .object_store
                .reader(&self.path)
                .await?
                .into_futures_async_read(0..meta.content_length())
                .await?
                .compat();
            self.reader = Some(reader);
        }

        Ok(())
    }
}

impl AsyncFileReader for LazyParquetFileReader {
    fn get_bytes(
        &mut self,
        range: std::ops::Range<u64>,
    ) -> BoxFuture<'_, ParquetResult<bytes::Bytes>> {
        Box::pin(async move {
            self.maybe_initialize()
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))?;
            // Safety: Must initialized
            self.reader.as_mut().unwrap().get_bytes(range).await
        })
    }

    fn get_metadata<'a>(
        &'a mut self,
        options: Option<&'a ArrowReaderOptions>,
    ) -> BoxFuture<'a, parquet::errors::Result<Arc<ParquetMetaData>>> {
        Box::pin(async move {
            self.maybe_initialize()
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))?;
            // Safety: Must initialized
            self.reader.as_mut().unwrap().get_metadata(options).await
        })
    }
}

impl DfRecordBatchEncoder for ArrowWriter<SharedBuffer> {
    fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.write(batch).context(error::EncodeRecordBatchSnafu)
    }
}

#[async_trait]
impl ArrowWriterCloser for ArrowWriter<SharedBuffer> {
    async fn close(self) -> Result<FileMetaData> {
        self.close().context(error::EncodeRecordBatchSnafu)
    }
}

/// Output the stream to a parquet file.
///
/// Returns number of rows written.
pub async fn stream_to_parquet(
    mut stream: SendableRecordBatchStream,
    schema: datatypes::schema::SchemaRef,
    store: ObjectStore,
    path: &str,
    concurrency: usize,
) -> Result<usize> {
    let write_props = column_wise_config(
        WriterProperties::builder().set_compression(Compression::ZSTD(ZstdLevel::default())),
        schema,
    )
    .build();
    let inner_writer = store
        .writer_with(path)
        .concurrent(concurrency)
        .chunk(DEFAULT_WRITE_BUFFER_SIZE.as_bytes() as usize)
        .await
        .map(|w| w.into_futures_async_write().compat_write())
        .context(WriteObjectSnafu { path })?;

    let mut writer = AsyncArrowWriter::try_new(inner_writer, stream.schema(), Some(write_props))
        .context(WriteParquetSnafu { path })?;
    let mut rows_written = 0;

    while let Some(batch) = stream.next().await {
        let batch = batch.context(error::ReadRecordBatchSnafu)?;
        writer
            .write(&batch)
            .await
            .context(WriteParquetSnafu { path })?;
        rows_written += batch.num_rows();
    }
    writer.close().await.context(WriteParquetSnafu { path })?;
    Ok(rows_written)
}

/// Customizes per-column properties.
fn column_wise_config(
    mut props: WriterPropertiesBuilder,
    schema: SchemaRef,
) -> WriterPropertiesBuilder {
    // Disable dictionary for timestamp column, since for increasing timestamp column,
    // the dictionary pages will be larger than data pages.
    for col in schema.column_schemas() {
        if col.data_type.is_timestamp() {
            let path = ColumnPath::new(vec![col.name.clone()]);
            props = props
                .set_column_dictionary_enabled(path.clone(), false)
                .set_column_encoding(path, Encoding::DELTA_BINARY_PACKED)
        }
    }
    props
}

#[cfg(test)]
mod tests {
    use common_test_util::find_workspace_path;

    use super::*;
    use crate::test_util::{format_schema, test_store};

    fn test_data_root() -> String {
        find_workspace_path("/src/common/datasource/tests/parquet")
            .display()
            .to_string()
    }

    #[tokio::test]
    async fn infer_schema_basic() {
        let json = ParquetFormat::default();
        let store = test_store(&test_data_root());
        let schema = json.infer_schema(&store, "basic.parquet").await.unwrap();
        let formatted: Vec<_> = format_schema(schema);

        assert_eq!(vec!["num: Int64: NULL", "str: Utf8: NULL"], formatted);
    }
}
