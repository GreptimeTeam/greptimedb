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
use arrow_schema::{Schema, SchemaRef};
use async_trait::async_trait;
use datafusion::datasource::physical_plan::{FileMeta, ParquetFileReaderFactory};
use datafusion::error::Result as DatafusionResult;
use datafusion::parquet::arrow::{ArrowWriter, parquet_to_arrow_schema};
use datafusion::parquet::arrow::async_reader::AsyncFileReader;
use datafusion::parquet::errors::{ParquetError, Result as ParquetResult};
use datafusion::parquet::file::metadata::ParquetMetaData;
use datafusion::parquet::format::FileMetaData;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::{StreamExt,};
use futures::future::BoxFuture;
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::{Compression, ZstdLevel};
use parquet::file::properties::WriterProperties;
use snafu::ResultExt;
use tokio_util::compat::{Compat, FuturesAsyncReadCompatExt, FuturesAsyncWriteCompatExt};

use object_store::{FuturesAsyncReader, ObjectStore};

use crate::buffered_writer::{ArrowWriterCloser, DfRecordBatchEncoder, LazyBufferedWriter};
use crate::DEFAULT_WRITE_BUFFER_SIZE;
use crate::error::{self, Result, WriteObjectSnafu, WriteParquetSnafu};
use crate::file_format::FileFormat;
use crate::share_buffer::SharedBuffer;

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
            .compat();

        let metadata = reader
            .get_metadata()
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
                .compat();
            self.reader = Some(reader);
        }

        Ok(())
    }
}

impl AsyncFileReader for LazyParquetFileReader {
    fn get_bytes(
        &mut self,
        range: std::ops::Range<usize>,
    ) -> BoxFuture<'_, ParquetResult<bytes::Bytes>> {
        Box::pin(async move {
            self.maybe_initialize()
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))?;
            // Safety: Must initialized
            self.reader.as_mut().unwrap().get_bytes(range).await
        })
    }

    fn get_metadata(&mut self) -> BoxFuture<'_, ParquetResult<Arc<ParquetMetaData>>> {
        Box::pin(async move {
            self.maybe_initialize()
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))?;
            // Safety: Must initialized
            self.reader.as_mut().unwrap().get_metadata().await
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

/// Parquet writer that buffers row groups in memory and writes buffered data to an underlying
/// storage by chunks to reduce memory consumption.
pub struct BufferedWriter {
    inner: InnerBufferedWriter,
}

type InnerBufferedWriter = LazyBufferedWriter<
    Compat<object_store::FuturesAsyncWriter>,
    ArrowWriter<SharedBuffer>,
    impl Fn(String) -> BoxFuture<'static, Result<Compat<object_store::FuturesAsyncWriter>>>,
>;

impl BufferedWriter {
    fn make_write_factory(
        store: ObjectStore,
        concurrency: usize,
    ) -> impl Fn(String) -> BoxFuture<'static, Result<Compat<object_store::FuturesAsyncWriter>>>
    {
        move |path| {
            let store = store.clone();
            Box::pin(async move {
                store
                    .writer_with(&path)
                    .concurrent(concurrency)
                    .chunk(DEFAULT_WRITE_BUFFER_SIZE.as_bytes() as usize)
                    .await
                    .map(|v| v.into_futures_async_write().compat_write())
                    .context(error::WriteObjectSnafu { path })
            })
        }
    }

    pub async fn try_new(
        path: String,
        store: ObjectStore,
        arrow_schema: SchemaRef,
        props: Option<WriterProperties>,
        buffer_threshold: usize,
        concurrency: usize,
    ) -> error::Result<Self> {
        let buffer = SharedBuffer::with_capacity(buffer_threshold);

        let arrow_writer = ArrowWriter::try_new(buffer.clone(), arrow_schema.clone(), props)
            .context(error::WriteParquetSnafu { path: &path })?;

        Ok(Self {
            inner: LazyBufferedWriter::new(
                buffer_threshold,
                buffer,
                arrow_writer,
                &path,
                Self::make_write_factory(store, concurrency),
            ),
        })
    }

    /// Write a record batch to stream writer.
    pub async fn write(&mut self, arrow_batch: &RecordBatch) -> error::Result<()> {
        self.inner.write(arrow_batch).await
    }

    /// Close parquet writer.
    ///
    /// Return file metadata and bytes written.
    pub async fn close(self) -> error::Result<(FileMetaData, u64)> {
        self.inner.close_with_arrow_writer().await
    }
}

/// Output the stream to a parquet file.
///
/// Returns number of rows written.
pub async fn stream_to_parquet(
    mut stream: SendableRecordBatchStream,
    store: ObjectStore,
    path: &str,
    concurrency: usize,
) -> Result<usize> {
    let write_props = WriterProperties::builder()
        .set_compression(Compression::ZSTD(ZstdLevel::default()))
        .build();
    let schema = stream.schema();
    let inner_writer = store
        .writer_with(path)
        .concurrent(concurrency)
        .await
        .map(|w| w.into_futures_async_write().compat_write())
        .context(WriteObjectSnafu{
            path,
        })?;

    let mut writer = AsyncArrowWriter::try_new(inner_writer, schema, Some(write_props)).context(WriteParquetSnafu{
        path,
    })?;
    let mut rows_written = 0;

    while let Some(batch) = stream.next().await {
        let batch = batch.context(error::ReadRecordBatchSnafu)?;
        writer.write(&batch).await.context(WriteParquetSnafu{
            path
        })?;
        rows_written += batch.num_rows();
    }
    writer.close().await.context(WriteParquetSnafu{
        path
    })?;
    Ok(rows_written)
}

#[cfg(test)]
mod tests {
    use std::env;
    use std::sync::Arc;

    use rand::{Rng, thread_rng};

    use common_telemetry::warn;
    use common_test_util::find_workspace_path;
    use datatypes::arrow::array::{ArrayRef, Int64Array, RecordBatch};
    use datatypes::arrow::datatypes::{DataType, Field, Schema};
    use object_store::ObjectStore;
    use object_store::services::S3;

    use crate::file_format::parquet::BufferedWriter;
    use crate::test_util::{format_schema, test_store};

    use super::*;

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

    #[tokio::test]
    async fn test_parquet_writer() {
        common_telemetry::init_default_ut_logging();
        let _ = dotenv::dotenv();
        let Ok(bucket) = env::var("GT_MINIO_BUCKET") else {
            warn!("ignoring test parquet writer");
            return;
        };

        let mut builder = S3::default();
        let _ = builder
            .root(&uuid::Uuid::new_v4().to_string())
            .access_key_id(&env::var("GT_MINIO_ACCESS_KEY_ID").unwrap())
            .secret_access_key(&env::var("GT_MINIO_ACCESS_KEY").unwrap())
            .bucket(&bucket)
            .region(&env::var("GT_MINIO_REGION").unwrap())
            .endpoint(&env::var("GT_MINIO_ENDPOINT_URL").unwrap());

        let object_store = ObjectStore::new(builder).unwrap().finish();
        let file_path = uuid::Uuid::new_v4().to_string();
        let fields = vec![
            Field::new("field1", DataType::Int64, true),
            Field::new("field0", DataType::Int64, true),
        ];
        let arrow_schema = Arc::new(Schema::new(fields));
        let mut buffered_writer = BufferedWriter::try_new(
            file_path.clone(),
            object_store.clone(),
            arrow_schema.clone(),
            None,
            // Sets a small value.
            128,
            8,
        )
        .await
        .unwrap();
        let rows = 200000;
        let generator = || {
            let columns: Vec<ArrayRef> = vec![
                Arc::new(Int64Array::from(
                    (0..rows)
                        .map(|_| thread_rng().gen::<i64>())
                        .collect::<Vec<_>>(),
                )),
                Arc::new(Int64Array::from(
                    (0..rows)
                        .map(|_| thread_rng().gen::<i64>())
                        .collect::<Vec<_>>(),
                )),
            ];
            RecordBatch::try_new(arrow_schema.clone(), columns).unwrap()
        };
        let batch = generator();
        // Writes about ~30Mi
        for _ in 0..10 {
            buffered_writer.write(&batch).await.unwrap();
        }
        buffered_writer.close().await.unwrap();
    }
}
