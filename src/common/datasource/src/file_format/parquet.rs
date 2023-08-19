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
use futures::future::BoxFuture;
use object_store::{ObjectStore, Reader};
use snafu::ResultExt;

use crate::buffered_writer::{ArrowWriterCloser, DfRecordBatchEncoder};
use crate::error::{self, Result};
use crate::file_format::FileFormat;
use crate::share_buffer::SharedBuffer;

#[derive(Debug, Clone, Copy, PartialEq, Eq, Default)]
pub struct ParquetFormat {}

#[async_trait]
impl FileFormat for ParquetFormat {
    async fn infer_schema(&self, store: &ObjectStore, path: &str) -> Result<Schema> {
        let mut reader = store
            .reader(path)
            .await
            .context(error::ReadObjectSnafu { path })?;

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
    reader: Option<Reader>,
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
            let reader = self.object_store.reader(&self.path).await?;
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
