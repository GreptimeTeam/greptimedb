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

use bytes::Bytes;
use datatypes::arrow::datatypes::SchemaRef;
use datatypes::arrow::record_batch::RecordBatch;
use futures::future::BoxFuture;
use object_store::{ObjectStore, Writer};
use parquet::arrow::AsyncArrowWriter;
use parquet::arrow::async_writer::AsyncFileWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use snafu::{OptionExt, ResultExt};

use crate::access_layer::TempFileCleaner;
use crate::error::{OpenDalSnafu, Result, UnexpectedSnafu, WriteParquetSnafu};
use crate::sst::{DEFAULT_WRITE_BUFFER_SIZE, DEFAULT_WRITE_CONCURRENCY};

type ArrowWriter = AsyncArrowWriter<AsyncWriter>;

/// Bridges an OpenDAL [`Writer`] with Parquet's [`AsyncFileWriter`] and tracks
/// the number of bytes successfully submitted to the object store.
struct AsyncWriter {
    inner: Writer,
    output_bytes: u64,
}

impl AsyncWriter {
    fn new(inner: Writer) -> Self {
        Self {
            inner,
            output_bytes: 0,
        }
    }

    fn output_bytes(&self) -> u64 {
        self.output_bytes
    }

    fn into_inner(self) -> Writer {
        self.inner
    }
}

impl AsyncFileWriter for AsyncWriter {
    fn write(&mut self, bytes: Bytes) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async move {
            let len = bytes.len() as u64;
            self.inner
                .write(bytes)
                .await
                .map_err(|error| ParquetError::External(Box::new(error)))?;
            self.output_bytes += len;
            Ok(())
        })
    }

    fn complete(&mut self) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async move {
            self.inner
                .close()
                .await
                .map(|_| ())
                .map_err(|error| ParquetError::External(Box::new(error)))
        })
    }
}

/// Shared Parquet output and cleanup lifecycle for index writers.
pub(crate) struct ParquetIndexWriter {
    name: &'static str,
    object_store: ObjectStore,
    file_name: String,
    writer: Option<ArrowWriter>,
}

impl ParquetIndexWriter {
    /// Opens an index file with the common Parquet writer configuration.
    pub(crate) async fn try_new(
        name: &'static str,
        object_store: ObjectStore,
        path: &str,
        schema: &SchemaRef,
        row_group_size: usize,
    ) -> Result<Self> {
        let file_name = path.rsplit('/').next().unwrap_or(path).to_string();
        let output = object_store
            .writer_with(path)
            .chunk(DEFAULT_WRITE_BUFFER_SIZE.as_bytes() as usize)
            .concurrent(DEFAULT_WRITE_CONCURRENCY)
            .await
            .context(OpenDalSnafu)?;
        let properties = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_encoding(Encoding::PLAIN)
            .set_max_row_group_row_count(Some(row_group_size))
            .set_column_index_truncate_length(None)
            .set_statistics_truncate_length(None)
            .build();
        let writer =
            AsyncArrowWriter::try_new(AsyncWriter::new(output), schema.clone(), Some(properties))
                .context(WriteParquetSnafu)?;

        Ok(Self {
            name,
            object_store,
            file_name,
            writer: Some(writer),
        })
    }

    /// Writes one batch to the index file.
    pub(crate) async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        self.writer
            .as_mut()
            .context(UnexpectedSnafu {
                reason: format!("{} Parquet writer is closed", self.name),
            })?
            .write(batch)
            .await
            .context(WriteParquetSnafu)
    }

    /// Finishes the index file and returns its committed size.
    pub(crate) async fn finish(&mut self) -> Result<u64> {
        self.writer
            .as_mut()
            .context(UnexpectedSnafu {
                reason: format!("{} Parquet writer is closed", self.name),
            })?
            .finish()
            .await
            .context(WriteParquetSnafu)?;
        let writer = self.writer.take().context(UnexpectedSnafu {
            reason: format!("{} Parquet writer is closed", self.name),
        })?;
        Ok(writer.into_inner().output_bytes())
    }

    /// Aborts an incomplete output and removes its atomic-write temporary files.
    pub(crate) async fn abort(&mut self) {
        if let Some(writer) = self.writer.take() {
            let mut writer = writer.into_inner().into_inner();
            if let Err(error) = writer.abort().await {
                common_telemetry::warn!(error; "Failed to abort {} writer", self.name);
            }
        }

        TempFileCleaner::clean_atomic_dir_files(&self.object_store, &[&self.file_name]).await;
    }
}
