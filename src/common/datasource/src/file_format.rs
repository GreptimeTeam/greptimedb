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

pub mod csv;
pub mod json;
pub mod orc;
pub mod parquet;
#[cfg(test)]
pub mod tests;

pub const DEFAULT_SCHEMA_INFER_MAX_RECORD: usize = 1000;

use std::collections::HashMap;
use std::result;
use std::sync::Arc;
use std::task::Poll;

use arrow::record_batch::RecordBatch;
use arrow_schema::{ArrowError, Schema as ArrowSchema};
use async_trait::async_trait;
use bytes::{Buf, Bytes};
use common_recordbatch::DfSendableRecordBatchStream;
use datafusion::datasource::file_format::file_compression_type::FileCompressionType as DfCompressionType;
use datafusion::datasource::listing::PartitionedFile;
use datafusion::datasource::object_store::ObjectStoreUrl;
use datafusion::datasource::physical_plan::{
    FileGroup, FileOpenFuture, FileScanConfigBuilder, FileSource, FileStream,
};
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::physical_plan::SendableRecordBatchStream;
use datafusion::physical_plan::metrics::ExecutionPlanMetricsSet;
use datatypes::arrow::datatypes::SchemaRef;
use futures::{StreamExt, TryStreamExt};
use object_store::ObjectStore;
use object_store_opendal::OpendalStore;
use snafu::ResultExt;
use tokio::io::AsyncWriteExt;
use tokio_util::compat::FuturesAsyncWriteCompatExt;

use self::csv::CsvFormat;
use self::json::JsonFormat;
use self::orc::OrcFormat;
use self::parquet::ParquetFormat;
use crate::DEFAULT_WRITE_BUFFER_SIZE;
use crate::buffered_writer::DfRecordBatchEncoder;
use crate::compressed_writer::{CompressedWriter, IntoCompressedWriter};
use crate::compression::CompressionType;
use crate::error::{self, Result};
use crate::share_buffer::SharedBuffer;

pub const FORMAT_COMPRESSION_TYPE: &str = "compression_type";
pub const FORMAT_DELIMITER: &str = "delimiter";
pub const FORMAT_SCHEMA_INFER_MAX_RECORD: &str = "schema_infer_max_record";
pub const FORMAT_HAS_HEADER: &str = "has_header";
pub const FORMAT_HEADER: &str = "header";
pub const FORMAT_CONTINUE_ON_ERROR: &str = "continue_on_error";
pub const FORMAT_TYPE: &str = "format";
pub const FILE_PATTERN: &str = "pattern";
pub const TIMESTAMP_FORMAT: &str = "timestamp_format";
pub const TIME_FORMAT: &str = "time_format";
pub const DATE_FORMAT: &str = "date_format";

#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Format {
    Csv(CsvFormat),
    Json(JsonFormat),
    Parquet(ParquetFormat),
    Orc(OrcFormat),
}

impl Format {
    pub fn suffix(&self) -> &'static str {
        match self {
            Format::Csv(_) => ".csv",
            Format::Json(_) => ".json",
            Format::Parquet(_) => ".parquet",
            &Format::Orc(_) => ".orc",
        }
    }
}

impl TryFrom<&HashMap<String, String>> for Format {
    type Error = error::Error;

    fn try_from(options: &HashMap<String, String>) -> Result<Self> {
        let format = options
            .get(FORMAT_TYPE)
            .map(|format| format.to_ascii_uppercase())
            .unwrap_or_else(|| "PARQUET".to_string());

        match format.as_str() {
            "CSV" => Ok(Self::Csv(CsvFormat::try_from(options)?)),
            "JSON" => Ok(Self::Json(JsonFormat::try_from(options)?)),
            "PARQUET" => Ok(Self::Parquet(ParquetFormat::default())),
            "ORC" => Ok(Self::Orc(OrcFormat)),
            _ => error::UnsupportedFormatSnafu { format: &format }.fail(),
        }
    }
}

#[async_trait]
pub trait FileFormat: Send + Sync + std::fmt::Debug {
    async fn infer_schema(&self, store: &ObjectStore, path: &str) -> Result<ArrowSchema>;
}

pub trait ArrowDecoder: Send + 'static {
    /// Decode records from `buf` returning the number of bytes read.
    ///
    /// This method returns `Ok(0)` once `batch_size` objects have been parsed since the
    /// last call to [`Self::flush`], or `buf` is exhausted.
    ///
    /// Any remaining bytes should be included in the next call to [`Self::decode`].
    fn decode(&mut self, buf: &[u8]) -> result::Result<usize, ArrowError>;

    /// Flushes the currently buffered data to a [`RecordBatch`].
    ///
    /// This should only be called after [`Self::decode`] has returned `Ok(0)`,
    /// otherwise may return an error if part way through decoding a record
    ///
    /// Returns `Ok(None)` if no buffered data.
    fn flush(&mut self) -> result::Result<Option<RecordBatch>, ArrowError>;
}

impl ArrowDecoder for arrow::csv::reader::Decoder {
    fn decode(&mut self, buf: &[u8]) -> result::Result<usize, ArrowError> {
        self.decode(buf)
    }

    fn flush(&mut self) -> result::Result<Option<RecordBatch>, ArrowError> {
        self.flush()
    }
}

impl ArrowDecoder for arrow::json::reader::Decoder {
    fn decode(&mut self, buf: &[u8]) -> result::Result<usize, ArrowError> {
        self.decode(buf)
    }

    fn flush(&mut self) -> result::Result<Option<RecordBatch>, ArrowError> {
        self.flush()
    }
}

pub fn open_with_decoder<T: ArrowDecoder, F: Fn() -> DataFusionResult<T>>(
    object_store: Arc<ObjectStore>,
    path: String,
    compression_type: CompressionType,
    decoder_factory: F,
) -> DataFusionResult<FileOpenFuture> {
    let mut decoder = decoder_factory()?;
    Ok(Box::pin(async move {
        let reader = object_store
            .reader(&path)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?
            .into_bytes_stream(..)
            .await
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let mut upstream = compression_type.convert_stream(reader).fuse();

        let mut buffered = Bytes::new();

        let stream = futures::stream::poll_fn(move |cx| {
            loop {
                if buffered.is_empty()
                    && let Some(result) = futures::ready!(upstream.poll_next_unpin(cx))
                {
                    buffered = result?;
                }

                let decoded = decoder.decode(buffered.as_ref())?;

                if decoded == 0 {
                    break;
                } else {
                    buffered.advance(decoded);
                }
            }

            Poll::Ready(decoder.flush().transpose())
        });

        Ok(stream.map_err(Into::into).boxed())
    }))
}

pub async fn infer_schemas(
    store: &ObjectStore,
    files: &[String],
    file_format: &dyn FileFormat,
) -> Result<ArrowSchema> {
    let mut schemas = Vec::with_capacity(files.len());
    for file in files {
        schemas.push(file_format.infer_schema(store, file).await?)
    }
    ArrowSchema::try_merge(schemas).context(error::MergeSchemaSnafu)
}

/// Writes data to a compressed writer if the data is not empty.
///
/// Does nothing if `data` is empty; otherwise writes all data and returns any error.
async fn write_to_compressed_writer(
    compressed_writer: &mut CompressedWriter,
    data: &[u8],
) -> Result<()> {
    if !data.is_empty() {
        compressed_writer
            .write_all(data)
            .await
            .context(error::AsyncWriteSnafu)?;
    }
    Ok(())
}

/// Streams [SendableRecordBatchStream] to a file with optional compression support.
/// Data is buffered and flushed according to the given `threshold`.
/// Ensures that writer resources are cleanly released and that an empty file is not
/// created if no rows are written.
///
/// Returns the total number of rows successfully written.
pub async fn stream_to_file<E>(
    mut stream: SendableRecordBatchStream,
    store: ObjectStore,
    path: &str,
    threshold: usize,
    concurrency: usize,
    compression_type: CompressionType,
    encoder_factory: impl Fn(SharedBuffer) -> E,
) -> Result<usize>
where
    E: DfRecordBatchEncoder,
{
    // Create the file writer with OpenDAL's built-in buffering
    let writer = store
        .writer_with(path)
        .concurrent(concurrency)
        .chunk(DEFAULT_WRITE_BUFFER_SIZE.as_bytes() as usize)
        .await
        .with_context(|_| error::WriteObjectSnafu { path })?
        .into_futures_async_write()
        .compat_write();

    // Apply compression if needed
    let mut compressed_writer = writer.into_compressed_writer(compression_type);

    // Create a buffer for the encoder
    let buffer = SharedBuffer::with_capacity(threshold);
    let mut encoder = encoder_factory(buffer.clone());

    let mut rows = 0;

    // Process each record batch
    while let Some(batch) = stream.next().await {
        let batch = batch.context(error::ReadRecordBatchSnafu)?;

        // Write batch using the encoder
        encoder.write(&batch)?;
        rows += batch.num_rows();

        loop {
            let chunk = {
                let mut buffer_guard = buffer.buffer.lock().unwrap();
                if buffer_guard.len() < threshold {
                    break;
                }
                buffer_guard.split_to(threshold)
            };
            write_to_compressed_writer(&mut compressed_writer, &chunk).await?;
        }
    }

    // If no row's been written, just simply close the underlying writer
    // without flush so that no file will be actually created.
    if rows != 0 {
        // Final flush of any remaining data
        let final_data = {
            let mut buffer_guard = buffer.buffer.lock().unwrap();
            buffer_guard.split()
        };
        write_to_compressed_writer(&mut compressed_writer, &final_data).await?;
    }

    // Shutdown compression and close writer
    compressed_writer.shutdown().await?;

    Ok(rows)
}

/// Creates a [FileStream] for reading data from a file with optional column projection
/// and compression support.
///
/// Returns [SendableRecordBatchStream].
pub async fn file_to_stream(
    store: &ObjectStore,
    filename: &str,
    file_schema: SchemaRef,
    file_source: Arc<dyn FileSource>,
    projection: Option<Vec<usize>>,
    compression_type: CompressionType,
) -> Result<DfSendableRecordBatchStream> {
    let df_compression: DfCompressionType = compression_type.into();
    let config = FileScanConfigBuilder::new(
        ObjectStoreUrl::local_filesystem(),
        file_schema,
        file_source.clone(),
    )
    .with_file_group(FileGroup::new(vec![PartitionedFile::new(
        filename.to_string(),
        0,
    )]))
    .with_projection(projection)
    .with_file_compression_type(df_compression)
    .build();

    let store = Arc::new(OpendalStore::new(store.clone()));
    let file_opener = file_source
        .with_projection(&config)
        .create_file_opener(store, &config, 0);
    let stream = FileStream::new(&config, 0, file_opener, &ExecutionPlanMetricsSet::new())
        .context(error::BuildFileStreamSnafu)?;

    Ok(Box::pin(stream))
}
