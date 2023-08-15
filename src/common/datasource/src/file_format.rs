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
use datafusion::datasource::physical_plan::FileOpenFuture;
use datafusion::error::{DataFusionError, Result as DataFusionResult};
use datafusion::physical_plan::SendableRecordBatchStream;
use futures::StreamExt;
use object_store::ObjectStore;
use snafu::ResultExt;

use self::csv::CsvFormat;
use self::json::JsonFormat;
use self::orc::OrcFormat;
use self::parquet::ParquetFormat;
use crate::buffered_writer::{DfRecordBatchEncoder, LazyBufferedWriter};
use crate::compression::CompressionType;
use crate::error::{self, Result};
use crate::share_buffer::SharedBuffer;

pub const FORMAT_COMPRESSION_TYPE: &str = "compression_type";
pub const FORMAT_DELIMITER: &str = "delimiter";
pub const FORMAT_SCHEMA_INFER_MAX_RECORD: &str = "schema_infer_max_record";
pub const FORMAT_HAS_HEADER: &str = "has_header";
pub const FORMAT_TYPE: &str = "format";
pub const FILE_PATTERN: &str = "pattern";

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
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

#[allow(deprecated)]
impl ArrowDecoder for arrow::json::RawDecoder {
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
            .map_err(|e| DataFusionError::External(Box::new(e)))?;

        let mut upstream = compression_type.convert_stream(reader).fuse();

        let mut buffered = Bytes::new();

        let stream = futures::stream::poll_fn(move |cx| {
            loop {
                if buffered.is_empty() {
                    if let Some(result) = futures::ready!(upstream.poll_next_unpin(cx)) {
                        buffered = result?;
                    };
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

        Ok(stream.boxed())
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

pub async fn stream_to_file<T: DfRecordBatchEncoder, U: Fn(SharedBuffer) -> T>(
    mut stream: SendableRecordBatchStream,
    store: ObjectStore,
    path: &str,
    threshold: usize,
    encoder_factory: U,
) -> Result<usize> {
    let buffer = SharedBuffer::with_capacity(threshold);
    let encoder = encoder_factory(buffer.clone());
    let mut writer = LazyBufferedWriter::new(threshold, buffer, encoder, path, |path| async {
        store
            .writer(&path)
            .await
            .context(error::WriteObjectSnafu { path })
    });

    let mut rows = 0;

    while let Some(batch) = stream.next().await {
        let batch = batch.context(error::ReadRecordBatchSnafu)?;
        writer.write(&batch).await?;
        rows += batch.num_rows();
    }

    // Flushes all pending writes
    let _ = writer.try_flush(true).await?;
    writer.close_inner_writer().await?;

    Ok(rows)
}
