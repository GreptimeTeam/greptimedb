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

use arrow::datatypes::{DataType, SchemaRef};
use arrow::record_batch::RecordBatch;
use bytes::Bytes;
use futures::future::BoxFuture;
use object_store::{ObjectStore, Writer};
use parquet::arrow::ArrowWriter;
use parquet::arrow::async_writer::AsyncFileWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use parquet::schema::types::ColumnPath;
use snafu::{IntoError, ResultExt, ensure};

use crate::DEFAULT_WRITE_BUFFER_SIZE;
use crate::error::{self, Result};

/// Limits for one Parquet file. Flush thresholds are not hard memory caps.
#[derive(Clone, Copy, Debug)]
pub struct ParquetWriterLimits {
    /// Maximum rows in a row group.
    pub row_group_rows: usize,
    /// Flush when encoder memory or encoded size reaches this threshold.
    pub writer_bytes: usize,
    /// Maximum row groups retained in the file footer.
    pub max_row_groups: usize,
}

/// Encodes batches on a blocking runtime and writes one object-store file.
/// Callers must await each operation before aborting; dropping an in-flight
/// operation can leave a filesystem worker running after cleanup.
pub struct ParquetFileWriter {
    encoder: Option<ArrowWriter<Vec<u8>>>,
    sink: Writer,
    store: ObjectStore,
    path: String,
    limits: Option<ParquetWriterLimits>,
}

impl ParquetFileWriter {
    /// Open a file using COPY's encoding settings. Destination ownership and
    /// overwrite policy belong to the caller. None preserves Parquet's defaults.
    pub async fn open(
        schema: SchemaRef,
        store: ObjectStore,
        path: &str,
        concurrency: usize,
        limits: Option<ParquetWriterLimits>,
    ) -> Result<Self> {
        let mut props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_statistics_truncate_length(None)
            .set_column_index_truncate_length(None);
        if let Some(limits) = limits {
            ensure!(
                limits.row_group_rows > 0 && limits.writer_bytes > 0 && limits.max_row_groups > 0,
                error::InvalidParquetWriterLimitsSnafu
            );
            props = props
                .set_max_row_group_row_count(Some(limits.row_group_rows))
                .set_max_row_group_bytes(None);
        }
        for field in schema.fields() {
            if matches!(field.data_type(), DataType::Timestamp(_, _)) {
                let column = ColumnPath::new(vec![field.name().clone()]);
                props = props
                    .set_column_dictionary_enabled(column.clone(), false)
                    .set_column_encoding(column, Encoding::DELTA_BINARY_PACKED);
            }
        }
        let encoder = ArrowWriter::try_new(Vec::new(), schema, Some(props.build()))
            .context(error::WriteParquetSnafu { path })?;
        let sink = store
            .writer_with(path)
            .concurrent(concurrency)
            .chunk(DEFAULT_WRITE_BUFFER_SIZE.as_bytes() as usize)
            .await
            .context(error::WriteObjectSnafu { path })?;
        Ok(Self {
            encoder: Some(encoder),
            sink,
            store,
            path: path.to_owned(),
            limits,
        })
    }

    /// Write a batch, enforcing file limits across batch and row-group boundaries.
    pub async fn write(&mut self, batch: RecordBatch) -> Result<()> {
        let mut offset = 0;
        while offset < batch.num_rows() {
            let mut encoder = self.encoder.take().ok_or_else(|| {
                error::WriteParquetSnafu { path: &self.path }
                    .into_error(ParquetError::General("Parquet writer is closed".into()))
            })?;
            let len = self.limits.map_or(batch.num_rows() - offset, |limits| {
                (limits.row_group_rows - encoder.in_progress_rows()).min(batch.num_rows() - offset)
            });
            let slice = batch.slice(offset, len);
            let limits = self.limits;
            let path = self.path.clone();
            let (encoder, bytes) = common_runtime::spawn_blocking_global(move || {
                if let Some(limits) = limits {
                    ensure!(
                        encoder.flushed_row_groups().len() < limits.max_row_groups,
                        error::ParquetWriterResourceSnafu {
                            reason: "Parquet row-group metadata budget exceeded"
                        }
                    );
                }
                encoder
                    .write(&slice)
                    .context(error::WriteParquetSnafu { path: &path })?;
                if limits.is_some_and(|limits| {
                    encoder.memory_size() >= limits.writer_bytes
                        || encoder.in_progress_size() >= limits.writer_bytes
                }) {
                    encoder
                        .flush()
                        .context(error::WriteParquetSnafu { path: &path })?;
                }
                // Draining preserves ArrowWriter's cumulative file offsets.
                let bytes = std::mem::take(encoder.inner_mut());
                Ok::<_, error::Error>((encoder, bytes))
            })
            .await
            .context(error::JoinHandleSnafu)??;
            self.encoder = Some(encoder);
            self.write_bytes(bytes).await?;
            offset += len;
        }
        Ok(())
    }

    async fn write_bytes(&mut self, bytes: Vec<u8>) -> Result<()> {
        if !bytes.is_empty() {
            self.sink
                .write(bytes)
                .await
                .context(error::WriteObjectSnafu { path: &self.path })?;
        }
        Ok(())
    }

    /// Write the footer and close the file. Retains the handle for cleanup on error.
    pub async fn finish(&mut self) -> Result<()> {
        let mut encoder = self.encoder.take().ok_or_else(|| {
            error::WriteParquetSnafu { path: &self.path }
                .into_error(ParquetError::General("Parquet writer is closed".into()))
        })?;
        let path = self.path.clone();
        let bytes = common_runtime::spawn_blocking_global(move || {
            encoder
                .finish()
                .context(error::WriteParquetSnafu { path: &path })?;
            Ok::<_, error::Error>(std::mem::take(encoder.inner_mut()))
        })
        .await
        .context(error::JoinHandleSnafu)??;
        self.write_bytes(bytes).await?;
        self.sink
            .close()
            .await
            .context(error::WriteObjectSnafu { path: &self.path })?;
        Ok(())
    }

    /// Abort an incomplete file after all in-flight operations have completed.
    pub async fn abort(mut self) -> Result<()> {
        let result = self.sink.abort().await;
        if result
            .as_ref()
            .is_err_and(|e| e.kind() == object_store::ErrorKind::Unsupported)
        {
            let store = self.store.clone();
            let path = self.path.clone();
            // Secure filesystem writers require dropping the handle before deletion.
            drop(self);
            store
                .delete(&path)
                .await
                .context(error::WriteObjectSnafu { path })?;
        } else {
            result.context(error::WriteObjectSnafu { path: &self.path })?;
        }
        Ok(())
    }
}

/// Bridges opendal [Writer] with parquet [AsyncFileWriter].
pub struct AsyncWriter {
    inner: Writer,
}

impl AsyncWriter {
    /// Create a [`AsyncWriter`] by given [`Writer`].
    pub fn new(writer: Writer) -> Self {
        Self { inner: writer }
    }
}

impl AsyncFileWriter for AsyncWriter {
    fn write(&mut self, bs: Bytes) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async move {
            self.inner
                .write(bs)
                .await
                .map_err(|err| ParquetError::External(Box::new(err)))
        })
    }

    fn complete(&mut self) -> BoxFuture<'_, parquet::errors::Result<()>> {
        Box::pin(async move {
            self.inner
                .close()
                .await
                .map(|_| ())
                .map_err(|err| ParquetError::External(Box::new(err)))
        })
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use arrow::array::{Int64Array, TimestampMillisecondArray};
    use arrow::datatypes::{Field, Schema, TimeUnit};
    use common_error::ext::ErrorExt;
    use common_error::status_code::StatusCode;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use super::*;
    use crate::file_format::parquet::stream_to_parquet;

    fn batch() -> RecordBatch {
        RecordBatch::try_new(
            Arc::new(Schema::new(vec![
                Field::new("value", DataType::Int64, true),
                Field::new(
                    "ts",
                    DataType::Timestamp(TimeUnit::Millisecond, None),
                    false,
                ),
            ])),
            vec![
                Arc::new(Int64Array::from(vec![Some(1), None, Some(3), Some(4)])),
                Arc::new(TimestampMillisecondArray::from(vec![1, 2, 3, 4])),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn row_group_limits_cross_batch_boundaries() {
        let store = ObjectStore::new(object_store::services::Memory::default()).unwrap();
        let batch = batch();
        let mut writer = ParquetFileWriter::open(
            batch.schema(),
            store.clone(),
            "groups.parquet",
            1,
            Some(ParquetWriterLimits {
                row_group_rows: 2,
                writer_bytes: usize::MAX,
                max_row_groups: 2,
            }),
        )
        .await
        .unwrap();
        writer.write(batch.slice(0, 3)).await.unwrap();
        writer.write(batch.slice(3, 1)).await.unwrap();
        writer.finish().await.unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(
            store.read("groups.parquet").await.unwrap().to_bytes(),
        )
        .unwrap();
        assert_eq!(reader.metadata().num_row_groups(), 2);
        for group in reader.metadata().row_groups() {
            assert_eq!(group.num_rows(), 2);
            assert!(
                group
                    .column(1)
                    .encodings()
                    .any(|encoding| encoding == Encoding::DELTA_BINARY_PACKED)
            );
            assert!(
                !group
                    .column(1)
                    .encodings()
                    .any(|encoding| encoding == Encoding::RLE_DICTIONARY)
            );
            assert_eq!(
                group.column(0).compression(),
                Compression::ZSTD(ZstdLevel::default())
            );
        }
        let actual = reader
            .build()
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        assert_eq!(
            arrow::compute::concat_batches(&batch.schema(), &actual).unwrap(),
            batch
        );
    }

    #[tokio::test]
    async fn oversized_batch_stops_at_footer_limit_and_can_abort() {
        let store = ObjectStore::new(object_store::services::Memory::default()).unwrap();
        let batch = batch();
        let mut writer = ParquetFileWriter::open(
            batch.schema(),
            store.clone(),
            "limited.parquet",
            1,
            Some(ParquetWriterLimits {
                row_group_rows: 2,
                writer_bytes: usize::MAX,
                max_row_groups: 1,
            }),
        )
        .await
        .unwrap();
        let err = writer.write(batch).await.unwrap_err();
        assert!(matches!(err, error::Error::ParquetWriterResource { .. }));
        assert_eq!(err.status_code(), StatusCode::Suspended);
        writer.abort().await.unwrap();
        assert!(!store.exists("limited.parquet").await.unwrap());
    }

    #[tokio::test]
    async fn encoder_threshold_flushes_before_row_limit() {
        let store = ObjectStore::new(object_store::services::Memory::default()).unwrap();
        let batch = batch();
        let mut writer = ParquetFileWriter::open(
            batch.schema(),
            store.clone(),
            "flush.parquet",
            1,
            Some(ParquetWriterLimits {
                row_group_rows: 100,
                writer_bytes: 1,
                max_row_groups: 2,
            }),
        )
        .await
        .unwrap();
        writer.write(batch.slice(0, 2)).await.unwrap();
        writer.write(batch.slice(2, 2)).await.unwrap();
        writer.finish().await.unwrap();
        let reader = ParquetRecordBatchReaderBuilder::try_new(
            store.read("flush.parquet").await.unwrap().to_bytes(),
        )
        .unwrap();
        assert_eq!(reader.metadata().num_row_groups(), 2);
        assert!(
            reader
                .metadata()
                .row_groups()
                .iter()
                .all(|group| group.num_rows() == 2)
        );
    }

    #[tokio::test]
    async fn copy_stream_preserves_values_and_empty_schema() {
        let store = ObjectStore::new(object_store::services::Memory::default()).unwrap();
        let batch = batch();
        for empty in [false, true] {
            let batches = if empty {
                vec![]
            } else {
                vec![Ok(batch.clone())]
            };
            let stream =
                RecordBatchStreamAdapter::new(batch.schema(), futures::stream::iter(batches));
            let path = if empty {
                "empty.parquet"
            } else {
                "copy.parquet"
            };
            assert_eq!(
                stream_to_parquet(Box::pin(stream), store.clone(), path, 2)
                    .await
                    .unwrap(),
                if empty { 0 } else { 4 }
            );
            let reader = ParquetRecordBatchReaderBuilder::try_new(
                store.read(path).await.unwrap().to_bytes(),
            )
            .unwrap();
            assert_eq!(reader.schema().fields(), batch.schema().fields());
            let actual = reader
                .build()
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap();
            if empty {
                assert!(actual.is_empty());
            } else {
                assert_eq!(actual, vec![batch.clone()]);
            }
        }
    }
}
