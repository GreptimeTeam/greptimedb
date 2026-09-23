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
use tokio_util::sync::CancellationToken;

use crate::DEFAULT_WRITE_BUFFER_SIZE;
use crate::error::{self, Result};

/// Destination creation policy; conditional failures never authorize path deletion.
#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum ParquetCreationPolicy {
    Overwrite,
    IfNotExists,
}

/// Limits for one Parquet file. Flush thresholds are not hard memory caps.
#[derive(Clone, Copy, Debug)]
pub struct ParquetWriterLimits {
    /// Maximum rows in a row group.
    pub row_group_rows: usize,
    /// Flush when encoder memory or encoded size reaches this threshold.
    pub flush_threshold_bytes: usize,
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
    creation: ParquetCreationPolicy,
    close_started: bool,
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
        Self::open_with_creation(
            schema,
            store,
            path,
            concurrency,
            limits,
            ParquetCreationPolicy::Overwrite,
        )
        .await
    }

    /// Open with a conditional policy only when the caller verified backend support.
    pub async fn open_with_creation(
        schema: SchemaRef,
        store: ObjectStore,
        path: &str,
        concurrency: usize,
        limits: Option<ParquetWriterLimits>,
        creation: ParquetCreationPolicy,
    ) -> Result<Self> {
        let mut props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_statistics_truncate_length(None)
            .set_column_index_truncate_length(None);
        if let Some(limits) = limits {
            ensure!(
                limits.row_group_rows > 0
                    && limits.flush_threshold_bytes > 0
                    && limits.max_row_groups > 0,
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
            .if_not_exists(creation == ParquetCreationPolicy::IfNotExists)
            .await
            .context(error::WriteObjectSnafu { path })?;
        Ok(Self {
            encoder: Some(encoder),
            sink,
            store,
            path: path.to_owned(),
            limits,
            creation,
            close_started: false,
        })
    }

    /// Write a batch, enforcing file limits across batch and row-group boundaries.
    pub async fn write(
        &mut self,
        batch: RecordBatch,
        cancellation: Option<&CancellationToken>,
    ) -> Result<()> {
        let mut offset = 0;
        while offset < batch.num_rows() {
            check_cancelled(cancellation)?;
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
                    encoder.memory_size() >= limits.flush_threshold_bytes
                        || encoder.in_progress_size() >= limits.flush_threshold_bytes
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
            check_cancelled(cancellation)?;
            self.write_bytes(bytes).await?;
            check_cancelled(cancellation)?;
            offset += len;
        }
        Ok(())
    }

    async fn write_bytes(&mut self, bytes: Vec<u8>) -> Result<()> {
        let bytes = Bytes::from(bytes);
        let chunk = DEFAULT_WRITE_BUFFER_SIZE.as_bytes() as usize;
        // Slices retain the complete encoded allocation until its last submission.
        for offset in (0..bytes.len()).step_by(chunk) {
            self.sink
                .write(bytes.slice(offset..(offset + chunk).min(bytes.len())))
                .await
                .context(error::WriteObjectSnafu { path: &self.path })?;
        }
        Ok(())
    }

    /// Write the footer and close the file. Retains the handle for cleanup on error.
    pub async fn finish(&mut self, cancellation: Option<&CancellationToken>) -> Result<()> {
        check_cancelled(cancellation)?;
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
        check_cancelled(cancellation)?;
        self.close_started = true;
        self.sink
            .close()
            .await
            .context(error::WriteObjectSnafu { path: &self.path })?;
        check_cancelled(cancellation)?;
        Ok(())
    }

    /// Abort after all in-flight operations complete. Preserve ambiguous commits;
    /// conditional callers delegate cleanup exclusively to the backend.
    pub async fn abort(mut self) -> Result<()> {
        let result = self.sink.abort().await;
        if self.creation == ParquetCreationPolicy::Overwrite
            && !self.close_started
            && result
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

fn check_cancelled(cancellation: Option<&CancellationToken>) -> Result<()> {
    ensure!(
        cancellation.is_none_or(|token| !token.is_cancelled()),
        error::ParquetWriteCancelledSnafu
    );
    Ok(())
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
    use common_error::ext::ErrorExt;
    use common_error::status_code::StatusCode;
    use datafusion::physical_plan::stream::RecordBatchStreamAdapter;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use super::*;
    use crate::file_format::parquet::stream_to_parquet;

    fn batch() -> RecordBatch {
        RecordBatch::try_from_iter([
            (
                "value",
                Arc::new(Int64Array::from(vec![Some(1), None, Some(3), Some(4)]))
                    as arrow::array::ArrayRef,
            ),
            (
                "ts",
                Arc::new(TimestampMillisecondArray::from(vec![1, 2, 3, 4])),
            ),
        ])
        .unwrap()
    }

    async fn read(store: &ObjectStore, path: &str) -> ParquetRecordBatchReaderBuilder<Bytes> {
        ParquetRecordBatchReaderBuilder::try_new(store.read(path).await.unwrap().to_bytes())
            .unwrap()
    }

    #[tokio::test]
    async fn abort_preserves_collisions_and_ambiguous_commits() {
        use object_store::layers::mock::{MockLayerBuilder, MockWriterFactory, oio};

        struct AmbiguousCommit(oio::Writer);
        impl oio::Write for AmbiguousCommit {
            async fn write(&mut self, bytes: object_store::Buffer) -> object_store::Result<()> {
                self.0.write(bytes).await
            }
            async fn close(
                &mut self,
            ) -> object_store::Result<object_store::layers::mock::Metadata> {
                self.0.close().await?;
                Err(object_store::Error::new(
                    object_store::ErrorKind::Unexpected,
                    "lost close reply",
                ))
            }
            async fn abort(&mut self) -> object_store::Result<()> {
                Err(object_store::Error::new(
                    object_store::ErrorKind::Unsupported,
                    "cannot abort",
                ))
            }
        }

        for (creation, existing) in [
            (ParquetCreationPolicy::Overwrite, false),
            (ParquetCreationPolicy::IfNotExists, false),
            (ParquetCreationPolicy::IfNotExists, true),
        ] {
            let directory = common_test_util::temp_dir::create_temp_dir("conditional_parquet");
            let store = object_store::secure_fs::SecureFsRoot::open(directory.path())
                .unwrap()
                .build_operator();
            let path = "conditional.parquet";
            if existing {
                store.write(path, "original").await.unwrap();
            }
            let factory: MockWriterFactory =
                Arc::new(|_, _, writer| Box::new(AmbiguousCommit(writer)));
            let store = store.layer(
                MockLayerBuilder::default()
                    .writer_factory(factory)
                    .build()
                    .unwrap(),
            );
            let mut writer = ParquetFileWriter::open_with_creation(
                batch().schema(),
                store.clone(),
                path,
                1,
                None,
                creation,
            )
            .await
            .unwrap();
            writer.write(batch(), None).await.unwrap();
            assert!(writer.finish(None).await.is_err());
            assert!(writer.abort().await.is_err());
            if existing {
                assert_eq!(
                    store.read(path).await.unwrap().to_bytes(),
                    Bytes::from_static(b"original")
                );
            } else {
                assert_eq!(
                    read(&store, path)
                        .await
                        .metadata()
                        .file_metadata()
                        .num_rows(),
                    4
                );
            }
        }
    }

    #[tokio::test]
    async fn large_footer_flush_uses_bounded_submissions_in_one_parquet_stream() {
        use std::sync::Mutex;

        use object_store::layers::mock::{Metadata, MockLayerBuilder, MockWriterFactory, oio};
        struct ObservedWriter(oio::Writer, Arc<Mutex<Vec<usize>>>);
        impl oio::Write for ObservedWriter {
            async fn write(&mut self, bytes: object_store::Buffer) -> object_store::Result<()> {
                self.1.lock().unwrap().push(bytes.len());
                self.0.write(bytes).await
            }
            async fn close(&mut self) -> object_store::Result<Metadata> {
                self.0.close().await
            }
            async fn abort(&mut self) -> object_store::Result<()> {
                self.0.abort().await
            }
        }
        let directory = common_test_util::temp_dir::create_temp_dir("bounded_parquet");
        let store = object_store::secure_fs::SecureFsRoot::open(directory.path())
            .unwrap()
            .build_operator();
        let sizes = Arc::new(Mutex::new(Vec::new()));
        let factory: MockWriterFactory = Arc::new({
            let sizes = sizes.clone();
            move |_, _, writer| Box::new(ObservedWriter(writer, sizes.clone()))
        });
        let store = store.layer(
            MockLayerBuilder::default()
                .writer_factory(factory)
                .build()
                .unwrap(),
        );
        let mut state = 17u64;
        let values = (0..600_000)
            .map(|_| {
                state ^= state << 13;
                state ^= state >> 7;
                state ^= state << 17;
                state as i64
            })
            .collect::<Vec<_>>();
        let array = Arc::new(Int64Array::from(values)) as arrow::array::ArrayRef;
        let batch = RecordBatch::try_from_iter([("a", array.clone()), ("b", array)]).unwrap();
        let mut writer =
            ParquetFileWriter::open(batch.schema(), store.clone(), "large.parquet", 1, None)
                .await
                .unwrap();
        // No storage-layer chunking: observe the application's actual submissions.
        writer.sink = store.writer("large.parquet").await.unwrap();
        writer.write(batch.clone(), None).await.unwrap();
        writer.finish(None).await.unwrap();
        let sizes = sizes.lock().unwrap().clone();
        let limit = DEFAULT_WRITE_BUFFER_SIZE.as_bytes() as usize;
        assert!(sizes.iter().sum::<usize>() > limit);
        assert!(sizes.iter().all(|size| *size <= limit), "{sizes:?}");
        let actual = read(&store, "large.parquet")
            .await
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
    async fn row_and_byte_limits_split_batches() {
        let store = ObjectStore::new(object_store::services::Memory::default()).unwrap();
        let batch = batch();
        for (row_group_rows, flush_threshold_bytes, split) in [(2, usize::MAX, 3), (100, 1, 2)] {
            let mut writer = ParquetFileWriter::open(
                batch.schema(),
                store.clone(),
                "groups.parquet",
                1,
                Some(ParquetWriterLimits {
                    row_group_rows,
                    flush_threshold_bytes,
                    max_row_groups: 2,
                }),
            )
            .await
            .unwrap();
            writer.write(batch.slice(0, split), None).await.unwrap();
            writer
                .write(batch.slice(split, batch.num_rows() - split), None)
                .await
                .unwrap();
            writer.finish(None).await.unwrap();
            let reader = read(&store, "groups.parquet").await;
            assert_eq!(reader.metadata().num_row_groups(), 2);
            for group in reader.metadata().row_groups() {
                assert_eq!(group.num_rows(), 2);
                let encodings = group.column(1).encodings().collect::<Vec<_>>();
                assert!(encodings.contains(&Encoding::DELTA_BINARY_PACKED));
                assert!(!encodings.contains(&Encoding::RLE_DICTIONARY));
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
                flush_threshold_bytes: usize::MAX,
                max_row_groups: 1,
            }),
        )
        .await
        .unwrap();
        let err = writer.write(batch, None).await.unwrap_err();
        assert!(matches!(err, error::Error::ParquetWriterResource { .. }));
        assert_eq!(err.status_code(), StatusCode::Suspended);
        writer.abort().await.unwrap();
        assert!(!store.exists("limited.parquet").await.unwrap());
    }

    #[tokio::test]
    async fn copy_stream_preserves_values_and_empty_schema() {
        let store = ObjectStore::new(object_store::services::Memory::default()).unwrap();
        let batch = batch();
        for (path, batches) in [
            ("copy.parquet", vec![batch.clone()]),
            ("empty.parquet", vec![]),
        ] {
            let stream = RecordBatchStreamAdapter::new(
                batch.schema(),
                futures::stream::iter(batches.clone().into_iter().map(Ok)),
            );
            assert_eq!(
                stream_to_parquet(Box::pin(stream), store.clone(), path, 2)
                    .await
                    .unwrap(),
                batches.iter().map(RecordBatch::num_rows).sum::<usize>()
            );
            let reader = read(&store, path).await;
            assert_eq!(reader.schema().fields(), batch.schema().fields());
            let actual = reader
                .build()
                .unwrap()
                .collect::<std::result::Result<Vec<_>, _>>()
                .unwrap();
            assert_eq!(actual, batches);
        }
    }

    #[tokio::test]
    async fn failed_copy_preserves_untouched_securefs_destination() {
        let directory = common_test_util::temp_dir::create_temp_dir("copy_existing");
        let path = directory.path().join("existing.parquet");
        std::fs::write(&path, b"original bytes").unwrap();
        let access = crate::object_store::LocalFileAccess::sandboxed(directory.path()).unwrap();
        let store = crate::object_store::build_backend_for_write(
            &format!("{}/", directory.path().display()),
            &Default::default(),
            &access,
        )
        .await
        .unwrap();
        let stream = RecordBatchStreamAdapter::new(
            batch().schema(),
            futures::stream::iter(vec![Err(datafusion::error::DataFusionError::Execution(
                "injected input failure".into(),
            ))]),
        );
        let err = stream_to_parquet(Box::pin(stream), store, "existing.parquet", 1)
            .await
            .unwrap_err();
        assert!(matches!(err, error::Error::ReadRecordBatch { .. }));
        assert_eq!(std::fs::read(path).unwrap(), b"original bytes");
    }

    struct PausedFooterWriter {
        inner: object_store::layers::mock::oio::Writer,
        paused: bool,
        started: Arc<tokio::sync::Notify>,
        release: Arc<tokio::sync::Notify>,
        closed: Arc<std::sync::atomic::AtomicBool>,
    }

    impl object_store::layers::mock::oio::Write for PausedFooterWriter {
        async fn write(&mut self, bytes: object_store::Buffer) -> object_store::Result<()> {
            if !self.paused {
                self.paused = true;
                self.started.notify_one();
                self.release.notified().await;
            }
            self.inner.write(bytes).await
        }
        async fn close(&mut self) -> object_store::Result<object_store::layers::mock::Metadata> {
            self.closed.store(true, std::sync::atomic::Ordering::SeqCst);
            self.inner.close().await
        }
        async fn abort(&mut self) -> object_store::Result<()> {
            self.inner.abort().await
        }
    }

    #[tokio::test]
    async fn cancellation_during_footer_write_waits_then_aborts_before_close() {
        use object_store::layers::mock::{MockLayerBuilder, MockWriterFactory};
        let started = Arc::new(tokio::sync::Notify::new());
        let release = Arc::new(tokio::sync::Notify::new());
        let closed = Arc::new(std::sync::atomic::AtomicBool::new(false));
        let factory: MockWriterFactory = Arc::new({
            let (started, release, closed) = (started.clone(), release.clone(), closed.clone());
            move |_, _, inner| {
                Box::new(PausedFooterWriter {
                    inner,
                    paused: false,
                    started: started.clone(),
                    release: release.clone(),
                    closed: closed.clone(),
                })
            }
        });
        let store = ObjectStore::new(object_store::services::Memory::default())
            .unwrap()
            .layer(
                MockLayerBuilder::default()
                    .writer_factory(factory)
                    .build()
                    .unwrap(),
            );
        let mut writer =
            ParquetFileWriter::open(batch().schema(), store.clone(), "cancel.parquet", 1, None)
                .await
                .unwrap();
        // Force footer bytes through the sink before its close operation.
        writer.sink = store.writer_with("cancel.parquet").chunk(1).await.unwrap();
        let cancellation = CancellationToken::new();
        let result = {
            let finish = writer.finish(Some(&cancellation));
            tokio::pin!(finish);
            tokio::select! {
                result = &mut finish => panic!("finished before footer write: {result:?}"),
                _ = started.notified() => {},
            }
            cancellation.cancel();
            assert!(futures::poll!(&mut finish).is_pending());
            release.notify_one();
            finish.await
        };
        assert!(matches!(
            result,
            Err(error::Error::ParquetWriteCancelled {})
        ));
        assert!(!closed.load(std::sync::atomic::Ordering::SeqCst));
        writer.abort().await.unwrap();
        assert!(!store.exists("cancel.parquet").await.unwrap());
    }
}
