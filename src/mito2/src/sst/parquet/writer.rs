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

//! Parquet writer.

use std::future::Future;
use std::mem;
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};
use std::time::Instant;

use common_telemetry::debug;
use common_time::Timestamp;
use datatypes::arrow::datatypes::SchemaRef;
use object_store::{FuturesAsyncWriter, ObjectStore};
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use parquet::schema::types::ColumnPath;
use smallvec::smallvec;
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::consts::SEQUENCE_COLUMN_NAME;
use store_api::storage::SequenceNumber;
use tokio::io::AsyncWrite;
use tokio_util::compat::{Compat, FuturesAsyncWriteCompatExt};

use crate::access_layer::{FilePathProvider, Metrics, SstInfoArray, TempFileCleaner};
use crate::error::{InvalidMetadataSnafu, OpenDalSnafu, Result, WriteParquetSnafu};
use crate::read::{Batch, Source};
use crate::sst::file::{FileId, RegionFileId};
use crate::sst::index::{Indexer, IndexerBuilder};
use crate::sst::parquet::format::PrimaryKeyWriteFormat;
use crate::sst::parquet::helper::parse_parquet_metadata;
use crate::sst::parquet::{SstInfo, WriteOptions, PARQUET_METADATA_KEY};
use crate::sst::{DEFAULT_WRITE_BUFFER_SIZE, DEFAULT_WRITE_CONCURRENCY};

/// Parquet SST writer.
pub struct ParquetWriter<F: WriterFactory, I: IndexerBuilder, P: FilePathProvider> {
    /// Path provider that creates SST and index file paths according to file id.
    path_provider: P,
    writer: Option<AsyncArrowWriter<SizeAwareWriter<F::Writer>>>,
    /// Current active file id.
    current_file: FileId,
    writer_factory: F,
    /// Region metadata of the source and the target SST.
    metadata: RegionMetadataRef,
    /// Indexer build that can create indexer for multiple files.
    indexer_builder: I,
    /// Current active indexer.
    current_indexer: Option<Indexer>,
    bytes_written: Arc<AtomicUsize>,
    /// Cleaner to remove temp files on failure.
    file_cleaner: Option<TempFileCleaner>,
    /// Write metrics
    metrics: Metrics,
}

pub trait WriterFactory {
    type Writer: AsyncWrite + Send + Unpin;
    fn create(&mut self, file_path: &str) -> impl Future<Output = Result<Self::Writer>>;
}

pub struct ObjectStoreWriterFactory {
    object_store: ObjectStore,
}

impl WriterFactory for ObjectStoreWriterFactory {
    type Writer = Compat<FuturesAsyncWriter>;

    async fn create(&mut self, file_path: &str) -> Result<Self::Writer> {
        self.object_store
            .writer_with(file_path)
            .chunk(DEFAULT_WRITE_BUFFER_SIZE.as_bytes() as usize)
            .concurrent(DEFAULT_WRITE_CONCURRENCY)
            .await
            .map(|v| v.into_futures_async_write().compat_write())
            .context(OpenDalSnafu)
    }
}

impl<I, P> ParquetWriter<ObjectStoreWriterFactory, I, P>
where
    P: FilePathProvider,
    I: IndexerBuilder,
{
    pub async fn new_with_object_store(
        object_store: ObjectStore,
        metadata: RegionMetadataRef,
        indexer_builder: I,
        path_provider: P,
        metrics: Metrics,
    ) -> ParquetWriter<ObjectStoreWriterFactory, I, P> {
        ParquetWriter::new(
            ObjectStoreWriterFactory { object_store },
            metadata,
            indexer_builder,
            path_provider,
            metrics,
        )
        .await
    }

    pub(crate) fn with_file_cleaner(mut self, cleaner: TempFileCleaner) -> Self {
        self.file_cleaner = Some(cleaner);
        self
    }
}

impl<F, I, P> ParquetWriter<F, I, P>
where
    F: WriterFactory,
    I: IndexerBuilder,
    P: FilePathProvider,
{
    /// Creates a new parquet SST writer.
    pub async fn new(
        factory: F,
        metadata: RegionMetadataRef,
        indexer_builder: I,
        path_provider: P,
        metrics: Metrics,
    ) -> ParquetWriter<F, I, P> {
        let init_file = FileId::random();
        let indexer = indexer_builder.build(init_file).await;

        ParquetWriter {
            path_provider,
            writer: None,
            current_file: init_file,
            writer_factory: factory,
            metadata,
            indexer_builder,
            current_indexer: Some(indexer),
            bytes_written: Arc::new(AtomicUsize::new(0)),
            file_cleaner: None,
            metrics,
        }
    }

    /// Finishes current SST file and index file.
    async fn finish_current_file(
        &mut self,
        ssts: &mut SstInfoArray,
        stats: &mut SourceStats,
    ) -> Result<()> {
        // maybe_init_writer will re-create a new file.
        if let Some(mut current_writer) = mem::take(&mut self.writer) {
            let stats = mem::take(stats);
            // At least one row has been written.
            assert!(stats.num_rows > 0);

            debug!(
                "Finishing current file {}, file size: {}, num rows: {}",
                self.current_file,
                self.bytes_written.load(Ordering::Relaxed),
                stats.num_rows
            );

            // Finish indexer and writer.
            // safety: writer and index can only be both present or not.
            let index_output = self.current_indexer.as_mut().unwrap().finish().await;
            current_writer.flush().await.context(WriteParquetSnafu)?;

            let file_meta = current_writer.close().await.context(WriteParquetSnafu)?;
            let file_size = self.bytes_written.load(Ordering::Relaxed) as u64;

            // Safety: num rows > 0 so we must have min/max.
            let time_range = stats.time_range.unwrap();

            // convert FileMetaData to ParquetMetaData
            let parquet_metadata = parse_parquet_metadata(file_meta)?;
            ssts.push(SstInfo {
                file_id: self.current_file,
                time_range,
                file_size,
                num_rows: stats.num_rows,
                num_row_groups: parquet_metadata.num_row_groups() as u64,
                file_metadata: Some(Arc::new(parquet_metadata)),
                index_metadata: index_output,
            });
            self.current_file = FileId::random();
            self.bytes_written.store(0, Ordering::Relaxed)
        };

        Ok(())
    }

    /// Iterates source and writes all rows to Parquet file.
    ///
    /// Returns the [SstInfo] if the SST is written.
    pub async fn write_all(
        &mut self,
        source: Source,
        override_sequence: Option<SequenceNumber>, // override the `sequence` field from `Source`
        opts: &WriteOptions,
    ) -> Result<SstInfoArray> {
        let res = self
            .write_all_without_cleaning(source, override_sequence, opts)
            .await;
        if res.is_err() {
            // Clean tmp files explicitly on failure.
            let file_id = self.current_file;
            if let Some(cleaner) = &self.file_cleaner {
                cleaner.clean_by_file_id(file_id).await;
            }
        }
        res
    }

    async fn write_all_without_cleaning(
        &mut self,
        mut source: Source,
        override_sequence: Option<SequenceNumber>, // override the `sequence` field from `Source`
        opts: &WriteOptions,
    ) -> Result<SstInfoArray> {
        let mut results = smallvec![];
        let write_format = PrimaryKeyWriteFormat::new(self.metadata.clone())
            .with_override_sequence(override_sequence);
        let mut stats = SourceStats::default();

        while let Some(res) = self
            .write_next_batch(&mut source, &write_format, opts)
            .await
            .transpose()
        {
            match res {
                Ok(mut batch) => {
                    stats.update(&batch);
                    let start = Instant::now();
                    // safety: self.current_indexer must be set when first batch has been written.
                    self.current_indexer
                        .as_mut()
                        .unwrap()
                        .update(&mut batch)
                        .await;
                    self.metrics.update_index += start.elapsed();
                    if let Some(max_file_size) = opts.max_file_size
                        && self.bytes_written.load(Ordering::Relaxed) > max_file_size
                    {
                        self.finish_current_file(&mut results, &mut stats).await?;
                    }
                }
                Err(e) => {
                    if let Some(indexer) = &mut self.current_indexer {
                        indexer.abort().await;
                    }
                    return Err(e);
                }
            }
        }

        self.finish_current_file(&mut results, &mut stats).await?;

        // object_store.write will make sure all bytes are written or an error is raised.
        Ok(results)
    }

    /// Customizes per-column config according to schema and maybe column cardinality.
    fn customize_column_config(
        builder: WriterPropertiesBuilder,
        region_metadata: &RegionMetadataRef,
    ) -> WriterPropertiesBuilder {
        let ts_col = ColumnPath::new(vec![region_metadata
            .time_index_column()
            .column_schema
            .name
            .clone()]);
        let seq_col = ColumnPath::new(vec![SEQUENCE_COLUMN_NAME.to_string()]);

        builder
            .set_column_encoding(seq_col.clone(), Encoding::DELTA_BINARY_PACKED)
            .set_column_dictionary_enabled(seq_col, false)
            .set_column_encoding(ts_col.clone(), Encoding::DELTA_BINARY_PACKED)
            .set_column_dictionary_enabled(ts_col, false)
    }

    async fn write_next_batch(
        &mut self,
        source: &mut Source,
        write_format: &PrimaryKeyWriteFormat,
        opts: &WriteOptions,
    ) -> Result<Option<Batch>> {
        let start = Instant::now();
        let Some(batch) = source.next_batch().await? else {
            return Ok(None);
        };
        self.metrics.iter_source += start.elapsed();

        let arrow_batch = write_format.convert_batch(&batch)?;

        let start = Instant::now();
        self.maybe_init_writer(write_format.arrow_schema(), opts)
            .await?
            .write(&arrow_batch)
            .await
            .context(WriteParquetSnafu)?;
        self.metrics.write_batch += start.elapsed();
        Ok(Some(batch))
    }

    async fn maybe_init_writer(
        &mut self,
        schema: &SchemaRef,
        opts: &WriteOptions,
    ) -> Result<&mut AsyncArrowWriter<SizeAwareWriter<F::Writer>>> {
        if let Some(ref mut w) = self.writer {
            Ok(w)
        } else {
            let json = self.metadata.to_json().context(InvalidMetadataSnafu)?;
            let key_value_meta = KeyValue::new(PARQUET_METADATA_KEY.to_string(), json);

            // TODO(yingwen): Find and set proper column encoding for internal columns: op type and tsid.
            let props_builder = WriterProperties::builder()
                .set_key_value_metadata(Some(vec![key_value_meta]))
                .set_compression(Compression::ZSTD(ZstdLevel::default()))
                .set_encoding(Encoding::PLAIN)
                .set_max_row_group_size(opts.row_group_size);

            let props_builder = Self::customize_column_config(props_builder, &self.metadata);
            let writer_props = props_builder.build();

            let sst_file_path = self.path_provider.build_sst_file_path(RegionFileId::new(
                self.metadata.region_id,
                self.current_file,
            ));
            let writer = SizeAwareWriter::new(
                self.writer_factory.create(&sst_file_path).await?,
                self.bytes_written.clone(),
            );
            let arrow_writer =
                AsyncArrowWriter::try_new(writer, schema.clone(), Some(writer_props))
                    .context(WriteParquetSnafu)?;
            self.writer = Some(arrow_writer);

            let indexer = self.indexer_builder.build(self.current_file).await;
            self.current_indexer = Some(indexer);

            // safety: self.writer is assigned above
            Ok(self.writer.as_mut().unwrap())
        }
    }

    /// Consumes write and return the collected metrics.
    pub fn into_metrics(self) -> Metrics {
        self.metrics
    }
}

#[derive(Default)]
struct SourceStats {
    /// Number of rows fetched.
    num_rows: usize,
    /// Time range of fetched batches.
    time_range: Option<(Timestamp, Timestamp)>,
}

impl SourceStats {
    fn update(&mut self, batch: &Batch) {
        if batch.is_empty() {
            return;
        }

        self.num_rows += batch.num_rows();
        // Safety: batch is not empty.
        let (min_in_batch, max_in_batch) = (
            batch.first_timestamp().unwrap(),
            batch.last_timestamp().unwrap(),
        );
        if let Some(time_range) = &mut self.time_range {
            time_range.0 = time_range.0.min(min_in_batch);
            time_range.1 = time_range.1.max(max_in_batch);
        } else {
            self.time_range = Some((min_in_batch, max_in_batch));
        }
    }
}

/// Workaround for [AsyncArrowWriter] does not provide a method to
/// get total bytes written after close.
struct SizeAwareWriter<W> {
    inner: W,
    size: Arc<AtomicUsize>,
}

impl<W> SizeAwareWriter<W> {
    fn new(inner: W, size: Arc<AtomicUsize>) -> Self {
        Self {
            inner,
            size: size.clone(),
        }
    }
}

impl<W> AsyncWrite for SizeAwareWriter<W>
where
    W: AsyncWrite + Unpin,
{
    fn poll_write(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
        buf: &[u8],
    ) -> Poll<std::result::Result<usize, std::io::Error>> {
        let this = self.as_mut().get_mut();

        match Pin::new(&mut this.inner).poll_write(cx, buf) {
            Poll::Ready(Ok(bytes_written)) => {
                this.size.fetch_add(bytes_written, Ordering::Relaxed);
                Poll::Ready(Ok(bytes_written))
            }
            other => other,
        }
    }

    fn poll_flush(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), std::io::Error>> {
        Pin::new(&mut self.inner).poll_flush(cx)
    }

    fn poll_shutdown(
        mut self: Pin<&mut Self>,
        cx: &mut Context<'_>,
    ) -> Poll<std::result::Result<(), std::io::Error>> {
        Pin::new(&mut self.inner).poll_shutdown(cx)
    }
}
