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
use std::pin::Pin;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::task::{Context, Poll};

use common_time::Timestamp;
use datatypes::arrow::datatypes::SchemaRef;
use object_store::{FuturesAsyncWriter, ObjectStore};
use parquet::arrow::AsyncArrowWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::{WriterProperties, WriterPropertiesBuilder};
use parquet::schema::types::ColumnPath;
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::consts::SEQUENCE_COLUMN_NAME;
use store_api::storage::SequenceNumber;
use tokio::io::AsyncWrite;
use tokio_util::compat::{Compat, FuturesAsyncWriteCompatExt};

use crate::error::{InvalidMetadataSnafu, OpenDalSnafu, Result, WriteParquetSnafu};
use crate::read::{Batch, Source};
use crate::sst::index::Indexer;
use crate::sst::parquet::format::WriteFormat;
use crate::sst::parquet::helper::parse_parquet_metadata;
use crate::sst::parquet::{SstInfo, WriteOptions, PARQUET_METADATA_KEY};
use crate::sst::{DEFAULT_WRITE_BUFFER_SIZE, DEFAULT_WRITE_CONCURRENCY};

/// Parquet SST writer.
pub struct ParquetWriter<F: WriterFactory> {
    writer: Option<AsyncArrowWriter<SizeAwareWriter<F::Writer>>>,
    writer_factory: F,
    /// Region metadata of the source and the target SST.
    metadata: RegionMetadataRef,
    indexer: Indexer,
    bytes_written: Arc<AtomicUsize>,
}

pub trait WriterFactory {
    type Writer: AsyncWrite + Send + Unpin;
    fn create(&mut self) -> impl Future<Output = Result<Self::Writer>>;
}

pub struct ObjectStoreWriterFactory {
    path: String,
    object_store: ObjectStore,
}

impl WriterFactory for ObjectStoreWriterFactory {
    type Writer = Compat<FuturesAsyncWriter>;

    async fn create(&mut self) -> Result<Self::Writer> {
        self.object_store
            .writer_with(&self.path)
            .chunk(DEFAULT_WRITE_BUFFER_SIZE.as_bytes() as usize)
            .concurrent(DEFAULT_WRITE_CONCURRENCY)
            .await
            .map(|v| v.into_futures_async_write().compat_write())
            .context(OpenDalSnafu)
    }
}

impl ParquetWriter<ObjectStoreWriterFactory> {
    pub fn new_with_object_store(
        object_store: ObjectStore,
        path: String,
        metadata: RegionMetadataRef,
        indexer: Indexer,
    ) -> ParquetWriter<ObjectStoreWriterFactory> {
        ParquetWriter::new(
            ObjectStoreWriterFactory { path, object_store },
            metadata,
            indexer,
        )
    }
}

impl<F> ParquetWriter<F>
where
    F: WriterFactory,
{
    /// Creates a new parquet SST writer.
    pub fn new(factory: F, metadata: RegionMetadataRef, indexer: Indexer) -> ParquetWriter<F> {
        ParquetWriter {
            writer: None,
            writer_factory: factory,
            metadata,
            indexer,
            bytes_written: Arc::new(AtomicUsize::new(0)),
        }
    }

    /// Iterates source and writes all rows to Parquet file.
    ///
    /// Returns the [SstInfo] if the SST is written.
    pub async fn write_all(
        &mut self,
        mut source: Source,
        override_sequence: Option<SequenceNumber>, // override the `sequence` field from `Source`
        opts: &WriteOptions,
    ) -> Result<Option<SstInfo>> {
        let write_format =
            WriteFormat::new(self.metadata.clone()).with_override_sequence(override_sequence);
        let mut stats = SourceStats::default();

        while let Some(res) = self
            .write_next_batch(&mut source, &write_format, opts)
            .await
            .transpose()
        {
            match res {
                Ok(batch) => {
                    stats.update(&batch);
                    self.indexer.update(&batch).await;
                }
                Err(e) => {
                    self.indexer.abort().await;
                    return Err(e);
                }
            }
        }

        let index_output = self.indexer.finish().await;

        if stats.num_rows == 0 {
            return Ok(None);
        }

        let Some(mut arrow_writer) = self.writer.take() else {
            // No batch actually written.
            return Ok(None);
        };

        arrow_writer.flush().await.context(WriteParquetSnafu)?;

        let file_meta = arrow_writer.close().await.context(WriteParquetSnafu)?;
        let file_size = self.bytes_written.load(Ordering::Relaxed) as u64;

        // Safety: num rows > 0 so we must have min/max.
        let time_range = stats.time_range.unwrap();

        // convert FileMetaData to ParquetMetaData
        let parquet_metadata = parse_parquet_metadata(file_meta)?;

        // object_store.write will make sure all bytes are written or an error is raised.
        Ok(Some(SstInfo {
            time_range,
            file_size,
            num_rows: stats.num_rows,
            num_row_groups: parquet_metadata.num_row_groups() as u64,
            file_metadata: Some(Arc::new(parquet_metadata)),
            index_metadata: index_output,
        }))
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
        write_format: &WriteFormat,
        opts: &WriteOptions,
    ) -> Result<Option<Batch>> {
        let Some(batch) = source.next_batch().await? else {
            return Ok(None);
        };

        let arrow_batch = write_format.convert_batch(&batch)?;
        self.maybe_init_writer(write_format.arrow_schema(), opts)
            .await?
            .write(&arrow_batch)
            .await
            .context(WriteParquetSnafu)?;
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

            let writer = SizeAwareWriter::new(
                self.writer_factory.create().await?,
                self.bytes_written.clone(),
            );
            let arrow_writer =
                AsyncArrowWriter::try_new(writer, schema.clone(), Some(writer_props))
                    .context(WriteParquetSnafu)?;
            self.writer = Some(arrow_writer);
            // safety: self.writer is assigned above
            Ok(self.writer.as_mut().unwrap())
        }
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
