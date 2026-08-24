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

use std::cmp::Ordering;
use std::sync::Arc;
use std::time::{Duration, Instant};

use bytes::Bytes;
use datatypes::arrow::array::{
    Array, ArrayRef, BinaryArray, DictionaryArray, Int64Array, UInt32Array, UInt64Array,
};
use datatypes::arrow::datatypes::{DataType, Field, Schema, SchemaRef, UInt32Type};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::prelude::ConcreteDataType;
use futures::future::BoxFuture;
use mito_codec::row_converter::SparsePrimaryKeyCodec;
use object_store::{ObjectStore, Writer};
use parquet::arrow::AsyncArrowWriter;
use parquet::arrow::async_writer::AsyncFileWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::consts::{PRIMARY_KEY_COLUMN_NAME, ReservedColumnId};

use crate::access_layer::TempFileCleaner;
use crate::error::{
    DecodeSnafu, InvalidMetaSnafu, InvalidRecordBatchSnafu, NewRecordBatchSnafu, OpenDalSnafu,
    Result, UnexpectedSnafu, WriteParquetSnafu,
};
use crate::sst::parquet::DEFAULT_ROW_GROUP_SIZE;
use crate::sst::range_index::{
    END_COLUMN, ROW_GROUP_ID_COLUMN, START_COLUMN, TABLE_ID_COLUMN, TSID_COLUMN,
};
use crate::sst::{DEFAULT_WRITE_BUFFER_SIZE, DEFAULT_WRITE_CONCURRENCY};

const WRITE_BATCH_SIZE: usize = 1024;

type ParquetWriter = AsyncArrowWriter<AsyncWriter>;

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

/// Options for writing a per-SST range index.
#[derive(Debug, Clone)]
pub struct SstRangeIndexWriterOptions {
    /// Maximum number of range entries in an index Parquet row group.
    pub index_row_group_size: usize,
}

impl Default for SstRangeIndexWriterOptions {
    fn default() -> Self {
        Self {
            index_row_group_size: DEFAULT_ROW_GROUP_SIZE,
        }
    }
}

/// Metrics collected by an [`SstRangeIndexWriter`].
#[derive(Debug, Clone, Default)]
pub struct SstRangeIndexWriterMetrics {
    /// Number of input record batches passed to the writer.
    pub input_batches: usize,
    /// Number of source SST rows passed to the writer.
    pub input_rows: usize,
    /// Number of non-empty source SST row groups passed to the writer.
    pub num_source_row_groups: usize,
    /// Number of series ranges written to the index.
    pub num_ranges: usize,
    /// Size of the committed index file. This remains zero for an aborted writer.
    pub output_bytes: u64,
    /// Time spent opening the object-store and Parquet writers.
    pub open_elapsed: Duration,
    /// Time spent validating input and aggregating ranges.
    pub aggregate_elapsed: Duration,
    /// Time spent encoding and writing Parquet batches.
    pub write_elapsed: Duration,
    /// Time spent closing a completed file.
    pub finish_elapsed: Duration,
    /// Time spent removing incomplete output.
    pub cleanup_elapsed: Duration,
    /// Whether this writer was explicitly aborted.
    pub aborted: bool,
}

impl SstRangeIndexWriterMetrics {
    /// Returns time spent by writer-owned work.
    pub fn total_elapsed(&self) -> Duration {
        self.open_elapsed
            + self.aggregate_elapsed
            + self.write_elapsed
            + self.finish_elapsed
            + self.cleanup_elapsed
    }
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct RangeIndexRow {
    row_group_id: u32,
    table_id: u32,
    tsid: u64,
    start: i64,
    end: i64,
}

/// Incrementally writes the series row ranges of one SST to a Parquet file.
///
/// Each non-empty input batch is tagged with its source SST row-group ID. A row
/// group may be supplied in multiple batches, but IDs must start at zero and be
/// contiguous. Input primary keys must preserve the SST's global sort order.
pub struct SstRangeIndexWriter {
    codec: SparsePrimaryKeyCodec,
    schema: SchemaRef,
    object_store: ObjectStore,
    file_name: String,
    writer: Option<ParquetWriter>,
    current_row_group_id: Option<u32>,
    current_row_group_offset: i64,
    last_primary_key: Option<Vec<u8>>,
    current_row: Option<RangeIndexRow>,
    buffered_rows: Vec<RangeIndexRow>,
    metrics: SstRangeIndexWriterMetrics,
    failed: bool,
}

impl SstRangeIndexWriter {
    /// Creates a writer for `path` in `object_store`.
    ///
    /// The file name in `path` must be unique in the object store so aborting
    /// the writer only removes temporary files that belong to this writer.
    pub async fn try_new(
        metadata: RegionMetadataRef,
        object_store: ObjectStore,
        path: &str,
        options: SstRangeIndexWriterOptions,
    ) -> Result<Self> {
        let open_start = Instant::now();
        ensure!(
            options.index_row_group_size > 0,
            InvalidMetaSnafu {
                reason: "range index row group size must be greater than zero",
            }
        );
        validate_metadata(&metadata)?;
        let schema = range_index_schema();
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
            .set_max_row_group_row_count(Some(options.index_row_group_size))
            .set_column_index_truncate_length(None)
            .set_statistics_truncate_length(None)
            .build();
        let writer =
            AsyncArrowWriter::try_new(AsyncWriter::new(output), schema.clone(), Some(properties))
                .context(WriteParquetSnafu)?;
        let codec = SparsePrimaryKeyCodec::new(&metadata);

        Ok(Self {
            codec,
            schema,
            object_store,
            file_name,
            writer: Some(writer),
            current_row_group_id: None,
            current_row_group_offset: 0,
            last_primary_key: None,
            current_row: None,
            buffered_rows: Vec::with_capacity(WRITE_BATCH_SIZE),
            metrics: SstRangeIndexWriterMetrics {
                open_elapsed: open_start.elapsed(),
                ..Default::default()
            },
            failed: false,
        })
    }

    /// Returns the metrics collected so far.
    pub fn metrics(&self) -> &SstRangeIndexWriterMetrics {
        &self.metrics
    }

    /// Adds one batch from `row_group_id` of the source SST.
    ///
    /// The batch may be a primary-key-only projection or a full flat batch. Its
    /// schema must contain a named `__primary_key` column. After this method
    /// returns an error, callers must call [`Self::abort`].
    pub async fn write(&mut self, row_group_id: u32, batch: &RecordBatch) -> Result<()> {
        ensure!(
            !self.failed,
            InvalidRecordBatchSnafu {
                reason: "cannot write to a failed range index writer",
            }
        );

        self.metrics.input_batches += 1;
        self.metrics.input_rows += batch.num_rows();
        let aggregate_start = Instant::now();
        let write_before = self.metrics.write_elapsed;
        let result = self.write_inner(row_group_id, batch).await;
        let write_cost = self.metrics.write_elapsed.saturating_sub(write_before);
        self.metrics.aggregate_elapsed += aggregate_start.elapsed().saturating_sub(write_cost);
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    /// Finishes and commits the index file.
    pub async fn finish(mut self) -> Result<SstRangeIndexWriterMetrics> {
        if self.failed {
            let error = InvalidRecordBatchSnafu {
                reason: "cannot finish a failed range index writer",
            }
            .build();
            self.cleanup().await;
            return Err(error);
        }

        let result = self.finish_inner().await;
        if result.is_err() {
            self.cleanup().await;
        }
        result.map(|_| self.metrics)
    }

    /// Aborts the writer and removes incomplete output files.
    pub async fn abort(mut self) -> Result<SstRangeIndexWriterMetrics> {
        self.metrics.aborted = true;
        self.cleanup().await;
        Ok(self.metrics)
    }

    async fn write_inner(&mut self, row_group_id: u32, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }

        self.enter_row_group(row_group_id).await?;
        let pk_idx = batch
            .schema()
            .column_with_name(PRIMARY_KEY_COLUMN_NAME)
            .map(|(idx, _)| idx)
            .context(InvalidRecordBatchSnafu {
                reason: "range index input does not contain __primary_key",
            })?;
        let primary_keys = batch.column(pk_idx);
        let batch_rows = i64::try_from(batch.num_rows()).map_err(|_| {
            InvalidRecordBatchSnafu {
                reason: format!(
                    "range index input batch exceeds Int64: {}",
                    batch.num_rows()
                ),
            }
            .build()
        })?;
        self.current_row_group_offset
            .checked_add(batch_rows)
            .context(InvalidRecordBatchSnafu {
                reason: "source SST row-group offset exceeds Int64",
            })?;

        if let Some(array) = primary_keys.as_any().downcast_ref::<BinaryArray>() {
            ensure!(
                array.null_count() == 0,
                InvalidRecordBatchSnafu {
                    reason: "range index input contains null primary keys",
                }
            );
            self.write_binary_primary_keys(array).await
        } else if let Some(array) = primary_keys
            .as_any()
            .downcast_ref::<DictionaryArray<UInt32Type>>()
        {
            ensure!(
                array.null_count() == 0,
                InvalidRecordBatchSnafu {
                    reason: "range index input contains null primary keys",
                }
            );
            self.write_dictionary_primary_keys(array).await
        } else {
            InvalidRecordBatchSnafu {
                reason: format!(
                    "range index requires Binary or Dictionary(UInt32, Binary) primary keys, got {:?}",
                    primary_keys.data_type()
                ),
            }
            .fail()
        }
    }

    async fn enter_row_group(&mut self, row_group_id: u32) -> Result<()> {
        let Some(current) = self.current_row_group_id else {
            ensure!(
                row_group_id == 0,
                InvalidRecordBatchSnafu {
                    reason: format!(
                        "range index row groups must start at zero, got {row_group_id}"
                    ),
                }
            );
            self.current_row_group_id = Some(row_group_id);
            self.metrics.num_source_row_groups = 1;
            return Ok(());
        };

        ensure!(
            row_group_id == current || current.checked_add(1) == Some(row_group_id),
            InvalidRecordBatchSnafu {
                reason: format!(
                    "range index row groups must be contiguous, current {current}, got {row_group_id}"
                ),
            }
        );
        if row_group_id != current {
            self.finish_current_row().await?;
            self.current_row_group_id = Some(row_group_id);
            self.current_row_group_offset = 0;
            self.metrics.num_source_row_groups += 1;
        }
        Ok(())
    }

    async fn write_binary_primary_keys(&mut self, primary_keys: &BinaryArray) -> Result<()> {
        let mut start = 0;
        while start < primary_keys.len() {
            let primary_key = primary_keys.value(start);
            let mut end = start + 1;
            while end < primary_keys.len() && primary_keys.value(end) == primary_key {
                end += 1;
            }
            self.update_primary_key(primary_key, end - start).await?;
            start = end;
        }
        Ok(())
    }

    async fn write_dictionary_primary_keys(
        &mut self,
        primary_keys: &DictionaryArray<UInt32Type>,
    ) -> Result<()> {
        let values = primary_keys
            .values()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .context(InvalidRecordBatchSnafu {
                reason: "range index primary-key dictionary values are not binary",
            })?;
        ensure!(
            values.null_count() == 0,
            InvalidRecordBatchSnafu {
                reason: "range index input contains null primary-key dictionary values",
            }
        );
        let keys = primary_keys.keys().values();
        let mut start = 0;
        while start < keys.len() {
            let key = keys[start];
            let mut end = start + 1;
            while end < keys.len() && keys[end] == key {
                end += 1;
            }
            self.update_primary_key(values.value(key as usize), end - start)
                .await?;
            start = end;
        }
        Ok(())
    }

    async fn update_primary_key(&mut self, primary_key: &[u8], run_len: usize) -> Result<()> {
        if let Some(last) = self.last_primary_key.as_deref() {
            ensure!(
                primary_key.cmp(last) != Ordering::Less,
                InvalidRecordBatchSnafu {
                    reason: "range index input is not sorted by primary key",
                }
            );
        }

        let (table_id, tsid) = self.codec.decode_ids(primary_key).context(DecodeSnafu)?;
        let run_len = i64::try_from(run_len).map_err(|_| {
            InvalidRecordBatchSnafu {
                reason: format!("range index primary-key run is too large: {run_len}"),
            }
            .build()
        })?;
        let start = self.current_row_group_offset;
        let end = start
            .checked_add(run_len)
            .context(InvalidRecordBatchSnafu {
                reason: "source SST row-group offset exceeds Int64",
            })?;
        let row_group_id = self.current_row_group_id.context(UnexpectedSnafu {
            reason: "range index writer has no active source row group",
        })?;

        match self.current_row.as_mut() {
            Some(row)
                if row.row_group_id == row_group_id
                    && row.table_id == table_id
                    && row.tsid == tsid =>
            {
                row.end = end;
            }
            _ => {
                self.finish_current_row().await?;
                self.current_row = Some(RangeIndexRow {
                    row_group_id,
                    table_id,
                    tsid,
                    start,
                    end,
                });
            }
        }
        self.current_row_group_offset = end;
        self.last_primary_key = Some(primary_key.to_vec());
        Ok(())
    }

    async fn finish_current_row(&mut self) -> Result<()> {
        if let Some(row) = self.current_row.take() {
            self.buffered_rows.push(row);
            self.metrics.num_ranges += 1;
        }
        if self.buffered_rows.len() >= WRITE_BATCH_SIZE {
            self.flush_rows().await?;
        }
        Ok(())
    }

    async fn flush_rows(&mut self) -> Result<()> {
        if self.buffered_rows.is_empty() {
            return Ok(());
        }
        let batch = rows_to_batch(&self.schema, &self.buffered_rows)?;
        let start = Instant::now();
        let result = self
            .writer
            .as_mut()
            .context(UnexpectedSnafu {
                reason: "range index Parquet writer is closed",
            })?
            .write(&batch)
            .await
            .context(WriteParquetSnafu);
        self.metrics.write_elapsed += start.elapsed();
        result?;
        self.buffered_rows.clear();
        Ok(())
    }

    async fn finish_inner(&mut self) -> Result<()> {
        let aggregate_start = Instant::now();
        let write_before = self.metrics.write_elapsed;
        self.finish_current_row().await?;
        self.flush_rows().await?;
        let write_cost = self.metrics.write_elapsed.saturating_sub(write_before);
        self.metrics.aggregate_elapsed += aggregate_start.elapsed().saturating_sub(write_cost);

        let finish_start = Instant::now();
        self.writer
            .as_mut()
            .context(UnexpectedSnafu {
                reason: "range index Parquet writer is closed",
            })?
            .finish()
            .await
            .context(WriteParquetSnafu)?;
        let writer = self.writer.take().context(UnexpectedSnafu {
            reason: "range index Parquet writer is closed",
        })?;
        self.metrics.output_bytes = writer.into_inner().output_bytes();
        self.metrics.finish_elapsed += finish_start.elapsed();
        Ok(())
    }

    async fn cleanup(&mut self) {
        let start = Instant::now();
        if let Some(writer) = self.writer.take() {
            let mut writer = writer.into_inner().into_inner();
            if let Err(error) = writer.abort().await {
                common_telemetry::warn!(error; "Failed to abort range index writer");
            }
        }
        self.current_row = None;
        self.buffered_rows.clear();

        TempFileCleaner::clean_atomic_dir_files(&self.object_store, &[&self.file_name]).await;
        self.metrics.output_bytes = 0;
        self.metrics.cleanup_elapsed += start.elapsed();
    }
}

/// Returns the Arrow schema of a per-SST range index.
pub fn range_index_schema() -> SchemaRef {
    Arc::new(Schema::new(vec![
        Field::new(ROW_GROUP_ID_COLUMN, DataType::UInt32, false),
        Field::new(TABLE_ID_COLUMN, DataType::UInt32, false),
        Field::new(TSID_COLUMN, DataType::UInt64, false),
        Field::new(START_COLUMN, DataType::Int64, false),
        Field::new(END_COLUMN, DataType::Int64, false),
    ]))
}

fn validate_metadata(metadata: &RegionMetadataRef) -> Result<()> {
    ensure!(
        metadata.primary_key_encoding == PrimaryKeyEncoding::Sparse,
        InvalidMetaSnafu {
            reason: "range index only supports sparse primary-key encoding",
        }
    );
    ensure!(
        metadata
            .primary_key
            .starts_with(&[ReservedColumnId::table_id(), ReservedColumnId::tsid()]),
        InvalidMetaSnafu {
            reason: "range index requires (__table_id, __tsid) as the primary-key prefix",
        }
    );
    let table_id = metadata
        .column_by_id(ReservedColumnId::table_id())
        .context(InvalidMetaSnafu {
            reason: "range index metadata is missing __table_id",
        })?;
    let tsid = metadata
        .column_by_id(ReservedColumnId::tsid())
        .context(InvalidMetaSnafu {
            reason: "range index metadata is missing __tsid",
        })?;
    ensure!(
        table_id.column_schema.data_type == ConcreteDataType::uint32_datatype()
            && tsid.column_schema.data_type == ConcreteDataType::uint64_datatype(),
        InvalidMetaSnafu {
            reason: "range index requires UInt32 __table_id and UInt64 __tsid",
        }
    );
    Ok(())
}

fn rows_to_batch(schema: &SchemaRef, rows: &[RangeIndexRow]) -> Result<RecordBatch> {
    let arrays: Vec<ArrayRef> = vec![
        Arc::new(UInt32Array::from_iter_values(
            rows.iter().map(|row| row.row_group_id),
        )),
        Arc::new(UInt32Array::from_iter_values(
            rows.iter().map(|row| row.table_id),
        )),
        Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|row| row.tsid),
        )),
        Arc::new(Int64Array::from_iter_values(
            rows.iter().map(|row| row.start),
        )),
        Arc::new(Int64Array::from_iter_values(rows.iter().map(|row| row.end))),
    ];
    RecordBatch::try_new(schema.clone(), arrays).context(NewRecordBatchSnafu)
}

#[cfg(test)]
mod tests {
    use datatypes::arrow::array::BinaryDictionaryBuilder;
    use object_store::ErrorKind;
    use object_store::services::Memory;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;

    use super::*;
    use crate::test_util::sst_util::{new_sparse_primary_key, sst_region_metadata_with_encoding};

    fn object_store() -> ObjectStore {
        ObjectStore::new(Memory::default()).unwrap().finish()
    }

    fn pk_schema(primary_key_type: DataType) -> SchemaRef {
        Arc::new(Schema::new(vec![Field::new(
            PRIMARY_KEY_COLUMN_NAME,
            primary_key_type,
            false,
        )]))
    }

    fn binary_batch(primary_keys: &[&[u8]]) -> RecordBatch {
        RecordBatch::try_new(
            pk_schema(DataType::Binary),
            vec![Arc::new(BinaryArray::from_iter_values(
                primary_keys.iter().copied(),
            ))],
        )
        .unwrap()
    }

    fn dictionary_batch(primary_keys: &[&[u8]]) -> RecordBatch {
        let mut builder = BinaryDictionaryBuilder::<UInt32Type>::new();
        for primary_key in primary_keys {
            builder.append(*primary_key).unwrap();
        }
        RecordBatch::try_new(
            pk_schema(DataType::Dictionary(
                Box::new(DataType::UInt32),
                Box::new(DataType::Binary),
            )),
            vec![Arc::new(builder.finish())],
        )
        .unwrap()
    }

    async fn read_index(store: &ObjectStore, path: &str) -> (u64, usize, Vec<RangeIndexRow>) {
        let bytes = store.read(path).await.unwrap().to_bytes();
        let output_bytes = bytes.len() as u64;
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        let row_groups = builder.metadata().num_row_groups();
        let batches = builder
            .build()
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        let mut rows = Vec::new();
        for batch in batches {
            let row_group_ids = batch
                .column(0)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap();
            let table_ids = batch
                .column(1)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap();
            let tsids = batch
                .column(2)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap();
            let starts = batch
                .column(3)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            let ends = batch
                .column(4)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap();
            for row in 0..batch.num_rows() {
                rows.push(RangeIndexRow {
                    row_group_id: row_group_ids.value(row),
                    table_id: table_ids.value(row),
                    tsid: tsids.value(row),
                    start: starts.value(row),
                    end: ends.value(row),
                });
            }
        }
        (output_bytes, row_groups, rows)
    }

    #[tokio::test]
    async fn test_write_ranges_and_metrics() {
        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let primary_key_1 = new_sparse_primary_key(&["a", "x"], &metadata, 1, 10);
        let primary_key_2 = new_sparse_primary_key(&["b", "y"], &metadata, 1, 20);
        let primary_key_3 = new_sparse_primary_key(&["c", "z"], &metadata, 2, 30);
        let store = object_store();
        let mut writer = SstRangeIndexWriter::try_new(
            metadata,
            store.clone(),
            "ranges.parquet",
            SstRangeIndexWriterOptions {
                index_row_group_size: 2,
            },
        )
        .await
        .unwrap();

        writer
            .write(
                0,
                &dictionary_batch(&[primary_key_1.as_slice(), primary_key_1.as_slice()]),
            )
            .await
            .unwrap();
        writer
            .write(
                0,
                &binary_batch(&[
                    primary_key_1.as_slice(),
                    primary_key_2.as_slice(),
                    primary_key_2.as_slice(),
                ]),
            )
            .await
            .unwrap();
        writer
            .write(
                1,
                &binary_batch(&[
                    primary_key_2.as_slice(),
                    primary_key_2.as_slice(),
                    primary_key_3.as_slice(),
                ]),
            )
            .await
            .unwrap();

        assert_eq!(writer.metrics().input_batches, 3);
        assert_eq!(writer.metrics().input_rows, 8);
        assert_eq!(writer.metrics().num_source_row_groups, 2);
        let metrics = writer.finish().await.unwrap();
        assert_eq!(metrics.num_ranges, 4);
        assert!(!metrics.aborted);

        let (output_bytes, row_groups, rows) = read_index(&store, "ranges.parquet").await;
        assert_eq!(metrics.output_bytes, output_bytes);
        assert_eq!(row_groups, 2);
        assert_eq!(
            rows,
            vec![
                RangeIndexRow {
                    row_group_id: 0,
                    table_id: 1,
                    tsid: 10,
                    start: 0,
                    end: 3,
                },
                RangeIndexRow {
                    row_group_id: 0,
                    table_id: 1,
                    tsid: 20,
                    start: 3,
                    end: 5,
                },
                RangeIndexRow {
                    row_group_id: 1,
                    table_id: 1,
                    tsid: 20,
                    start: 0,
                    end: 2,
                },
                RangeIndexRow {
                    row_group_id: 1,
                    table_id: 2,
                    tsid: 30,
                    start: 2,
                    end: 3,
                },
            ]
        );
    }

    #[tokio::test]
    async fn test_reject_invalid_row_groups_and_sorted_order() {
        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let primary_key_1 = new_sparse_primary_key(&["a", "x"], &metadata, 1, 10);
        let primary_key_2 = new_sparse_primary_key(&["b", "y"], &metadata, 1, 20);
        let store = object_store();

        let mut writer = SstRangeIndexWriter::try_new(
            metadata.clone(),
            store.clone(),
            "groups-gap.parquet",
            SstRangeIndexWriterOptions::default(),
        )
        .await
        .unwrap();
        writer
            .write(0, &binary_batch(&[primary_key_1.as_slice()]))
            .await
            .unwrap();
        let error = writer
            .write(2, &binary_batch(&[primary_key_2.as_slice()]))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("must be contiguous"), "{error}");
        writer.abort().await.unwrap();

        let mut writer = SstRangeIndexWriter::try_new(
            metadata,
            store.clone(),
            "sort-error.parquet",
            SstRangeIndexWriterOptions::default(),
        )
        .await
        .unwrap();
        let error = writer
            .write(
                0,
                &dictionary_batch(&[primary_key_2.as_slice(), primary_key_1.as_slice()]),
            )
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not sorted"), "{error}");
        let metrics = writer.abort().await.unwrap();
        assert!(metrics.aborted);
        assert_eq!(metrics.output_bytes, 0);
        assert_eq!(
            store.stat("sort-error.parquet").await.unwrap_err().kind(),
            ErrorKind::NotFound
        );
    }
}
