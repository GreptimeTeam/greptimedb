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
    Array, ArrayRef, BinaryArray, DictionaryArray, Int64Array, StringArray, UInt32Array,
    UInt64Array,
};
use datatypes::arrow::datatypes::{DataType, Field, Schema, SchemaRef, UInt32Type};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::prelude::ConcreteDataType;
use datatypes::timestamp::timestamp_array_to_primitive;
use datatypes::value::Value;
use futures::future::BoxFuture;
use mito_codec::row_converter::{CompositeValues, PrimaryKeyCodec, build_primary_key_codec};
use object_store::{ObjectStore, Writer};
use parquet::arrow::AsyncArrowWriter;
use parquet::arrow::async_writer::AsyncFileWriter;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::errors::ParquetError;
use parquet::file::properties::WriterProperties;
use snafu::{OptionExt, ResultExt, ensure};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;
use store_api::storage::consts::ReservedColumnId;

use crate::access_layer::TempFileCleaner;
use crate::error::{
    DecodeSnafu, InvalidMetaSnafu, InvalidRecordBatchSnafu, NewRecordBatchSnafu, OpenDalSnafu,
    Result, UnexpectedSnafu, WriteParquetSnafu,
};
use crate::series_index::{
    MAX_TS_COLUMN, MIN_TS_COLUMN, ROW_COUNT_COLUMN, TABLE_ID_COLUMN, TSID_COLUMN,
};
use crate::sst::parquet::DEFAULT_ROW_GROUP_SIZE;
use crate::sst::parquet::flat_format::{primary_key_column_index, time_index_column_index};
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

/// Options for writing a series index.
#[derive(Debug, Clone)]
pub struct SeriesIndexWriterOptions {
    /// Maximum number of rows in a Parquet row group.
    pub row_group_size: usize,
}

impl Default for SeriesIndexWriterOptions {
    fn default() -> Self {
        Self {
            row_group_size: DEFAULT_ROW_GROUP_SIZE,
        }
    }
}

/// Metrics collected by a [`SeriesIndexWriter`].
#[derive(Debug, Clone, Default)]
pub struct SeriesIndexWriterMetrics {
    /// Number of input record batches passed to the writer.
    pub input_batches: usize,
    /// Number of logical rows passed to the writer.
    pub input_rows: usize,
    /// Number of unique time series (primary keys) aggregated by the writer.
    pub num_series: usize,
    /// Size of the committed index file. This remains zero for an aborted writer.
    pub output_bytes: u64,
    /// Time spent opening the object-store and Parquet writers.
    pub open_elapsed: Duration,
    /// Time spent validating, aggregating, and decoding primary keys.
    pub aggregate_elapsed: Duration,
    /// Time spent encoding and writing Parquet batches.
    pub write_elapsed: Duration,
    /// Time spent closing a completed file.
    pub finish_elapsed: Duration,
    /// Time spent removing an incomplete output.
    pub cleanup_elapsed: Duration,
    /// Whether this writer was explicitly aborted.
    pub aborted: bool,
}

impl SeriesIndexWriterMetrics {
    /// Returns time spent by writer-owned work.
    pub fn total_elapsed(&self) -> Duration {
        self.open_elapsed
            + self.aggregate_elapsed
            + self.write_elapsed
            + self.finish_elapsed
            + self.cleanup_elapsed
    }
}

#[derive(Debug)]
struct SeriesIndexRow {
    min_ts: i64,
    max_ts: i64,
    row_count: u64,
    table_id: u32,
    tsid: u64,
    tags: Vec<Option<String>>,
}

/// Incrementally aggregates sorted flat record batches into series-index Parquet files.
pub struct SeriesIndexWriter {
    codec: Arc<dyn PrimaryKeyCodec>,
    tag_columns: Vec<(ColumnId, String)>,
    schema: SchemaRef,
    object_store: ObjectStore,
    file_name: String,
    writer: Option<ParquetWriter>,
    current_primary_key: Option<Vec<u8>>,
    current_row: Option<SeriesIndexRow>,
    buffered_rows: Vec<SeriesIndexRow>,
    metrics: SeriesIndexWriterMetrics,
    failed: bool,
}

impl SeriesIndexWriter {
    /// Creates a writer for `path` in `object_store`.
    ///
    /// The file name in `path` must be unique in the object store so aborting
    /// the writer only removes temporary files that belong to this writer.
    pub async fn try_new(
        metadata: RegionMetadataRef,
        object_store: ObjectStore,
        path: &str,
        options: SeriesIndexWriterOptions,
    ) -> Result<Self> {
        let open_start = Instant::now();
        ensure!(
            options.row_group_size > 0,
            InvalidMetaSnafu {
                reason: "series index row group size must be greater than zero",
            }
        );
        let schema = series_index_schema(&metadata)?;
        let tag_columns = tag_columns(&metadata);
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
            .set_max_row_group_row_count(Some(options.row_group_size))
            .set_column_index_truncate_length(None)
            .set_statistics_truncate_length(None)
            .build();
        let writer =
            AsyncArrowWriter::try_new(AsyncWriter::new(output), schema.clone(), Some(properties))
                .context(WriteParquetSnafu)?;
        let codec = build_primary_key_codec(&metadata);

        Ok(Self {
            codec,
            tag_columns,
            schema,
            object_store,
            file_name,
            writer: Some(writer),
            current_primary_key: None,
            current_row: None,
            buffered_rows: Vec::with_capacity(WRITE_BATCH_SIZE),
            metrics: SeriesIndexWriterMetrics {
                open_elapsed: open_start.elapsed(),
                ..Default::default()
            },
            failed: false,
        })
    }

    /// Returns the metrics collected so far.
    pub fn metrics(&self) -> &SeriesIndexWriterMetrics {
        &self.metrics
    }

    /// Aggregates one sorted flat record batch.
    ///
    /// After this method returns an error, callers must call [`Self::abort`].
    pub async fn write(&mut self, batch: &RecordBatch) -> Result<()> {
        ensure!(
            !self.failed,
            InvalidRecordBatchSnafu {
                reason: "cannot write to a failed series index writer",
            }
        );

        self.metrics.input_batches += 1;
        self.metrics.input_rows += batch.num_rows();
        let aggregate_start = Instant::now();
        let write_before = self.metrics.write_elapsed;
        let result = self.write_inner(batch).await;
        let write_cost = self.metrics.write_elapsed.saturating_sub(write_before);
        self.metrics.aggregate_elapsed += aggregate_start.elapsed().saturating_sub(write_cost);
        if result.is_err() {
            self.failed = true;
        }
        result
    }

    /// Finishes and commits the index file.
    pub async fn finish(mut self) -> Result<SeriesIndexWriterMetrics> {
        if self.failed {
            let error = InvalidRecordBatchSnafu {
                reason: "cannot finish a failed series index writer",
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
    pub async fn abort(mut self) -> Result<SeriesIndexWriterMetrics> {
        self.metrics.aborted = true;
        self.cleanup().await;
        Ok(self.metrics)
    }

    async fn write_inner(&mut self, batch: &RecordBatch) -> Result<()> {
        if batch.num_rows() == 0 {
            return Ok(());
        }
        ensure!(
            batch.num_columns() >= 4,
            InvalidRecordBatchSnafu {
                reason: format!(
                    "series index input has too few columns: {}",
                    batch.num_columns()
                ),
            }
        );

        let pk_idx = primary_key_column_index(batch.num_columns());
        let ts_idx = time_index_column_index(batch.num_columns());
        let primary_keys = batch.column(pk_idx);
        let timestamps = timestamp_values(batch.column(ts_idx))?;
        ensure!(
            primary_keys.len() == batch.num_rows() && timestamps.len() == batch.num_rows(),
            InvalidRecordBatchSnafu {
                reason: "primary-key or timestamp array length does not match the batch",
            }
        );

        if let Some(array) = primary_keys.as_any().downcast_ref::<BinaryArray>() {
            ensure!(
                array.null_count() == 0,
                InvalidRecordBatchSnafu {
                    reason: "series index input contains null primary keys",
                }
            );
            self.write_binary_primary_keys(array, timestamps.values())
                .await
        } else if let Some(array) = primary_keys
            .as_any()
            .downcast_ref::<DictionaryArray<UInt32Type>>()
        {
            ensure!(
                array.null_count() == 0,
                InvalidRecordBatchSnafu {
                    reason: "series index input contains null primary keys",
                }
            );
            self.write_dictionary_primary_keys(array, timestamps.values())
                .await
        } else {
            InvalidRecordBatchSnafu {
                reason: format!(
                    "series index requires Binary or Dictionary(UInt32, Binary) primary keys, got {:?}",
                    primary_keys.data_type()
                ),
            }
            .fail()
        }
    }

    async fn write_binary_primary_keys(
        &mut self,
        primary_keys: &BinaryArray,
        timestamps: &[i64],
    ) -> Result<()> {
        let mut start = 0;
        while start < primary_keys.len() {
            let primary_key = primary_keys.value(start);
            let mut end = start + 1;
            while end < primary_keys.len() && primary_keys.value(end) == primary_key {
                end += 1;
            }

            self.update_primary_key(
                primary_key,
                timestamps[start],
                timestamps[end - 1],
                (end - start) as u64,
            )
            .await?;
            start = end;
        }
        Ok(())
    }

    async fn write_dictionary_primary_keys(
        &mut self,
        primary_keys: &DictionaryArray<UInt32Type>,
        timestamps: &[i64],
    ) -> Result<()> {
        let values = primary_keys
            .values()
            .as_any()
            .downcast_ref::<BinaryArray>()
            .context(InvalidRecordBatchSnafu {
                reason: "primary-key dictionary values are not binary",
            })?;
        let keys = primary_keys.keys().values();
        let mut start = 0;
        while start < keys.len() {
            let key = keys[start];
            let mut end = start + 1;
            while end < keys.len() && keys[end] == key {
                end += 1;
            }

            self.update_primary_key(
                values.value(key as usize),
                timestamps[start],
                timestamps[end - 1],
                (end - start) as u64,
            )
            .await?;
            start = end;
        }
        Ok(())
    }

    async fn update_primary_key(
        &mut self,
        primary_key: &[u8],
        min_ts: i64,
        max_ts: i64,
        row_count: u64,
    ) -> Result<()> {
        if let Some(current) = self.current_primary_key.as_deref() {
            match primary_key.cmp(current) {
                Ordering::Less => {
                    return InvalidRecordBatchSnafu {
                        reason: "series index input is not sorted by primary key",
                    }
                    .fail();
                }
                Ordering::Equal => {
                    let row = self.current_row.as_mut().context(InvalidRecordBatchSnafu {
                        reason: "series index aggregation state is incomplete",
                    })?;
                    row.min_ts = row.min_ts.min(min_ts);
                    row.max_ts = row.max_ts.max(max_ts);
                    row.row_count += row_count;
                    return Ok(());
                }
                Ordering::Greater => self.finish_current_row().await?,
            }
        }

        let row = decode_primary_key(
            self.codec.as_ref(),
            primary_key,
            min_ts,
            max_ts,
            row_count,
            &self.tag_columns,
        )?;
        self.current_primary_key = Some(primary_key.to_vec());
        self.current_row = Some(row);
        Ok(())
    }

    async fn finish_current_row(&mut self) -> Result<()> {
        self.current_primary_key = None;
        if let Some(row) = self.current_row.take() {
            self.buffered_rows.push(row);
            self.metrics.num_series += 1;
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
                reason: "series index Parquet writer is closed",
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
                reason: "series index Parquet writer is closed",
            })?
            .finish()
            .await
            .context(WriteParquetSnafu)?;
        let writer = self.writer.take().context(UnexpectedSnafu {
            reason: "series index Parquet writer is closed",
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
                common_telemetry::warn!(error; "Failed to abort series index writer");
            }
        }
        self.current_primary_key = None;
        self.current_row = None;
        self.buffered_rows.clear();

        TempFileCleaner::clean_atomic_dir_files(&self.object_store, &[&self.file_name]).await;
        self.metrics.output_bytes = 0;
        self.metrics.cleanup_elapsed += start.elapsed();
    }
}

/// Returns the Arrow schema of a series index.
pub fn series_index_schema(metadata: &RegionMetadataRef) -> Result<SchemaRef> {
    validate_metadata(metadata)?;
    let mut fields = vec![
        Field::new(MIN_TS_COLUMN, DataType::Int64, false),
        Field::new(MAX_TS_COLUMN, DataType::Int64, false),
        Field::new(ROW_COUNT_COLUMN, DataType::UInt64, false),
        Field::new(TABLE_ID_COLUMN, DataType::UInt32, false),
        Field::new(TSID_COLUMN, DataType::UInt64, false),
    ];
    fields.extend(
        tag_columns(metadata)
            .into_iter()
            .map(|(_, name)| Field::new(name, DataType::Utf8, true)),
    );
    Ok(Arc::new(Schema::new(fields)))
}

fn validate_metadata(metadata: &RegionMetadataRef) -> Result<()> {
    for column in &metadata.column_metadatas {
        ensure!(
            !matches!(
                column.column_schema.name.as_str(),
                MIN_TS_COLUMN | MAX_TS_COLUMN | ROW_COUNT_COLUMN
            ),
            InvalidMetaSnafu {
                reason: format!(
                    "series index internal column name {} is already in use",
                    column.column_schema.name
                ),
            }
        );
    }
    ensure!(
        metadata.primary_key_encoding == PrimaryKeyEncoding::Sparse,
        InvalidMetaSnafu {
            reason: "series index only supports sparse primary-key encoding",
        }
    );
    ensure!(
        metadata
            .primary_key
            .starts_with(&[ReservedColumnId::table_id(), ReservedColumnId::tsid()]),
        InvalidMetaSnafu {
            reason: "series index requires (__table_id, __tsid) as the primary-key prefix",
        }
    );
    let table_id = metadata
        .column_by_id(ReservedColumnId::table_id())
        .context(InvalidMetaSnafu {
            reason: "series index metadata is missing __table_id",
        })?;
    let tsid = metadata
        .column_by_id(ReservedColumnId::tsid())
        .context(InvalidMetaSnafu {
            reason: "series index metadata is missing __tsid",
        })?;
    ensure!(
        table_id.column_schema.data_type == ConcreteDataType::uint32_datatype()
            && tsid.column_schema.data_type == ConcreteDataType::uint64_datatype(),
        InvalidMetaSnafu {
            reason: "series index requires UInt32 __table_id and UInt64 __tsid",
        }
    );
    for column in metadata.primary_key_columns() {
        if is_reserved_column(column.column_id) {
            continue;
        }
        ensure!(
            column.column_schema.data_type == ConcreteDataType::string_datatype(),
            InvalidMetaSnafu {
                reason: format!(
                    "series index requires string tag column {}, got {}",
                    column.column_schema.name, column.column_schema.data_type
                ),
            }
        );
    }
    Ok(())
}

fn tag_columns(metadata: &RegionMetadataRef) -> Vec<(ColumnId, String)> {
    metadata
        .primary_key_columns()
        .filter(|column| !is_reserved_column(column.column_id))
        .map(|column| (column.column_id, column.column_schema.name.clone()))
        .collect()
}

fn is_reserved_column(column_id: ColumnId) -> bool {
    column_id == ReservedColumnId::table_id() || column_id == ReservedColumnId::tsid()
}

fn timestamp_values(array: &ArrayRef) -> Result<Int64Array> {
    let timestamps = if let Some(array) = array.as_any().downcast_ref::<Int64Array>() {
        array.clone()
    } else {
        timestamp_array_to_primitive(array)
            .map(|(array, _)| array)
            .with_context(|| InvalidRecordBatchSnafu {
                reason: format!(
                    "series index requires an Int64 or timestamp time index, got {:?}",
                    array.data_type()
                ),
            })?
    };
    ensure!(
        timestamps.null_count() == 0,
        InvalidRecordBatchSnafu {
            reason: "series index input contains null timestamps",
        }
    );
    Ok(timestamps)
}

// TODO(yingwen): Bench and optimize the performance if this is costly.
fn decode_primary_key(
    codec: &dyn PrimaryKeyCodec,
    primary_key: &[u8],
    min_ts: i64,
    max_ts: i64,
    row_count: u64,
    tag_columns: &[(ColumnId, String)],
) -> Result<SeriesIndexRow> {
    let CompositeValues::Sparse(values) = codec.decode(primary_key).context(DecodeSnafu)? else {
        return InvalidRecordBatchSnafu {
            reason: "decoded primary key is not sparse",
        }
        .fail();
    };
    let table_id = match values.get(&ReservedColumnId::table_id()) {
        Some(Value::UInt32(value)) => *value,
        value => {
            return InvalidRecordBatchSnafu {
                reason: format!("missing or invalid sparse __table_id: {value:?}"),
            }
            .fail();
        }
    };
    let tsid = match values.get(&ReservedColumnId::tsid()) {
        Some(Value::UInt64(value)) => *value,
        value => {
            return InvalidRecordBatchSnafu {
                reason: format!("missing or invalid sparse __tsid: {value:?}"),
            }
            .fail();
        }
    };
    let tags = tag_columns
        .iter()
        .map(|(column_id, _)| match values.get(column_id) {
            None | Some(Value::Null) => Ok(None),
            Some(Value::String(value)) => Ok(Some(value.as_utf8().to_string())),
            value => InvalidRecordBatchSnafu {
                reason: format!(
                    "invalid sparse string tag value for column {column_id}: {value:?}"
                ),
            }
            .fail(),
        })
        .collect::<Result<Vec<_>>>()?;
    Ok(SeriesIndexRow {
        min_ts,
        max_ts,
        row_count,
        table_id,
        tsid,
        tags,
    })
}

fn rows_to_batch(schema: &SchemaRef, rows: &[SeriesIndexRow]) -> Result<RecordBatch> {
    let mut arrays: Vec<ArrayRef> = vec![
        Arc::new(Int64Array::from_iter_values(
            rows.iter().map(|row| row.min_ts),
        )),
        Arc::new(Int64Array::from_iter_values(
            rows.iter().map(|row| row.max_ts),
        )),
        Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|row| row.row_count),
        )),
        Arc::new(UInt32Array::from_iter_values(
            rows.iter().map(|row| row.table_id),
        )),
        Arc::new(UInt64Array::from_iter_values(
            rows.iter().map(|row| row.tsid),
        )),
    ];
    for tag_idx in 0..schema.fields().len() - 5 {
        arrays.push(Arc::new(StringArray::from_iter(
            rows.iter().map(|row| row.tags[tag_idx].as_deref()),
        )));
    }
    RecordBatch::try_new(schema.clone(), arrays).context(NewRecordBatchSnafu)
}

#[cfg(test)]
mod tests {
    use api::v1::SemanticType;
    use datatypes::arrow::array::{
        BinaryDictionaryBuilder, TimestampMicrosecondArray, TimestampMillisecondArray,
        TimestampNanosecondArray, TimestampSecondArray, UInt8Array,
    };
    use datatypes::arrow::datatypes::{TimeUnit, UInt32Type};
    use datatypes::schema::ColumnSchema;
    use mito_codec::row_converter::{PrimaryKeyCodec, SparsePrimaryKeyCodec};
    use object_store::ErrorKind;
    use object_store::services::Memory;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use store_api::codec::PrimaryKeyEncoding;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};

    use super::*;
    use crate::test_util::sst_util::{new_sparse_primary_key, sst_region_metadata_with_encoding};

    fn object_store() -> ObjectStore {
        ObjectStore::new(Memory::default()).unwrap().finish()
    }

    fn flat_schema(primary_key_type: DataType) -> SchemaRef {
        Arc::new(Schema::new(vec![
            Field::new(
                "ts",
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new("__primary_key", primary_key_type, false),
            Field::new("__sequence", DataType::UInt64, false),
            Field::new("__op_type", DataType::UInt8, false),
        ]))
    }

    fn binary_batch(primary_keys: &[&[u8]], timestamps: &[i64]) -> RecordBatch {
        RecordBatch::try_new(
            flat_schema(DataType::Binary),
            vec![
                Arc::new(TimestampMillisecondArray::from(timestamps.to_vec())),
                Arc::new(BinaryArray::from_iter_values(primary_keys.iter().copied())),
                Arc::new(UInt64Array::from(vec![1; timestamps.len()])),
                Arc::new(UInt8Array::from(vec![0; timestamps.len()])),
            ],
        )
        .unwrap()
    }

    fn dictionary_batch(primary_keys: &[&[u8]], timestamps: &[i64]) -> RecordBatch {
        let mut builder = BinaryDictionaryBuilder::<UInt32Type>::new();
        for primary_key in primary_keys {
            builder.append(*primary_key).unwrap();
        }
        RecordBatch::try_new(
            flat_schema(DataType::Dictionary(
                Box::new(DataType::UInt32),
                Box::new(DataType::Binary),
            )),
            vec![
                Arc::new(TimestampMillisecondArray::from(timestamps.to_vec())),
                Arc::new(builder.finish()),
                Arc::new(UInt64Array::from(vec![1; timestamps.len()])),
                Arc::new(UInt8Array::from(vec![0; timestamps.len()])),
            ],
        )
        .unwrap()
    }

    async fn read_index(store: &ObjectStore, path: &str) -> (u64, usize, Vec<RecordBatch>) {
        let bytes = store.read(path).await.unwrap().to_bytes();
        let output_bytes = bytes.len() as u64;
        let builder = ParquetRecordBatchReaderBuilder::try_new(bytes).unwrap();
        let row_groups = builder.metadata().num_row_groups();
        let batches = builder
            .build()
            .unwrap()
            .collect::<std::result::Result<Vec<_>, _>>()
            .unwrap();
        (output_bytes, row_groups, batches)
    }

    #[test]
    fn test_series_index_schema() {
        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let schema = series_index_schema(&metadata).unwrap();
        assert_eq!(
            schema
                .fields()
                .iter()
                .map(|field| field.name().as_str())
                .collect::<Vec<_>>(),
            [
                "__series_min_ts",
                "__series_max_ts",
                "__series_row_count",
                "__table_id",
                "__tsid",
                "tag_0",
                "tag_1",
            ]
        );
        assert!(!schema.field(4).is_nullable());
        assert!(schema.field(5).is_nullable());

        let dense = Arc::new(sst_region_metadata_with_encoding(PrimaryKeyEncoding::Dense));
        assert!(series_index_schema(&dense).is_err());
    }

    #[tokio::test]
    async fn test_reject_series_index_internal_column_names_before_opening_writer() {
        for (index, name) in [MIN_TS_COLUMN, MAX_TS_COLUMN, ROW_COUNT_COLUMN]
            .into_iter()
            .enumerate()
        {
            let mut builder = RegionMetadataBuilder::from_existing(
                sst_region_metadata_with_encoding(PrimaryKeyEncoding::Sparse),
            );
            builder.push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(name, ConcreteDataType::string_datatype(), true),
                semantic_type: SemanticType::Field,
                column_id: 100 + index as u32,
            });
            let metadata = Arc::new(builder.build().unwrap());
            let store = object_store();
            let path = format!("collision-{index}.parquet");
            let error = SeriesIndexWriter::try_new(
                metadata,
                store.clone(),
                &path,
                SeriesIndexWriterOptions::default(),
            )
            .await
            .err()
            .unwrap();

            assert!(error.to_string().contains(name), "{error}");
            assert_eq!(
                store.stat(&path).await.unwrap_err().kind(),
                ErrorKind::NotFound
            );
        }
    }

    #[test]
    fn test_timestamp_values() {
        let arrays: Vec<ArrayRef> = vec![
            Arc::new(Int64Array::from(vec![1, 2])),
            Arc::new(TimestampSecondArray::from(vec![1, 2])),
            Arc::new(TimestampMillisecondArray::from(vec![1, 2])),
            Arc::new(TimestampMicrosecondArray::from(vec![1, 2])),
            Arc::new(TimestampNanosecondArray::from(vec![1, 2])),
        ];
        for array in arrays {
            assert_eq!(timestamp_values(&array).unwrap().values().as_ref(), &[1, 2]);
        }

        let nulls: ArrayRef = Arc::new(TimestampMillisecondArray::from(vec![Some(1), None]));
        let error = timestamp_values(&nulls).unwrap_err();
        assert!(error.to_string().contains("null timestamps"), "{error}");

        let unsupported: ArrayRef = Arc::new(UInt8Array::from(vec![1, 2]));
        let error = timestamp_values(&unsupported).unwrap_err();
        assert!(
            error.to_string().contains("requires an Int64 or timestamp"),
            "{error}"
        );
    }

    #[tokio::test]
    async fn test_write_batches_and_metrics() {
        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let primary_key_1 = new_sparse_primary_key(&["a", "x"], &metadata, 1, 10);
        let primary_key_2 = new_sparse_primary_key(&["b", "y"], &metadata, 1, 20);
        let store = object_store();
        let mut writer = SeriesIndexWriter::try_new(
            metadata,
            store.clone(),
            "series.parquet",
            SeriesIndexWriterOptions { row_group_size: 2 },
        )
        .await
        .unwrap();

        writer
            .write(&dictionary_batch(
                &[
                    primary_key_1.as_slice(),
                    primary_key_1.as_slice(),
                    primary_key_1.as_slice(),
                    primary_key_1.as_slice(),
                ],
                &[70, 80, 90, 100],
            ))
            .await
            .unwrap();
        writer
            .write(&binary_batch(
                &[
                    primary_key_1.as_slice(),
                    primary_key_1.as_slice(),
                    primary_key_1.as_slice(),
                    primary_key_2.as_slice(),
                    primary_key_2.as_slice(),
                    primary_key_2.as_slice(),
                    primary_key_2.as_slice(),
                ],
                &[110, 120, 130, 200, 210, 220, 230],
            ))
            .await
            .unwrap();
        assert_eq!(writer.metrics().input_batches, 2);
        assert_eq!(writer.metrics().input_rows, 11);

        let metrics = writer.finish().await.unwrap();
        assert_eq!(metrics.input_batches, 2);
        assert_eq!(metrics.input_rows, 11);
        assert_eq!(metrics.num_series, 2);
        assert!(!metrics.aborted);

        let (output_bytes, row_groups, batches) = read_index(&store, "series.parquet").await;
        assert_eq!(metrics.output_bytes, output_bytes);
        assert_eq!(row_groups, 1);
        assert_eq!(batches.len(), 1);
        let batch = &batches[0];
        assert_eq!(batch.num_rows(), 2);
        assert_eq!(
            batch
                .column(0)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap(),
            &Int64Array::from(vec![70, 200])
        );
        assert_eq!(
            batch
                .column(1)
                .as_any()
                .downcast_ref::<Int64Array>()
                .unwrap(),
            &Int64Array::from(vec![130, 230])
        );
        assert_eq!(
            batch
                .column(2)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap(),
            &UInt64Array::from(vec![7, 4])
        );
        assert_eq!(
            batch
                .column(3)
                .as_any()
                .downcast_ref::<UInt32Array>()
                .unwrap(),
            &UInt32Array::from(vec![1, 1])
        );
        assert_eq!(
            batch
                .column(4)
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap(),
            &UInt64Array::from(vec![10, 20])
        );
        assert_eq!(
            batch
                .column(5)
                .as_any()
                .downcast_ref::<StringArray>()
                .unwrap(),
            &StringArray::from(vec![Some("a"), Some("b")])
        );
    }

    #[tokio::test]
    async fn test_row_group_size_and_empty_file() {
        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let store = object_store();
        let mut writer = SeriesIndexWriter::try_new(
            metadata.clone(),
            store.clone(),
            "groups.parquet",
            SeriesIndexWriterOptions { row_group_size: 2 },
        )
        .await
        .unwrap();
        let keys = (0..5)
            .map(|tsid| new_sparse_primary_key(&["a", "x"], &metadata, 1, tsid))
            .collect::<Vec<_>>();
        let key_refs = keys.iter().map(Vec::as_slice).collect::<Vec<_>>();
        writer
            .write(&binary_batch(&key_refs, &[1, 2, 3, 4, 5]))
            .await
            .unwrap();
        writer.finish().await.unwrap();
        let (_, row_groups, _) = read_index(&store, "groups.parquet").await;
        assert_eq!(row_groups, 3);

        let empty = SeriesIndexWriter::try_new(
            metadata,
            store.clone(),
            "empty.parquet",
            SeriesIndexWriterOptions::default(),
        )
        .await
        .unwrap()
        .finish()
        .await
        .unwrap();
        assert_eq!(empty.input_rows, 0);
        assert_eq!(empty.num_series, 0);
        let (output_bytes, _, batches) = read_index(&store, "empty.parquet").await;
        assert_eq!(empty.output_bytes, output_bytes);
        assert!(batches.is_empty());
    }

    #[tokio::test]
    async fn test_abort_and_out_of_order_input() {
        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let primary_key_1 = new_sparse_primary_key(&["a", "x"], &metadata, 1, 10);
        let primary_key_2 = new_sparse_primary_key(&["b", "y"], &metadata, 1, 20);
        let store = object_store();
        let mut writer = SeriesIndexWriter::try_new(
            metadata.clone(),
            store.clone(),
            "abort.parquet",
            SeriesIndexWriterOptions { row_group_size: 1 },
        )
        .await
        .unwrap();
        let error = writer
            .write(&binary_batch(
                &[primary_key_2.as_slice(), primary_key_1.as_slice()],
                &[1, 2],
            ))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not sorted"), "{error}");
        store
            .write("abort.parquet", Bytes::from_static(b"existing"))
            .await
            .unwrap();
        let metrics = writer.abort().await.unwrap();
        assert!(metrics.aborted);
        assert_eq!(metrics.output_bytes, 0);
        assert_eq!(
            store.read("abort.parquet").await.unwrap().to_bytes(),
            Bytes::from_static(b"existing")
        );

        let mut writer = SeriesIndexWriter::try_new(
            metadata,
            store.clone(),
            "dictionary-abort.parquet",
            SeriesIndexWriterOptions { row_group_size: 1 },
        )
        .await
        .unwrap();
        let error = writer
            .write(&dictionary_batch(
                &[
                    primary_key_2.as_slice(),
                    primary_key_2.as_slice(),
                    primary_key_1.as_slice(),
                ],
                &[1, 2, 3],
            ))
            .await
            .unwrap_err();
        assert!(error.to_string().contains("not sorted"), "{error}");
        writer.abort().await.unwrap();
        assert_eq!(
            store
                .stat("dictionary-abort.parquet")
                .await
                .unwrap_err()
                .kind(),
            ErrorKind::NotFound
        );
    }

    #[tokio::test]
    async fn test_nullable_tag_and_invalid_options() {
        let metadata = Arc::new(sst_region_metadata_with_encoding(
            PrimaryKeyEncoding::Sparse,
        ));
        let codec = SparsePrimaryKeyCodec::new(&metadata);
        let mut primary_key = Vec::new();
        codec
            .encode_value_refs(
                &[
                    (
                        ReservedColumnId::table_id(),
                        datatypes::value::ValueRef::UInt32(1),
                    ),
                    (
                        ReservedColumnId::tsid(),
                        datatypes::value::ValueRef::UInt64(10),
                    ),
                    (0, datatypes::value::ValueRef::String("a")),
                ],
                &mut primary_key,
            )
            .unwrap();
        let store = object_store();
        let mut writer = SeriesIndexWriter::try_new(
            metadata.clone(),
            store.clone(),
            "nullable.parquet",
            SeriesIndexWriterOptions::default(),
        )
        .await
        .unwrap();
        writer
            .write(&binary_batch(&[primary_key.as_slice()], &[1]))
            .await
            .unwrap();
        writer.finish().await.unwrap();
        let (_, _, batches) = read_index(&store, "nullable.parquet").await;
        let tag_1 = batches[0]
            .column(6)
            .as_any()
            .downcast_ref::<StringArray>()
            .unwrap();
        assert!(tag_1.is_null(0));

        assert!(
            SeriesIndexWriter::try_new(
                metadata,
                store,
                "invalid.parquet",
                SeriesIndexWriterOptions { row_group_size: 0 },
            )
            .await
            .is_err()
        );
    }
}
