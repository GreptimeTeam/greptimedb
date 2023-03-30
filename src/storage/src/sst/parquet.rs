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

//! Parquet sst format.

use std::collections::HashMap;
use std::pin::Pin;
use std::sync::Arc;

use arrow::datatypes::DataType;
use arrow_array::types::Int64Type;
use arrow_array::{
    Array, PrimitiveArray, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use async_compat::CompatExt;
use async_stream::try_stream;
use async_trait::async_trait;
use common_telemetry::{error, warn};
use common_time::range::TimestampRange;
use common_time::timestamp::TimeUnit;
use common_time::Timestamp;
use datatypes::arrow::array::BooleanArray;
use datatypes::arrow::error::ArrowError;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::prelude::ConcreteDataType;
use futures_util::{Stream, StreamExt, TryStreamExt};
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::{ArrowPredicate, RowFilter};
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use parquet::format::FileMetaData;
use parquet::schema::types::SchemaDescriptor;
use snafu::{OptionExt, ResultExt};
use table::predicate::Predicate;
use tokio::io::BufReader;

use crate::error::{
    self, DecodeParquetTimeRangeSnafu, NewRecordBatchSnafu, ReadObjectSnafu, ReadParquetSnafu,
    Result, WriteObjectSnafu,
};
use crate::read::{Batch, BatchReader};
use crate::schema::compat::ReadAdapter;
use crate::schema::{ProjectedSchemaRef, StoreSchema, StoreSchemaRef};
use crate::sst;
use crate::sst::stream_writer::BufferedWriter;
use crate::sst::{FileHandle, Source, SstInfo};

/// Parquet sst writer.
pub struct ParquetWriter<'a> {
    file_path: &'a str,
    source: Source,
    object_store: ObjectStore,
    max_row_group_size: usize,
}

impl<'a> ParquetWriter<'a> {
    pub fn new(file_path: &'a str, source: Source, object_store: ObjectStore) -> ParquetWriter {
        ParquetWriter {
            file_path,
            source,
            object_store,
            max_row_group_size: 4096, // TODO(hl): make this configurable
        }
    }

    pub async fn write_sst(self, _opts: &sst::WriteOptions) -> Result<Option<SstInfo>> {
        self.write_rows(None).await
    }

    /// Iterates memtable and writes rows to Parquet file.
    /// A chunk of records yielded from each iteration with a size given
    /// in config will be written to a single row group.
    async fn write_rows(
        mut self,
        extra_meta: Option<HashMap<String, String>>,
    ) -> Result<Option<SstInfo>> {
        let projected_schema = self.source.projected_schema();
        let store_schema = projected_schema.schema_to_read();
        let schema = store_schema.arrow_schema().clone();

        let writer_props = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_encoding(Encoding::PLAIN)
            .set_max_row_group_size(self.max_row_group_size)
            .set_key_value_metadata(extra_meta.map(|map| {
                map.iter()
                    .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
                    .collect::<Vec<_>>()
            }))
            .build();

        let writer = self
            .object_store
            .writer(self.file_path)
            .await
            .context(WriteObjectSnafu {
                path: self.file_path,
            })?;

        let mut stream_writer = BufferedWriter::try_new(
            self.file_path.to_string(),
            writer,
            schema.clone(),
            Some(writer_props),
            4 * 1024 * 1024,
        )?;
        let mut batches_written = 0;

        while let Some(batch) = self.source.next_batch().await? {
            let arrow_batch = RecordBatch::try_new(
                schema.clone(),
                batch
                    .columns()
                    .iter()
                    .map(|v| v.to_arrow_array())
                    .collect::<Vec<_>>(),
            )
            .context(NewRecordBatchSnafu)?;
            stream_writer.write(&arrow_batch).await?;
            batches_written += 1;
        }

        if batches_written == 0 {
            // if the source does not contain any batch, we skip writing an empty parquet file.
            if !stream_writer.abort().await {
                warn!(
                    "Partial file {} has been uploaded to remote storage",
                    self.file_path
                );
            }
            return Ok(None);
        }

        let (file_meta, file_size) = stream_writer.close().await?;
        let time_range = decode_timestamp_range(&file_meta, store_schema)
            .ok()
            .flatten();

        // object_store.write will make sure all bytes are written or an error is raised.
        Ok(Some(SstInfo {
            time_range,
            file_size,
        }))
    }
}

fn decode_timestamp_range(
    file_meta: &FileMetaData,
    store_schema: &StoreSchemaRef,
) -> Result<Option<(Timestamp, Timestamp)>> {
    let schema = store_schema.schema();
    let (Some(ts_col_idx), Some(ts_col)) = (schema.timestamp_index(), schema.timestamp_column()) else { return Ok(None); };
    let ts_datatype = &ts_col.data_type;
    decode_timestamp_range_inner(file_meta, ts_col_idx, ts_datatype)
}

fn decode_timestamp_range_inner(
    file_meta: &FileMetaData,
    ts_index: usize,
    ts_datatype: &ConcreteDataType,
) -> Result<Option<(Timestamp, Timestamp)>> {
    let mut start = i64::MAX;
    let mut end = i64::MIN;

    let unit = match ts_datatype {
        ConcreteDataType::Int64(_) => TimeUnit::Millisecond,
        ConcreteDataType::Timestamp(type_) => type_.unit(),
        _ => {
            return DecodeParquetTimeRangeSnafu {
                msg: format!("Unexpected timestamp column datatype: {ts_datatype:?}"),
            }
            .fail();
        }
    };

    for rg in &file_meta.row_groups {
        let Some(ref metadata) = rg
            .columns
            .get(ts_index)
            .context(DecodeParquetTimeRangeSnafu {
                msg: format!("Cannot find ts column by index: {ts_index}"),
            })?
            .meta_data else { return Ok(None) };
        let Some(stats) = &metadata.statistics else { return Ok(None) };
        let (Some(min_value), Some(max_value)) = (&stats.min_value, &stats.max_value) else { return Ok(None); };

        // according to [parquet's spec](https://parquet.apache.org/docs/file-format/data-pages/encodings/), min/max value in stats uses plain encoding with little endian.
        // also see https://github.com/apache/arrow-rs/blob/5fb337db04a1a19f7d40da46f19b7b5fd4051593/parquet/src/file/statistics.rs#L172
        let min = i64::from_le_bytes(min_value[..8].try_into().map_err(|e| {
            error!(
                "Failed to decode min value from stats, bytes: {:?}, source: {:?}",
                min_value, e
            );
            DecodeParquetTimeRangeSnafu {
                msg: "decode min value",
            }
            .build()
        })?);
        let max = i64::from_le_bytes(max_value[..8].try_into().map_err(|e| {
            error!(
                "Failed to decode max value from stats, bytes: {:?}, source: {:?}",
                max_value, e
            );
            DecodeParquetTimeRangeSnafu {
                msg: "decode max value",
            }
            .build()
        })?);
        start = start.min(min);
        end = end.max(max);
    }

    assert!(
        start <= end,
        "Illegal timestamp range decoded from SST file {:?}, start: {}, end: {}",
        file_meta,
        start,
        end
    );
    Ok(Some((
        Timestamp::new(start, unit),
        Timestamp::new(end, unit),
    )))
}

pub struct ParquetReader {
    // Holds the file handle to avoid the file purge purge it.
    file_handle: FileHandle,
    object_store: ObjectStore,
    projected_schema: ProjectedSchemaRef,
    predicate: Predicate,
    time_range: TimestampRange,
}

impl ParquetReader {
    pub fn new(
        file_handle: FileHandle,
        object_store: ObjectStore,
        projected_schema: ProjectedSchemaRef,
        predicate: Predicate,
        time_range: TimestampRange,
    ) -> ParquetReader {
        ParquetReader {
            file_handle,
            object_store,
            projected_schema,
            predicate,
            time_range,
        }
    }

    pub async fn chunk_stream(&self) -> Result<ChunkStream> {
        let file_path = self.file_handle.file_path();
        let operator = self.object_store.clone();

        let reader = operator
            .reader(&file_path)
            .await
            .context(ReadObjectSnafu { path: &file_path })?
            .compat();
        let buf_reader = BufReader::new(reader);
        let builder = ParquetRecordBatchStreamBuilder::new(buf_reader)
            .await
            .context(ReadParquetSnafu { file: &file_path })?;
        let arrow_schema = builder.schema().clone();

        let store_schema = Arc::new(
            StoreSchema::try_from(arrow_schema)
                .context(error::ConvertStoreSchemaSnafu { file: &file_path })?,
        );

        let adapter = ReadAdapter::new(store_schema.clone(), self.projected_schema.clone())?;

        let pruned_row_groups = self
            .predicate
            .prune_row_groups(
                store_schema.schema().clone(),
                builder.metadata().row_groups(),
            )
            .into_iter()
            .enumerate()
            .filter_map(|(idx, valid)| if valid { Some(idx) } else { None })
            .collect::<Vec<_>>();

        let parquet_schema_desc = builder.metadata().file_metadata().schema_descr_ptr();

        let projection = ProjectionMask::roots(&parquet_schema_desc, adapter.fields_to_read());
        let mut builder = builder
            .with_projection(projection)
            .with_row_groups(pruned_row_groups);

        // if time range row filter is present, we can push down the filter to reduce rows to scan.
        if let Some(row_filter) = self.build_time_range_row_filter(&parquet_schema_desc) {
            builder = builder.with_row_filter(row_filter);
        }

        let mut stream = builder
            .build()
            .context(ReadParquetSnafu { file: &file_path })?;

        let chunk_stream = try_stream!({
            while let Some(res) = stream.next().await {
                yield res.context(ReadParquetSnafu { file: &file_path })?
            }
        });

        ChunkStream::new(self.file_handle.clone(), adapter, Box::pin(chunk_stream))
    }

    /// Builds time range row filter.
    fn build_time_range_row_filter(&self, schema_desc: &SchemaDescriptor) -> Option<RowFilter> {
        let ts_col_idx = self
            .projected_schema
            .schema_to_read()
            .schema()
            .timestamp_index()?;
        let ts_col = self
            .projected_schema
            .schema_to_read()
            .schema()
            .timestamp_column()?;

        let ts_col_unit = match &ts_col.data_type {
            ConcreteDataType::Int64(_) => TimeUnit::Millisecond,
            ConcreteDataType::Timestamp(ts_type) => ts_type.unit(),
            _ => unreachable!(),
        };

        let projection = ProjectionMask::roots(schema_desc, vec![ts_col_idx]);

        // checks if converting time range unit into ts col unit will result into rounding error.
        if time_unit_lossy(&self.time_range, ts_col_unit) {
            let filter = RowFilter::new(vec![Box::new(PlainTimestampRowFilter::new(
                self.time_range,
                projection,
            ))]);
            return Some(filter);
        }

        // If any of the conversion overflows, we cannot use arrow's computation method, instead
        // we resort to plain filter that compares timestamp with given range, less efficient,
        // but simpler.
        // TODO(hl): If the range is gt_eq/lt, we also use PlainTimestampRowFilter, but these cases
        // can also use arrow's gt_eq_scalar/lt_scalar methods.
        let row_filter = if let (Some(lower), Some(upper)) = (
            self.time_range
                .start()
                .and_then(|s| s.convert_to(ts_col_unit))
                .map(|t| t.value()),
            self.time_range
                .end()
                .and_then(|s| s.convert_to(ts_col_unit))
                .map(|t| t.value()),
        ) {
            Box::new(FastTimestampRowFilter::new(projection, lower, upper)) as _
        } else {
            Box::new(PlainTimestampRowFilter::new(self.time_range, projection)) as _
        };
        let filter = RowFilter::new(vec![row_filter]);
        Some(filter)
    }
}

fn time_unit_lossy(range: &TimestampRange, ts_col_unit: TimeUnit) -> bool {
    range
        .start()
        .map(|start| start.unit().factor() < ts_col_unit.factor())
        .unwrap_or(false)
        || range
            .end()
            .map(|end| end.unit().factor() < ts_col_unit.factor())
            .unwrap_or(false)
}

/// `FastTimestampRowFilter` is used to filter rows within given timestamp range when reading
/// row groups from parquet files, while avoids fetching all columns from SSTs file.
struct FastTimestampRowFilter {
    lower_bound: i64,
    upper_bound: i64,
    projection: ProjectionMask,
}

impl FastTimestampRowFilter {
    fn new(projection: ProjectionMask, lower_bound: i64, upper_bound: i64) -> Self {
        Self {
            lower_bound,
            upper_bound,
            projection,
        }
    }
}

impl ArrowPredicate for FastTimestampRowFilter {
    fn projection(&self) -> &ProjectionMask {
        &self.projection
    }

    /// Selects the rows matching given time range.
    fn evaluate(&mut self, batch: RecordBatch) -> std::result::Result<BooleanArray, ArrowError> {
        // the projection has only timestamp column, so we can safely take the first column in batch.
        let ts_col = batch.column(0);

        macro_rules! downcast_and_compute {
            ($typ: ty) => {
                {
                    let ts_col = ts_col
                        .as_any()
                        .downcast_ref::<$typ>()
                        .unwrap(); // safety: we've checked the data type of timestamp column.
                    let left = arrow::compute::gt_eq_scalar(ts_col, self.lower_bound)?;
                    let right = arrow::compute::lt_scalar(ts_col, self.upper_bound)?;
                    arrow::compute::and(&left, &right)
                }
            };
        }

        match ts_col.data_type() {
            DataType::Timestamp(unit, _) => match unit {
                arrow::datatypes::TimeUnit::Second => {
                    downcast_and_compute!(TimestampSecondArray)
                }
                arrow::datatypes::TimeUnit::Millisecond => {
                    downcast_and_compute!(TimestampMillisecondArray)
                }
                arrow::datatypes::TimeUnit::Microsecond => {
                    downcast_and_compute!(TimestampMicrosecondArray)
                }
                arrow::datatypes::TimeUnit::Nanosecond => {
                    downcast_and_compute!(TimestampNanosecondArray)
                }
            },
            DataType::Int64 => downcast_and_compute!(PrimitiveArray<Int64Type>),
            _ => {
                unreachable!()
            }
        }
    }
}

/// [PlainTimestampRowFilter] iterates each element in timestamp column, build a [Timestamp] struct
/// and checks if given time range contains the timestamp.
struct PlainTimestampRowFilter {
    time_range: TimestampRange,
    projection: ProjectionMask,
}

impl PlainTimestampRowFilter {
    fn new(time_range: TimestampRange, projection: ProjectionMask) -> Self {
        Self {
            time_range,
            projection,
        }
    }
}

impl ArrowPredicate for PlainTimestampRowFilter {
    fn projection(&self) -> &ProjectionMask {
        &self.projection
    }

    fn evaluate(&mut self, batch: RecordBatch) -> std::result::Result<BooleanArray, ArrowError> {
        // the projection has only timestamp column, so we can safely take the first column in batch.
        let ts_col = batch.column(0);

        macro_rules! downcast_and_compute {
            ($array_ty: ty, $unit: ident) => {{
                    let ts_col = ts_col
                    .as_any()
                    .downcast_ref::<$array_ty>()
                    .unwrap(); // safety: we've checked the data type of timestamp column.
                    Ok(BooleanArray::from_iter(ts_col.iter().map(|ts| {
                        ts.map(|val| {
                            Timestamp::new(val, TimeUnit::$unit)
                        }).map(|ts| {
                            self.time_range.contains(&ts)
                        })
                    })))

            }};
        }

        match ts_col.data_type() {
            DataType::Timestamp(unit, _) => match unit {
                arrow::datatypes::TimeUnit::Second => {
                    downcast_and_compute!(TimestampSecondArray, Second)
                }
                arrow::datatypes::TimeUnit::Millisecond => {
                    downcast_and_compute!(TimestampMillisecondArray, Millisecond)
                }
                arrow::datatypes::TimeUnit::Microsecond => {
                    downcast_and_compute!(TimestampMicrosecondArray, Microsecond)
                }
                arrow::datatypes::TimeUnit::Nanosecond => {
                    downcast_and_compute!(TimestampNanosecondArray, Nanosecond)
                }
            },
            DataType::Int64 => {
                downcast_and_compute!(PrimitiveArray<Int64Type>, Millisecond)
            }
            _ => {
                unreachable!()
            }
        }
    }
}

pub type SendableChunkStream = Pin<Box<dyn Stream<Item = Result<RecordBatch>> + Send>>;

pub struct ChunkStream {
    // Holds the file handle in the stream to avoid the purger purge it.
    _file_handle: FileHandle,
    adapter: ReadAdapter,
    stream: SendableChunkStream,
}

impl ChunkStream {
    pub fn new(
        file_handle: FileHandle,
        adapter: ReadAdapter,
        stream: SendableChunkStream,
    ) -> Result<Self> {
        Ok(Self {
            _file_handle: file_handle,
            adapter,
            stream,
        })
    }
}

#[async_trait]
impl BatchReader for ChunkStream {
    async fn next_batch(&mut self) -> Result<Option<Batch>> {
        self.stream
            .try_next()
            .await?
            .map(|rb| self.adapter.arrow_record_batch_to_batch(&rb))
            .transpose()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_test_util::temp_dir::create_temp_dir;
    use datatypes::arrow::array::{Array, ArrayRef, UInt64Array, UInt8Array};
    use datatypes::prelude::{ScalarVector, Vector};
    use datatypes::types::{TimestampMillisecondType, TimestampType};
    use datatypes::vectors::TimestampMillisecondVector;
    use object_store::services::Fs;
    use store_api::storage::OpType;

    use super::*;
    use crate::file_purger::noop::new_noop_file_purger;
    use crate::memtable::{
        tests as memtable_tests, DefaultMemtableBuilder, IterContext, MemtableBuilder,
    };
    use crate::schema::ProjectedSchema;
    use crate::sst::{FileId, FileMeta};

    fn create_object_store(root: &str) -> ObjectStore {
        let mut builder = Fs::default();
        builder.root(root);
        ObjectStore::new(builder).unwrap().finish()
    }

    #[tokio::test]
    async fn test_parquet_writer() {
        common_telemetry::init_default_ut_logging();
        let schema = memtable_tests::schema_for_test();
        let memtable = DefaultMemtableBuilder::default().build(schema);

        memtable_tests::write_kvs(
            &*memtable,
            10, // sequence
            OpType::Put,
            &[
                (1000, 1),
                (1000, 2),
                (2002, 1),
                (2003, 1),
                (2003, 5),
                (1001, 1),
            ], // keys
            &[
                (Some(1), Some(1234)),
                (Some(2), Some(1234)),
                (Some(7), Some(1234)),
                (Some(8), Some(1234)),
                (Some(9), Some(1234)),
                (Some(3), Some(1234)),
            ], // values
        );

        let dir = create_temp_dir("write_parquet");
        let path = dir.path().to_str().unwrap();

        let object_store = create_object_store(path);
        let sst_file_name = "test-flush.parquet";
        let iter = memtable.iter(&IterContext::default()).unwrap();
        let writer = ParquetWriter::new(sst_file_name, Source::Iter(iter), object_store.clone());

        writer
            .write_sst(&sst::WriteOptions::default())
            .await
            .unwrap();

        // verify parquet file
        let reader = BufReader::new(object_store.reader(sst_file_name).await.unwrap().compat());

        let builder = ParquetRecordBatchStreamBuilder::new(reader).await.unwrap();

        let mut stream = builder.build().unwrap();
        // chunk schema: timestamp, __version, v1, __sequence, __op_type
        let chunk = stream.next().await.unwrap().unwrap();
        assert_eq!(6, chunk.columns().len());

        // timestamp
        assert_eq!(
            &TimestampMillisecondVector::from_slice([
                1000.into(),
                1000.into(),
                1001.into(),
                2002.into(),
                2003.into(),
                2003.into()
            ])
            .to_arrow_array(),
            chunk.column(0)
        );

        // version
        assert_eq!(
            &(Arc::new(UInt64Array::from(vec![1, 2, 1, 1, 1, 5])) as ArrayRef),
            chunk.column(1)
        );

        // v0
        assert_eq!(
            &(Arc::new(UInt64Array::from(vec![1, 2, 3, 7, 8, 9])) as Arc<dyn Array>),
            chunk.column(2)
        );

        // v1
        assert_eq!(
            &(Arc::new(UInt64Array::from(vec![1234; 6])) as Arc<dyn Array>),
            chunk.column(3)
        );

        // sequence
        assert_eq!(
            &(Arc::new(UInt64Array::from(vec![10; 6])) as Arc<dyn Array>),
            chunk.column(4)
        );

        // op_type
        assert_eq!(
            &(Arc::new(UInt8Array::from(vec![1; 6])) as Arc<dyn Array>),
            chunk.column(5)
        );
    }

    #[tokio::test]
    async fn test_parquet_read_large_batch() {
        common_telemetry::init_default_ut_logging();
        let schema = memtable_tests::schema_for_test();
        let memtable = DefaultMemtableBuilder::default().build(schema.clone());

        let rows_total = 4096 * 4;
        let mut keys_vec = Vec::with_capacity(rows_total);
        let mut values_vec = Vec::with_capacity(rows_total);

        for i in 0..rows_total {
            keys_vec.push((i as i64, i as u64));
            values_vec.push((Some(i as u64), Some(i as u64)));
        }

        memtable_tests::write_kvs(
            &*memtable,
            10, // sequence
            OpType::Put,
            &keys_vec,   // keys
            &values_vec, // values
        );

        let dir = create_temp_dir("write_parquet");
        let path = dir.path().to_str().unwrap();
        let object_store = create_object_store(path);
        let sst_file_handle = new_file_handle(FileId::random());
        let sst_file_name = sst_file_handle.file_name();
        let iter = memtable.iter(&IterContext::default()).unwrap();
        let writer = ParquetWriter::new(&sst_file_name, Source::Iter(iter), object_store.clone());

        let SstInfo {
            time_range,
            file_size,
        } = writer
            .write_sst(&sst::WriteOptions::default())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            Some((
                Timestamp::new_millisecond(0),
                Timestamp::new_millisecond((rows_total - 1) as i64)
            )),
            time_range
        );
        assert_ne!(file_size, 0);
        let operator = create_object_store(dir.path().to_str().unwrap());

        let projected_schema = Arc::new(ProjectedSchema::new(schema, Some(vec![1])).unwrap());
        let reader = ParquetReader::new(
            sst_file_handle,
            operator,
            projected_schema,
            Predicate::empty(),
            TimestampRange::min_to_max(),
        );

        let mut rows_fetched = 0;
        let mut stream = reader.chunk_stream().await.unwrap();
        while let Some(res) = stream.next_batch().await.unwrap() {
            rows_fetched += res.num_rows();
        }
        assert_eq!(rows_total, rows_fetched);
    }

    fn new_file_handle(file_id: FileId) -> FileHandle {
        let file_purger = new_noop_file_purger();
        let layer = Arc::new(crate::test_util::access_layer_util::MockAccessLayer {});
        FileHandle::new(
            FileMeta {
                region_id: 0,
                file_id,
                time_range: Some((
                    Timestamp::new_millisecond(0),
                    Timestamp::new_millisecond(1000),
                )),
                level: 0,
                file_size: 0,
            },
            layer,
            file_purger,
        )
    }

    #[tokio::test]
    async fn test_parquet_reader() {
        common_telemetry::init_default_ut_logging();
        let schema = memtable_tests::schema_for_test();
        let memtable = DefaultMemtableBuilder::default().build(schema.clone());

        memtable_tests::write_kvs(
            &*memtable,
            10, // sequence
            OpType::Put,
            &[
                (1000, 1),
                (1000, 2),
                (2002, 1),
                (2003, 1),
                (2003, 5),
                (1001, 1),
            ], // keys
            &[
                (Some(1), Some(1234)),
                (Some(2), Some(1234)),
                (Some(7), Some(1234)),
                (Some(8), Some(1234)),
                (Some(9), Some(1234)),
                (Some(3), Some(1234)),
            ], // values
        );

        let dir = create_temp_dir("write_parquet");
        let path = dir.path().to_str().unwrap();

        let object_store = create_object_store(path);
        let file_handle = new_file_handle(FileId::random());
        let sst_file_name = file_handle.file_name();
        let iter = memtable.iter(&IterContext::default()).unwrap();
        let writer = ParquetWriter::new(&sst_file_name, Source::Iter(iter), object_store.clone());

        let SstInfo {
            time_range,
            file_size,
        } = writer
            .write_sst(&sst::WriteOptions::default())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            Some((
                Timestamp::new_millisecond(1000),
                Timestamp::new_millisecond(2003)
            )),
            time_range
        );
        assert_ne!(file_size, 0);
        let operator = create_object_store(dir.path().to_str().unwrap());

        let projected_schema = Arc::new(ProjectedSchema::new(schema, Some(vec![1])).unwrap());
        let reader = ParquetReader::new(
            file_handle,
            operator,
            projected_schema,
            Predicate::empty(),
            TimestampRange::min_to_max(),
        );

        let mut stream = reader.chunk_stream().await.unwrap();
        assert_eq!(
            6,
            stream
                .next_batch()
                .await
                .transpose()
                .unwrap()
                .unwrap()
                .num_rows()
        );
    }

    async fn check_range_read(
        file_handle: FileHandle,
        object_store: ObjectStore,
        schema: ProjectedSchemaRef,
        range: TimestampRange,
        expect: Vec<i64>,
    ) {
        let reader =
            ParquetReader::new(file_handle, object_store, schema, Predicate::empty(), range);
        let mut stream = reader.chunk_stream().await.unwrap();
        let result = stream.next_batch().await;

        let Some(batch) = result.unwrap() else {
            // if batch does not contain any row
            assert!(expect.is_empty());
            return;
        };

        assert_eq!(
            ConcreteDataType::Timestamp(TimestampType::Millisecond(TimestampMillisecondType)),
            batch.column(0).data_type()
        );

        let ts = batch
            .column(0)
            .as_any()
            .downcast_ref::<TimestampMillisecondVector>()
            .unwrap()
            .iter_data()
            .map(|t| t.unwrap().0.value())
            .collect::<Vec<_>>();
        assert_eq!(expect, ts);
    }

    #[tokio::test]
    async fn test_parquet_reader_with_time_range_filter() {
        common_telemetry::init_default_ut_logging();
        let schema = memtable_tests::schema_for_test();
        let memtable = DefaultMemtableBuilder::default().build(schema.clone());

        memtable_tests::write_kvs(
            &*memtable,
            10, // sequence
            OpType::Put,
            &[
                (1000, 1),
                (1000, 2),
                (2002, 1),
                (2003, 1),
                (2003, 5),
                (1001, 1),
                (3001, 1),
            ], // keys
            &[
                (Some(1), Some(1234)),
                (Some(2), Some(1234)),
                (Some(7), Some(1234)),
                (Some(8), Some(1234)),
                (Some(9), Some(1234)),
                (Some(3), Some(1234)),
                (Some(7), Some(1234)),
            ], // values
        );

        let dir = create_temp_dir("read-parquet-by-range");
        let path = dir.path().to_str().unwrap();
        let object_store = create_object_store(path);
        let sst_file_handle = new_file_handle(FileId::random());
        let sst_file_name = sst_file_handle.file_name();
        let iter = memtable.iter(&IterContext::default()).unwrap();
        let writer = ParquetWriter::new(&sst_file_name, Source::Iter(iter), object_store.clone());

        let SstInfo {
            time_range,
            file_size,
        } = writer
            .write_sst(&sst::WriteOptions::default())
            .await
            .unwrap()
            .unwrap();

        assert_eq!(
            Some((
                Timestamp::new_millisecond(1000),
                Timestamp::new_millisecond(3001)
            )),
            time_range
        );
        assert_ne!(file_size, 0);

        let projected_schema =
            Arc::new(ProjectedSchema::new(schema, Some(vec![1, 0, 3, 2])).unwrap());

        check_range_read(
            sst_file_handle.clone(),
            object_store.clone(),
            projected_schema.clone(),
            TimestampRange::with_unit(1000, 2003, TimeUnit::Millisecond).unwrap(),
            vec![1000, 1000, 1001, 2002],
        )
        .await;

        check_range_read(
            sst_file_handle.clone(),
            object_store.clone(),
            projected_schema.clone(),
            TimestampRange::with_unit(2002, 3001, TimeUnit::Millisecond).unwrap(),
            vec![2002, 2003, 2003],
        )
        .await;

        // read a range without any rows.
        check_range_read(
            sst_file_handle.clone(),
            object_store.clone(),
            projected_schema.clone(),
            TimestampRange::with_unit(3002, 3003, TimeUnit::Millisecond).unwrap(),
            vec![],
        )
        .await;

        //
        check_range_read(
            sst_file_handle.clone(),
            object_store.clone(),
            projected_schema.clone(),
            TimestampRange::with_unit(1000, 3000, TimeUnit::Millisecond).unwrap(),
            vec![1000, 1000, 1001, 2002, 2003, 2003],
        )
        .await;

        // read full range
        check_range_read(
            sst_file_handle,
            object_store,
            projected_schema,
            TimestampRange::min_to_max(),
            vec![1000, 1000, 1001, 2002, 2003, 2003, 3001],
        )
        .await;
    }

    fn check_unit_lossy(range_unit: TimeUnit, col_unit: TimeUnit, expect: bool) {
        assert_eq!(
            expect,
            time_unit_lossy(
                &TimestampRange::with_unit(0, 1, range_unit).unwrap(),
                col_unit
            )
        )
    }

    #[tokio::test]
    async fn test_write_empty_file() {
        common_telemetry::init_default_ut_logging();
        let schema = memtable_tests::schema_for_test();
        let memtable = DefaultMemtableBuilder::default().build(schema.clone());

        let dir = create_temp_dir("read-parquet-by-range");
        let path = dir.path().to_str().unwrap();
        let mut builder = Fs::default();
        builder.root(path);
        let object_store = ObjectStore::new(builder).unwrap().finish();
        let sst_file_name = "test-read.parquet";
        let iter = memtable.iter(&IterContext::default()).unwrap();
        let writer = ParquetWriter::new(sst_file_name, Source::Iter(iter), object_store.clone());

        let sst_info_opt = writer
            .write_sst(&sst::WriteOptions::default())
            .await
            .unwrap();

        assert!(sst_info_opt.is_none());
    }

    #[test]
    fn test_time_unit_lossy() {
        // converting a range with unit second to millisecond will not cause rounding error
        check_unit_lossy(TimeUnit::Second, TimeUnit::Second, false);
        check_unit_lossy(TimeUnit::Second, TimeUnit::Millisecond, false);
        check_unit_lossy(TimeUnit::Second, TimeUnit::Microsecond, false);
        check_unit_lossy(TimeUnit::Second, TimeUnit::Nanosecond, false);

        check_unit_lossy(TimeUnit::Millisecond, TimeUnit::Second, true);
        check_unit_lossy(TimeUnit::Millisecond, TimeUnit::Millisecond, false);
        check_unit_lossy(TimeUnit::Millisecond, TimeUnit::Microsecond, false);
        check_unit_lossy(TimeUnit::Millisecond, TimeUnit::Nanosecond, false);

        check_unit_lossy(TimeUnit::Microsecond, TimeUnit::Second, true);
        check_unit_lossy(TimeUnit::Microsecond, TimeUnit::Millisecond, true);
        check_unit_lossy(TimeUnit::Microsecond, TimeUnit::Microsecond, false);
        check_unit_lossy(TimeUnit::Microsecond, TimeUnit::Nanosecond, false);

        check_unit_lossy(TimeUnit::Nanosecond, TimeUnit::Second, true);
        check_unit_lossy(TimeUnit::Nanosecond, TimeUnit::Millisecond, true);
        check_unit_lossy(TimeUnit::Nanosecond, TimeUnit::Microsecond, true);
        check_unit_lossy(TimeUnit::Nanosecond, TimeUnit::Nanosecond, false);
    }
}
