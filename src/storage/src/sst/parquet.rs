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

use async_compat::CompatExt;
use async_stream::try_stream;
use async_trait::async_trait;
use common_telemetry::{debug, error};
use common_time::range::TimestampRange;
use common_time::timestamp::TimeUnit;
use common_time::Timestamp;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::prelude::ConcreteDataType;
use futures_util::{Stream, StreamExt, TryStreamExt};
use object_store::ObjectStore;
use parquet::arrow::{ParquetRecordBatchStreamBuilder, ProjectionMask};
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use parquet::format::FileMetaData;
use parquet::schema::types::ColumnPath;
use snafu::{OptionExt, ResultExt};
use store_api::storage::consts::SEQUENCE_COLUMN_NAME;
use table::predicate::Predicate;
use tokio::io::BufReader;

use crate::error::{self, DecodeParquetTimeRangeSnafu, ReadObjectSnafu, ReadParquetSnafu, Result};
use crate::read::{Batch, BatchReader};
use crate::schema::compat::ReadAdapter;
use crate::schema::{ProjectedSchemaRef, StoreSchema};
use crate::sst;
use crate::sst::pruning::build_row_filter;
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

    pub async fn write_sst(self, opts: &sst::WriteOptions) -> Result<Option<SstInfo>> {
        self.write_rows(None, opts).await
    }

    /// Iterates memtable and writes rows to Parquet file.
    /// A chunk of records yielded from each iteration with a size given
    /// in config will be written to a single row group.
    async fn write_rows(
        mut self,
        extra_meta: Option<HashMap<String, String>>,
        opts: &sst::WriteOptions,
    ) -> Result<Option<SstInfo>> {
        let schema = self.source.schema();

        let mut props_builder = WriterProperties::builder()
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_encoding(Encoding::PLAIN)
            .set_max_row_group_size(self.max_row_group_size)
            .set_key_value_metadata(extra_meta.map(|map| {
                map.iter()
                    .map(|(k, v)| KeyValue::new(k.clone(), v.clone()))
                    .collect::<Vec<_>>()
            }))
            .set_column_encoding(
                ColumnPath::new(vec![SEQUENCE_COLUMN_NAME.to_string()]),
                Encoding::DELTA_BINARY_PACKED,
            )
            .set_column_dictionary_enabled(
                ColumnPath::new(vec![SEQUENCE_COLUMN_NAME.to_string()]),
                false,
            );

        if let Some(ts_col) = schema.timestamp_column() {
            props_builder = props_builder.set_column_encoding(
                ColumnPath::new(vec![ts_col.name.clone()]),
                Encoding::DELTA_BINARY_PACKED,
            );
        }

        let writer_props = props_builder.build();

        let mut buffered_writer = BufferedWriter::try_new(
            self.file_path.to_string(),
            self.object_store.clone(),
            &schema,
            Some(writer_props),
            opts.sst_write_buffer_size.as_bytes() as usize,
        )
        .await?;
        let mut rows_written = 0;

        while let Some(batch) = self.source.next_batch().await? {
            buffered_writer.write(&batch).await?;
            rows_written += batch.num_rows();
        }

        if rows_written == 0 {
            debug!("No data written, try abort writer: {}", self.file_path);
            let _ = buffered_writer.close().await?;
            return Ok(None);
        }

        let (file_meta, file_size) = buffered_writer.close().await?;
        let time_range = decode_timestamp_range(&file_meta, &schema).ok().flatten();

        // object_store.write will make sure all bytes are written or an error is raised.
        Ok(Some(SstInfo {
            time_range,
            file_size,
            num_rows: rows_written,
        }))
    }
}

fn decode_timestamp_range(
    file_meta: &FileMetaData,
    schema: &datatypes::schema::SchemaRef,
) -> Result<Option<(Timestamp, Timestamp)>> {
    let (Some(ts_col_idx), Some(ts_col)) = (schema.timestamp_index(), schema.timestamp_column())
    else {
        return Ok(None);
    };
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
            .meta_data
        else {
            return Ok(None);
        };
        let Some(stats) = &metadata.statistics else {
            return Ok(None);
        };
        let (Some(min_value), Some(max_value)) = (&stats.min_value, &stats.max_value) else {
            return Ok(None);
        };

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
            .prune_row_groups(builder.metadata().row_groups())
            .into_iter()
            .enumerate()
            .filter_map(|(idx, valid)| if valid { Some(idx) } else { None })
            .collect::<Vec<_>>();

        let parquet_schema_desc = builder.metadata().file_metadata().schema_descr_ptr();

        let projection_mask = ProjectionMask::roots(&parquet_schema_desc, adapter.fields_to_read());
        let mut builder = builder
            .with_projection(projection_mask.clone())
            .with_row_groups(pruned_row_groups);

        if let Some(row_filter) = build_row_filter(
            self.time_range,
            &self.predicate,
            &store_schema,
            &parquet_schema_desc,
            projection_mask,
        ) {
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
    use std::ops::Range;
    use std::sync::Arc;

    use api::v1::OpType;
    use common_base::readable_size::ReadableSize;
    use common_test_util::temp_dir::create_temp_dir;
    use datatypes::arrow::array::{Array, UInt64Array, UInt8Array};
    use datatypes::prelude::{ScalarVector, Vector};
    use datatypes::types::{TimestampMillisecondType, TimestampType};
    use datatypes::vectors::TimestampMillisecondVector;
    use object_store::services::Fs;

    use super::*;
    use crate::file_purger::noop::new_noop_file_purger;
    use crate::memtable::{
        tests as memtable_tests, DefaultMemtableBuilder, IterContext, MemtableBuilder,
    };
    use crate::schema::ProjectedSchema;
    use crate::sst::{FileId, FileMeta};

    fn create_object_store(root: &str) -> ObjectStore {
        let mut builder = Fs::default();
        let _ = builder.root(root);
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
            &[1000, 1002, 2002, 2003, 2003, 1001], // keys
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
        let iter = memtable.iter(IterContext::default()).unwrap();
        let writer = ParquetWriter::new(sst_file_name, Source::Iter(iter), object_store.clone());

        assert!(writer
            .write_sst(&sst::WriteOptions::default())
            .await
            .is_ok());

        // verify parquet file
        let reader = BufReader::new(object_store.reader(sst_file_name).await.unwrap().compat());

        let builder = ParquetRecordBatchStreamBuilder::new(reader).await.unwrap();

        let mut stream = builder.build().unwrap();
        // chunk schema: timestamp, v1, __sequence, __op_type
        let chunk = stream.next().await.unwrap().unwrap();
        assert_eq!(5, chunk.columns().len());

        // timestamp
        assert_eq!(
            &TimestampMillisecondVector::from_slice([
                1000.into(),
                1001.into(),
                1002.into(),
                2002.into(),
                2003.into(),
            ])
            .to_arrow_array(),
            chunk.column(0)
        );

        // v0
        assert_eq!(
            &(Arc::new(UInt64Array::from(vec![1, 3, 2, 7, 9])) as Arc<dyn Array>),
            chunk.column(1)
        );

        // v1
        assert_eq!(
            &(Arc::new(UInt64Array::from(vec![1234; 5])) as Arc<dyn Array>),
            chunk.column(2)
        );

        // sequence
        assert_eq!(
            &(Arc::new(UInt64Array::from(vec![10; 5])) as Arc<dyn Array>),
            chunk.column(3)
        );

        // op_type
        assert_eq!(
            &(Arc::new(UInt8Array::from(vec![1; 5])) as Arc<dyn Array>),
            chunk.column(4)
        );
    }

    #[tokio::test]
    async fn test_write_large_data() {
        common_telemetry::init_default_ut_logging();
        let schema = memtable_tests::schema_for_test();
        let memtable = DefaultMemtableBuilder::default().build(schema);

        let mut rows_written = 0;
        for i in 0..16 {
            let range: Range<i64> = i * 1024..(i + 1) * 1024;
            let keys = range.clone().collect::<Vec<_>>();
            let values = range
                .map(|idx| (Some(idx as u64), Some(idx as u64)))
                .collect::<Vec<_>>();
            memtable_tests::write_kvs(&*memtable, i as u64, OpType::Put, &keys, &values);
            rows_written += keys.len();
        }

        let dir = create_temp_dir("write_large_parquet");
        let path = dir.path().to_str().unwrap();

        let object_store = create_object_store(path);
        let sst_file_name = "test-large.parquet";
        let iter = memtable.iter(IterContext::default()).unwrap();
        let writer = ParquetWriter::new(sst_file_name, Source::Iter(iter), object_store.clone());

        let sst_info = writer
            .write_sst(&sst::WriteOptions {
                sst_write_buffer_size: ReadableSize::kb(4),
            })
            .await
            .unwrap()
            .unwrap();
        let file_meta = object_store.stat(sst_file_name).await.unwrap();
        assert!(file_meta.is_file());
        assert_eq!(sst_info.file_size, file_meta.content_length());
        assert_eq!(rows_written, sst_info.num_rows);
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
            keys_vec.push(i as i64);
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
        let iter = memtable.iter(IterContext::default()).unwrap();
        let writer = ParquetWriter::new(&sst_file_name, Source::Iter(iter), object_store.clone());

        let SstInfo {
            time_range,
            file_size,
            ..
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
        let user_schema = projected_schema.projected_user_schema().clone();
        let reader = ParquetReader::new(
            sst_file_handle,
            operator,
            projected_schema,
            Predicate::empty(user_schema),
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
                region_id: 0.into(),
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
            &[1000, 1002, 2002, 2003, 2003, 1001], // keys
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
        let iter = memtable.iter(IterContext::default()).unwrap();
        let writer = ParquetWriter::new(&sst_file_name, Source::Iter(iter), object_store.clone());

        let SstInfo {
            time_range,
            file_size,
            ..
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
        let user_schema = projected_schema.projected_user_schema().clone();
        let reader = ParquetReader::new(
            file_handle,
            operator,
            projected_schema,
            Predicate::empty(user_schema),
            TimestampRange::min_to_max(),
        );

        let mut stream = reader.chunk_stream().await.unwrap();
        assert_eq!(
            5,
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
        let store_schema = schema.schema_to_read().clone();
        let reader = ParquetReader::new(
            file_handle,
            object_store,
            schema,
            Predicate::empty(store_schema.schema().clone()),
            range,
        );
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
            &[1000, 1002, 2002, 2003, 2003, 1001, 3001], // keys
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
        let iter = memtable.iter(IterContext::default()).unwrap();
        let writer = ParquetWriter::new(&sst_file_name, Source::Iter(iter), object_store.clone());

        let SstInfo {
            time_range,
            file_size,
            ..
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

        let projected_schema = Arc::new(ProjectedSchema::new(schema, Some(vec![1, 0, 2])).unwrap());

        check_range_read(
            sst_file_handle.clone(),
            object_store.clone(),
            projected_schema.clone(),
            TimestampRange::with_unit(1000, 2003, TimeUnit::Millisecond).unwrap(),
            vec![1000, 1001, 1002, 2002],
        )
        .await;

        check_range_read(
            sst_file_handle.clone(),
            object_store.clone(),
            projected_schema.clone(),
            TimestampRange::with_unit(2002, 3001, TimeUnit::Millisecond).unwrap(),
            vec![2002, 2003],
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
            vec![1000, 1001, 1002, 2002, 2003],
        )
        .await;

        // read full range
        check_range_read(
            sst_file_handle,
            object_store,
            projected_schema,
            TimestampRange::min_to_max(),
            vec![1000, 1001, 1002, 2002, 2003, 3001],
        )
        .await;
    }

    #[tokio::test]
    async fn test_write_empty_file() {
        common_telemetry::init_default_ut_logging();
        let schema = memtable_tests::schema_for_test();
        let memtable = DefaultMemtableBuilder::default().build(schema.clone());

        let dir = create_temp_dir("write-empty-file");
        let path = dir.path().to_str().unwrap();
        let mut builder = Fs::default();
        let _ = builder.root(path);
        let object_store = ObjectStore::new(builder).unwrap().finish();
        let sst_file_name = "test-empty.parquet";
        let iter = memtable.iter(IterContext::default()).unwrap();
        let writer = ParquetWriter::new(sst_file_name, Source::Iter(iter), object_store.clone());

        let sst_info_opt = writer
            .write_sst(&sst::WriteOptions::default())
            .await
            .unwrap();
        assert!(sst_info_opt.is_none());
        // The file should not exist when no row has been written.
        assert!(!object_store.is_exist(sst_file_name).await.unwrap());
    }
}
