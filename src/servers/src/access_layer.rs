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

use arrow::array::{
    Array, PrimitiveArray, RecordBatch, TimestampMicrosecondArray, TimestampMillisecondArray,
    TimestampNanosecondArray, TimestampSecondArray,
};
use arrow::datatypes::Int64Type;
use arrow_schema::TimeUnit;
use common_datasource::parquet_writer::AsyncWriter;
use datafusion::parquet::arrow::AsyncArrowWriter;
use mito2::sst::file::{FileId, FileMeta};
use mito2::sst::parquet::{DEFAULT_ROW_GROUP_SIZE, PARQUET_METADATA_KEY};
use object_store::config::ObjectStoreConfig;
use object_store::util::{join_dir, join_path};
use object_store::ObjectStore;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;
use store_api::metric_engine_consts::DATA_REGION_SUBDIR;
use store_api::storage::RegionId;

use crate::error;

type AsyncParquetWriter = AsyncArrowWriter<AsyncWriter>;

#[derive(Clone)]
pub struct AccessLayerFactory {
    object_store: ObjectStore,
}

impl AccessLayerFactory {
    pub async fn new(config: &ObjectStoreConfig) -> error::Result<AccessLayerFactory> {
        let object_store = object_store::factory::new_raw_object_store(config, "")
            .await
            .context(error::ObjectStoreSnafu)?;
        Ok(Self { object_store })
    }

    pub(crate) async fn create_sst_writer(
        &self,
        catalog: &str,
        schema: &str,
        region_metadata: RegionMetadataRef,
    ) -> error::Result<ParquetWriter> {
        let region_dir = build_data_region_dir(catalog, schema, region_metadata.region_id);
        let file_id = FileId::random();
        let file_path = join_path(&region_dir, &file_id.as_parquet());
        let writer = self
            .object_store
            .writer(&file_path)
            .await
            .context(error::OpendalSnafu)?;
        let schema = region_metadata.schema.arrow_schema().clone();

        let key_value_meta = KeyValue::new(
            PARQUET_METADATA_KEY.to_string(),
            region_metadata.to_json().unwrap(),
        );

        let props = WriterProperties::builder()
            .set_key_value_metadata(Some(vec![key_value_meta]))
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_encoding(Encoding::PLAIN)
            .set_max_row_group_size(DEFAULT_ROW_GROUP_SIZE)
            .build();

        let writer = AsyncParquetWriter::try_new(AsyncWriter::new(writer), schema, Some(props))
            .context(error::ParquetSnafu)?;
        Ok(ParquetWriter {
            region_id: region_metadata.region_id,
            file_id,
            region_metadata,
            writer,
            timestamp_range: None,
        })
    }
}

pub struct ParquetWriter {
    region_id: RegionId,
    file_id: FileId,
    region_metadata: RegionMetadataRef,
    writer: AsyncParquetWriter,
    timestamp_range: Option<(i64, i64)>,
}

impl ParquetWriter {
    pub async fn write_record_batch(
        &mut self,
        batch: &RecordBatch,
        timestamp_range: Option<(i64, i64)>,
    ) -> error::Result<()> {
        self.writer
            .write(&batch)
            .await
            .context(error::ParquetSnafu)?;

        let (batch_min, batch_max) =
            get_or_calculate_timestamp_range(timestamp_range, batch, &self.region_metadata)?;

        if let Some((min, max)) = &mut self.timestamp_range {
            *min = (*min).min(batch_min);
            *max = (*max).max(batch_max);
        } else {
            self.timestamp_range = Some((batch_min, batch_max));
        };
        Ok(())
    }

    pub async fn finish(&mut self) -> error::Result<FileMeta> {
        let (min, max) = self.timestamp_range.unwrap();
        let timestamp_type = self
            .region_metadata
            .time_index_column()
            .column_schema
            .data_type
            .as_timestamp()
            .unwrap();
        let min_ts = timestamp_type.create_timestamp(min);
        let max_ts = timestamp_type.create_timestamp(max);
        let file_meta = self.writer.finish().await.context(error::ParquetSnafu)?;
        let meta = FileMeta {
            region_id: self.region_id,
            file_id: self.file_id,
            time_range: (min_ts, max_ts),
            level: 0,
            file_size: self.writer.bytes_written() as u64,
            available_indexes: Default::default(),
            index_file_size: 0,
            num_rows: file_meta.num_rows as u64,
            num_row_groups: file_meta.row_groups.len() as u64,
            sequence: None, //todo(hl): use flushed sequence here.
        };
        Ok(meta)
    }
}

/// Builds the data region subdir for metric physical tables.
fn build_data_region_dir(catalog: &str, schema: &str, physical_region_id: RegionId) -> String {
    let storage_path = common_meta::ddl::utils::region_storage_path(&catalog, &schema);
    join_dir(
        &store_api::path_utils::region_dir(&storage_path, physical_region_id),
        DATA_REGION_SUBDIR,
    )
}

fn get_or_calculate_timestamp_range(
    timestamp_range: Option<(i64, i64)>,
    rb: &RecordBatch,
    region_metadata: &RegionMetadataRef,
) -> error::Result<(i64, i64)> {
    if let Some(range) = timestamp_range {
        return Ok(range);
    };

    let ts = rb
        .column_by_name(&region_metadata.time_index_column().column_schema.name)
        .expect("column not found");
    let arrow::datatypes::DataType::Timestamp(unit, _) = ts.data_type() else {
        unreachable!("expected timestamp types");
    };
    let primitives: PrimitiveArray<Int64Type> = match unit {
        TimeUnit::Second => ts
            .as_any()
            .downcast_ref::<TimestampSecondArray>()
            .unwrap()
            .reinterpret_cast(),
        TimeUnit::Millisecond => ts
            .as_any()
            .downcast_ref::<TimestampMillisecondArray>()
            .unwrap()
            .reinterpret_cast(),
        TimeUnit::Microsecond => ts
            .as_any()
            .downcast_ref::<TimestampMicrosecondArray>()
            .unwrap()
            .reinterpret_cast(),
        TimeUnit::Nanosecond => ts
            .as_any()
            .downcast_ref::<TimestampNanosecondArray>()
            .unwrap()
            .reinterpret_cast(),
    };

    let min = arrow::compute::min(&primitives).unwrap();
    let max = arrow::compute::max(&primitives).unwrap();
    Ok((min, max))
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::SemanticType;
    use arrow::array::{Float64Array, StringArray};
    use arrow::datatypes::{DataType, Field, Schema, TimeUnit};
    use common_query::prelude::{GREPTIME_TIMESTAMP, GREPTIME_VALUE};
    use common_time::Timestamp;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use object_store::services::MemoryConfig;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};

    use super::*;

    #[test]
    fn test_build_data_region_dir_basic() {
        let result = build_data_region_dir("greptime", "public", RegionId::new(1024, 0));
        assert_eq!(&result, "data/greptime/public/1024/1024_0000000000/data/");
    }

    fn create_test_region_metadata() -> RegionMetadataRef {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1024, 0));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    GREPTIME_TIMESTAMP,
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    GREPTIME_VALUE,
                    ConcreteDataType::float64_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Field,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("tag", ConcreteDataType::string_datatype(), true),
                semantic_type: SemanticType::Tag,
                column_id: 3,
            })
            .primary_key(vec![3]);
        let metadata = builder.build().unwrap();
        Arc::new(metadata)
    }

    fn create_test_record_batch() -> RecordBatch {
        let schema = Arc::new(Schema::new(vec![
            Field::new(
                GREPTIME_TIMESTAMP,
                DataType::Timestamp(TimeUnit::Millisecond, None),
                false,
            ),
            Field::new(GREPTIME_VALUE, DataType::Float64, true),
            Field::new("tag", DataType::Utf8, true),
        ]));

        let timestamp_array = TimestampMillisecondArray::from(vec![1000, 2000, 3000]);
        let value_array = Float64Array::from(vec![Some(10.0), None, Some(30.0)]);
        let tag_array = StringArray::from(vec![Some("a"), Some("b"), Some("c")]);

        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(timestamp_array),
                Arc::new(value_array),
                Arc::new(tag_array),
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_parquet_writer_write_and_finish() {
        let object_store = ObjectStore::from_config(MemoryConfig::default())
            .unwrap()
            .finish();
        let factory = AccessLayerFactory { object_store };

        let region_metadata = create_test_region_metadata();
        let mut writer = factory
            .create_sst_writer("test_catalog", "test_schema", region_metadata.clone())
            .await
            .unwrap();

        let batch = create_test_record_batch();

        // Test writing a record batch
        writer.write_record_batch(&batch, None).await.unwrap();

        // Test finishing the writer
        let file_meta = writer.finish().await.unwrap();

        assert_eq!(file_meta.region_id, RegionId::new(1024, 0));
        assert_eq!(file_meta.level, 0);
        assert_eq!(file_meta.num_rows, 3);
        assert_eq!(file_meta.num_row_groups, 1);
        assert!(file_meta.file_size > 0);

        assert_eq!(file_meta.time_range.0, Timestamp::new_millisecond(1000));
        assert_eq!(file_meta.time_range.1, Timestamp::new_millisecond(3000));
    }

    #[tokio::test]
    async fn test_parquet_writer_multiple_batches() {
        let object_store = ObjectStore::from_config(MemoryConfig::default())
            .unwrap()
            .finish();
        let factory = AccessLayerFactory { object_store };

        let region_metadata = create_test_region_metadata();
        let mut writer = factory
            .create_sst_writer("test_catalog", "test_schema", region_metadata.clone())
            .await
            .unwrap();

        // Write first batch
        let batch1 = create_test_record_batch();
        writer.write_record_batch(&batch1, None).await.unwrap();

        // Create second batch with different timestamp range
        let schema = region_metadata.schema.arrow_schema().clone();
        let timestamp_array = TimestampMillisecondArray::from(vec![4000, 5000]);
        let value_array = Float64Array::from(vec![Some(40.0), Some(50.0)]);
        let tag_array = StringArray::from(vec![Some("d"), Some("e")]);

        let batch2 = RecordBatch::try_new(
            schema,
            vec![
                Arc::new(timestamp_array),
                Arc::new(value_array),
                Arc::new(tag_array),
            ],
        )
        .unwrap();

        writer.write_record_batch(&batch2, None).await.unwrap();

        let file_meta = writer.finish().await.unwrap();

        // Should have combined rows from both batches
        assert_eq!(file_meta.num_rows, 5);
        assert_eq!(file_meta.time_range.0, Timestamp::new_millisecond(1000));
        assert_eq!(file_meta.time_range.1, Timestamp::new_millisecond(5000));
    }

    #[tokio::test]
    async fn test_parquet_writer_with_provided_timestamp_range() {
        let object_store = ObjectStore::from_config(MemoryConfig::default())
            .unwrap()
            .finish();
        let factory = AccessLayerFactory { object_store };

        let region_metadata = create_test_region_metadata();
        let mut writer = factory
            .create_sst_writer("test_catalog", "test_schema", region_metadata.clone())
            .await
            .unwrap();

        let batch = create_test_record_batch();

        // Provide explicit timestamp range that differs from actual data
        let provided_range = (500, 6000);
        writer
            .write_record_batch(&batch, Some(provided_range))
            .await
            .unwrap();

        let file_meta = writer.finish().await.unwrap();

        assert_eq!(file_meta.time_range.0, Timestamp::new_millisecond(500));
        assert_eq!(file_meta.time_range.1, Timestamp::new_millisecond(6000));
    }

    #[test]
    fn test_get_or_calculate_timestamp_range_with_provided_range() {
        let region_metadata = create_test_region_metadata();
        let batch = create_test_record_batch();

        let provided_range = Some((100, 200));
        let result = get_or_calculate_timestamp_range(provided_range, &batch, &region_metadata);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), (100, 200));
    }

    #[test]
    fn test_get_or_calculate_timestamp_range_calculated() {
        let region_metadata = create_test_region_metadata();
        let batch = create_test_record_batch();

        let result = get_or_calculate_timestamp_range(None, &batch, &region_metadata);

        assert!(result.is_ok());
        assert_eq!(result.unwrap(), (1000, 3000));
    }
}
