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
use common_time::range::TimestampRange;
use datafusion::parquet::arrow::AsyncArrowWriter;
use datatypes::timestamp::TimestampSecond;
use mito2::sst::file::{FileId, FileMeta};
use mito2::sst::parquet::{DEFAULT_ROW_GROUP_SIZE, PARQUET_METADATA_KEY};
use object_store::config::ObjectStoreConfig;
use object_store::util::join_dir;
use object_store::ObjectStore;
use parquet::basic::{Compression, Encoding, ZstdLevel};
use parquet::file::metadata::KeyValue;
use parquet::file::properties::WriterProperties;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::metric_engine_consts::DATA_REGION_SUBDIR;
use store_api::storage::RegionId;
use table::metadata::{TableId, TableInfoRef};

use crate::error;

type AsyncParquetWriter = AsyncArrowWriter<AsyncWriter>;

pub struct AccessLayerFactory {
    object_store: ObjectStore,
}

impl AccessLayerFactory {
    async fn new(config: &ObjectStoreConfig) -> error::Result<AccessLayerFactory> {
        let object_store = object_store::factory::new_raw_object_store(config, "")
            .await
            .context(error::ObjectStoreSnafu)?;
        Ok(Self { object_store })
    }

    async fn create_sst_writer(
        &self,
        catalog: &str,
        schema: &str,
        region_metadata: RegionMetadataRef,
    ) -> error::Result<ParquetWriter> {
        let region_dir = build_data_region_dir(catalog, schema, region_metadata.region_id);
        let file_id = FileId::random();
        let file_path = join_dir(&region_dir, &file_id.as_parquet());
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
    use super::*;

    #[test]
    fn test_build_data_region_dir_basic() {
        let result = build_data_region_dir("greptime", "public", RegionId::new(1024, 0));
        assert_eq!(&result, "data/greptime/public/1024/1024_0000000000/data/");
    }
}
