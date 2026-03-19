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

//! Utilities for testing cache.

use std::sync::Arc;

use bytes::Bytes;
use datatypes::arrow::array::{ArrayRef, Int64Array};
use datatypes::arrow::record_batch::RecordBatch;
use object_store::ObjectStore;
use object_store::services::Fs;
use parquet::arrow::ArrowWriter;
use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
use parquet::file::metadata::{KeyValue, ParquetMetaData};
use parquet::file::properties::WriterProperties;
use parquet::file::statistics::Statistics;
use store_api::metadata::RegionMetadataRef;

use crate::sst::parquet::PARQUET_METADATA_KEY;
use crate::test_util::sst_util::sst_region_metadata;

/// Returns a parquet meta data.
pub(crate) fn parquet_meta() -> Arc<ParquetMetaData> {
    let file_data = parquet_file_data();
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(file_data)).unwrap();
    builder.metadata().clone()
}

/// Returns parquet metadata for an SST parquet file and its decoded region metadata.
pub(crate) fn sst_parquet_meta() -> (Arc<ParquetMetaData>, RegionMetadataRef) {
    let region_metadata = Arc::new(sst_region_metadata());
    let file_data = parquet_file_data_with_region_metadata(&region_metadata);
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(file_data)).unwrap();
    (builder.metadata().clone(), region_metadata)
}

/// Returns parquet metadata for an SST parquet file with custom region metadata.
pub(crate) fn sst_parquet_meta_with_region_metadata(
    region_metadata: RegionMetadataRef,
) -> Arc<ParquetMetaData> {
    let file_data = parquet_file_data_with_region_metadata(&region_metadata);
    let builder = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(file_data)).unwrap();
    builder.metadata().clone()
}

/// Write a test parquet file to a buffer
fn parquet_file_data() -> Vec<u8> {
    parquet_file_data_inner(None)
}

fn parquet_file_data_with_region_metadata(region_metadata: &RegionMetadataRef) -> Vec<u8> {
    let json = region_metadata.to_json().unwrap();
    let key_value = KeyValue::new(PARQUET_METADATA_KEY.to_string(), json);
    parquet_file_data_inner(Some(vec![key_value]))
}

fn parquet_file_data_inner(key_value_metadata: Option<Vec<KeyValue>>) -> Vec<u8> {
    let col = Arc::new(Int64Array::from_iter_values([1, 2, 3])) as ArrayRef;
    let to_write = RecordBatch::try_from_iter([("col", col)]).unwrap();

    let mut buffer = Vec::new();
    let props = WriterProperties::builder()
        .set_key_value_metadata(key_value_metadata)
        .build();
    let mut writer = ArrowWriter::try_new(&mut buffer, to_write.schema(), Some(props)).unwrap();
    writer.write(&to_write).unwrap();
    writer.close().unwrap();

    buffer
}

pub(crate) fn new_fs_store(path: &str) -> ObjectStore {
    let builder = Fs::default();
    ObjectStore::new(builder.root(path)).unwrap().finish()
}

pub(crate) fn assert_parquet_metadata_equal(x: Arc<ParquetMetaData>, y: Arc<ParquetMetaData>) {
    // Normalize the statistics in parquet metadata because the flag "min_max_backwards_compatible"
    // is not persisted across parquet metadata writer and reader.
    fn normalize_statistics(metadata: ParquetMetaData) -> ParquetMetaData {
        let unset_min_max_backwards_compatible_flag = |stats: Statistics| -> Statistics {
            match stats {
                Statistics::Boolean(stats) => {
                    Statistics::Boolean(stats.with_backwards_compatible_min_max(false))
                }
                Statistics::Int32(stats) => {
                    Statistics::Int32(stats.with_backwards_compatible_min_max(false))
                }
                Statistics::Int64(stats) => {
                    Statistics::Int64(stats.with_backwards_compatible_min_max(false))
                }
                Statistics::Int96(stats) => {
                    Statistics::Int96(stats.with_backwards_compatible_min_max(false))
                }
                Statistics::Float(stats) => {
                    Statistics::Float(stats.with_backwards_compatible_min_max(false))
                }
                Statistics::Double(stats) => {
                    Statistics::Double(stats.with_backwards_compatible_min_max(false))
                }
                Statistics::ByteArray(stats) => {
                    Statistics::ByteArray(stats.with_backwards_compatible_min_max(false))
                }
                Statistics::FixedLenByteArray(stats) => {
                    Statistics::FixedLenByteArray(stats.with_backwards_compatible_min_max(false))
                }
            }
        };

        let mut metadata_builder = metadata.into_builder();
        for rg in metadata_builder.take_row_groups() {
            let mut rg_builder = rg.into_builder();
            for col in rg_builder.take_columns() {
                let stats = col
                    .statistics()
                    .cloned()
                    .map(unset_min_max_backwards_compatible_flag);
                let mut col_builder = col.into_builder().clear_statistics();
                if let Some(stats) = stats {
                    col_builder = col_builder.set_statistics(stats);
                }
                rg_builder = rg_builder.add_column_metadata(col_builder.build().unwrap());
            }
            metadata_builder = metadata_builder.add_row_group(rg_builder.build().unwrap());
        }
        metadata_builder.build()
    }

    let x = normalize_statistics(Arc::unwrap_or_clone(x));
    let y = normalize_statistics(Arc::unwrap_or_clone(y));
    assert_eq!(x, y);
}
