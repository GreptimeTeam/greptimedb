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

//! SST in parquet format.

mod format;
mod helper;
mod page_reader;
pub mod reader;
pub mod row_group;
mod stats;
pub mod writer;

use common_base::readable_size::ReadableSize;
use parquet::file::metadata::ParquetMetaData;

use crate::sst::file::FileTimeRange;

/// Key of metadata in parquet SST.
pub const PARQUET_METADATA_KEY: &str = "greptime:metadata";
const DEFAULT_WRITE_BUFFER_SIZE: ReadableSize = ReadableSize::mb(8);
/// Default batch size to read parquet files.
pub(crate) const DEFAULT_READ_BATCH_SIZE: usize = 1024;
/// Default row group size for parquet files.
const DEFAULT_ROW_GROUP_SIZE: usize = 100 * DEFAULT_READ_BATCH_SIZE;

/// Parquet write options.
#[derive(Debug)]
pub struct WriteOptions {
    /// Buffer size for async writer.
    pub write_buffer_size: ReadableSize,
    /// Row group size.
    pub row_group_size: usize,
}

impl Default for WriteOptions {
    fn default() -> Self {
        WriteOptions {
            write_buffer_size: DEFAULT_WRITE_BUFFER_SIZE,
            row_group_size: DEFAULT_ROW_GROUP_SIZE,
        }
    }
}

/// Parquet SST info returned by the writer.
pub struct SstInfo {
    /// Time range of the SST.
    pub time_range: FileTimeRange,
    /// File size in bytes.
    pub file_size: u64,
    /// Number of rows.
    pub num_rows: usize,
    /// File Meta Data
    pub file_metadata: Option<ParquetMetaData>,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::OpType;
    use common_time::Timestamp;

    use super::*;
    use crate::cache::{CacheManager, PageKey};
    use crate::read::Batch;
    use crate::sst::parquet::reader::ParquetReaderBuilder;
    use crate::sst::parquet::writer::ParquetWriter;
    use crate::test_util::sst_util::{
        new_primary_key, new_source, sst_file_handle, sst_region_metadata,
    };
    use crate::test_util::{check_reader_result, new_batch_builder, TestEnv};

    const FILE_DIR: &str = "/";

    fn new_batch_by_range(tags: &[&str], start: usize, end: usize) -> Batch {
        assert!(end > start);
        let pk = new_primary_key(tags);
        let timestamps: Vec<_> = (start..end).map(|v| v as i64).collect();
        let sequences = vec![1000; end - start];
        let op_types = vec![OpType::Put; end - start];
        let field: Vec<_> = (start..end).map(|v| v as u64).collect();
        new_batch_builder(&pk, &timestamps, &sequences, &op_types, 2, &field)
            .build()
            .unwrap()
    }

    #[tokio::test]
    async fn test_write_read() {
        let mut env = TestEnv::new();
        let object_store = env.init_object_store_manager();
        let handle = sst_file_handle(0, 1000);
        let file_path = handle.file_path(FILE_DIR);
        let metadata = Arc::new(sst_region_metadata());
        let source = new_source(&[
            new_batch_by_range(&["a", "d"], 0, 60),
            new_batch_by_range(&["b", "f"], 0, 40),
            new_batch_by_range(&["b", "h"], 100, 200),
        ]);
        // Use a small row group size for test.
        let write_opts = WriteOptions {
            row_group_size: 50,
            ..Default::default()
        };

        let mut writer = ParquetWriter::new(file_path, metadata, object_store.clone());
        let info = writer
            .write_all(source, &write_opts)
            .await
            .unwrap()
            .unwrap();
        assert_eq!(200, info.num_rows);
        assert!(info.file_size > 0);
        assert_eq!(
            (
                Timestamp::new_millisecond(0),
                Timestamp::new_millisecond(199)
            ),
            info.time_range
        );

        let builder = ParquetReaderBuilder::new(FILE_DIR.to_string(), handle.clone(), object_store);
        let mut reader = builder.build().await.unwrap();
        check_reader_result(
            &mut reader,
            &[
                new_batch_by_range(&["a", "d"], 0, 50),
                new_batch_by_range(&["a", "d"], 50, 60),
                new_batch_by_range(&["b", "f"], 0, 40),
                new_batch_by_range(&["b", "h"], 100, 150),
                new_batch_by_range(&["b", "h"], 150, 200),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_read_with_cache() {
        let mut env = TestEnv::new();
        let object_store = env.init_object_store_manager();
        let handle = sst_file_handle(0, 1000);
        let file_path = handle.file_path(FILE_DIR);
        let metadata = Arc::new(sst_region_metadata());
        let source = new_source(&[
            new_batch_by_range(&["a", "d"], 0, 60),
            new_batch_by_range(&["b", "f"], 0, 40),
            new_batch_by_range(&["b", "h"], 100, 200),
        ]);
        // Use a small row group size for test.
        let write_opts = WriteOptions {
            row_group_size: 50,
            ..Default::default()
        };
        // Prepare data.
        let mut writer = ParquetWriter::new(file_path, metadata.clone(), object_store.clone());
        writer
            .write_all(source, &write_opts)
            .await
            .unwrap()
            .unwrap();

        let cache = Some(Arc::new(CacheManager::new(0, 0, 64 * 1024 * 1024)));
        let builder = ParquetReaderBuilder::new(FILE_DIR.to_string(), handle.clone(), object_store)
            .cache(cache.clone());
        for _ in 0..3 {
            let mut reader = builder.build().await.unwrap();
            check_reader_result(
                &mut reader,
                &[
                    new_batch_by_range(&["a", "d"], 0, 50),
                    new_batch_by_range(&["a", "d"], 50, 60),
                    new_batch_by_range(&["b", "f"], 0, 40),
                    new_batch_by_range(&["b", "h"], 100, 150),
                    new_batch_by_range(&["b", "h"], 150, 200),
                ],
            )
            .await;
        }

        // Cache 4 row groups.
        for i in 0..4 {
            let page_key = PageKey {
                region_id: metadata.region_id,
                file_id: handle.file_id(),
                row_group_idx: i,
                column_idx: 0,
            };
            assert!(cache.as_ref().unwrap().get_pages(&page_key).is_some());
        }
        let page_key = PageKey {
            region_id: metadata.region_id,
            file_id: handle.file_id(),
            row_group_idx: 5,
            column_idx: 0,
        };
        assert!(cache.as_ref().unwrap().get_pages(&page_key).is_none());
    }

    #[tokio::test]
    async fn test_parquet_metadata_eq() {
        // create test env
        let mut env = crate::test_util::TestEnv::new();
        let object_store = env.init_object_store_manager();
        let handle = sst_file_handle(0, 1000);
        let file_path = handle.file_path(FILE_DIR);
        let metadata = Arc::new(sst_region_metadata());
        let source = new_source(&[
            new_batch_by_range(&["a", "d"], 0, 60),
            new_batch_by_range(&["b", "f"], 0, 40),
            new_batch_by_range(&["b", "h"], 100, 200),
        ]);
        let write_opts = WriteOptions {
            row_group_size: 50,
            ..Default::default()
        };

        // write the sst file and get sst info
        // sst info contains the parquet metadata, which is converted from FileMetaData
        let mut writer = ParquetWriter::new(file_path, metadata.clone(), object_store.clone());
        let sst_info = writer
            .write_all(source, &write_opts)
            .await
            .unwrap()
            .expect("write_all should return sst info");
        let writer_metadata = sst_info.file_metadata.unwrap();

        // read the sst file metadata
        let builder = ParquetReaderBuilder::new(FILE_DIR.to_string(), handle.clone(), object_store);
        let reader = builder.build().await.unwrap();
        let reader_metadata = reader.parquet_metadata();

        // Because ParquetMetaData doesn't implement PartialEq,
        // check all fields manually
        macro_rules! assert_metadata {
            ( $writer:expr, $reader:expr, $($method:ident,)+ ) => {
                $(
                    assert_eq!($writer.$method(), $reader.$method());
                )+
            }
        }

        assert_metadata!(
            writer_metadata.file_metadata(),
            reader_metadata.file_metadata(),
            version,
            num_rows,
            created_by,
            key_value_metadata,
            schema_descr,
            column_orders,
        );

        assert_metadata!(
            writer_metadata,
            reader_metadata,
            row_groups,
            column_index,
            offset_index,
        );
    }
}
