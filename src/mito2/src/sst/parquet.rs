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
pub(crate) mod helper;
pub(crate) mod metadata;
mod page_reader;
pub mod reader;
pub mod row_group;
mod row_selection;
mod stats;
pub mod writer;

use std::sync::Arc;

use common_base::readable_size::ReadableSize;
use parquet::file::metadata::ParquetMetaData;

use crate::sst::file::FileTimeRange;
use crate::sst::DEFAULT_WRITE_BUFFER_SIZE;

/// Key of metadata in parquet SST.
pub const PARQUET_METADATA_KEY: &str = "greptime:metadata";

/// Default batch size to read parquet files.
pub(crate) const DEFAULT_READ_BATCH_SIZE: usize = 1024;
/// Default row group size for parquet files.
const DEFAULT_ROW_GROUP_SIZE: usize = 100 * DEFAULT_READ_BATCH_SIZE;
/// Default segment row count for inverted index.
const DEFAULT_INDEX_SEGMENT_ROW_COUNT: usize = 1024;

/// Parquet write options.
#[derive(Debug)]
pub struct WriteOptions {
    /// Buffer size for async writer.
    pub write_buffer_size: ReadableSize,
    /// Row group size.
    pub row_group_size: usize,
    /// Segment row count for inverted index.
    pub index_segment_row_count: usize,
}

impl Default for WriteOptions {
    fn default() -> Self {
        WriteOptions {
            write_buffer_size: DEFAULT_WRITE_BUFFER_SIZE,
            row_group_size: DEFAULT_ROW_GROUP_SIZE,
            index_segment_row_count: DEFAULT_INDEX_SEGMENT_ROW_COUNT,
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
    pub file_metadata: Option<Arc<ParquetMetaData>>,
    /// Whether inverted index is available.
    pub inverted_index_available: bool,
    /// Index file size in bytes.
    pub index_file_size: u64,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_time::Timestamp;
    use datafusion_common::{Column, ScalarValue};
    use datafusion_expr::{BinaryExpr, Expr, Operator};
    use table::predicate::Predicate;

    use super::*;
    use crate::cache::{CacheManager, PageKey};
    use crate::sst::index::Indexer;
    use crate::sst::parquet::reader::ParquetReaderBuilder;
    use crate::sst::parquet::writer::ParquetWriter;
    use crate::test_util::sst_util::{
        assert_parquet_metadata_eq, new_batch_by_range, new_source, sst_file_handle,
        sst_region_metadata,
    };
    use crate::test_util::{check_reader_result, TestEnv};

    const FILE_DIR: &str = "/";

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

        let mut writer = ParquetWriter::new(
            file_path,
            metadata,
            object_store.clone(),
            Indexer::default(),
        );
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
        let mut writer = ParquetWriter::new(
            file_path,
            metadata.clone(),
            object_store.clone(),
            Indexer::default(),
        );
        writer
            .write_all(source, &write_opts)
            .await
            .unwrap()
            .unwrap();

        // Enable page cache.
        let cache = Some(Arc::new(
            CacheManager::builder()
                .page_cache_size(64 * 1024 * 1024)
                .build(),
        ));
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
        let mut writer = ParquetWriter::new(
            file_path,
            metadata.clone(),
            object_store.clone(),
            Indexer::default(),
        );
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

        assert_parquet_metadata_eq(writer_metadata, reader_metadata)
    }

    #[tokio::test]
    async fn test_read_with_tag_filter() {
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
        let mut writer = ParquetWriter::new(
            file_path,
            metadata.clone(),
            object_store.clone(),
            Indexer::default(),
        );
        writer
            .write_all(source, &write_opts)
            .await
            .unwrap()
            .unwrap();

        // Predicate
        let predicate = Some(Predicate::new(vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column {
                relation: None,
                name: "tag_0".to_string(),
            })),
            op: Operator::Eq,
            right: Box::new(Expr::Literal(ScalarValue::Utf8(Some("a".to_string())))),
        })
        .into()]));

        let builder = ParquetReaderBuilder::new(FILE_DIR.to_string(), handle.clone(), object_store)
            .predicate(predicate);
        let mut reader = builder.build().await.unwrap();
        check_reader_result(
            &mut reader,
            &[
                new_batch_by_range(&["a", "d"], 0, 50),
                new_batch_by_range(&["a", "d"], 50, 60),
            ],
        )
        .await;
    }

    #[tokio::test]
    async fn test_read_empty_batch() {
        let mut env = TestEnv::new();
        let object_store = env.init_object_store_manager();
        let handle = sst_file_handle(0, 1000);
        let file_path = handle.file_path(FILE_DIR);
        let metadata = Arc::new(sst_region_metadata());
        let source = new_source(&[
            new_batch_by_range(&["a", "z"], 0, 0),
            new_batch_by_range(&["a", "z"], 100, 100),
            new_batch_by_range(&["a", "z"], 200, 230),
        ]);
        // Use a small row group size for test.
        let write_opts = WriteOptions {
            row_group_size: 50,
            ..Default::default()
        };
        // Prepare data.
        let mut writer = ParquetWriter::new(
            file_path,
            metadata.clone(),
            object_store.clone(),
            Indexer::default(),
        );
        writer
            .write_all(source, &write_opts)
            .await
            .unwrap()
            .unwrap();

        let builder = ParquetReaderBuilder::new(FILE_DIR.to_string(), handle.clone(), object_store);
        let mut reader = builder.build().await.unwrap();
        check_reader_result(&mut reader, &[new_batch_by_range(&["a", "z"], 200, 230)]).await;
    }

    #[tokio::test]
    async fn test_read_with_field_filter() {
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
        let mut writer = ParquetWriter::new(
            file_path,
            metadata.clone(),
            object_store.clone(),
            Indexer::default(),
        );
        writer
            .write_all(source, &write_opts)
            .await
            .unwrap()
            .unwrap();

        // Predicate
        let predicate = Some(Predicate::new(vec![Expr::BinaryExpr(BinaryExpr {
            left: Box::new(Expr::Column(Column {
                relation: None,
                name: "field_0".to_string(),
            })),
            op: Operator::GtEq,
            right: Box::new(Expr::Literal(ScalarValue::UInt64(Some(150)))),
        })
        .into()]));

        let builder = ParquetReaderBuilder::new(FILE_DIR.to_string(), handle.clone(), object_store)
            .predicate(predicate);
        let mut reader = builder.build().await.unwrap();
        check_reader_result(&mut reader, &[new_batch_by_range(&["b", "h"], 150, 200)]).await;
    }
}
