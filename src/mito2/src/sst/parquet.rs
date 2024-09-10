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

use std::sync::Arc;

use common_base::readable_size::ReadableSize;
use parquet::file::metadata::ParquetMetaData;

use crate::sst::file::FileTimeRange;
use crate::sst::index::IndexOutput;
use crate::sst::DEFAULT_WRITE_BUFFER_SIZE;

pub(crate) mod file_range;
pub(crate) mod format;
pub(crate) mod helper;
pub(crate) mod metadata;
mod page_reader;
pub mod reader;
pub mod row_group;
mod row_selection;
mod stats;
pub mod writer;

/// Key of metadata in parquet SST.
pub const PARQUET_METADATA_KEY: &str = "greptime:metadata";

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
    /// Number of row groups
    pub num_row_groups: u64,
    /// File Meta Data
    pub file_metadata: Option<Arc<ParquetMetaData>>,
    /// Index Meta Data
    pub index_metadata: IndexOutput,
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use common_time::Timestamp;
    use datafusion_common::{Column, ScalarValue};
    use datafusion_expr::{BinaryExpr, Expr, Operator};
    use datatypes::arrow;
    use datatypes::arrow::array::RecordBatch;
    use datatypes::arrow::datatypes::{DataType, Field, Schema};
    use parquet::arrow::AsyncArrowWriter;
    use parquet::basic::{Compression, Encoding, ZstdLevel};
    use parquet::file::metadata::KeyValue;
    use parquet::file::properties::WriterProperties;
    use table::predicate::Predicate;
    use tokio_util::compat::FuturesAsyncWriteCompatExt;

    use super::*;
    use crate::cache::{CacheManager, PageKey};
    use crate::sst::index::Indexer;
    use crate::sst::parquet::format::WriteFormat;
    use crate::sst::parquet::reader::ParquetReaderBuilder;
    use crate::sst::parquet::writer::ParquetWriter;
    use crate::sst::DEFAULT_WRITE_CONCURRENCY;
    use crate::test_util::sst_util::{
        assert_parquet_metadata_eq, build_test_binary_test_region_metadata, new_batch_by_range,
        new_batch_with_binary, new_source, sst_file_handle, sst_region_metadata,
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
        let mut writer = ParquetWriter::new_with_object_store(
            object_store.clone(),
            file_path,
            metadata,
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
        let mut writer = ParquetWriter::new_with_object_store(
            object_store.clone(),
            file_path,
            metadata.clone(),
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

        // Doesn't have compressed page cached.
        let page_key = PageKey::new_compressed(metadata.region_id, handle.file_id(), 0, 0);
        assert!(cache.as_ref().unwrap().get_pages(&page_key).is_none());

        // Cache 4 row groups.
        for i in 0..4 {
            let page_key = PageKey::new_uncompressed(metadata.region_id, handle.file_id(), i, 0);
            assert!(cache.as_ref().unwrap().get_pages(&page_key).is_some());
        }
        let page_key = PageKey::new_uncompressed(metadata.region_id, handle.file_id(), 5, 0);
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
        let mut writer = ParquetWriter::new_with_object_store(
            object_store.clone(),
            file_path,
            metadata.clone(),
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
        let mut writer = ParquetWriter::new_with_object_store(
            object_store.clone(),
            file_path,
            metadata.clone(),
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
        })]));

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
        let mut writer = ParquetWriter::new_with_object_store(
            object_store.clone(),
            file_path,
            metadata.clone(),
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
        let mut writer = ParquetWriter::new_with_object_store(
            object_store.clone(),
            file_path,
            metadata.clone(),
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
        })]));

        let builder = ParquetReaderBuilder::new(FILE_DIR.to_string(), handle.clone(), object_store)
            .predicate(predicate);
        let mut reader = builder.build().await.unwrap();
        check_reader_result(&mut reader, &[new_batch_by_range(&["b", "h"], 150, 200)]).await;
    }

    #[tokio::test]
    async fn test_read_large_binary() {
        let mut env = TestEnv::new();
        let object_store = env.init_object_store_manager();
        let handle = sst_file_handle(0, 1000);
        let file_path = handle.file_path(FILE_DIR);

        let write_opts = WriteOptions {
            row_group_size: 50,
            ..Default::default()
        };

        let metadata = build_test_binary_test_region_metadata();
        let json = metadata.to_json().unwrap();
        let key_value_meta = KeyValue::new(PARQUET_METADATA_KEY.to_string(), json);

        let props_builder = WriterProperties::builder()
            .set_key_value_metadata(Some(vec![key_value_meta]))
            .set_compression(Compression::ZSTD(ZstdLevel::default()))
            .set_encoding(Encoding::PLAIN)
            .set_max_row_group_size(write_opts.row_group_size);

        let writer_props = props_builder.build();

        let write_format = WriteFormat::new(metadata);
        let fields: Vec<_> = write_format
            .arrow_schema()
            .fields()
            .into_iter()
            .map(|field| {
                let data_type = field.data_type().clone();
                if data_type == DataType::Binary {
                    Field::new(field.name(), DataType::LargeBinary, field.is_nullable())
                } else {
                    Field::new(field.name(), data_type, field.is_nullable())
                }
            })
            .collect();

        let arrow_schema = Arc::new(Schema::new(fields));

        // Ensures field_0 has LargeBinary type.
        assert_eq!(
            &DataType::LargeBinary,
            arrow_schema.field_with_name("field_0").unwrap().data_type()
        );
        let mut writer = AsyncArrowWriter::try_new(
            object_store
                .writer_with(&file_path)
                .concurrent(DEFAULT_WRITE_CONCURRENCY)
                .await
                .map(|w| w.into_futures_async_write().compat_write())
                .unwrap(),
            arrow_schema.clone(),
            Some(writer_props),
        )
        .unwrap();

        let batch = new_batch_with_binary(&["a"], 0, 60);
        let arrow_batch = write_format.convert_batch(&batch).unwrap();
        let arrays: Vec<_> = arrow_batch
            .columns()
            .iter()
            .map(|array| {
                let data_type = array.data_type().clone();
                if data_type == DataType::Binary {
                    arrow::compute::cast(array, &DataType::LargeBinary).unwrap()
                } else {
                    array.clone()
                }
            })
            .collect();
        let result = RecordBatch::try_new(arrow_schema, arrays).unwrap();

        writer.write(&result).await.unwrap();
        writer.close().await.unwrap();

        let builder = ParquetReaderBuilder::new(FILE_DIR.to_string(), handle.clone(), object_store);
        let mut reader = builder.build().await.unwrap();
        check_reader_result(
            &mut reader,
            &[
                new_batch_with_binary(&["a"], 0, 50),
                new_batch_with_binary(&["a"], 50, 60),
            ],
        )
        .await;
    }
}
