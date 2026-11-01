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

use std::result::Result as StdResult;
use std::sync::atomic::{AtomicU64, AtomicUsize, Ordering};

use bytes::Bytes;
use futures::FutureExt;
use futures::future::BoxFuture;
use object_store::ObjectStore;
use parquet::arrow::async_reader::MetadataFetch;
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::metadata::{PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader};
use parquet::file::statistics::Statistics;
use snafu::{IntoError as _, ResultExt};
use store_api::metadata::RegionMetadata;
use store_api::storage::consts::PRIMARY_KEY_COLUMN_NAME;

use crate::error::{self, Result};
use crate::sst::parquet::reader::MetadataCacheMetrics;

/// The estimated size of the footer and metadata need to read from the end of parquet file.
const DEFAULT_PREFETCH_SIZE: u64 = 64 * 1024;

pub struct MetadataLoader<'a> {
    // An object store that supports async read
    object_store: ObjectStore,
    // The path of parquet file
    file_path: &'a str,
    // The size of parquet file
    file_size: u64,
    page_index_policy: PageIndexPolicy,
    prefetch_size: usize,
    max_bytes: Option<u64>,
}

impl<'a> MetadataLoader<'a> {
    /// Create a new parquet metadata loader.
    pub fn new(
        object_store: ObjectStore,
        file_path: &'a str,
        file_size: u64,
    ) -> MetadataLoader<'a> {
        Self {
            object_store,
            file_path,
            file_size,
            page_index_policy: Default::default(),
            prefetch_size: DEFAULT_PREFETCH_SIZE as usize,
            max_bytes: None,
        }
    }

    pub(crate) fn with_page_index_policy(&mut self, page_index_policy: PageIndexPolicy) {
        self.page_index_policy = page_index_policy;
    }

    /// Restricts an optional statistics read to the trailer and footer only.
    /// The fetcher reserves bytes before issuing each object-store request.
    pub(crate) fn with_read_budget(&mut self, max_bytes: u64) {
        self.prefetch_size = 8;
        self.max_bytes = Some(max_bytes);
        self.page_index_policy = PageIndexPolicy::Skip;
    }

    /// Get the size of parquet file. If file_size is 0, stat the object store to get the size.
    async fn get_file_size(&self) -> Result<u64> {
        let file_size = match self.file_size {
            0 => self
                .object_store
                .stat(self.file_path)
                .await
                .context(error::OpenDalSnafu)?
                .content_length(),
            other => other,
        };
        Ok(file_size)
    }

    pub async fn load(&self, cache_metrics: &mut MetadataCacheMetrics) -> Result<ParquetMetaData> {
        let path = self.file_path;
        let file_size = self.get_file_size().await?;
        let reader = ParquetMetaDataReader::new()
            .with_prefetch_hint(Some(self.prefetch_size))
            // Mito uses offset indexes to translate row selections into byte ranges. It does not
            // consume Parquet column indexes, so decoding them only bloats the metadata cache.
            .with_column_index_policy(PageIndexPolicy::Skip)
            .with_offset_index_policy(self.page_index_policy);

        let num_reads = AtomicUsize::new(0);
        let bytes_read = AtomicU64::new(0);
        let fetch = ObjectStoreFetch {
            object_store: &self.object_store,
            file_path: self.file_path,
            num_reads: &num_reads,
            bytes_read: &bytes_read,
            max_bytes: self.max_bytes,
        };

        let metadata =
            reader.load_and_finish(fetch, file_size).await.map_err(
                |e| match unbox_external_error(e) {
                    Ok(os_err) => error::OpenDalSnafu {}.into_error(os_err),
                    Err(parquet_err) => error::ReadParquetSnafu { path }.into_error(parquet_err),
                },
            );

        cache_metrics.num_reads = num_reads.into_inner();
        cache_metrics.bytes_read = bytes_read.into_inner();

        metadata
    }
}

/// Unpack ParquetError to get object_store::Error if possible.
fn unbox_external_error(e: ParquetError) -> StdResult<object_store::Error, ParquetError> {
    match e {
        ParquetError::External(boxed_err) => match boxed_err.downcast::<object_store::Error>() {
            Ok(os_err) => Ok(*os_err),
            Err(parquet_error) => Err(ParquetError::External(parquet_error)),
        },
        other => Err(other),
    }
}

pub(crate) fn extract_primary_key_range(
    parquet_meta: &ParquetMetaData,
    region_metadata: &RegionMetadata,
) -> Option<(Bytes, Bytes)> {
    if region_metadata.primary_key.is_empty() {
        return None;
    }

    let pk_column_idx = parquet_meta
        .file_metadata()
        .schema_descr()
        .columns()
        .iter()
        .position(|column| column.name() == PRIMARY_KEY_COLUMN_NAME)?;

    let mut min: Option<Bytes> = None;
    let mut max: Option<Bytes> = None;

    for row_group in parquet_meta.row_groups() {
        let Statistics::ByteArray(stats) = row_group.column(pk_column_idx).statistics()? else {
            return None;
        };

        // Truncated bounds can end at a field boundary and masquerade as an
        // older Dense schema. Only complete endpoints can be schema-normalized.
        if !stats.min_is_exact() || !stats.max_is_exact() {
            return None;
        }
        let row_group_min = Bytes::copy_from_slice(stats.min_bytes_opt()?);
        let row_group_max = Bytes::copy_from_slice(stats.max_bytes_opt()?);
        min = Some(match min {
            Some(current) => current.min(row_group_min),
            None => row_group_min,
        });
        max = Some(match max {
            Some(current) => current.max(row_group_max),
            None => row_group_max,
        });
    }

    min.zip(max)
}

struct ObjectStoreFetch<'a> {
    object_store: &'a ObjectStore,
    file_path: &'a str,
    num_reads: &'a AtomicUsize,
    bytes_read: &'a AtomicU64,
    max_bytes: Option<u64>,
}

impl MetadataFetch for ObjectStoreFetch<'_> {
    fn fetch(&mut self, range: std::ops::Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        let bytes_to_read = range.end - range.start;
        async move {
            if let Some(max_bytes) = self.max_bytes {
                self.bytes_read
                    .fetch_update(Ordering::Relaxed, Ordering::Relaxed, |read| {
                        read.checked_add(bytes_to_read)
                            .filter(|next| *next <= max_bytes)
                    })
                    .map_err(|_| ParquetError::General("footer read budget exceeded".into()))?;
            } else {
                self.bytes_read.fetch_add(bytes_to_read, Ordering::Relaxed);
            }
            self.num_reads.fetch_add(1, Ordering::Relaxed);
            let data = self
                .object_store
                .read_with(self.file_path)
                .range(range)
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))?;
            Ok(data.to_bytes())
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::arrow::array::{
        ArrayRef, BinaryArray, DictionaryArray, Int64Array, UInt32Array,
    };
    use datatypes::arrow::datatypes::{DataType as ArrowDataType, Field, Schema};
    use datatypes::arrow::record_batch::RecordBatch;
    use object_store::services::Memory;
    use parquet::arrow::ArrowWriter;
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::file::metadata::{KeyValue, ParquetMetaData};
    use parquet::file::properties::{EnabledStatistics, WriterProperties};

    use super::*;
    use crate::sst::parquet::PARQUET_METADATA_KEY;
    use crate::test_util::sst_util::sst_region_metadata;

    #[tokio::test]
    async fn test_metadata_count_footer_budget_reads_no_data_pages() {
        let bytes = build_test_parquet_bytes(false, &[], &[1000], EnabledStatistics::Chunk);
        let len = bytes.len();
        let footer_size = u32::from_le_bytes(bytes[len - 8..len - 4].try_into().unwrap()) as u64;
        let store = ObjectStore::new(Memory::default()).unwrap();
        store.write("file.parquet", bytes).await.unwrap();
        for budget in [0, 8, footer_size + 7, footer_size + 8] {
            let mut loader = MetadataLoader::new(store.clone(), "file.parquet", len as u64);
            loader.with_read_budget(budget);
            let mut metrics = MetadataCacheMetrics::default();
            let result = loader.load(&mut metrics).await;
            assert!(metrics.bytes_read <= budget);
            if budget == footer_size + 8 {
                assert_eq!(result.unwrap().file_metadata().num_rows(), 1000);
                assert_eq!(metrics.bytes_read, footer_size + 8);
                assert_eq!(metrics.num_reads, 2);
            } else {
                assert!(result.is_err());
                assert_eq!(metrics.bytes_read, if budget < 8 { 0 } else { 8 });
            }
        }
    }

    struct PendingRead {
        active: Arc<AtomicUsize>,
    }

    struct ReadGuard(Arc<AtomicUsize>);

    impl Drop for ReadGuard {
        fn drop(&mut self) {
            self.0.fetch_sub(1, Ordering::Relaxed);
        }
    }

    impl PendingRead {
        async fn block<T>(&self) -> T {
            self.active.fetch_add(1, Ordering::Relaxed);
            let _guard = ReadGuard(self.active.clone());
            std::future::pending().await
        }
    }

    impl object_store::layers::mock::Read for PendingRead {
        async fn read(
            &self,
            _: object_store::layers::mock::BytesRange,
        ) -> object_store::layers::mock::Result<(
            object_store::layers::mock::RpRead,
            object_store::layers::mock::Buffer,
        )> {
            self.block().await
        }
        async fn open(
            &self,
            _: object_store::layers::mock::BytesRange,
        ) -> object_store::layers::mock::Result<(
            object_store::layers::mock::RpRead,
            Box<dyn object_store::layers::mock::ReadStreamDyn>,
        )> {
            self.block().await
        }
    }

    #[tokio::test]
    async fn test_metadata_count_footer_timeout_and_cancellation() {
        let active = Arc::new(AtomicUsize::new(0));
        let observed = active.clone();
        let layer = object_store::layers::mock::MockLayerBuilder::default()
            .reader_factory(Arc::new(move |_, _, _| {
                Box::new(PendingRead {
                    active: observed.clone(),
                })
            }))
            .build()
            .unwrap();
        let store = ObjectStore::new(Memory::default()).unwrap().layer(layer);
        store.write("file.parquet", vec![0u8; 100]).await.unwrap();
        let mut loader = MetadataLoader::new(store, "file.parquet", 100);
        loader.with_read_budget(100);
        let mut metrics = MetadataCacheMetrics::default();
        assert!(
            tokio::time::timeout(
                std::time::Duration::from_millis(5),
                loader.load(&mut metrics)
            )
            .await
            .is_err()
        );
        assert_eq!(active.load(Ordering::Relaxed), 0);
        let mut future = Box::pin(loader.load(&mut metrics));
        assert!(futures::poll!(&mut future).is_pending());
        assert_eq!(active.load(Ordering::Relaxed), 1);
        drop(future);
        assert_eq!(active.load(Ordering::Relaxed), 0);
    }

    fn build_test_parquet_bytes(
        include_primary_key: bool,
        primary_keys: &[&[u8]],
        row_group_sizes: &[usize],
        stats_enabled: EnabledStatistics,
    ) -> Vec<u8> {
        let total_rows = row_group_sizes.iter().sum::<usize>();
        let mut fields = vec![Field::new("field", ArrowDataType::Int64, true)];
        let mut columns: Vec<ArrayRef> =
            vec![Arc::new(Int64Array::from_iter_values(0..total_rows as i64))];
        if include_primary_key {
            assert_eq!(total_rows, primary_keys.len());
            fields.push(Field::new(
                "__primary_key",
                ArrowDataType::Dictionary(
                    Box::new(ArrowDataType::UInt32),
                    Box::new(ArrowDataType::Binary),
                ),
                false,
            ));
            let values = Arc::new(BinaryArray::from_iter_values(primary_keys.iter().copied()));
            let keys = UInt32Array::from_iter_values(0..primary_keys.len() as u32);
            columns.push(Arc::new(DictionaryArray::new(keys, values)));
        }

        let schema = Arc::new(Schema::new(fields));
        let region_metadata = Arc::new(sst_region_metadata());
        let key_value = KeyValue::new(
            PARQUET_METADATA_KEY.to_string(),
            region_metadata.to_json().unwrap(),
        );
        let props = WriterProperties::builder()
            .set_key_value_metadata(Some(vec![key_value]))
            .set_statistics_enabled(stats_enabled)
            .build();

        let mut parquet_bytes = Vec::new();
        let mut writer =
            ArrowWriter::try_new(&mut parquet_bytes, schema.clone(), Some(props)).unwrap();
        let mut offset = 0;
        for row_group_size in row_group_sizes {
            let batch = RecordBatch::try_new(
                schema.clone(),
                columns
                    .iter()
                    .map(|column| column.slice(offset, *row_group_size))
                    .collect(),
            )
            .unwrap();
            writer.write(&batch).unwrap();
            offset += row_group_size;
        }
        writer.close().unwrap();

        parquet_bytes
    }

    fn build_test_metadata(
        include_primary_key: bool,
        primary_keys: &[&[u8]],
        row_group_sizes: &[usize],
        stats_enabled: EnabledStatistics,
    ) -> ParquetMetaData {
        ParquetRecordBatchReaderBuilder::try_new(Bytes::from(build_test_parquet_bytes(
            include_primary_key,
            primary_keys,
            row_group_sizes,
            stats_enabled,
        )))
        .unwrap()
        .metadata()
        .as_ref()
        .clone()
    }

    #[tokio::test]
    async fn test_metadata_loader_only_loads_offset_indexes() {
        let parquet_bytes = build_test_parquet_bytes(false, &[], &[4], EnabledStatistics::Page);
        let file_size = parquet_bytes.len() as u64;
        let file_path = "test.parquet";
        let object_store = ObjectStore::new(Memory::default()).unwrap();
        object_store.write(file_path, parquet_bytes).await.unwrap();

        let mut loader = MetadataLoader::new(object_store, file_path, file_size);
        loader.with_page_index_policy(PageIndexPolicy::Required);
        let metadata = loader
            .load(&mut MetadataCacheMetrics::default())
            .await
            .unwrap();

        assert!(metadata.column_index().is_none());
        assert!(metadata.offset_index().is_some());
    }

    #[test]
    fn test_extract_primary_key_range_returns_none_when_column_absent() {
        let metadata = build_test_metadata(false, &[], &[1], EnabledStatistics::Page);
        let region_metadata = sst_region_metadata();

        assert_eq!(None, extract_primary_key_range(&metadata, &region_metadata));
    }

    #[test]
    fn test_extract_primary_key_range_folds_row_group_stats() {
        let metadata = build_test_metadata(
            true,
            &[b"bbb", b"ccc", b"aaa", b"zzz"],
            &[2, 2],
            EnabledStatistics::Page,
        );
        let region_metadata = sst_region_metadata();

        assert_eq!(
            Some((Bytes::from_static(b"aaa"), Bytes::from_static(b"zzz"))),
            extract_primary_key_range(&metadata, &region_metadata)
        );
    }

    #[test]
    fn test_extract_primary_key_range_rejects_truncated_statistics() {
        let key = vec![b'a'; 1024];
        let metadata = build_test_metadata(true, &[&key], &[1], EnabledStatistics::Page);
        assert_eq!(
            None,
            extract_primary_key_range(&metadata, &sst_region_metadata())
        );
    }

    #[test]
    fn test_extract_primary_key_range_returns_none_when_any_rg_stats_missing() {
        let metadata = build_test_metadata(
            true,
            &[b"bbb", b"ccc", b"aaa", b"zzz"],
            &[2, 2],
            EnabledStatistics::None,
        );
        let region_metadata = sst_region_metadata();

        assert_eq!(None, extract_primary_key_range(&metadata, &region_metadata));
    }
}
