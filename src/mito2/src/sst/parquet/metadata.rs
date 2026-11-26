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

use api::v1::OpType;
use bytes::Bytes;
use futures::FutureExt;
use futures::future::BoxFuture;
use object_store::ObjectStore;
use parquet::arrow::async_reader::MetadataFetch;
use parquet::basic::{ConvertedType, LogicalType, Type};
use parquet::errors::{ParquetError, Result as ParquetResult};
use parquet::file::metadata::{
    KeyValue, PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader, RowGroupMetaData,
};
use parquet::file::statistics::Statistics;
use parquet::schema::types::SchemaDescriptor;
use snafu::{IntoError as _, ResultExt};
use store_api::metadata::RegionMetadata;
use store_api::storage::consts::{OP_TYPE_COLUMN_NAME, PRIMARY_KEY_COLUMN_NAME};

use crate::error::{self, Result};
use crate::region::options::MergeMode;
use crate::sst::parquet::reader::MetadataCacheMetrics;

/// Presence certifies at most one physical row per (primary key, timestamp).
/// The value has no meaning. Any new claim must use a different key.
pub(crate) const DEDUPED_SST_KEY: &str = "greptimedb.deduped";

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
        }
    }

    pub(crate) fn with_page_index_policy(&mut self, page_index_policy: PageIndexPolicy) {
        self.page_index_policy = page_index_policy;
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
            .with_prefetch_hint(Some(DEFAULT_PREFETCH_SIZE as usize))
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
        };

        let metadata = reader
            .load_and_finish(fetch, file_size)
            .await
            .map_err(|e| match unbox_external_error(e) {
                Ok(os_err) => error::OpenDalSnafu {}.into_error(os_err),
                Err(parquet_err) => error::ReadParquetSnafu { path }.into_error(parquet_err),
            })?;

        cache_metrics.num_reads = num_reads.into_inner();
        cache_metrics.bytes_read = bytes_read.into_inner();

        Ok(metadata)
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

/// Returns the leaf index of the top-level `__primary_key` column. A leaf name alone also
/// matches nested fields, such as a JSON child named `__primary_key`.
pub(crate) fn primary_key_leaf_index(schema: &SchemaDescriptor) -> Option<usize> {
    top_level_leaf_index(schema, PRIMARY_KEY_COLUMN_NAME, Type::BYTE_ARRAY)
}

/// Returns a leaf only if exactly one top-level column has this name and type.
fn top_level_leaf_index(
    schema: &SchemaDescriptor,
    name: &str,
    physical_type: Type,
) -> Option<usize> {
    let mut matches = schema
        .columns()
        .iter()
        .enumerate()
        .filter(|(_, column)| matches!(column.path().parts(), [part] if part == name));
    let (index, column) = matches.next()?;
    (matches.next().is_none() && column.physical_type() == physical_type).then_some(index)
}

/// Whether the internal op-type statistics prove that every row is a put.
pub(crate) fn has_only_puts(row_groups: &[RowGroupMetaData]) -> bool {
    let Some(column) = row_groups.first().and_then(|group| {
        let schema = group.schema_descr();
        let index = top_level_leaf_index(schema, OP_TYPE_COLUMN_NAME, Type::INT32)?;
        let column = schema.column(index);
        let is_uint8 = match column.logical_type_ref() {
            Some(logical_type) => *logical_type == LogicalType::integer(8, false),
            None => column.converted_type() == ConvertedType::UINT_8,
        };
        is_uint8.then_some(index)
    }) else {
        return false;
    };
    let put = OpType::Put as i32;
    row_groups.iter().all(|group| {
        matches!(
            group.column(column).statistics(),
            Some(Statistics::Int32(stats))
                if stats.null_count_opt() == Some(0)
                    && stats.min_is_exact()
                    && stats.max_is_exact()
                    && stats.min_opt() == Some(&put)
                    && stats.max_opt() == Some(&put)
        )
    })
}

/// Adds the uniqueness claim to an output of a writer that folded versions.
/// LastNonNull may preserve multiple rows for a key across delete barriers.
pub(crate) fn deduped_sst_marker(
    merge_mode: Option<MergeMode>,
    row_groups: &[RowGroupMetaData],
) -> Option<KeyValue> {
    let deduped = match merge_mode? {
        MergeMode::LastRow => true,
        MergeMode::LastNonNull => has_only_puts(row_groups),
    };
    deduped.then(|| KeyValue::new(DEDUPED_SST_KEY.to_owned(), None))
}

pub(crate) fn extract_primary_key_range(
    parquet_meta: &ParquetMetaData,
    region_metadata: &RegionMetadata,
) -> Option<(Bytes, Bytes)> {
    if region_metadata.primary_key.is_empty() {
        return None;
    }

    let pk_column_idx = primary_key_leaf_index(parquet_meta.file_metadata().schema_descr())?;

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
}

impl MetadataFetch for ObjectStoreFetch<'_> {
    fn fetch(&mut self, range: std::ops::Range<u64>) -> BoxFuture<'_, ParquetResult<Bytes>> {
        let bytes_to_read = range.end - range.start;
        async move {
            let data = self
                .object_store
                .read_with(self.file_path)
                .range(range)
                .await
                .map_err(|e| ParquetError::External(Box::new(e)))?;
            self.num_reads.fetch_add(1, Ordering::Relaxed);
            self.bytes_read.fetch_add(bytes_to_read, Ordering::Relaxed);
            Ok(data.to_bytes())
        }
        .boxed()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use datatypes::arrow::array::{
        ArrayRef, BinaryArray, DictionaryArray, Int64Array, StructArray, UInt32Array,
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
    fn test_extract_primary_key_range_ignores_nested_primary_key_name() {
        let nested = Field::new("__primary_key", ArrowDataType::Binary, false);
        let json = StructArray::from(vec![(
            Arc::new(nested.clone()),
            Arc::new(BinaryArray::from_iter_values([b"jjj", b"kkk"])) as ArrayRef,
        )]);
        let schema = Arc::new(Schema::new(vec![
            Field::new("json", ArrowDataType::Struct(vec![nested].into()), false),
            Field::new("__primary_key", ArrowDataType::Binary, false),
        ]));
        let batch = RecordBatch::try_new(
            schema.clone(),
            vec![
                Arc::new(json),
                Arc::new(BinaryArray::from_iter_values([b"aaa", b"zzz"])),
            ],
        )
        .unwrap();
        let mut parquet_bytes = Vec::new();
        let mut writer = ArrowWriter::try_new(&mut parquet_bytes, schema, None).unwrap();
        writer.write(&batch).unwrap();
        writer.close().unwrap();
        let metadata = ParquetRecordBatchReaderBuilder::try_new(Bytes::from(parquet_bytes))
            .unwrap()
            .metadata()
            .clone();

        assert_eq!(
            Some((Bytes::from_static(b"aaa"), Bytes::from_static(b"zzz"))),
            extract_primary_key_range(&metadata, &sst_region_metadata())
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
