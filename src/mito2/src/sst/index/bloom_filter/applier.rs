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

mod builder;

use std::collections::BTreeMap;
use std::ops::Range;
use std::sync::Arc;
use std::time::Instant;

use common_base::range_read::RangeReader;
use common_telemetry::{tracing, warn};
use datatypes::data_type::ConcreteDataType;
use index::bloom_filter::applier::{BloomFilterApplier, InListPredicate};
use index::bloom_filter::reader::{
    BloomFilterReadMetrics, BloomFilterReader, BloomFilterReaderImpl,
};
use index::target::IndexTarget;
use object_store::ObjectStore;
use puffin::puffin_manager::cache::PuffinMetadataCacheRef;
use puffin::puffin_manager::{PuffinManager, PuffinReader};
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;
use store_api::region_request::PathType;

use crate::access_layer::{RegionFilePathFactory, WriteCachePathProvider};
use crate::cache::file_cache::{FileCacheRef, FileType, IndexKey};
use crate::cache::index::bloom_filter_index::{
    BloomFilterIndexCacheRef, CachedBloomFilterIndexBlobReader, Tag,
};
use crate::error::{
    ApplyBloomFilterIndexSnafu, Error, MetadataSnafu, PuffinBuildReaderSnafu, PuffinReadBlobSnafu,
    Result,
};
use crate::metrics::INDEX_APPLY_ELAPSED;
use crate::sst::file::RegionIndexId;
use crate::sst::index::bloom_filter::INDEX_BLOB_TYPE;
pub use crate::sst::index::bloom_filter::applier::builder::BloomFilterIndexApplierBuilder;
use crate::sst::index::puffin_manager::{BlobReader, PuffinManagerFactory};
use crate::sst::index::{TYPE_BLOOM_FILTER_INDEX, trigger_index_background_download};

/// Metrics for tracking bloom filter index apply operations.
#[derive(Default, Clone)]
pub struct BloomFilterIndexApplyMetrics {
    /// Total time spent applying the index.
    pub apply_elapsed: std::time::Duration,
    /// Number of blob cache misses.
    pub blob_cache_miss: usize,
    /// Total size of blobs read (in bytes).
    pub blob_read_bytes: u64,
    /// Metrics for bloom filter read operations.
    pub read_metrics: BloomFilterReadMetrics,
}

impl std::fmt::Debug for BloomFilterIndexApplyMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            apply_elapsed,
            blob_cache_miss,
            blob_read_bytes,
            read_metrics,
        } = self;

        if self.is_empty() {
            return write!(f, "{{}}");
        }
        write!(f, "{{")?;

        write!(f, "\"apply_elapsed\":\"{:?}\"", apply_elapsed)?;

        if *blob_cache_miss > 0 {
            write!(f, ", \"blob_cache_miss\":{}", blob_cache_miss)?;
        }
        if *blob_read_bytes > 0 {
            write!(f, ", \"blob_read_bytes\":{}", blob_read_bytes)?;
        }
        write!(f, ", \"read_metrics\":{:?}", read_metrics)?;

        write!(f, "}}")
    }
}

impl BloomFilterIndexApplyMetrics {
    /// Returns true if the metrics are empty (contain no meaningful data).
    pub fn is_empty(&self) -> bool {
        self.apply_elapsed.is_zero()
    }

    /// Merges another metrics into this one.
    pub fn merge_from(&mut self, other: &Self) {
        self.apply_elapsed += other.apply_elapsed;
        self.blob_cache_miss += other.blob_cache_miss;
        self.blob_read_bytes += other.blob_read_bytes;
        self.read_metrics.merge_from(&other.read_metrics);
    }
}

pub(crate) type BloomFilterIndexApplierRef = Arc<BloomFilterIndexApplier>;

/// `BloomFilterIndexApplier` applies bloom filter predicates to the SST file.
pub struct BloomFilterIndexApplier {
    /// Directory of the table.
    table_dir: String,

    /// Path type for generating file paths.
    path_type: PathType,

    /// Object store to read the index file.
    object_store: ObjectStore,

    /// File cache to read the index file.
    file_cache: Option<FileCacheRef>,

    /// Factory to create puffin manager.
    puffin_manager_factory: PuffinManagerFactory,

    /// Cache for puffin metadata.
    puffin_metadata_cache: Option<PuffinMetadataCacheRef>,

    /// Cache for bloom filter index.
    bloom_filter_index_cache: Option<BloomFilterIndexCacheRef>,

    /// Bloom filter predicates.
    /// For each column, the value will be retained only if it contains __all__ predicates.
    default_predicates: Arc<BTreeMap<IndexTarget, Vec<InListPredicate>>>,

    /// Expected predicate target types from the latest region metadata.
    expected_predicate_target_types: BTreeMap<IndexTarget, ConcreteDataType>,
}

impl BloomFilterIndexApplier {
    /// Creates a new `BloomFilterIndexApplier`.
    ///
    /// For each column, the value will be retained only if it contains __all__ predicates.
    pub fn new(
        table_dir: String,
        path_type: PathType,
        object_store: ObjectStore,
        puffin_manager_factory: PuffinManagerFactory,
        predicates: BTreeMap<IndexTarget, Vec<InListPredicate>>,
        expected_predicate_target_types: BTreeMap<IndexTarget, ConcreteDataType>,
    ) -> Self {
        let default_predicates = Arc::new(predicates);
        Self {
            table_dir,
            path_type,
            object_store,
            file_cache: None,
            puffin_manager_factory,
            puffin_metadata_cache: None,
            bloom_filter_index_cache: None,
            default_predicates,
            expected_predicate_target_types,
        }
    }

    pub fn with_file_cache(mut self, file_cache: Option<FileCacheRef>) -> Self {
        self.file_cache = file_cache;
        self
    }

    pub fn with_puffin_metadata_cache(
        mut self,
        puffin_metadata_cache: Option<PuffinMetadataCacheRef>,
    ) -> Self {
        self.puffin_metadata_cache = puffin_metadata_cache;
        self
    }

    pub fn with_bloom_filter_cache(
        mut self,
        bloom_filter_index_cache: Option<BloomFilterIndexCacheRef>,
    ) -> Self {
        self.bloom_filter_index_cache = bloom_filter_index_cache;
        self
    }

    /// Applies bloom filter predicates to the provided SST file and returns a
    /// list of row group ranges that match the predicates.
    ///
    /// The `row_groups` iterator provides the row group lengths and whether to search in the row group.
    ///
    /// Row group id existing in the returned result means that the row group is searched.
    /// Empty ranges means that the row group is searched but no rows are found.
    ///
    ///
    /// # Arguments
    /// * `file_id` - The region file ID to apply predicates to
    /// * `file_size_hint` - Optional hint for file size to avoid extra metadata reads
    /// * `row_groups` - Iterator of row group lengths and whether to search in the row group
    /// * `metrics` - Optional mutable reference to collect metrics on demand
    #[tracing::instrument(
        skip_all,
        fields(file_id = %file_id)
    )]
    pub async fn apply(
        &self,
        file_id: RegionIndexId,
        file_size_hint: Option<u64>,
        predicates: &BTreeMap<IndexTarget, Vec<InListPredicate>>,
        row_groups: impl Iterator<Item = (usize, bool)>,
        mut metrics: Option<&mut BloomFilterIndexApplyMetrics>,
    ) -> Result<Vec<(usize, Vec<Range<usize>>)>> {
        let apply_start = Instant::now();

        // Calculates row groups' ranges based on start of the file.
        let mut input = Vec::with_capacity(row_groups.size_hint().0);
        let mut start = 0;
        for (i, (len, to_search)) in row_groups.enumerate() {
            let end = start + len;
            if to_search {
                input.push((i, start..end));
            }
            start = end;
        }

        // Initializes output with input ranges, but ranges are based on start of the file not the row group,
        // so we need to adjust them later.
        let mut output = input
            .iter()
            .map(|(i, range)| (*i, vec![range.clone()]))
            .collect::<Vec<_>>();

        for (column_id, predicates) in predicates {
            let blob = match self
                .blob_reader(file_id, column_id, file_size_hint, metrics.as_deref_mut())
                .await?
            {
                Some(blob) => blob,
                None => continue,
            };

            // Create appropriate reader based on whether we have caching enabled
            if let Some(bloom_filter_cache) = &self.bloom_filter_index_cache {
                let blob_size = blob.metadata().await.context(MetadataSnafu)?.content_length;
                if let Some(m) = &mut metrics {
                    m.blob_read_bytes += blob_size;
                }
                let reader = CachedBloomFilterIndexBlobReader::new(
                    file_id.file_id(),
                    file_id.version,
                    match column_id {
                        IndexTarget::ColumnId(id) => *id,
                        IndexTarget::JsonPath(path) => path.column_id(),
                    },
                    Tag::Skipping,
                    blob_size,
                    BloomFilterReaderImpl::new(blob),
                    bloom_filter_cache.clone(),
                )
                .with_target(column_id.clone());
                self.apply_predicates(reader, predicates, &mut output, metrics.as_deref_mut())
                    .await
                    .context(ApplyBloomFilterIndexSnafu)?;
            } else {
                let reader = BloomFilterReaderImpl::new(blob);
                self.apply_predicates(reader, predicates, &mut output, metrics.as_deref_mut())
                    .await
                    .context(ApplyBloomFilterIndexSnafu)?;
            }
        }

        // adjust ranges to be based on row group
        for ((_, output), (_, input)) in output.iter_mut().zip(input) {
            let start = input.start;
            for range in output.iter_mut() {
                range.start -= start;
                range.end -= start;
            }
        }

        // Record elapsed time to histogram and collect metrics if requested
        let elapsed = apply_start.elapsed();
        INDEX_APPLY_ELAPSED
            .with_label_values(&[TYPE_BLOOM_FILTER_INDEX])
            .observe(elapsed.as_secs_f64());

        if let Some(m) = metrics {
            m.apply_elapsed += elapsed;
        }

        Ok(output)
    }

    /// Creates a blob reader from the cached or remote index file.
    ///
    /// Returus `None` if the column does not have an index.
    async fn blob_reader(
        &self,
        file_id: RegionIndexId,
        column_id: &IndexTarget,
        file_size_hint: Option<u64>,
        metrics: Option<&mut BloomFilterIndexApplyMetrics>,
    ) -> Result<Option<BlobReader>> {
        let reader = match self
            .cached_blob_reader(file_id, column_id, file_size_hint)
            .await
        {
            Ok(Some(puffin_reader)) => puffin_reader,
            other => {
                if let Some(m) = metrics {
                    m.blob_cache_miss += 1;
                }
                if let Err(err) = other {
                    // Blob not found means no index for this column
                    if is_blob_not_found(&err) {
                        return Ok(None);
                    }
                    warn!(err; "An unexpected error occurred while reading the cached index file. Fallback to remote index file.")
                }
                let res = self
                    .remote_blob_reader(file_id, column_id, file_size_hint)
                    .await;
                if let Err(err) = res {
                    // Blob not found means no index for this column
                    if is_blob_not_found(&err) {
                        return Ok(None);
                    }
                    return Err(err);
                }

                res?
            }
        };

        Ok(Some(reader))
    }

    /// Creates a blob reader from the cached index file
    async fn cached_blob_reader(
        &self,
        file_id: RegionIndexId,
        column_id: &IndexTarget,
        file_size_hint: Option<u64>,
    ) -> Result<Option<BlobReader>> {
        let Some(file_cache) = &self.file_cache else {
            return Ok(None);
        };

        let index_key = IndexKey::new(
            file_id.region_id(),
            file_id.file_id(),
            FileType::Puffin(file_id.version),
        );
        if file_cache.get(index_key).await.is_none() {
            return Ok(None);
        };

        let puffin_manager = self.puffin_manager_factory.build(
            file_cache.local_store(),
            WriteCachePathProvider::new(file_cache.clone()),
        );
        let blob_name = Self::target_blob_name(column_id);

        let reader = puffin_manager
            .reader(&file_id)
            .await
            .context(PuffinBuildReaderSnafu)?
            .with_file_size_hint(file_size_hint)
            .blob(&blob_name)
            .await
            .context(PuffinReadBlobSnafu)?
            .reader()
            .await
            .context(PuffinBuildReaderSnafu)?;
        Ok(Some(reader))
    }

    // TODO(ruihang): use the same util with the code in creator
    fn target_blob_name(column_id: &IndexTarget) -> String {
        let target_key = column_id.encode();
        format!("{INDEX_BLOB_TYPE}-{target_key}")
    }

    /// Creates a blob reader from the remote index file
    async fn remote_blob_reader(
        &self,
        file_id: RegionIndexId,
        column_id: &IndexTarget,
        file_size_hint: Option<u64>,
    ) -> Result<BlobReader> {
        let path_factory = RegionFilePathFactory::new(self.table_dir.clone(), self.path_type);

        // Trigger background download if file cache and file size are available
        trigger_index_background_download(
            self.file_cache.as_ref(),
            &file_id,
            file_size_hint,
            &path_factory,
            &self.object_store,
        );

        let puffin_manager = self
            .puffin_manager_factory
            .build(self.object_store.clone(), path_factory)
            .with_puffin_metadata_cache(self.puffin_metadata_cache.clone());

        let blob_name = Self::target_blob_name(column_id);

        puffin_manager
            .reader(&file_id)
            .await
            .context(PuffinBuildReaderSnafu)?
            .with_file_size_hint(file_size_hint)
            .blob(&blob_name)
            .await
            .context(PuffinReadBlobSnafu)?
            .reader()
            .await
            .context(PuffinBuildReaderSnafu)
    }

    async fn apply_predicates<R: BloomFilterReader + Send + 'static>(
        &self,
        reader: R,
        predicates: &[InListPredicate],
        output: &mut [(usize, Vec<Range<usize>>)],
        metrics: Option<&mut BloomFilterIndexApplyMetrics>,
    ) -> std::result::Result<(), index::bloom_filter::error::Error> {
        let mut applier = BloomFilterApplier::new(Box::new(reader)).await?;
        let mut row_groups = output.iter_mut().map(|(_, r)| r).collect::<Vec<_>>();
        let read_metrics = metrics.map(|m| &mut m.read_metrics);
        applier
            .search_groups(predicates, &mut row_groups, read_metrics)
            .await
    }

    /// Returns compatible bloom filter predicates with the given SST metadata.
    ///
    /// Returns `None` when no compatible predicate remains for this SST.
    pub fn compatible_predicate_for_sst(
        &self,
        sst_metadata: &RegionMetadataRef,
    ) -> Option<Arc<BTreeMap<IndexTarget, Vec<InListPredicate>>>> {
        let mut has_type_mismatch = false;
        let mut compatible_col_ids = Vec::new();

        for (col_id, expected) in &self.expected_predicate_target_types {
            if crate::sst::index::column::skipping_target_data_type(sst_metadata, col_id).as_ref()
                != Some(expected)
            {
                has_type_mismatch = true;
                continue;
            }
            compatible_col_ids.push(col_id.clone());
        }

        if compatible_col_ids.is_empty() {
            return None;
        }

        if !has_type_mismatch {
            return Some(self.default_predicates.clone());
        }

        let mut compatible_predicates = BTreeMap::new();
        for col_id in compatible_col_ids {
            if let Some(predicates) = self.default_predicates.get(&col_id) {
                compatible_predicates.insert(col_id, predicates.clone());
            }
        }

        Some(Arc::new(compatible_predicates))
    }
}

fn is_blob_not_found(err: &Error) -> bool {
    matches!(
        err,
        Error::PuffinReadBlob {
            source: puffin::error::Error::BlobNotFound { .. },
            ..
        }
    )
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeSet;

    use datafusion_expr::{Expr, col, lit};
    use futures::future::BoxFuture;
    use index::Bytes;
    use object_store::services::Memory;
    use puffin::puffin_manager::PuffinWriter;
    use store_api::metadata::RegionMetadata;
    use store_api::storage::FileId;

    use super::*;
    use crate::sst::file::RegionFileId;
    use crate::sst::index::IndexBuildType;
    use crate::sst::index::bloom_filter::creator::BloomFilterIndexer;
    use crate::sst::index::bloom_filter::creator::tests::{
        mock_object_store, mock_region_metadata, new_batch, new_intm_mgr,
    };

    #[tokio::test]
    async fn test_json_skipping_build_and_prune_both_formats() {
        use crate::read::{Batch, BatchColumn};
        use common_function::scalars::json::json_get::JsonGetWithType;
        use common_function::scalars::udf::create_udf;
        use datafusion_common::ScalarValue;
        use datafusion_expr::expr::ScalarFunction;
        use datatypes::arrow::array::{ArrayRef, Int64Array, StructArray};
        use datatypes::arrow::buffer::NullBuffer;
        use datatypes::arrow::datatypes::{Field, Schema};
        use datatypes::arrow::record_batch::RecordBatch;
        use datatypes::schema::SkippingIndexOptions;
        use datatypes::vectors::{Helper, UInt8Vector, UInt64Vector};

        for flat in [false, true] {
            let (dir, factory) =
                PuffinManagerFactory::new_for_test_async("json_skipping_index").await;
            let mut metadata = (*mock_region_metadata()).clone();
            let column = &mut metadata.column_metadatas[2].column_schema;
            column.data_type =
                ConcreteDataType::json2(datatypes::types::json_type::JsonNativeType::Object(
                    datatypes::types::json_type::JsonObjectType::new(),
                ));
            column.with_extension_type(&datatypes::extension::json::Json2ExtensionType::new(
                Arc::new(datatypes::extension::json::JsonMetadata::new(
                    datatypes::json::JsonSettings::try_new(
                        vec![datatypes::json::JsonTypeHint {
                            path: vec!["value".into()],
                            data_type: ConcreteDataType::int64_datatype(),
                            inverted_index: false,
                            skipping_index: Some(
                                SkippingIndexOptions::new(2, 0.01, Default::default()).unwrap(),
                            ),
                        }],
                        Some(0),
                    )
                    .unwrap(),
                )),
            ));
            // Other ordinary indexed columns are absent in this synthetic batch.
            for col in &mut metadata.column_metadatas {
                col.column_schema.unset_skipping_options().unwrap();
            }
            let metadata = Arc::new(metadata);
            let file_id = FileId::random();
            let target = IndexTarget::new_json_path(3, vec!["value".into()]).unwrap();
            let mut creator = BloomFilterIndexer::new(
                file_id,
                &metadata,
                new_intm_mgr(dir.path().to_string_lossy()).await,
                None,
                IndexBuildType::Flush,
            )
            .unwrap()
            .unwrap();
            let values: ArrayRef = Arc::new(Int64Array::from(vec![Some(1), None, Some(1), None]));
            let root: ArrayRef = Arc::new(StructArray::new(
                vec![Field::new("value", values.data_type().clone(), true)].into(),
                vec![values],
                Some(NullBuffer::from(vec![true, false, true, true])),
            ));
            for start in [0, 2] {
                let array = root.slice(start, 2);
                if flat {
                    let batch = RecordBatch::try_new(
                        Arc::new(Schema::new(vec![Field::new(
                            "field_u64",
                            array.data_type().clone(),
                            true,
                        )])),
                        vec![array],
                    )
                    .unwrap();
                    creator.update_flat(&batch).await.unwrap();
                } else {
                    let mut batch = Batch::new(
                        vec![],
                        Arc::new(UInt64Vector::from_vec(vec![0, 1])),
                        Arc::new(UInt64Vector::from_vec(vec![0, 0])),
                        Arc::new(UInt8Vector::from_vec(vec![1, 1])),
                        vec![BatchColumn {
                            column_id: 3,
                            data: Helper::try_into_vector(array).unwrap(),
                        }],
                    )
                    .unwrap();
                    creator.update(&mut batch).await.unwrap();
                }
            }

            assert_eq!(creator.column_ids().count(), 0);
            let object_store = mock_object_store();
            let manager = factory.build(
                object_store.clone(),
                RegionFilePathFactory::new("json_test".into(), PathType::Bare),
            );
            let index_id = RegionIndexId::new(RegionFileId::new(metadata.region_id, file_id), 0);
            let mut writer = manager.writer(&index_id).await.unwrap();
            let (rows, bytes) = creator.finish(&mut writer).await.unwrap();
            assert_eq!(rows, 4);
            assert!(bytes > 0);
            writer.finish().await.unwrap();
            let json_expr = |path: &str, ty| {
                Expr::ScalarFunction(ScalarFunction::new_udf(
                    Arc::new(create_udf(Arc::new(JsonGetWithType::default()))),
                    vec![col("field_u64"), lit(path), Expr::Literal(ty, None)],
                ))
            };
            let cache = Arc::new(
                crate::cache::index::bloom_filter_index::BloomFilterIndexCache::new(
                    1024 * 1024,
                    1024 * 1024,
                    1024,
                ),
            );
            for expr in [
                json_expr("$.value", ScalarValue::Int64(None)).eq(lit(9_i64)),
                json_expr("$.value", ScalarValue::Int64(None))
                    .in_list(vec![lit(8_i64), lit(9_i64)], false),
            ] {
                let applier = BloomFilterIndexApplierBuilder::new(
                    "json_test".into(),
                    PathType::Bare,
                    object_store.clone(),
                    &metadata,
                    factory.clone(),
                )
                .with_bloom_filter_index_cache(Some(cache.clone()))
                .build(&[expr])
                .unwrap()
                .unwrap();
                let predicates = applier.compatible_predicate_for_sst(&metadata).unwrap();
                assert!(predicates.contains_key(&target));
                // Run twice to cover cold reads and cached index reads.
                for _ in 0..2 {
                    let result = applier
                        .apply(index_id, None, &predicates, [(4, true)].into_iter(), None)
                        .await
                        .unwrap();
                    assert!(result[0].1.is_empty());
                }
                assert!(
                    applier
                        .compatible_predicate_for_sst(&mock_region_metadata())
                        .is_none()
                );
            }
            let applier = BloomFilterIndexApplierBuilder::new(
                "json_test".into(),
                PathType::Bare,
                object_store.clone(),
                &metadata,
                factory.clone(),
            )
            .build(&[json_expr("$.value", ScalarValue::Int64(None)).eq(lit(1_i64))])
            .unwrap()
            .unwrap();
            let predicates = applier.compatible_predicate_for_sst(&metadata).unwrap();
            let result = applier
                .apply(index_id, None, &predicates, [(4, true)].into_iter(), None)
                .await
                .unwrap();
            assert_eq!(result[0].1.len(), 1);
            assert_eq!(result[0].1[0], 0..4);
            for expr in [
                json_expr("$.value", ScalarValue::Float64(None)),
                json_expr("$.missing", ScalarValue::Int64(None)),
            ] {
                assert!(
                    BloomFilterIndexApplierBuilder::new(
                        "json_test".into(),
                        PathType::Bare,
                        object_store.clone(),
                        &metadata,
                        factory.clone()
                    )
                    .build(&[expr.eq(lit(1_i64))])
                    .unwrap()
                    .is_none()
                );
            }
        }
    }

    #[tokio::test]
    async fn test_compatible_predicate_for_sst() {
        let (_d, puffin_manager_factory) =
            PuffinManagerFactory::new_for_test_async("test_plan_for_sst_basic_").await;
        let object_store = ObjectStore::new(Memory::default()).unwrap();
        let table_dir = "table_dir".to_string();

        let predicates = BTreeMap::from_iter([(
            IndexTarget::ColumnId(1),
            vec![InListPredicate {
                list: BTreeSet::from_iter([Bytes::from("foo")]),
            }],
        )]);
        let expected_predicate_target_types = BTreeMap::from_iter([(
            IndexTarget::ColumnId(1),
            ConcreteDataType::string_datatype(),
        )]);

        let applier = BloomFilterIndexApplier::new(
            table_dir,
            PathType::Bare,
            object_store,
            puffin_manager_factory,
            predicates,
            expected_predicate_target_types,
        );
        let predicates = applier.compatible_predicate_for_sst(&mock_region_metadata());
        assert!(predicates.is_some());
    }

    #[tokio::test]
    async fn test_compatible_predicate_for_sst_type_mismatch() {
        let (_d, puffin_manager_factory) =
            PuffinManagerFactory::new_for_test_async("test_plan_for_sst_type_mismatch_").await;
        let object_store = ObjectStore::new(Memory::default()).unwrap();
        let table_dir = "table_dir".to_string();

        let predicates = BTreeMap::from_iter([(
            IndexTarget::ColumnId(1),
            vec![InListPredicate {
                list: BTreeSet::from_iter([Bytes::from("foo")]),
            }],
        )]);
        let expected_predicate_target_types =
            BTreeMap::from_iter([(IndexTarget::ColumnId(1), ConcreteDataType::int64_datatype())]);

        let applier = BloomFilterIndexApplier::new(
            table_dir,
            PathType::Bare,
            object_store,
            puffin_manager_factory,
            predicates,
            expected_predicate_target_types,
        );
        let predicates = applier.compatible_predicate_for_sst(&mock_region_metadata());
        assert!(predicates.is_none());
    }

    #[tokio::test]
    async fn test_compatible_predicate_for_sst_partial_type_mismatch() {
        let (_d, puffin_manager_factory) =
            PuffinManagerFactory::new_for_test_async("test_plan_for_sst_partial_mismatch_").await;
        let object_store = ObjectStore::new(Memory::default()).unwrap();
        let table_dir = "table_dir".to_string();

        // Column 1 (tag_str): expected string — matches SST (compatible).
        // Column 3 (field_u64): expected int64 — SST has uint64 (mismatched).
        let predicates = BTreeMap::from_iter([
            (
                IndexTarget::ColumnId(1),
                vec![InListPredicate {
                    list: BTreeSet::from_iter([Bytes::from("foo")]),
                }],
            ),
            (
                IndexTarget::ColumnId(3),
                vec![InListPredicate {
                    list: BTreeSet::from_iter([Bytes::from("bar")]),
                }],
            ),
        ]);
        let expected_predicate_target_types = BTreeMap::from_iter([
            (
                IndexTarget::ColumnId(1),
                ConcreteDataType::string_datatype(),
            ),
            (IndexTarget::ColumnId(3), ConcreteDataType::int64_datatype()), // intentional mismatch
        ]);

        let applier = BloomFilterIndexApplier::new(
            table_dir,
            PathType::Bare,
            object_store,
            puffin_manager_factory,
            predicates,
            expected_predicate_target_types,
        );
        let result = applier.compatible_predicate_for_sst(&mock_region_metadata());

        // The subset containing only the compatible column must be returned.
        let result = result.expect("expected Some with compatible subset");
        assert!(
            result.contains_key(&IndexTarget::ColumnId(1)),
            "compatible column 1 must be present"
        );
        assert!(
            !result.contains_key(&IndexTarget::ColumnId(3)),
            "mismatched column 3 must be absent"
        );
        assert_eq!(result.len(), 1, "only the compatible predicate must remain");
    }

    #[allow(clippy::type_complexity)]
    fn tester(
        table_dir: String,
        object_store: ObjectStore,
        metadata: &RegionMetadata,
        puffin_manager_factory: PuffinManagerFactory,
        file_id: RegionIndexId,
    ) -> impl Fn(&[Expr], Vec<(usize, bool)>) -> BoxFuture<'static, Vec<(usize, Vec<Range<usize>>)>>
    + use<'_> {
        move |exprs, row_groups| {
            let table_dir = table_dir.clone();
            let object_store: ObjectStore = object_store.clone();
            let metadata = metadata.clone();
            let puffin_manager_factory = puffin_manager_factory.clone();
            let exprs = exprs.to_vec();

            Box::pin(async move {
                let builder = BloomFilterIndexApplierBuilder::new(
                    table_dir,
                    PathType::Bare,
                    object_store,
                    &metadata,
                    puffin_manager_factory,
                );

                let applier = builder.build(&exprs).unwrap().unwrap();
                let predicates = applier
                    .compatible_predicate_for_sst(&Arc::new(metadata.clone()))
                    .unwrap();
                applier
                    .apply(file_id, None, &predicates, row_groups.into_iter(), None)
                    .await
                    .unwrap()
                    .into_iter()
                    .filter(|(_, ranges)| !ranges.is_empty())
                    .collect()
            })
        }
    }

    #[tokio::test]
    #[allow(clippy::single_range_in_vec_init)]
    async fn test_bloom_filter_applier() {
        // tag_str:
        //   - type: string
        //   - index: bloom filter
        //   - granularity: 2
        //   - column_id: 1
        //
        // ts:
        //   - type: timestamp
        //   - index: time index
        //   - column_id: 2
        //
        // field_u64:
        //   - type: uint64
        //   - index: bloom filter
        //   - granularity: 4
        //   - column_id: 3
        let region_metadata = mock_region_metadata();
        let prefix = "test_bloom_filter_applier_";
        let (d, factory) = PuffinManagerFactory::new_for_test_async(prefix).await;
        let object_store = mock_object_store();
        let intm_mgr = new_intm_mgr(d.path().to_string_lossy()).await;
        let memory_usage_threshold = Some(1024);
        let file_id = RegionFileId::new(region_metadata.region_id, FileId::random());
        let file_id = RegionIndexId::new(file_id, 0);
        let table_dir = "table_dir".to_string();

        let mut indexer = BloomFilterIndexer::new(
            file_id.file_id(),
            &region_metadata,
            intm_mgr,
            memory_usage_threshold,
            IndexBuildType::Flush,
        )
        .unwrap()
        .unwrap();

        // push 20 rows
        let mut batch = new_batch("tag1", 0..10);
        indexer.update(&mut batch).await.unwrap();
        let mut batch = new_batch("tag2", 10..20);
        indexer.update(&mut batch).await.unwrap();

        let puffin_manager = factory.build(
            object_store.clone(),
            RegionFilePathFactory::new(table_dir.clone(), PathType::Bare),
        );

        let mut puffin_writer = puffin_manager.writer(&file_id).await.unwrap();
        indexer.finish(&mut puffin_writer).await.unwrap();
        puffin_writer.finish().await.unwrap();

        let tester = tester(
            table_dir.clone(),
            object_store.clone(),
            &region_metadata,
            factory.clone(),
            file_id,
        );

        // rows        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
        // row group: | o  row group |  o row group | o  row group     |  o row group     |
        // tag_str:   |      o pred                 |   x pred                            |
        let res = tester(
            &[col("tag_str").eq(lit("tag1"))],
            vec![(5, true), (5, true), (5, true), (5, true)],
        )
        .await;
        assert_eq!(res, vec![(0, vec![0..5]), (1, vec![0..5])]);

        // rows        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
        // row group: | o  row group |  x row group | o  row group     |  o row group     |
        // tag_str:   |      o pred                 |   x pred                            |
        let res = tester(
            &[col("tag_str").eq(lit("tag1"))],
            vec![(5, true), (5, false), (5, true), (5, true)],
        )
        .await;
        assert_eq!(res, vec![(0, vec![0..5])]);

        // rows        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
        // row group: | o  row group |  o row group | o  row group     |  o row group     |
        // tag_str:   |      o pred                 |   x pred                            |
        // field_u64: | o pred   | x pred    |  x pred     |  x pred       | x pred       |
        let res = tester(
            &[
                col("tag_str").eq(lit("tag1")),
                col("field_u64").eq(lit(1u64)),
            ],
            vec![(5, true), (5, true), (5, true), (5, true)],
        )
        .await;
        assert_eq!(res, vec![(0, vec![0..4])]);

        // rows        0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 18, 19
        // row group: | o  row group |  o row group | x  row group     |  o row group     |
        // field_u64: | o pred   | x pred    |  o pred     |  x pred       | x pred       |
        let res = tester(
            &[col("field_u64").in_list(vec![lit(1u64), lit(11u64)], false)],
            vec![(5, true), (5, true), (5, false), (5, true)],
        )
        .await;
        assert_eq!(res, vec![(0, vec![0..4]), (1, vec![3..5])]);
    }
}
