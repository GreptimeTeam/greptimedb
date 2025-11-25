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

pub mod builder;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Instant;

use common_base::range_read::RangeReader;
use common_telemetry::warn;
use index::inverted_index::format::reader::{InvertedIndexBlobReader, InvertedIndexReadMetrics};
use index::inverted_index::search::index_apply::{
    ApplyOutput, IndexApplier, IndexNotFoundStrategy, SearchContext,
};
use index::inverted_index::search::predicate::Predicate;
use object_store::ObjectStore;
use puffin::puffin_manager::cache::PuffinMetadataCacheRef;
use puffin::puffin_manager::{PuffinManager, PuffinReader};
use snafu::ResultExt;
use store_api::region_request::PathType;
use store_api::storage::ColumnId;

use crate::access_layer::{RegionFilePathFactory, WriteCachePathProvider};
use crate::cache::file_cache::{FileCacheRef, FileType, IndexKey};
use crate::cache::index::inverted_index::{CachedInvertedIndexBlobReader, InvertedIndexCacheRef};
use crate::cache::index::result_cache::PredicateKey;
use crate::error::{
    ApplyInvertedIndexSnafu, MetadataSnafu, PuffinBuildReaderSnafu, PuffinReadBlobSnafu, Result,
};
use crate::metrics::{INDEX_APPLY_ELAPSED, INDEX_APPLY_MEMORY_USAGE};
use crate::sst::file::RegionFileId;
use crate::sst::index::TYPE_INVERTED_INDEX;
use crate::sst::index::inverted_index::INDEX_BLOB_TYPE;
use crate::sst::index::puffin_manager::{BlobReader, PuffinManagerFactory};

/// Metrics for tracking inverted index apply operations.
#[derive(Default, Clone)]
pub struct InvertedIndexApplyMetrics {
    /// Total time spent applying the index.
    pub apply_elapsed: std::time::Duration,
    /// Number of blob cache misses (0 or 1).
    pub blob_cache_miss: usize,
    /// Total size of blobs read (in bytes).
    pub blob_read_bytes: u64,
    /// Metrics for inverted index reads.
    pub inverted_index_read_metrics: InvertedIndexReadMetrics,
}

impl std::fmt::Debug for InvertedIndexApplyMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        write!(f, "{{")?;
        let mut first = true;

        if !self.apply_elapsed.is_zero() {
            write!(f, "\"apply_elapsed\":\"{:?}\"", self.apply_elapsed)?;
            first = false;
        }
        if self.blob_cache_miss > 0 {
            if !first {
                write!(f, ", ")?;
            }
            write!(f, "\"blob_cache_miss\":{}", self.blob_cache_miss)?;
            first = false;
        }
        if self.blob_read_bytes > 0 {
            if !first {
                write!(f, ", ")?;
            }
            write!(f, "\"blob_read_bytes\":{}", self.blob_read_bytes)?;
        }

        write!(f, "}}")
    }
}

impl InvertedIndexApplyMetrics {
    /// Merges another metrics into this one.
    pub fn merge_from(&mut self, other: &Self) {
        self.apply_elapsed += other.apply_elapsed;
        self.blob_cache_miss += other.blob_cache_miss;
        self.blob_read_bytes += other.blob_read_bytes;
        self.inverted_index_read_metrics
            .merge_from(&other.inverted_index_read_metrics);
    }
}

/// `InvertedIndexApplier` is responsible for applying predicates to the provided SST files
/// and returning the relevant row group ids for further scan.
pub(crate) struct InvertedIndexApplier {
    /// The root directory of the table.
    table_dir: String,

    /// Path type for generating file paths.
    path_type: PathType,

    /// Store responsible for accessing remote index files.
    store: ObjectStore,

    /// The cache of index files.
    file_cache: Option<FileCacheRef>,

    /// Predefined index applier used to apply predicates to index files
    /// and return the relevant row group ids for further scan.
    index_applier: Box<dyn IndexApplier>,

    /// The puffin manager factory.
    puffin_manager_factory: PuffinManagerFactory,

    /// In-memory cache for inverted index.
    inverted_index_cache: Option<InvertedIndexCacheRef>,

    /// Puffin metadata cache.
    puffin_metadata_cache: Option<PuffinMetadataCacheRef>,

    /// Predicate key. Used to identify the predicate and fetch result from cache.
    predicate_key: PredicateKey,
}

pub(crate) type InvertedIndexApplierRef = Arc<InvertedIndexApplier>;

impl InvertedIndexApplier {
    /// Creates a new `InvertedIndexApplier`.
    pub fn new(
        table_dir: String,
        path_type: PathType,
        store: ObjectStore,
        index_applier: Box<dyn IndexApplier>,
        puffin_manager_factory: PuffinManagerFactory,
        predicates: BTreeMap<ColumnId, Vec<Predicate>>,
    ) -> Self {
        INDEX_APPLY_MEMORY_USAGE.add(index_applier.memory_usage() as i64);

        Self {
            table_dir,
            path_type,
            store,
            file_cache: None,
            index_applier,
            puffin_manager_factory,
            inverted_index_cache: None,
            puffin_metadata_cache: None,
            predicate_key: PredicateKey::new_inverted(Arc::new(predicates)),
        }
    }

    /// Sets the file cache.
    pub fn with_file_cache(mut self, file_cache: Option<FileCacheRef>) -> Self {
        self.file_cache = file_cache;
        self
    }

    /// Sets the index cache.
    pub fn with_index_cache(mut self, index_cache: Option<InvertedIndexCacheRef>) -> Self {
        self.inverted_index_cache = index_cache;
        self
    }

    /// Sets the puffin metadata cache.
    pub fn with_puffin_metadata_cache(
        mut self,
        puffin_metadata_cache: Option<PuffinMetadataCacheRef>,
    ) -> Self {
        self.puffin_metadata_cache = puffin_metadata_cache;
        self
    }

    /// Applies predicates to the provided SST file id and returns the relevant row group ids.
    ///
    /// # Arguments
    /// * `file_id` - The region file ID to apply predicates to
    /// * `file_size_hint` - Optional hint for file size to avoid extra metadata reads
    /// * `metrics` - Optional mutable reference to collect metrics on demand
    pub async fn apply(
        &self,
        file_id: RegionFileId,
        file_size_hint: Option<u64>,
        mut metrics: Option<&mut InvertedIndexApplyMetrics>,
    ) -> Result<ApplyOutput> {
        let start = Instant::now();

        let context = SearchContext {
            // Encountering a non-existing column indicates that it doesn't match predicates.
            index_not_found_strategy: IndexNotFoundStrategy::ReturnEmpty,
        };

        let mut cache_miss = 0;
        let blob = match self.cached_blob_reader(file_id, file_size_hint).await {
            Ok(Some(puffin_reader)) => puffin_reader,
            other => {
                cache_miss += 1;
                if let Err(err) = other {
                    warn!(err; "An unexpected error occurred while reading the cached index file. Fallback to remote index file.")
                }
                self.remote_blob_reader(file_id, file_size_hint).await?
            }
        };

        let blob_size = blob.metadata().await.context(MetadataSnafu)?.content_length;

        let result = if let Some(index_cache) = &self.inverted_index_cache {
            let mut index_reader = CachedInvertedIndexBlobReader::new(
                file_id.file_id(),
                blob_size,
                InvertedIndexBlobReader::new(blob),
                index_cache.clone(),
            );
            self.index_applier
                .apply(
                    context,
                    &mut index_reader,
                    metrics
                        .as_deref_mut()
                        .map(|m| &mut m.inverted_index_read_metrics),
                )
                .await
                .context(ApplyInvertedIndexSnafu)
        } else {
            let mut index_reader = InvertedIndexBlobReader::new(blob);
            self.index_applier
                .apply(
                    context,
                    &mut index_reader,
                    metrics
                        .as_deref_mut()
                        .map(|m| &mut m.inverted_index_read_metrics),
                )
                .await
                .context(ApplyInvertedIndexSnafu)
        };

        // Record elapsed time to histogram and collect metrics if requested
        let elapsed = start.elapsed();
        INDEX_APPLY_ELAPSED
            .with_label_values(&[TYPE_INVERTED_INDEX])
            .observe(elapsed.as_secs_f64());

        if let Some(metrics) = metrics {
            metrics.apply_elapsed = elapsed;
            metrics.blob_cache_miss = cache_miss;
            metrics.blob_read_bytes = blob_size;
        }

        result
    }

    /// Creates a blob reader from the cached index file.
    async fn cached_blob_reader(
        &self,
        file_id: RegionFileId,
        file_size_hint: Option<u64>,
    ) -> Result<Option<BlobReader>> {
        let Some(file_cache) = &self.file_cache else {
            return Ok(None);
        };

        let index_key = IndexKey::new(file_id.region_id(), file_id.file_id(), FileType::Puffin);
        if file_cache.get(index_key).await.is_none() {
            return Ok(None);
        };

        let puffin_manager = self.puffin_manager_factory.build(
            file_cache.local_store(),
            WriteCachePathProvider::new(file_cache.clone()),
        );

        // Adds file size hint to the puffin reader to avoid extra metadata read.
        let reader = puffin_manager
            .reader(&file_id)
            .await
            .context(PuffinBuildReaderSnafu)?
            .with_file_size_hint(file_size_hint)
            .blob(INDEX_BLOB_TYPE)
            .await
            .context(PuffinReadBlobSnafu)?
            .reader()
            .await
            .context(PuffinBuildReaderSnafu)?;
        Ok(Some(reader))
    }

    /// Creates a blob reader from the remote index file.
    async fn remote_blob_reader(
        &self,
        file_id: RegionFileId,
        file_size_hint: Option<u64>,
    ) -> Result<BlobReader> {
        let puffin_manager = self
            .puffin_manager_factory
            .build(
                self.store.clone(),
                RegionFilePathFactory::new(self.table_dir.clone(), self.path_type),
            )
            .with_puffin_metadata_cache(self.puffin_metadata_cache.clone());

        puffin_manager
            .reader(&file_id)
            .await
            .context(PuffinBuildReaderSnafu)?
            .with_file_size_hint(file_size_hint)
            .blob(INDEX_BLOB_TYPE)
            .await
            .context(PuffinReadBlobSnafu)?
            .reader()
            .await
            .context(PuffinBuildReaderSnafu)
    }

    /// Returns the predicate key.
    pub fn predicate_key(&self) -> &PredicateKey {
        &self.predicate_key
    }
}

impl Drop for InvertedIndexApplier {
    fn drop(&mut self) {
        INDEX_APPLY_MEMORY_USAGE.sub(self.index_applier.memory_usage() as i64);
    }
}

#[cfg(test)]
mod tests {
    use futures::io::Cursor;
    use index::bitmap::Bitmap;
    use index::inverted_index::search::index_apply::MockIndexApplier;
    use object_store::services::Memory;
    use puffin::puffin_manager::PuffinWriter;
    use store_api::storage::FileId;

    use super::*;

    #[tokio::test]
    async fn test_index_applier_apply_basic() {
        let (_d, puffin_manager_factory) =
            PuffinManagerFactory::new_for_test_async("test_index_applier_apply_basic_").await;
        let object_store = ObjectStore::new(Memory::default()).unwrap().finish();
        let file_id = RegionFileId::new(0.into(), FileId::random());
        let table_dir = "table_dir".to_string();

        let puffin_manager = puffin_manager_factory.build(
            object_store.clone(),
            RegionFilePathFactory::new(table_dir.clone(), PathType::Bare),
        );
        let mut writer = puffin_manager.writer(&file_id).await.unwrap();
        writer
            .put_blob(
                INDEX_BLOB_TYPE,
                Cursor::new(vec![]),
                Default::default(),
                Default::default(),
            )
            .await
            .unwrap();
        writer.finish().await.unwrap();

        let mut mock_index_applier = MockIndexApplier::new();
        mock_index_applier.expect_memory_usage().returning(|| 100);
        mock_index_applier.expect_apply().returning(|_, _, _| {
            Ok(ApplyOutput {
                matched_segment_ids: Bitmap::new_bitvec(),
                total_row_count: 100,
                segment_row_count: 10,
            })
        });

        let sst_index_applier = InvertedIndexApplier::new(
            table_dir.clone(),
            PathType::Bare,
            object_store,
            Box::new(mock_index_applier),
            puffin_manager_factory,
            Default::default(),
        );
        let output = sst_index_applier.apply(file_id, None, None).await.unwrap();
        assert_eq!(
            output,
            ApplyOutput {
                matched_segment_ids: Bitmap::new_bitvec(),
                total_row_count: 100,
                segment_row_count: 10,
            }
        );
    }

    #[tokio::test]
    async fn test_index_applier_apply_invalid_blob_type() {
        let (_d, puffin_manager_factory) =
            PuffinManagerFactory::new_for_test_async("test_index_applier_apply_invalid_blob_type_")
                .await;
        let object_store = ObjectStore::new(Memory::default()).unwrap().finish();
        let file_id = RegionFileId::new(0.into(), FileId::random());
        let table_dir = "table_dir".to_string();

        let puffin_manager = puffin_manager_factory.build(
            object_store.clone(),
            RegionFilePathFactory::new(table_dir.clone(), PathType::Bare),
        );
        let mut writer = puffin_manager.writer(&file_id).await.unwrap();
        writer
            .put_blob(
                "invalid_blob_type",
                Cursor::new(vec![]),
                Default::default(),
                Default::default(),
            )
            .await
            .unwrap();
        writer.finish().await.unwrap();

        let mut mock_index_applier = MockIndexApplier::new();
        mock_index_applier.expect_memory_usage().returning(|| 100);
        mock_index_applier.expect_apply().never();

        let sst_index_applier = InvertedIndexApplier::new(
            table_dir.clone(),
            PathType::Bare,
            object_store,
            Box::new(mock_index_applier),
            puffin_manager_factory,
            Default::default(),
        );
        let res = sst_index_applier.apply(file_id, None, None).await;
        assert!(format!("{:?}", res.unwrap_err()).contains("Blob not found"));
    }
}
