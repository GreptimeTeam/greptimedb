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

use std::sync::Arc;

use common_base::range_read::RangeReaderAdapter;
use common_telemetry::warn;
use index::inverted_index::format::reader::InvertedIndexBlobReader;
use index::inverted_index::search::index_apply::{
    ApplyOutput, IndexApplier, IndexNotFoundStrategy, SearchContext,
};
use object_store::ObjectStore;
use puffin::puffin_manager::{BlobGuard, PuffinManager, PuffinReader};
use snafu::ResultExt;
use store_api::storage::RegionId;

use crate::cache::file_cache::{FileCacheRef, FileType, IndexKey};
use crate::cache::index::{CachedInvertedIndexBlobReader, InvertedIndexCacheRef};
use crate::error::{ApplyInvertedIndexSnafu, PuffinBuildReaderSnafu, PuffinReadBlobSnafu, Result};
use crate::metrics::{INDEX_APPLY_ELAPSED, INDEX_APPLY_MEMORY_USAGE};
use crate::sst::file::FileId;
use crate::sst::index::inverted_index::INDEX_BLOB_TYPE;
use crate::sst::index::puffin_manager::{BlobReader, PuffinManagerFactory};
use crate::sst::index::TYPE_INVERTED_INDEX;
use crate::sst::location;

/// `InvertedIndexApplier` is responsible for applying predicates to the provided SST files
/// and returning the relevant row group ids for further scan.
pub(crate) struct InvertedIndexApplier {
    /// The root directory of the region.
    region_dir: String,

    /// Region ID.
    region_id: RegionId,

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
}

pub(crate) type InvertedIndexApplierRef = Arc<InvertedIndexApplier>;

impl InvertedIndexApplier {
    /// Creates a new `InvertedIndexApplier`.
    pub fn new(
        region_dir: String,
        region_id: RegionId,
        store: ObjectStore,
        file_cache: Option<FileCacheRef>,
        index_cache: Option<InvertedIndexCacheRef>,
        index_applier: Box<dyn IndexApplier>,
        puffin_manager_factory: PuffinManagerFactory,
    ) -> Self {
        INDEX_APPLY_MEMORY_USAGE.add(index_applier.memory_usage() as i64);

        Self {
            region_dir,
            region_id,
            store,
            file_cache,
            index_applier,
            puffin_manager_factory,
            inverted_index_cache: index_cache,
        }
    }

    /// Applies predicates to the provided SST file id and returns the relevant row group ids
    pub async fn apply(&self, file_id: FileId) -> Result<ApplyOutput> {
        let _timer = INDEX_APPLY_ELAPSED
            .with_label_values(&[TYPE_INVERTED_INDEX])
            .start_timer();

        let context = SearchContext {
            // Encountering a non-existing column indicates that it doesn't match predicates.
            index_not_found_strategy: IndexNotFoundStrategy::ReturnEmpty,
        };

        let blob = match self.cached_blob_reader(file_id).await {
            Ok(Some(puffin_reader)) => puffin_reader,
            other => {
                if let Err(err) = other {
                    warn!(err; "An unexpected error occurred while reading the cached index file. Fallback to remote index file.")
                }
                self.remote_blob_reader(file_id).await?
            }
        };
        let blob = RangeReaderAdapter(blob);

        if let Some(index_cache) = &self.inverted_index_cache {
            let mut index_reader = CachedInvertedIndexBlobReader::new(
                file_id,
                InvertedIndexBlobReader::new(blob),
                index_cache.clone(),
            );
            self.index_applier
                .apply(context, &mut index_reader)
                .await
                .context(ApplyInvertedIndexSnafu)
        } else {
            let mut index_reader = InvertedIndexBlobReader::new(blob);
            self.index_applier
                .apply(context, &mut index_reader)
                .await
                .context(ApplyInvertedIndexSnafu)
        }
    }

    /// Creates a blob reader from the cached index file.
    async fn cached_blob_reader(&self, file_id: FileId) -> Result<Option<BlobReader>> {
        let Some(file_cache) = &self.file_cache else {
            return Ok(None);
        };

        let index_key = IndexKey::new(self.region_id, file_id, FileType::Puffin);
        if file_cache.get(index_key).await.is_none() {
            return Ok(None);
        };

        let puffin_manager = self.puffin_manager_factory.build(file_cache.local_store());
        let puffin_file_name = file_cache.cache_file_path(index_key);

        let reader = puffin_manager
            .reader(&puffin_file_name)
            .await
            .context(PuffinBuildReaderSnafu)?
            .blob(INDEX_BLOB_TYPE)
            .await
            .context(PuffinReadBlobSnafu)?
            .reader()
            .await
            .context(PuffinBuildReaderSnafu)?;
        Ok(Some(reader))
    }

    /// Creates a blob reader from the remote index file.
    async fn remote_blob_reader(&self, file_id: FileId) -> Result<BlobReader> {
        let puffin_manager = self.puffin_manager_factory.build(self.store.clone());
        let file_path = location::index_file_path(&self.region_dir, file_id);
        puffin_manager
            .reader(&file_path)
            .await
            .context(PuffinBuildReaderSnafu)?
            .blob(INDEX_BLOB_TYPE)
            .await
            .context(PuffinReadBlobSnafu)?
            .reader()
            .await
            .context(PuffinBuildReaderSnafu)
    }
}

impl Drop for InvertedIndexApplier {
    fn drop(&mut self) {
        INDEX_APPLY_MEMORY_USAGE.sub(self.index_applier.memory_usage() as i64);
    }
}

#[cfg(test)]
mod tests {
    use common_base::BitVec;
    use futures::io::Cursor;
    use index::inverted_index::search::index_apply::MockIndexApplier;
    use object_store::services::Memory;
    use puffin::puffin_manager::PuffinWriter;

    use super::*;

    #[tokio::test]
    async fn test_index_applier_apply_basic() {
        let (_d, puffin_manager_factory) =
            PuffinManagerFactory::new_for_test_async("test_index_applier_apply_basic_").await;
        let object_store = ObjectStore::new(Memory::default()).unwrap().finish();
        let file_id = FileId::random();
        let region_dir = "region_dir".to_string();
        let path = location::index_file_path(&region_dir, file_id);

        let puffin_manager = puffin_manager_factory.build(object_store.clone());
        let mut writer = puffin_manager.writer(&path).await.unwrap();
        writer
            .put_blob(INDEX_BLOB_TYPE, Cursor::new(vec![]), Default::default())
            .await
            .unwrap();
        writer.finish().await.unwrap();

        let mut mock_index_applier = MockIndexApplier::new();
        mock_index_applier.expect_memory_usage().returning(|| 100);
        mock_index_applier.expect_apply().returning(|_, _| {
            Ok(ApplyOutput {
                matched_segment_ids: BitVec::EMPTY,
                total_row_count: 100,
                segment_row_count: 10,
            })
        });

        let sst_index_applier = InvertedIndexApplier::new(
            region_dir.clone(),
            RegionId::new(0, 0),
            object_store,
            None,
            None,
            Box::new(mock_index_applier),
            puffin_manager_factory,
        );
        let output = sst_index_applier.apply(file_id).await.unwrap();
        assert_eq!(
            output,
            ApplyOutput {
                matched_segment_ids: BitVec::EMPTY,
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
        let file_id = FileId::random();
        let region_dir = "region_dir".to_string();
        let path = location::index_file_path(&region_dir, file_id);

        let puffin_manager = puffin_manager_factory.build(object_store.clone());
        let mut writer = puffin_manager.writer(&path).await.unwrap();
        writer
            .put_blob("invalid_blob_type", Cursor::new(vec![]), Default::default())
            .await
            .unwrap();
        writer.finish().await.unwrap();

        let mut mock_index_applier = MockIndexApplier::new();
        mock_index_applier.expect_memory_usage().returning(|| 100);
        mock_index_applier.expect_apply().never();

        let sst_index_applier = InvertedIndexApplier::new(
            region_dir.clone(),
            RegionId::new(0, 0),
            object_store,
            None,
            None,
            Box::new(mock_index_applier),
            puffin_manager_factory,
        );
        let res = sst_index_applier.apply(file_id).await;
        assert!(format!("{:?}", res.unwrap_err()).contains("Blob not found"));
    }
}
