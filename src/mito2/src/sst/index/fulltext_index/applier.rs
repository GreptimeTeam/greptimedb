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

use std::collections::{BTreeSet, HashMap};
use std::sync::Arc;

use common_telemetry::warn;
use index::fulltext_index::search::{FulltextIndexSearcher, RowId, TantivyFulltextIndexSearcher};
use object_store::ObjectStore;
use puffin::puffin_manager::cache::PuffinMetadataCacheRef;
use puffin::puffin_manager::{DirGuard, PuffinManager, PuffinReader};
use snafu::ResultExt;
use store_api::storage::{ColumnId, RegionId};

use crate::access_layer::{RegionFilePathFactory, WriteCachePathProvider};
use crate::cache::file_cache::{FileCacheRef, FileType, IndexKey};
use crate::error::{ApplyFulltextIndexSnafu, PuffinBuildReaderSnafu, PuffinReadBlobSnafu, Result};
use crate::metrics::INDEX_APPLY_ELAPSED;
use crate::sst::file::FileId;
use crate::sst::index::fulltext_index::applier::builder::FulltextRequest;
use crate::sst::index::fulltext_index::INDEX_BLOB_TYPE_TANTIVY;
use crate::sst::index::puffin_manager::{PuffinManagerFactory, SstPuffinDir};
use crate::sst::index::TYPE_FULLTEXT_INDEX;

pub mod builder;

/// `FulltextIndexApplier` is responsible for applying fulltext index to the provided SST files
pub struct FulltextIndexApplier {
    /// The root directory of the region.
    region_dir: String,

    /// The region ID.
    region_id: RegionId,

    /// Requests to be applied.
    requests: HashMap<ColumnId, FulltextRequest>,

    /// The puffin manager factory.
    puffin_manager_factory: PuffinManagerFactory,

    /// Store responsible for accessing index files.
    store: ObjectStore,

    /// File cache to be used by the `FulltextIndexApplier`.
    file_cache: Option<FileCacheRef>,

    /// The puffin metadata cache.
    puffin_metadata_cache: Option<PuffinMetadataCacheRef>,
}

pub type FulltextIndexApplierRef = Arc<FulltextIndexApplier>;

impl FulltextIndexApplier {
    /// Creates a new `FulltextIndexApplier`.
    pub fn new(
        region_dir: String,
        region_id: RegionId,
        store: ObjectStore,
        requests: HashMap<ColumnId, FulltextRequest>,
        puffin_manager_factory: PuffinManagerFactory,
    ) -> Self {
        Self {
            region_dir,
            region_id,
            store,
            requests,
            puffin_manager_factory,
            file_cache: None,
            puffin_metadata_cache: None,
        }
    }

    /// Sets the file cache.
    pub fn with_file_cache(mut self, file_cache: Option<FileCacheRef>) -> Self {
        self.file_cache = file_cache;
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

    /// Applies the queries to the fulltext index of the specified SST file.
    pub async fn apply(
        &self,
        file_id: FileId,
        file_size_hint: Option<u64>,
    ) -> Result<BTreeSet<RowId>> {
        let _timer = INDEX_APPLY_ELAPSED
            .with_label_values(&[TYPE_FULLTEXT_INDEX])
            .start_timer();

        let mut inited = false;
        let mut row_ids = BTreeSet::new();

        'outer: for (column_id, request) in &self.requests {
            let dir = self
                .index_dir_path(file_id, *column_id, file_size_hint)
                .await?;
            let path = match &dir {
                Some(dir) => dir.path(),
                None => {
                    // Return empty set if the index not found.
                    return Ok(BTreeSet::new());
                }
            };

            let searcher =
                TantivyFulltextIndexSearcher::new(path).context(ApplyFulltextIndexSnafu)?;

            for query in &request.queries {
                let result = searcher
                    .search(&query.0)
                    .await
                    .context(ApplyFulltextIndexSnafu)?;

                if !inited {
                    row_ids = result;
                    inited = true;
                    continue;
                }

                row_ids.retain(|id| result.contains(id));
                if row_ids.is_empty() {
                    break 'outer;
                }
            }
        }

        Ok(row_ids)
    }

    /// Returns `None` if the index not found.
    async fn index_dir_path(
        &self,
        file_id: FileId,
        column_id: ColumnId,
        file_size_hint: Option<u64>,
    ) -> Result<Option<SstPuffinDir>> {
        let blob_key = format!("{INDEX_BLOB_TYPE_TANTIVY}-{column_id}");

        // FAST PATH: Try to read the index from the file cache.
        if let Some(file_cache) = &self.file_cache {
            let index_key = IndexKey::new(self.region_id, file_id, FileType::Puffin);
            if file_cache.get(index_key).await.is_some() {
                match self
                    .get_index_from_file_cache(file_cache, file_id, file_size_hint, &blob_key)
                    .await
                {
                    Ok(dir) => return Ok(dir),
                    Err(err) => {
                        warn!(err; "An unexpected error occurred while reading the cached index file. Fallback to remote index file.")
                    }
                }
            }
        }

        // SLOW PATH: Try to read the index from the remote file.
        self.get_index_from_remote_file(file_id, file_size_hint, &blob_key)
            .await
    }

    async fn get_index_from_file_cache(
        &self,
        file_cache: &FileCacheRef,
        file_id: FileId,
        file_size_hint: Option<u64>,
        blob_key: &str,
    ) -> Result<Option<SstPuffinDir>> {
        match self
            .puffin_manager_factory
            .build(
                file_cache.local_store(),
                WriteCachePathProvider::new(self.region_id, file_cache.clone()),
            )
            .reader(&file_id)
            .await
            .context(PuffinBuildReaderSnafu)?
            .with_file_size_hint(file_size_hint)
            .dir(blob_key)
            .await
        {
            Ok(dir) => Ok(Some(dir)),
            Err(puffin::error::Error::BlobNotFound { .. }) => Ok(None),
            Err(err) => Err(err).context(PuffinReadBlobSnafu),
        }
    }

    async fn get_index_from_remote_file(
        &self,
        file_id: FileId,
        file_size_hint: Option<u64>,
        blob_key: &str,
    ) -> Result<Option<SstPuffinDir>> {
        match self
            .puffin_manager_factory
            .build(
                self.store.clone(),
                RegionFilePathFactory::new(self.region_dir.clone()),
            )
            .reader(&file_id)
            .await
            .context(PuffinBuildReaderSnafu)?
            .with_file_size_hint(file_size_hint)
            .dir(blob_key)
            .await
        {
            Ok(dir) => Ok(Some(dir)),
            Err(puffin::error::Error::BlobNotFound { .. }) => Ok(None),
            Err(err) => Err(err).context(PuffinReadBlobSnafu),
        }
    }
}
