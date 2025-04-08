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
use puffin::puffin_manager::{BlobWithMetadata, DirGuard, PuffinManager, PuffinReader};
use snafu::ResultExt;
use store_api::storage::{ColumnId, RegionId};

use crate::access_layer::{RegionFilePathFactory, WriteCachePathProvider};
use crate::cache::file_cache::{FileCacheRef, FileType, IndexKey};
use crate::error::{ApplyFulltextIndexSnafu, PuffinBuildReaderSnafu, PuffinReadBlobSnafu, Result};
use crate::metrics::INDEX_APPLY_ELAPSED;
use crate::sst::file::FileId;
use crate::sst::index::fulltext_index::applier::builder::FulltextRequest;
use crate::sst::index::fulltext_index::INDEX_BLOB_TYPE_TANTIVY;
use crate::sst::index::puffin_manager::{
    PuffinManagerFactory, SstPuffinBlob, SstPuffinDir, SstPuffinReader,
};
use crate::sst::index::TYPE_FULLTEXT_INDEX;

pub mod builder;

/// `FulltextIndexApplier` is responsible for applying fulltext index to the provided SST files
pub struct FulltextIndexApplier {
    /// Requests to be applied.
    requests: HashMap<ColumnId, FulltextRequest>,

    /// The source of the index.
    index_source: IndexSource,
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
        let index_source = IndexSource::new(region_dir, region_id, puffin_manager_factory, store);

        Self {
            requests,
            index_source,
        }
    }

    /// Sets the file cache.
    pub fn with_file_cache(mut self, file_cache: Option<FileCacheRef>) -> Self {
        self.index_source.set_file_cache(file_cache);
        self
    }

    /// Sets the puffin metadata cache.
    pub fn with_puffin_metadata_cache(
        mut self,
        puffin_metadata_cache: Option<PuffinMetadataCacheRef>,
    ) -> Self {
        self.index_source
            .set_puffin_metadata_cache(puffin_metadata_cache);
        self
    }

    /// Applies the queries to the fulltext index of the specified SST file.
    pub async fn apply(
        &self,
        file_id: FileId,
        file_size_hint: Option<u64>,
    ) -> Result<Option<BTreeSet<RowId>>> {
        let _timer = INDEX_APPLY_ELAPSED
            .with_label_values(&[TYPE_FULLTEXT_INDEX])
            .start_timer();

        let mut row_ids: Option<BTreeSet<RowId>> = None;
        for (column_id, request) in &self.requests {
            if request.queries.is_empty() {
                continue;
            }

            let Some(result) = self
                .apply_one_column(file_size_hint, file_id, *column_id, request)
                .await?
            else {
                continue;
            };

            if let Some(ids) = row_ids.as_mut() {
                ids.retain(|id| result.contains(id));
            } else {
                row_ids = Some(result);
            }

            if let Some(ids) = row_ids.as_ref() {
                if ids.is_empty() {
                    break;
                }
            }
        }

        Ok(row_ids)
    }

    async fn apply_one_column(
        &self,
        file_size_hint: Option<u64>,
        file_id: FileId,
        column_id: ColumnId,
        request: &FulltextRequest,
    ) -> Result<Option<BTreeSet<RowId>>> {
        let blob_key = format!("{INDEX_BLOB_TYPE_TANTIVY}-{column_id}");
        let dir = self
            .index_source
            .dir(file_id, &blob_key, file_size_hint)
            .await?;

        let path = match &dir {
            Some(dir) => dir.path(),
            None => {
                return Ok(None);
            }
        };

        let searcher = TantivyFulltextIndexSearcher::new(path).context(ApplyFulltextIndexSnafu)?;
        let mut row_ids: Option<BTreeSet<RowId>> = None;
        for query in &request.queries {
            let result = searcher
                .search(&query.0)
                .await
                .context(ApplyFulltextIndexSnafu)?;

            if let Some(ids) = row_ids.as_mut() {
                ids.retain(|id| result.contains(id));
            } else {
                row_ids = Some(result);
            }

            if let Some(ids) = row_ids.as_ref() {
                if ids.is_empty() {
                    break;
                }
            }
        }

        Ok(row_ids)
    }
}

/// The source of the index.
struct IndexSource {
    region_dir: String,
    region_id: RegionId,

    /// The puffin manager factory.
    puffin_manager_factory: PuffinManagerFactory,

    /// Store responsible for accessing remote index files.
    remote_store: ObjectStore,

    /// Local file cache.
    file_cache: Option<FileCacheRef>,

    /// The puffin metadata cache.
    puffin_metadata_cache: Option<PuffinMetadataCacheRef>,
}

impl IndexSource {
    fn new(
        region_dir: String,
        region_id: RegionId,
        puffin_manager_factory: PuffinManagerFactory,
        remote_store: ObjectStore,
    ) -> Self {
        Self {
            region_dir,
            region_id,
            puffin_manager_factory,
            remote_store,
            file_cache: None,
            puffin_metadata_cache: None,
        }
    }

    fn set_file_cache(&mut self, file_cache: Option<FileCacheRef>) {
        self.file_cache = file_cache;
    }

    fn set_puffin_metadata_cache(&mut self, puffin_metadata_cache: Option<PuffinMetadataCacheRef>) {
        self.puffin_metadata_cache = puffin_metadata_cache;
    }

    /// Returns the blob with the specified key from local cache or remote store.
    ///
    /// Returns `None` if the blob is not found.
    #[allow(unused)]
    async fn blob(
        &self,
        file_id: FileId,
        key: &str,
        file_size_hint: Option<u64>,
    ) -> Result<Option<BlobWithMetadata<SstPuffinBlob>>> {
        let (reader, fallbacked) = self.ensure_reader(file_id, file_size_hint).await?;
        let res = reader.blob(key).await;
        match res {
            Ok(blob) => Ok(Some(blob)),
            Err(err) if err.is_blob_not_found() => Ok(None),
            Err(err) => {
                if fallbacked {
                    Err(err).context(PuffinReadBlobSnafu)
                } else {
                    warn!(err; "An unexpected error occurred while reading the cached index file. Fallback to remote index file.");
                    let reader = self.build_remote(file_id, file_size_hint).await?;
                    let res = reader.blob(key).await;
                    match res {
                        Ok(blob) => Ok(Some(blob)),
                        Err(err) if err.is_blob_not_found() => Ok(None),
                        Err(err) => Err(err).context(PuffinReadBlobSnafu),
                    }
                }
            }
        }
    }

    /// Returns the directory with the specified key from local cache or remote store.
    ///
    /// Returns `None` if the directory is not found.
    async fn dir(
        &self,
        file_id: FileId,
        key: &str,
        file_size_hint: Option<u64>,
    ) -> Result<Option<SstPuffinDir>> {
        let (reader, fallbacked) = self.ensure_reader(file_id, file_size_hint).await?;
        let res = reader.dir(key).await;
        match res {
            Ok(dir) => Ok(Some(dir)),
            Err(err) if err.is_blob_not_found() => Ok(None),
            Err(err) => {
                if fallbacked {
                    Err(err).context(PuffinReadBlobSnafu)
                } else {
                    warn!(err; "An unexpected error occurred while reading the cached index file. Fallback to remote index file.");
                    let reader = self.build_remote(file_id, file_size_hint).await?;
                    let res = reader.dir(key).await;
                    match res {
                        Ok(dir) => Ok(Some(dir)),
                        Err(err) if err.is_blob_not_found() => Ok(None),
                        Err(err) => Err(err).context(PuffinReadBlobSnafu),
                    }
                }
            }
        }
    }

    /// Return reader and whether it is fallbacked to remote store.
    async fn ensure_reader(
        &self,
        file_id: FileId,
        file_size_hint: Option<u64>,
    ) -> Result<(SstPuffinReader, bool)> {
        match self.build_local_cache(file_id, file_size_hint).await {
            Ok(Some(r)) => Ok((r, false)),
            Ok(None) => Ok((self.build_remote(file_id, file_size_hint).await?, true)),
            Err(err) => Err(err),
        }
    }

    async fn build_local_cache(
        &self,
        file_id: FileId,
        file_size_hint: Option<u64>,
    ) -> Result<Option<SstPuffinReader>> {
        let Some(file_cache) = &self.file_cache else {
            return Ok(None);
        };

        let index_key = IndexKey::new(self.region_id, file_id, FileType::Puffin);
        if file_cache.get(index_key).await.is_none() {
            return Ok(None);
        };

        let puffin_manager = self
            .puffin_manager_factory
            .build(
                file_cache.local_store(),
                WriteCachePathProvider::new(self.region_id, file_cache.clone()),
            )
            .with_puffin_metadata_cache(self.puffin_metadata_cache.clone());
        let reader = puffin_manager
            .reader(&file_id)
            .await
            .context(PuffinBuildReaderSnafu)?
            .with_file_size_hint(file_size_hint);
        Ok(Some(reader))
    }

    async fn build_remote(
        &self,
        file_id: FileId,
        file_size_hint: Option<u64>,
    ) -> Result<SstPuffinReader> {
        let puffin_manager = self
            .puffin_manager_factory
            .build(
                self.remote_store.clone(),
                RegionFilePathFactory::new(self.region_dir.clone()),
            )
            .with_puffin_metadata_cache(self.puffin_metadata_cache.clone());

        let reader = puffin_manager
            .reader(&file_id)
            .await
            .context(PuffinBuildReaderSnafu)?
            .with_file_size_hint(file_size_hint);

        Ok(reader)
    }
}
