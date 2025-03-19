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

use std::collections::{BTreeSet, HashMap, HashSet};
use std::ops::Range;
use std::sync::Arc;

use common_base::range_read::RangeReader;
use common_telemetry::warn;
use index::bloom_filter::applier::BloomFilterApplier;
use index::bloom_filter::reader::BloomFilterReaderImpl;
use index::fulltext_index::create::read_fulltext_config;
use index::fulltext_index::search::{FulltextIndexSearcher, RowId, TantivyFulltextIndexSearcher};
use object_store::ObjectStore;
use puffin::puffin_manager::cache::PuffinMetadataCacheRef;
use puffin::puffin_manager::{BlobWithMetadata, DirGuard, PuffinManager, PuffinReader};
use smallvec::SmallVec;
use snafu::ResultExt;
use store_api::storage::{ColumnId, RegionId};

use crate::access_layer::{RegionFilePathFactory, WriteCachePathProvider};
use crate::cache::file_cache::{FileCacheRef, FileType, IndexKey};
use crate::cache::index::bloom_filter_index::{
    BloomFilterIndexCacheRef, CachedBloomFilterIndexBlobReader,
};
use crate::error::{
    ApplyFulltextIndexSnafu, MetadataSnafu, PuffinBuildReaderSnafu, PuffinReadBlobSnafu, Result,
};
use crate::metrics::INDEX_APPLY_ELAPSED;
use crate::sst::file::FileId;
use crate::sst::index::fulltext_index::{
    FulltextPredicate, INDEX_BLOB_TYPE_BLOOM, INDEX_BLOB_TYPE_TANTIVY,
};
use crate::sst::index::puffin_manager::{
    PuffinManagerFactory, SstPuffinBlob, SstPuffinDir, SstPuffinReader,
};
use crate::sst::index::TYPE_FULLTEXT_INDEX;

pub mod builder;

/// `FulltextIndexApplier` is responsible for applying fulltext index to the provided SST files
pub struct FulltextIndexApplier {
    predicates: HashMap<ColumnId, SmallVec<[FulltextPredicate; 1]>>,

    /// Cache for bloom filter index.
    bloom_filter_index_cache: Option<BloomFilterIndexCacheRef>,

    index_source: IndexSource,
}

pub type FulltextIndexApplierRef = Arc<FulltextIndexApplier>;

impl FulltextIndexApplier {
    /// Creates a new `FulltextIndexApplier`.
    pub fn new(
        region_dir: String,
        region_id: RegionId,
        store: ObjectStore,
        predicates: HashMap<ColumnId, SmallVec<[FulltextPredicate; 1]>>,
        puffin_manager_factory: PuffinManagerFactory,
    ) -> Self {
        let index_source = IndexSource::new(region_dir, region_id, puffin_manager_factory, store);
        Self {
            predicates,
            bloom_filter_index_cache: None,
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

    pub fn with_bloom_filter_cache(
        mut self,
        bloom_filter_index_cache: Option<BloomFilterIndexCacheRef>,
    ) -> Self {
        self.bloom_filter_index_cache = bloom_filter_index_cache;
        self
    }
}

impl FulltextIndexApplier {
    /// Applies the queries to the fulltext index of the specified SST file.
    pub async fn apply_matches(
        &self,
        file_id: FileId,
        file_size_hint: Option<u64>,
    ) -> Result<Option<BTreeSet<RowId>>> {
        let _timer = INDEX_APPLY_ELAPSED
            .with_label_values(&[TYPE_FULLTEXT_INDEX])
            .start_timer();

        let mut row_ids: Option<BTreeSet<RowId>> = None;

        for (column_id, predicates) in &self.predicates {
            if !predicates
                .iter()
                .any(|p| matches!(p, FulltextPredicate::Matches(_)))
            {
                continue;
            }

            let Some(result) = self
                .apply_matches_one_col(file_size_hint, file_id, *column_id, predicates)
                .await?
            else {
                continue;
            };

            match row_ids.as_mut() {
                Some(row_ids) => {
                    row_ids.retain(|id| result.contains(id));
                }
                None => row_ids = Some(result),
            };

            if let Some(row_ids) = &row_ids {
                if row_ids.is_empty() {
                    break;
                }
            }
        }

        Ok(row_ids)
    }

    async fn apply_matches_one_col(
        &self,
        file_size_hint: Option<u64>,
        file_id: FileId,
        column_id: ColumnId,
        predicates: &[FulltextPredicate],
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

        let mut inited = false;
        let mut row_ids = BTreeSet::new();
        for predicate in predicates {
            if let FulltextPredicate::Matches(m) = predicate {
                let result = searcher
                    .search(&m.query)
                    .await
                    .context(ApplyFulltextIndexSnafu)?;

                if !inited {
                    row_ids = result;
                    inited = true;
                    continue;
                }

                row_ids.retain(|id| result.contains(id));
            }
        }

        Ok(Some(row_ids))
    }
}

impl FulltextIndexApplier {
    pub async fn apply_matches_term(
        &self,
        file_id: FileId,
        file_size_hint: Option<u64>,
        row_groups: impl Iterator<Item = (usize, bool)>,
    ) -> Result<Vec<(usize, Vec<Range<usize>>)>> {
        let _timer = INDEX_APPLY_ELAPSED
            .with_label_values(&[TYPE_FULLTEXT_INDEX])
            .start_timer();

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

        for (column_id, predicates) in &self.predicates {
            if !predicates
                .iter()
                .any(|p| matches!(p, FulltextPredicate::MatchesTerm(_)))
            {
                continue;
            }

            self.apply_matches_term_one_col(
                file_id,
                *column_id,
                predicates,
                file_size_hint,
                &mut output,
            )
            .await?;
        }

        // adjust ranges to be based on row group
        for ((_, output), (_, input)) in output.iter_mut().zip(input) {
            let start = input.start;
            for range in output.iter_mut() {
                range.start -= start;
                range.end -= start;
            }
        }
        output.retain(|(_, ranges)| !ranges.is_empty());

        Ok(output)
    }

    async fn apply_matches_term_one_col(
        &self,
        file_id: FileId,
        column_id: ColumnId,
        predicates: &[FulltextPredicate],
        file_size_hint: Option<u64>,
        output: &mut [(usize, Vec<Range<usize>>)],
    ) -> Result<()> {
        let blob_key = format!("{INDEX_BLOB_TYPE_BLOOM}-{column_id}");
        let Some(reader) = self
            .index_source
            .blob(file_id, &blob_key, file_size_hint)
            .await?
        else {
            return Ok(());
        };

        let blob_metadata = reader.metadata();
        let fulltext_config =
            read_fulltext_config(blob_metadata).context(ApplyFulltextIndexSnafu)?;

        let mut probes = HashSet::new();
        for predicate in predicates {
            if let FulltextPredicate::MatchesTerm(pred) = predicate {
                if fulltext_config.case_sensitive && pred.col_lowered {
                    // The index is useless since lowercased column is not indexed.
                    continue;
                }
                let ps = pred
                    .term
                    .split(|c: char| !c.is_alphanumeric())
                    .filter(|&t| !t.is_empty())
                    .map(|t| {
                        if !fulltext_config.case_sensitive {
                            t.to_lowercase()
                        } else {
                            t.to_string()
                        }
                        .into_bytes()
                    });

                probes.extend(ps);
            }
        }
        if probes.is_empty() {
            return Ok(());
        }

        let range_reader = reader.reader().await.context(PuffinBuildReaderSnafu)?;
        let reader = if let Some(bloom_filter_cache) = &self.bloom_filter_index_cache {
            let blob_size = range_reader
                .metadata()
                .await
                .context(MetadataSnafu)?
                .content_length;
            let reader = CachedBloomFilterIndexBlobReader::new(
                file_id,
                column_id,
                blob_size,
                BloomFilterReaderImpl::new(range_reader),
                bloom_filter_cache.clone(),
            );
            Box::new(reader) as _
        } else {
            Box::new(BloomFilterReaderImpl::new(range_reader)) as _
        };

        let mut applier = BloomFilterApplier::new(reader).await.unwrap();

        for (_, output) in output.iter_mut() {
            if output.is_empty() {
                continue;
            }

            *output = applier.search(&probes, output).await.unwrap();
        }

        Ok(())
    }
}

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
