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

use std::collections::{BTreeMap, BTreeSet, HashSet};
use std::iter;
use std::ops::Range;
use std::sync::Arc;

use common_base::range_read::RangeReader;
use common_telemetry::warn;
use index::bloom_filter::applier::{BloomFilterApplier, InListPredicate};
use index::bloom_filter::reader::BloomFilterReaderImpl;
use index::fulltext_index::search::{FulltextIndexSearcher, RowId, TantivyFulltextIndexSearcher};
use index::fulltext_index::Config;
use object_store::ObjectStore;
use puffin::puffin_manager::cache::PuffinMetadataCacheRef;
use puffin::puffin_manager::{GuardWithMetadata, PuffinManager, PuffinReader};
use snafu::ResultExt;
use store_api::storage::{ColumnId, RegionId};

use crate::access_layer::{RegionFilePathFactory, WriteCachePathProvider};
use crate::cache::file_cache::{FileCacheRef, FileType, IndexKey};
use crate::cache::index::bloom_filter_index::{
    BloomFilterIndexCacheRef, CachedBloomFilterIndexBlobReader, Tag,
};
use crate::cache::index::result_cache::PredicateKey;
use crate::error::{
    ApplyBloomFilterIndexSnafu, ApplyFulltextIndexSnafu, MetadataSnafu, PuffinBuildReaderSnafu,
    PuffinReadBlobSnafu, Result,
};
use crate::metrics::INDEX_APPLY_ELAPSED;
use crate::sst::file::FileId;
use crate::sst::index::fulltext_index::applier::builder::{FulltextRequest, FulltextTerm};
use crate::sst::index::fulltext_index::{INDEX_BLOB_TYPE_BLOOM, INDEX_BLOB_TYPE_TANTIVY};
use crate::sst::index::puffin_manager::{
    PuffinManagerFactory, SstPuffinBlob, SstPuffinDir, SstPuffinReader,
};
use crate::sst::index::TYPE_FULLTEXT_INDEX;

pub mod builder;

/// `FulltextIndexApplier` is responsible for applying fulltext index to the provided SST files
pub struct FulltextIndexApplier {
    /// Requests to be applied.
    requests: Arc<BTreeMap<ColumnId, FulltextRequest>>,

    /// The source of the index.
    index_source: IndexSource,

    /// Cache for bloom filter index.
    bloom_filter_index_cache: Option<BloomFilterIndexCacheRef>,

    /// Predicate key. Used to identify the predicate and fetch result from cache.
    predicate_key: PredicateKey,
}

pub type FulltextIndexApplierRef = Arc<FulltextIndexApplier>;

impl FulltextIndexApplier {
    /// Creates a new `FulltextIndexApplier`.
    pub fn new(
        region_dir: String,
        region_id: RegionId,
        store: ObjectStore,
        requests: BTreeMap<ColumnId, FulltextRequest>,
        puffin_manager_factory: PuffinManagerFactory,
    ) -> Self {
        let requests = Arc::new(requests);
        let index_source = IndexSource::new(region_dir, region_id, puffin_manager_factory, store);

        Self {
            predicate_key: PredicateKey::new_fulltext(requests.clone()),
            requests,
            index_source,
            bloom_filter_index_cache: None,
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

    /// Sets the bloom filter cache.
    pub fn with_bloom_filter_cache(
        mut self,
        bloom_filter_index_cache: Option<BloomFilterIndexCacheRef>,
    ) -> Self {
        self.bloom_filter_index_cache = bloom_filter_index_cache;
        self
    }

    /// Returns the predicate key.
    pub fn predicate_key(&self) -> &PredicateKey {
        &self.predicate_key
    }
}

impl FulltextIndexApplier {
    /// Applies fine-grained fulltext index to the specified SST file.
    /// Returns the row ids that match the queries.
    pub async fn apply_fine(
        &self,
        file_id: FileId,
        file_size_hint: Option<u64>,
    ) -> Result<Option<BTreeSet<RowId>>> {
        let timer = INDEX_APPLY_ELAPSED
            .with_label_values(&[TYPE_FULLTEXT_INDEX])
            .start_timer();

        let mut row_ids: Option<BTreeSet<RowId>> = None;
        for (column_id, request) in self.requests.iter() {
            if request.queries.is_empty() && request.terms.is_empty() {
                continue;
            }

            let Some(result) = self
                .apply_fine_one_column(file_size_hint, file_id, *column_id, request)
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

        if row_ids.is_none() {
            timer.stop_and_discard();
        }
        Ok(row_ids)
    }

    async fn apply_fine_one_column(
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

        let dir = match &dir {
            Some(dir) => dir,
            None => {
                return Ok(None);
            }
        };

        let config = Config::from_blob_metadata(dir.metadata()).context(ApplyFulltextIndexSnafu)?;
        let path = dir.path();

        let searcher =
            TantivyFulltextIndexSearcher::new(path, config).context(ApplyFulltextIndexSnafu)?;
        let mut row_ids: Option<BTreeSet<RowId>> = None;

        // 1. Apply queries
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

        // 2. Apply terms
        let query = request.terms_as_query(config.case_sensitive);
        if !query.0.is_empty() {
            let result = searcher
                .search(&query.0)
                .await
                .context(ApplyFulltextIndexSnafu)?;

            if let Some(ids) = row_ids.as_mut() {
                ids.retain(|id| result.contains(id));
            } else {
                row_ids = Some(result);
            }
        }

        Ok(row_ids)
    }
}

impl FulltextIndexApplier {
    /// Applies coarse-grained fulltext index to the specified SST file.
    /// Returns (row group id -> ranges) that match the queries.
    pub async fn apply_coarse(
        &self,
        file_id: FileId,
        file_size_hint: Option<u64>,
        row_groups: impl Iterator<Item = (usize, bool)>,
    ) -> Result<Option<Vec<(usize, Vec<Range<usize>>)>>> {
        let timer = INDEX_APPLY_ELAPSED
            .with_label_values(&[TYPE_FULLTEXT_INDEX])
            .start_timer();

        let (input, mut output) = Self::init_coarse_output(row_groups);
        let mut applied = false;

        for (column_id, request) in self.requests.iter() {
            if request.terms.is_empty() {
                // only apply terms
                continue;
            }

            applied |= self
                .apply_coarse_one_column(
                    file_id,
                    file_size_hint,
                    *column_id,
                    &request.terms,
                    &mut output,
                )
                .await?;
        }

        if !applied {
            timer.stop_and_discard();
            return Ok(None);
        }

        Self::adjust_coarse_output(input, &mut output);
        Ok(Some(output))
    }

    async fn apply_coarse_one_column(
        &self,
        file_id: FileId,
        file_size_hint: Option<u64>,
        column_id: ColumnId,
        terms: &[FulltextTerm],
        output: &mut [(usize, Vec<Range<usize>>)],
    ) -> Result<bool> {
        let blob_key = format!("{INDEX_BLOB_TYPE_BLOOM}-{column_id}");
        let Some(reader) = self
            .index_source
            .blob(file_id, &blob_key, file_size_hint)
            .await?
        else {
            return Ok(false);
        };
        let config =
            Config::from_blob_metadata(reader.metadata()).context(ApplyFulltextIndexSnafu)?;

        let predicates = Self::terms_to_predicates(terms, &config);
        if predicates.is_empty() {
            return Ok(false);
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
                Tag::Fulltext,
                blob_size,
                BloomFilterReaderImpl::new(range_reader),
                bloom_filter_cache.clone(),
            );
            Box::new(reader) as _
        } else {
            Box::new(BloomFilterReaderImpl::new(range_reader)) as _
        };

        let mut applier = BloomFilterApplier::new(reader)
            .await
            .context(ApplyBloomFilterIndexSnafu)?;
        for (_, row_group_output) in output.iter_mut() {
            // All rows are filtered out, skip the search
            if row_group_output.is_empty() {
                continue;
            }

            *row_group_output = applier
                .search(&predicates, row_group_output)
                .await
                .context(ApplyBloomFilterIndexSnafu)?;
        }

        Ok(true)
    }

    /// Initializes the coarse output. Must call `adjust_coarse_output` after applying bloom filters.
    ///
    /// `row_groups` is a list of (row group length, whether to search).
    ///
    /// Returns (`input`, `output`):
    /// * `input` is a list of (row group index to search, row group range based on start of the file).
    /// * `output` is a list of (row group index to search, row group ranges based on start of the file).
    #[allow(clippy::type_complexity)]
    fn init_coarse_output(
        row_groups: impl Iterator<Item = (usize, bool)>,
    ) -> (Vec<(usize, Range<usize>)>, Vec<(usize, Vec<Range<usize>>)>) {
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
        let output = input
            .iter()
            .map(|(i, range)| (*i, vec![range.clone()]))
            .collect::<Vec<_>>();

        (input, output)
    }

    /// Adjusts the coarse output. Makes the output ranges based on row group start.
    fn adjust_coarse_output(
        input: Vec<(usize, Range<usize>)>,
        output: &mut Vec<(usize, Vec<Range<usize>>)>,
    ) {
        // adjust ranges to be based on row group
        for ((_, output), (_, input)) in output.iter_mut().zip(input) {
            let start = input.start;
            for range in output.iter_mut() {
                range.start -= start;
                range.end -= start;
            }
        }
        output.retain(|(_, ranges)| !ranges.is_empty());
    }

    /// Converts terms to predicates.
    ///
    /// Split terms by non-alphanumeric characters and convert them to lowercase if case-insensitive.
    /// Multiple terms are combined with AND semantics.
    fn terms_to_predicates(terms: &[FulltextTerm], config: &Config) -> Vec<InListPredicate> {
        let mut probes = HashSet::new();
        for term in terms {
            if config.case_sensitive && term.col_lowered {
                // lowercased terms are not indexed
                continue;
            }

            let ts = term
                .term
                .split(|c: char| !c.is_alphanumeric())
                .filter(|&t| !t.is_empty())
                .map(|t| {
                    if !config.case_sensitive {
                        t.to_lowercase()
                    } else {
                        t.to_string()
                    }
                    .into_bytes()
                });

            probes.extend(ts);
        }

        probes
            .into_iter()
            .map(|p| InListPredicate {
                list: iter::once(p).collect(),
            })
            .collect::<Vec<_>>()
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
    ) -> Result<Option<GuardWithMetadata<SstPuffinBlob>>> {
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
    ) -> Result<Option<GuardWithMetadata<SstPuffinDir>>> {
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
