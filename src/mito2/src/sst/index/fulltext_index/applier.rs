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
use std::time::Instant;

use common_base::range_read::RangeReader;
use common_telemetry::warn;
use index::bloom_filter::applier::{BloomFilterApplier, InListPredicate};
use index::bloom_filter::reader::{BloomFilterReadMetrics, BloomFilterReaderImpl};
use index::fulltext_index::search::{FulltextIndexSearcher, RowId, TantivyFulltextIndexSearcher};
use index::fulltext_index::tokenizer::{ChineseTokenizer, EnglishTokenizer, Tokenizer};
use index::fulltext_index::{Analyzer, Config};
use index::target::IndexTarget;
use object_store::ObjectStore;
use puffin::puffin_manager::cache::PuffinMetadataCacheRef;
use puffin::puffin_manager::{GuardWithMetadata, PuffinManager, PuffinReader};
use snafu::ResultExt;
use store_api::region_request::PathType;
use store_api::storage::ColumnId;

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
use crate::sst::file::RegionIndexId;
use crate::sst::index::TYPE_FULLTEXT_INDEX;
use crate::sst::index::fulltext_index::applier::builder::{FulltextRequest, FulltextTerm};
use crate::sst::index::fulltext_index::{INDEX_BLOB_TYPE_BLOOM, INDEX_BLOB_TYPE_TANTIVY};
use crate::sst::index::puffin_manager::{
    PuffinManagerFactory, SstPuffinBlob, SstPuffinDir, SstPuffinReader,
};

pub mod builder;

/// Metrics for tracking fulltext index apply operations.
#[derive(Default, Clone)]
pub struct FulltextIndexApplyMetrics {
    /// Total time spent applying the index.
    pub apply_elapsed: std::time::Duration,
    /// Number of blob cache misses.
    pub blob_cache_miss: usize,
    /// Number of directory cache hits.
    pub dir_cache_hit: usize,
    /// Number of directory cache misses.
    pub dir_cache_miss: usize,
    /// Elapsed time to initialize directory data.
    pub dir_init_elapsed: std::time::Duration,
    /// Metrics for bloom filter reads.
    pub bloom_filter_read_metrics: BloomFilterReadMetrics,
}

impl std::fmt::Debug for FulltextIndexApplyMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        let Self {
            apply_elapsed,
            blob_cache_miss,
            dir_cache_hit,
            dir_cache_miss,
            dir_init_elapsed,
            bloom_filter_read_metrics,
        } = self;

        if self.is_empty() {
            return write!(f, "{{}}");
        }
        write!(f, "{{")?;

        write!(f, "\"apply_elapsed\":\"{:?}\"", apply_elapsed)?;

        if *blob_cache_miss > 0 {
            write!(f, ", \"blob_cache_miss\":{}", blob_cache_miss)?;
        }
        if *dir_cache_hit > 0 {
            write!(f, ", \"dir_cache_hit\":{}", dir_cache_hit)?;
        }
        if *dir_cache_miss > 0 {
            write!(f, ", \"dir_cache_miss\":{}", dir_cache_miss)?;
        }
        if !dir_init_elapsed.is_zero() {
            write!(f, ", \"dir_init_elapsed\":\"{:?}\"", dir_init_elapsed)?;
        }
        write!(
            f,
            ", \"bloom_filter_read_metrics\":{:?}",
            bloom_filter_read_metrics
        )?;

        write!(f, "}}")
    }
}

impl FulltextIndexApplyMetrics {
    /// Returns true if the metrics are empty (contain no meaningful data).
    pub fn is_empty(&self) -> bool {
        self.apply_elapsed.is_zero()
    }

    /// Collects metrics from a directory read operation.
    pub fn collect_dir_metrics(
        &mut self,
        elapsed: std::time::Duration,
        dir_metrics: puffin::puffin_manager::DirMetrics,
    ) {
        self.dir_init_elapsed += elapsed;
        if dir_metrics.cache_hit {
            self.dir_cache_hit += 1;
        } else {
            self.dir_cache_miss += 1;
        }
    }

    /// Merges another metrics into this one.
    pub fn merge_from(&mut self, other: &Self) {
        self.apply_elapsed += other.apply_elapsed;
        self.blob_cache_miss += other.blob_cache_miss;
        self.dir_cache_hit += other.dir_cache_hit;
        self.dir_cache_miss += other.dir_cache_miss;
        self.dir_init_elapsed += other.dir_init_elapsed;
        self.bloom_filter_read_metrics
            .merge_from(&other.bloom_filter_read_metrics);
    }
}

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
        table_dir: String,
        path_type: PathType,
        store: ObjectStore,
        requests: BTreeMap<ColumnId, FulltextRequest>,
        puffin_manager_factory: PuffinManagerFactory,
    ) -> Self {
        let requests = Arc::new(requests);
        let index_source = IndexSource::new(table_dir, path_type, puffin_manager_factory, store);

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
    ///
    /// # Arguments
    /// * `file_id` - The region file ID to apply predicates to
    /// * `file_size_hint` - Optional hint for file size to avoid extra metadata reads
    /// * `metrics` - Optional mutable reference to collect metrics on demand
    pub async fn apply_fine(
        &self,
        file_id: RegionIndexId,
        file_size_hint: Option<u64>,
        mut metrics: Option<&mut FulltextIndexApplyMetrics>,
    ) -> Result<Option<BTreeSet<RowId>>> {
        let apply_start = Instant::now();

        let mut row_ids: Option<BTreeSet<RowId>> = None;
        for (column_id, request) in self.requests.iter() {
            if request.queries.is_empty() && request.terms.is_empty() {
                continue;
            }

            let Some(result) = self
                .apply_fine_one_column(
                    file_size_hint,
                    file_id,
                    *column_id,
                    request,
                    metrics.as_deref_mut(),
                )
                .await?
            else {
                continue;
            };

            if let Some(ids) = row_ids.as_mut() {
                ids.retain(|id| result.contains(id));
            } else {
                row_ids = Some(result);
            }

            if let Some(ids) = row_ids.as_ref()
                && ids.is_empty()
            {
                break;
            }
        }

        // Record elapsed time to histogram and collect metrics if requested
        let elapsed = apply_start.elapsed();
        INDEX_APPLY_ELAPSED
            .with_label_values(&[TYPE_FULLTEXT_INDEX])
            .observe(elapsed.as_secs_f64());

        if let Some(m) = metrics {
            m.apply_elapsed += elapsed;
        }

        Ok(row_ids)
    }

    async fn apply_fine_one_column(
        &self,
        file_size_hint: Option<u64>,
        file_id: RegionIndexId,
        column_id: ColumnId,
        request: &FulltextRequest,
        metrics: Option<&mut FulltextIndexApplyMetrics>,
    ) -> Result<Option<BTreeSet<RowId>>> {
        let blob_key = format!(
            "{INDEX_BLOB_TYPE_TANTIVY}-{}",
            IndexTarget::ColumnId(column_id)
        );
        let dir = self
            .index_source
            .dir(file_id, &blob_key, file_size_hint, metrics)
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

            if let Some(ids) = row_ids.as_ref()
                && ids.is_empty()
            {
                break;
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
    ///
    /// Row group id existing in the returned result means that the row group is searched.
    /// Empty ranges means that the row group is searched but no rows are found.
    ///
    /// # Arguments
    /// * `file_id` - The region file ID to apply predicates to
    /// * `file_size_hint` - Optional hint for file size to avoid extra metadata reads
    /// * `row_groups` - Iterator of row group lengths and whether to search in the row group
    /// * `metrics` - Optional mutable reference to collect metrics on demand
    pub async fn apply_coarse(
        &self,
        file_id: RegionIndexId,
        file_size_hint: Option<u64>,
        row_groups: impl Iterator<Item = (usize, bool)>,
        mut metrics: Option<&mut FulltextIndexApplyMetrics>,
    ) -> Result<Option<Vec<(usize, Vec<Range<usize>>)>>> {
        let apply_start = Instant::now();

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
                    metrics.as_deref_mut(),
                )
                .await?;
        }

        if !applied {
            return Ok(None);
        }

        Self::adjust_coarse_output(input, &mut output);

        // Record elapsed time to histogram and collect metrics if requested
        let elapsed = apply_start.elapsed();
        INDEX_APPLY_ELAPSED
            .with_label_values(&[TYPE_FULLTEXT_INDEX])
            .observe(elapsed.as_secs_f64());

        if let Some(m) = metrics {
            m.apply_elapsed += elapsed;
        }

        Ok(Some(output))
    }

    async fn apply_coarse_one_column(
        &self,
        file_id: RegionIndexId,
        file_size_hint: Option<u64>,
        column_id: ColumnId,
        terms: &[FulltextTerm],
        output: &mut [(usize, Vec<Range<usize>>)],
        mut metrics: Option<&mut FulltextIndexApplyMetrics>,
    ) -> Result<bool> {
        let blob_key = format!(
            "{INDEX_BLOB_TYPE_BLOOM}-{}",
            IndexTarget::ColumnId(column_id)
        );
        let Some(reader) = self
            .index_source
            .blob(file_id, &blob_key, file_size_hint, metrics.as_deref_mut())
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
                file_id.file_id(),
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
                .search(
                    &predicates,
                    row_group_output,
                    metrics
                        .as_deref_mut()
                        .map(|m| &mut m.bloom_filter_read_metrics),
                )
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
        output: &mut [(usize, Vec<Range<usize>>)],
    ) {
        // adjust ranges to be based on row group
        for ((_, output), (_, input)) in output.iter_mut().zip(input) {
            let start = input.start;
            for range in output.iter_mut() {
                range.start -= start;
                range.end -= start;
            }
        }
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
            probes.extend(Self::term_to_probes(&term.term, config));
        }

        probes
            .into_iter()
            .map(|p| InListPredicate {
                list: iter::once(p).collect(),
            })
            .collect::<Vec<_>>()
    }

    fn term_to_probes<'a>(term: &'a str, config: &'a Config) -> impl Iterator<Item = Vec<u8>> + 'a {
        let tokens = match config.analyzer {
            Analyzer::English => EnglishTokenizer {}.tokenize(term),
            Analyzer::Chinese => ChineseTokenizer {}.tokenize(term),
        };

        tokens.into_iter().map(|t| {
            if !config.case_sensitive {
                t.to_lowercase()
            } else {
                t.to_string()
            }
            .into_bytes()
        })
    }
}

/// The source of the index.
struct IndexSource {
    table_dir: String,

    /// Path type for generating file paths.
    path_type: PathType,

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
        table_dir: String,
        path_type: PathType,
        puffin_manager_factory: PuffinManagerFactory,
        remote_store: ObjectStore,
    ) -> Self {
        Self {
            table_dir,
            path_type,
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
        file_id: RegionIndexId,
        key: &str,
        file_size_hint: Option<u64>,
        metrics: Option<&mut FulltextIndexApplyMetrics>,
    ) -> Result<Option<GuardWithMetadata<SstPuffinBlob>>> {
        let (reader, fallbacked) = self.ensure_reader(file_id, file_size_hint).await?;

        // Track cache miss if fallbacked to remote
        if fallbacked && let Some(m) = metrics {
            m.blob_cache_miss += 1;
        }

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
        file_id: RegionIndexId,
        key: &str,
        file_size_hint: Option<u64>,
        mut metrics: Option<&mut FulltextIndexApplyMetrics>,
    ) -> Result<Option<GuardWithMetadata<SstPuffinDir>>> {
        let (reader, fallbacked) = self.ensure_reader(file_id, file_size_hint).await?;

        // Track cache miss if fallbacked to remote
        if fallbacked && let Some(m) = &mut metrics {
            m.blob_cache_miss += 1;
        }

        let start = metrics.as_ref().map(|_| Instant::now());
        let res = reader.dir(key).await;
        match res {
            Ok((dir, dir_metrics)) => {
                if let Some(m) = metrics {
                    // Safety: start is Some when metrics is Some
                    m.collect_dir_metrics(start.unwrap().elapsed(), dir_metrics);
                }
                Ok(Some(dir))
            }
            Err(err) if err.is_blob_not_found() => Ok(None),
            Err(err) => {
                if fallbacked {
                    Err(err).context(PuffinReadBlobSnafu)
                } else {
                    warn!(err; "An unexpected error occurred while reading the cached index file. Fallback to remote index file.");
                    let reader = self.build_remote(file_id, file_size_hint).await?;
                    let start = metrics.as_ref().map(|_| Instant::now());
                    let res = reader.dir(key).await;
                    match res {
                        Ok((dir, dir_metrics)) => {
                            if let Some(m) = metrics {
                                // Safety: start is Some when metrics is Some
                                m.collect_dir_metrics(start.unwrap().elapsed(), dir_metrics);
                            }
                            Ok(Some(dir))
                        }
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
        file_id: RegionIndexId,
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
        file_id: RegionIndexId,
        file_size_hint: Option<u64>,
    ) -> Result<Option<SstPuffinReader>> {
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

        let puffin_manager = self
            .puffin_manager_factory
            .build(
                file_cache.local_store(),
                WriteCachePathProvider::new(file_cache.clone()),
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
        file_id: RegionIndexId,
        file_size_hint: Option<u64>,
    ) -> Result<SstPuffinReader> {
        let puffin_manager = self
            .puffin_manager_factory
            .build(
                self.remote_store.clone(),
                RegionFilePathFactory::new(self.table_dir.clone(), self.path_type),
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
