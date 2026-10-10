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
use common_base::term_token::{like_probes, term_probes};
use common_telemetry::{tracing, warn};
use index::bloom_filter::applier::{BloomFilterApplier, InListPredicate};
use index::bloom_filter::reader::{BloomFilterReadMetrics, BloomFilterReaderImpl};
use index::fulltext_index::Config;
use index::fulltext_index::search::{FulltextIndexSearcher, RowId, TantivyFulltextIndexSearcher};
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
use crate::sst::index::fulltext_index::applier::builder::FulltextRequest;
use crate::sst::index::fulltext_index::{INDEX_BLOB_TYPE_BLOOM, INDEX_BLOB_TYPE_TANTIVY};
use crate::sst::index::puffin_manager::{
    PuffinManagerFactory, SstPuffinBlob, SstPuffinDir, SstPuffinReader,
};
use crate::sst::index::{TYPE_FULLTEXT_INDEX, trigger_index_background_download};

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
    #[tracing::instrument(
        skip_all,
        fields(file_id = %file_id)
    )]
    pub async fn apply_fine(
        &self,
        file_id: RegionIndexId,
        file_size_hint: Option<u64>,
        mut metrics: Option<&mut FulltextIndexApplyMetrics>,
    ) -> Result<Option<BTreeSet<RowId>>> {
        let apply_start = Instant::now();

        let mut row_ids: Option<BTreeSet<RowId>> = None;
        for (column_id, request) in self.requests.iter() {
            // Tantivy tokens don't follow `matches_term` boundaries, e.g. `SimpleTokenizer`
            // keeps "错误error日志" as one token, so terms and patterns are left to the
            // bloom backend.
            if request.queries.is_empty() {
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
    #[allow(clippy::type_complexity)]
    #[tracing::instrument(
        skip_all,
        fields(file_id = %file_id)
    )]
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
            if request.terms.is_empty() && request.like_patterns.is_empty() {
                continue;
            }

            applied |= self
                .apply_coarse_one_column(
                    file_id,
                    file_size_hint,
                    *column_id,
                    request,
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
        request: &FulltextRequest,
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

        let predicates = Self::request_to_predicates(request, &config);
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
                file_id.version,
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
        let mut row_groups = output.iter_mut().map(|(_, r)| r).collect::<Vec<_>>();
        applier
            .search_groups(
                &predicates,
                &mut row_groups,
                metrics.map(|m| &mut m.bloom_filter_read_metrics),
            )
            .await
            .context(ApplyBloomFilterIndexSnafu)?;

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

    /// Converts terms and `LIKE` patterns to predicates, one per token that a matching
    /// row must contain. Multiple predicates are combined with AND semantics.
    fn request_to_predicates(request: &FulltextRequest, config: &Config) -> Vec<InListPredicate> {
        // `matches_term(lower(col), ..)` matches against the lowercased text, whose word
        // boundaries may differ from the indexed text's, e.g. U+212A KELVIN SIGN
        // lowercases to an ASCII 'k'. So lowercased columns get no probes.
        let term_probes = request
            .terms
            .iter()
            .filter(|term| !term.col_lowered)
            .flat_map(|term| term_probes(&term.term));
        let like_probes = request
            .like_patterns
            .iter()
            .flat_map(|pattern| like_probes(pattern));

        let probes = term_probes
            .chain(like_probes)
            .map(|probe| {
                if config.case_sensitive {
                    probe
                } else {
                    probe.to_lowercase()
                }
                .into_bytes()
            })
            .collect::<HashSet<_>>();

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
        let path_factory = RegionFilePathFactory::new(self.table_dir.clone(), self.path_type);

        // Trigger background download if file cache and file size are available
        trigger_index_background_download(
            self.file_cache.as_ref(),
            &file_id,
            file_size_hint,
            &path_factory,
            &self.remote_store,
        );

        let puffin_manager = self
            .puffin_manager_factory
            .build(self.remote_store.clone(), path_factory)
            .with_puffin_metadata_cache(self.puffin_metadata_cache.clone());

        let reader = puffin_manager
            .reader(&file_id)
            .await
            .context(PuffinBuildReaderSnafu)?
            .with_file_size_hint(file_size_hint);

        Ok(reader)
    }
}

#[cfg(test)]
mod tests {
    use common_function::scalars::matches_term::MatchesTermFinder;
    use datatypes::arrow::array::{Scalar, StringArray};
    use datatypes::arrow::compute::kernels::comparison::like;
    use index::fulltext_index::Analyzer;
    use index::fulltext_index::tokenizer::{self, ScriptTokenizer};
    use rand::rngs::StdRng;
    use rand::{Rng, SeedableRng};

    use super::*;
    use crate::sst::index::fulltext_index::applier::builder::FulltextTerm;

    enum Predicate<'a> {
        Term(&'a str),
        Like(&'a str),
    }

    impl Predicate<'_> {
        fn eval(&self, text: &str) -> bool {
            match self {
                Predicate::Term(term) => MatchesTermFinder::new(term).find(text),
                Predicate::Like(pattern) => like(
                    &StringArray::from(vec![text]),
                    &Scalar::new(StringArray::from(vec![*pattern])),
                )
                .unwrap()
                .value(0),
            }
        }

        fn request(&self) -> FulltextRequest {
            let mut request = FulltextRequest::default();
            match self {
                Predicate::Term(term) => request.terms.push(FulltextTerm {
                    col_lowered: false,
                    term: term.to_string(),
                }),
                Predicate::Like(pattern) => request.like_patterns.push(pattern.to_string()),
            }
            request
        }

        /// Asserts that every probe is among `text`'s indexed tokens, i.e. the bloom
        /// filter keeps the row, and returns the number of probes.
        fn assert_probes_indexed(&self, text: &str) -> usize {
            let mut num_probes = 0;
            for case_sensitive in [true, false] {
                let config = Config {
                    analyzer: Analyzer::English,
                    case_sensitive,
                };
                let tokens = tokenizer::Analyzer::new(Box::new(ScriptTokenizer), case_sensitive)
                    .analyze_text(text)
                    .unwrap()
                    .into_iter()
                    .collect::<HashSet<_>>();
                let predicates =
                    FulltextIndexApplier::request_to_predicates(&self.request(), &config);
                for predicate in &predicates {
                    assert!(
                        predicate.list.iter().any(|probe| tokens.contains(probe)),
                        "probe {:?} not indexed, text: {text:?}, case_sensitive: {case_sensitive}",
                        predicate
                            .list
                            .iter()
                            .map(|p| String::from_utf8_lossy(p))
                            .collect::<Vec<_>>(),
                    );
                }
                num_probes = predicates.len();
            }
            num_probes
        }
    }

    #[test]
    fn test_probes_keep_matching_rows() {
        // (text, predicate, whether the predicate yields probes)
        let cases = [
            ("hello_world", Predicate::Term("world"), true),
            ("trace_id=abc", Predicate::Term("id"), true),
            ("错误error日志", Predicate::Term("error"), true),
            (
                "登录手机号18888888888的动态key",
                Predicate::Term("手机号"),
                true,
            ),
            (
                "登录手机号18888888888的动态key",
                Predicate::Term("机号"),
                true,
            ),
            (
                "登录手机号18888888888的动态key",
                Predicate::Term("18888888888"),
                true,
            ),
            // The ASCII edge of a Han term may continue in the text.
            (
                "登录手机号18888888888的动态key",
                Predicate::Term("机号1888"),
                true,
            ),
            ("中国农业银行", Predicate::Term("农业"), true),
            ("中国农业银行", Predicate::Term("农"), false),
            ("连接timeout.5次", Predicate::Term("timeout"), true),
            ("用户user-123登录", Predicate::Term("user"), true),
            (
                "CRITICAL error: disk",
                Predicate::Term("CRITICAL error"),
                true,
            ),
            ("café>", Predicate::Term("café"), true),
            (
                "connection timeout after 5s",
                Predicate::Like("%timeout%"),
                false,
            ),
            (
                "connection timeout after 5s",
                Predicate::Like("% timeout %"),
                true,
            ),
            (
                "connection timeout after 5s",
                Predicate::Like("connection%"),
                false,
            ),
            (
                "connection timeout after 5s",
                Predicate::Like("connection %"),
                true,
            ),
            (
                "connection timeout after 5s",
                Predicate::Like("%after 5s"),
                true,
            ),
            ("trace_id=abc", Predicate::Like(r"trace\_id=%"), true),
            ("trace_id=abc", Predicate::Like("trace_id=%"), false),
            ("100% done", Predicate::Like(r"100\% done"), true),
            ("错误error日志", Predicate::Like("%错误error日%"), true),
            ("错误error日志", Predicate::Like("%r日志"), true),
        ];
        for (text, predicate, has_probes) in cases {
            assert!(predicate.eval(text), "case doesn't match: {text:?}");
            assert_eq!(
                predicate.assert_probes_indexed(text) > 0,
                has_probes,
                "text: {text:?}"
            );
        }
    }

    #[test]
    fn test_probes_keep_matching_rows_random() {
        // Word characters of every class, separators, LIKE metacharacters, and
        // characters whose lowercase changes length or class.
        const ALPHABET: &[char] = &[
            'a', 'b', 'A', '1', '_', '-', ' ', '.', '%', '\\', '错', '误', 'é', 'Д', '\u{212A}',
            '\u{130}', 'Σ',
        ];
        let mut rng = StdRng::seed_from_u64(42);
        let random_char = |rng: &mut StdRng| ALPHABET[rng.random_range(0..ALPHABET.len())];

        for _ in 0..20000 {
            let text = (0..rng.random_range(0..12))
                .map(|_| random_char(&mut rng))
                .collect::<String>();
            let chars = text.chars().collect::<Vec<_>>();
            let start = rng.random_range(0..=chars.len());
            let end = rng.random_range(start..=chars.len());

            let term = chars[start..end].iter().collect::<String>();
            let predicate = Predicate::Term(&term);
            if predicate.eval(&text) {
                predicate.assert_probes_indexed(&text);
            }

            let mut pattern = String::new();
            if start > 0 && rng.random_bool(0.5) {
                pattern.push('%');
            }
            for &c in &chars[start..end] {
                match rng.random_range(0..10) {
                    0 => pattern.push('%'),
                    1 => pattern.push('_'),
                    _ if matches!(c, '%' | '_' | '\\') => {
                        pattern.push('\\');
                        pattern.push(c);
                    }
                    _ => pattern.push(c),
                }
            }
            if end < chars.len() && rng.random_bool(0.5) {
                pattern.push('%');
            }
            let predicate = Predicate::Like(&pattern);
            if predicate.eval(&text) {
                predicate.assert_probes_indexed(&text);
            }
        }
    }
}
