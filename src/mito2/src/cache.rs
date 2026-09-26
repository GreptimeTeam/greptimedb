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

//! Cache for the engine.

pub(crate) mod cache_size;

pub(crate) mod file_cache;
pub(crate) mod file_keys;
pub(crate) mod index;
pub(crate) mod manifest_cache;
#[cfg(test)]
pub(crate) mod test_util;
pub(crate) mod write_cache;

use std::collections::{BTreeMap, HashMap};
use std::mem;
use std::ops::Range;
use std::sync::{Arc, RwLock, Weak};

use bytes::Bytes;
use common_base::readable_size::ReadableSize;
use common_datasource::compression::CompressionType;
use common_runtime::Runtime;
use common_runtime::runtime::RuntimeTrait;
use common_telemetry::warn;
use datatypes::arrow::buffer::BooleanBuffer;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::types::json_type::JsonNativeType;
use datatypes::value::Value;
use datatypes::vectors::VectorRef;
use index::bloom_filter_index::{BloomFilterIndexCache, BloomFilterIndexCacheRef};
use index::result_cache::IndexResultCache;
use moka::notification::RemovalCause;
use moka::sync::Cache;
use object_store::ObjectStore;
use parquet::arrow::arrow_reader::{RowSelection, RowSelector};
use parquet::file::metadata::{
    FileMetaData, PageIndexPolicy, ParquetMetaData, ParquetMetaDataReader, ParquetMetaDataWriter,
};
use puffin::puffin_manager::cache::{PuffinMetadataCache, PuffinMetadataCacheRef};
use smallvec::SmallVec;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::{RegionMetadata, RegionMetadataRef};
use store_api::storage::{ColumnId, ConcreteDataType, FileId, RegionId, TimeSeriesRowSelector};
pub use write_cache::{WriteCacheUploadStoreWrapper, WriteCacheUploadStoreWrapperRef};

use crate::cache::cache_size::parquet_meta_size;
use crate::cache::file_cache::{FileType, IndexKey};
use crate::cache::file_keys::{FileKeys, Tracked, insert_tracked};
use crate::cache::index::inverted_index::{InvertedIndexCache, InvertedIndexCacheRef};
use crate::cache::write_cache::WriteCacheRef;
use crate::error::{
    CompressObjectSnafu, DecompressObjectSnafu, InvalidMetadataSnafu, InvalidParquetSnafu,
    JoinSnafu, ReadParquetSnafu, Result, UnexpectedSnafu, WriteParquetSnafu,
};
use crate::memtable::record_batch_estimated_size;
use crate::metrics::{CACHE_BYTES, CACHE_EVICTION, CACHE_HIT, CACHE_MISS};
use crate::read::Batch;
use crate::read::range_cache::{RangeScanCacheKey, RangeScanCacheValue};
use crate::read::read_columns::JsonTargetTypes;
use crate::sst::file::{RegionFileId, RegionIndexId};
use crate::sst::parquet::PARQUET_METADATA_KEY;
use crate::sst::parquet::read_columns::ParquetReadColumns;
use crate::sst::parquet::reader::MetadataCacheMetrics;

/// Metrics type key for sst meta.
const SST_META_TYPE: &str = "sst_meta";
/// Metrics type key for the optional decoded SST metadata acceleration tier.
const SST_META_DECODED_TYPE: &str = "sst_meta_decoded";
/// Metrics type key for vector.
const VECTOR_TYPE: &str = "vector";
/// Metrics type key for pages.
const PAGE_TYPE: &str = "page";
/// Metrics type key for files on the local store.
const FILE_TYPE: &str = "file";
/// Metrics type key for index files (puffin) on the local store.
const INDEX_TYPE: &str = "index";
/// Metrics type key for selector result cache.
const SELECTOR_RESULT_TYPE: &str = "selector_result";
/// Metrics type key for range scan result cache.
const RANGE_RESULT_TYPE: &str = "range_result";
/// Metrics type key for prefilter result cache.
const PREFILTER_RESULT_TYPE: &str = "prefilter_result";
const RANGE_RESULT_CONCAT_MEMORY_LIMIT: ReadableSize = ReadableSize::mb(512);
const RANGE_RESULT_CONCAT_MEMORY_PERMIT: ReadableSize = ReadableSize::kb(1);

#[derive(Debug)]
pub(crate) struct RangeResultMemoryLimiter {
    semaphore: Arc<tokio::sync::Semaphore>,
    permit_bytes: usize,
    total_permits: usize,
    /// Number of acquisitions that found too few permits and parked. Test-only
    /// signal to synchronize with a caller waiting inside [Self::acquire].
    #[cfg(test)]
    waited_acquires: std::sync::atomic::AtomicUsize,
}

impl Default for RangeResultMemoryLimiter {
    fn default() -> Self {
        Self::new(
            RANGE_RESULT_CONCAT_MEMORY_LIMIT.as_bytes() as usize,
            RANGE_RESULT_CONCAT_MEMORY_PERMIT.as_bytes() as usize,
        )
    }
}

impl RangeResultMemoryLimiter {
    pub(crate) fn new(limit_bytes: usize, permit_bytes: usize) -> Self {
        let permit_bytes = permit_bytes.max(1);
        let total_permits = limit_bytes
            .div_ceil(permit_bytes)
            .clamp(1, tokio::sync::Semaphore::MAX_PERMITS);
        Self {
            semaphore: Arc::new(tokio::sync::Semaphore::new(total_permits)),
            permit_bytes,
            total_permits,
            #[cfg(test)]
            waited_acquires: std::sync::atomic::AtomicUsize::new(0),
        }
    }

    #[cfg(test)]
    pub(crate) fn permit_bytes(&self) -> usize {
        self.permit_bytes
    }

    #[cfg(test)]
    pub(crate) fn waited_acquires(&self) -> usize {
        self.waited_acquires
            .load(std::sync::atomic::Ordering::Acquire)
    }

    #[cfg(test)]
    pub(crate) fn available_permits(&self) -> usize {
        self.semaphore.available_permits()
    }

    pub(crate) async fn acquire(&self, bytes: usize) -> Result<tokio::sync::SemaphorePermit<'_>> {
        let permits = bytes.div_ceil(self.permit_bytes).max(1);
        if permits > self.total_permits {
            return UnexpectedSnafu {
                reason: format!(
                    "range result memory request of {bytes} bytes exceeds limiter capacity of {} bytes",
                    self.total_permits.saturating_mul(self.permit_bytes)
                ),
            }
            .fail();
        }
        // Nothing awaits between this check and the parking below, so an observed
        // increment means the caller is about to wait for the missing permits.
        #[cfg(test)]
        if self.semaphore.available_permits() < permits {
            self.waited_acquires
                .fetch_add(1, std::sync::atomic::Ordering::Release);
        }

        self.semaphore
            .acquire_many(permits as u32)
            .await
            .map_err(|_| {
                UnexpectedSnafu {
                    reason: "range result memory limiter is unexpectedly closed",
                }
                .build()
            })
    }
}

/// Cached SST metadata combines the parquet footer with the decoded region metadata.
///
/// The cached parquet footer strips the `greptime:metadata` JSON payload and stores the decoded
/// [RegionMetadata] separately so readers can skip repeated deserialization work.
#[derive(Debug)]
pub(crate) struct CachedSstMeta {
    parquet_metadata: Arc<ParquetMetaData>,
    parquet_metadata_size: usize,
    region_metadata: RegionMetadataRef,
    region_metadata_weight: usize,
    page_index_policy: PageIndexPolicy,
}

/// Compact, authoritative form of one SST's metadata.
///
/// The entry contains a zstd-compressed, self-contained Parquet metadata stream. The decoded
/// representation is held weakly to coalesce concurrent decodes without retaining memory outside
/// the cache capacity.
#[derive(Debug)]
pub(crate) struct CompactSstMeta {
    encoded_metadata: Bytes,
    decoded_size: usize,
    region_metadata: Weak<RegionMetadata>,
    page_index_policy: PageIndexPolicy,
    decoded: tokio::sync::Mutex<Weak<CachedSstMeta>>,
}

/// Both forms produced by decoding metadata after a cache miss.
#[derive(Debug)]
pub(crate) struct PreparedSstMeta {
    compact: Arc<CompactSstMeta>,
    decoded: Arc<CachedSstMeta>,
}

impl PreparedSstMeta {
    pub(crate) fn decoded(&self) -> Arc<CachedSstMeta> {
        self.decoded.clone()
    }
}

/// Result of decoding SST metadata and attempting to encode its compact cache entry.
#[derive(Debug)]
pub(crate) enum SstMetaPreparation {
    /// Both cache representations are ready for admission.
    Prepared(PreparedSstMeta),
    /// The metadata is usable by the reader, but its compact cache encoding failed.
    DecodedOnly {
        decoded: Arc<CachedSstMeta>,
        encoding_error: crate::error::Error,
    },
}

impl SstMetaPreparation {
    pub(crate) fn decoded(&self) -> Arc<CachedSstMeta> {
        match self {
            Self::Prepared(metadata) => metadata.decoded(),
            Self::DecodedOnly { decoded, .. } => decoded.clone(),
        }
    }
}

impl CompactSstMeta {
    async fn decode(&self, runtime: &Runtime) -> Result<Arc<CachedSstMeta>> {
        let mut decoded_guard = self.decoded.lock().await;
        if let Some(decoded) = decoded_guard.upgrade() {
            return Ok(decoded);
        }

        let encoded_metadata = self.encoded_metadata.clone();
        let decoded_size = self.decoded_size;
        let region_metadata = self.region_metadata.upgrade();
        let page_index_policy = self.page_index_policy;
        let decoded = runtime
            .spawn_blocking(move || {
                let bytes = zstd::bulk::decompress(&encoded_metadata, decoded_size).context(
                    DecompressObjectSnafu {
                        compress_type: CompressionType::Zstd,
                        path: "cached SST metadata",
                    },
                )?;
                let bytes = Bytes::from(bytes);
                let mut reader = ParquetMetaDataReader::new()
                    .with_column_index_policy(PageIndexPolicy::Skip)
                    .with_offset_index_policy(page_index_policy);
                reader.try_parse(&bytes).context(ReadParquetSnafu {
                    path: "cached SST metadata",
                })?;
                let metadata = reader.finish().context(ReadParquetSnafu {
                    path: "cached SST metadata",
                })?;
                CachedSstMeta::try_new_with_page_index_policy(
                    "cached SST metadata",
                    metadata,
                    region_metadata,
                    page_index_policy,
                )
                .map(Arc::new)
            })
            .await
            .context(JoinSnafu)??;

        *decoded_guard = Arc::downgrade(&decoded);
        Ok(decoded)
    }

    fn satisfies_page_index_policy(&self, requested: PageIndexPolicy) -> bool {
        satisfies_page_index_policy(self.page_index_policy, requested)
    }
}

/// Decodes SST metadata on the given blocking runtime without preparing a compact cache entry.
pub(crate) async fn decode_sst_meta(
    file_path: &str,
    parquet_metadata: ParquetMetaData,
    region_metadata: Option<RegionMetadataRef>,
    page_index_policy: PageIndexPolicy,
    runtime: &Runtime,
) -> Result<Arc<CachedSstMeta>> {
    let file_path = file_path.to_string();
    runtime
        .spawn_blocking(move || {
            let parquet_metadata = strip_column_indexes(parquet_metadata);
            CachedSstMeta::try_new_with_page_index_policy(
                &file_path,
                parquet_metadata,
                region_metadata,
                page_index_policy,
            )
            .map(Arc::new)
        })
        .await
        .context(JoinSnafu)?
}

/// Decodes SST metadata and attempts to encode both cache representations on the given blocking runtime.
pub(crate) async fn prepare_sst_meta(
    file_path: &str,
    parquet_metadata: ParquetMetaData,
    region_metadata: Option<RegionMetadataRef>,
    page_index_policy: PageIndexPolicy,
    runtime: &Runtime,
) -> Result<SstMetaPreparation> {
    let file_path = file_path.to_string();
    runtime
        .spawn_blocking(move || {
            prepare_sst_meta_sync(
                &file_path,
                parquet_metadata,
                region_metadata,
                page_index_policy,
            )
        })
        .await
        .context(JoinSnafu)?
}

/// Synchronously prepares SST metadata. Callers must run this on a blocking runtime.
pub(crate) fn prepare_sst_meta_sync(
    file_path: &str,
    parquet_metadata: ParquetMetaData,
    region_metadata: Option<RegionMetadataRef>,
    page_index_policy: PageIndexPolicy,
) -> Result<SstMetaPreparation> {
    let parquet_metadata = strip_column_indexes(parquet_metadata);
    let cache_encoding = encode_compact_sst_meta(file_path, &parquet_metadata);
    finish_sst_meta_preparation(
        file_path,
        parquet_metadata,
        region_metadata,
        page_index_policy,
        cache_encoding,
    )
}

fn strip_column_indexes(parquet_metadata: ParquetMetaData) -> ParquetMetaData {
    // Defensively discard column indexes supplied by external metadata producers. Mito only
    // consumes offset indexes.
    let mut builder = parquet_metadata.into_builder();
    builder.take_column_index();
    builder.build()
}

fn encode_compact_sst_meta(
    file_path: &str,
    parquet_metadata: &ParquetMetaData,
) -> Result<(Bytes, usize)> {
    let mut encoded = Vec::new();
    ParquetMetaDataWriter::new(&mut encoded, parquet_metadata)
        .finish()
        .context(WriteParquetSnafu)?;
    let decoded_size = encoded.len();
    let encoded_metadata = zstd::bulk::compress(&encoded, 3).context(CompressObjectSnafu {
        compress_type: CompressionType::Zstd,
        path: file_path,
    })?;

    // `zstd::bulk::compress` allocates for the compression upper bound and leaves the excess
    // capacity in its `Vec`. Convert through a boxed slice so the retained allocation matches the
    // cache weight.
    Ok((
        Bytes::from(encoded_metadata.into_boxed_slice()),
        decoded_size,
    ))
}

fn finish_sst_meta_preparation(
    file_path: &str,
    parquet_metadata: ParquetMetaData,
    region_metadata: Option<RegionMetadataRef>,
    page_index_policy: PageIndexPolicy,
    cache_encoding: Result<(Bytes, usize)>,
) -> Result<SstMetaPreparation> {
    let decoded = Arc::new(CachedSstMeta::try_new_with_page_index_policy(
        file_path,
        parquet_metadata,
        region_metadata,
        page_index_policy,
    )?);
    let (encoded_metadata, decoded_size) = match cache_encoding {
        Ok(encoded) => encoded,
        Err(encoding_error) => {
            return Ok(SstMetaPreparation::DecodedOnly {
                decoded,
                encoding_error,
            });
        }
    };
    let compact = Arc::new(CompactSstMeta {
        encoded_metadata,
        decoded_size,
        region_metadata: Arc::downgrade(&decoded.region_metadata),
        page_index_policy,
        decoded: tokio::sync::Mutex::new(Arc::downgrade(&decoded)),
    });

    Ok(SstMetaPreparation::Prepared(PreparedSstMeta {
        compact,
        decoded,
    }))
}

impl CachedSstMeta {
    #[cfg(test)]
    pub(crate) fn try_new(file_path: &str, parquet_metadata: ParquetMetaData) -> Result<Self> {
        let page_index_policy = infer_loaded_page_index_policy(&parquet_metadata);
        Self::try_new_with_page_index_policy(file_path, parquet_metadata, None, page_index_policy)
    }

    pub(crate) fn try_new_with_region_metadata(
        file_path: &str,
        parquet_metadata: ParquetMetaData,
        region_metadata: Option<RegionMetadataRef>,
    ) -> Result<Self> {
        let page_index_policy = infer_loaded_page_index_policy(&parquet_metadata);
        Self::try_new_with_page_index_policy(
            file_path,
            parquet_metadata,
            region_metadata,
            page_index_policy,
        )
    }

    pub(crate) fn try_new_with_page_index_policy(
        file_path: &str,
        parquet_metadata: ParquetMetaData,
        region_metadata: Option<RegionMetadataRef>,
        page_index_policy: PageIndexPolicy,
    ) -> Result<Self> {
        let file_metadata = parquet_metadata.file_metadata();
        let key_values = file_metadata
            .key_value_metadata()
            .context(InvalidParquetSnafu {
                file: file_path,
                reason: "missing key value meta",
            })?;
        let meta_value = key_values
            .iter()
            .find(|kv| kv.key == PARQUET_METADATA_KEY)
            .with_context(|| InvalidParquetSnafu {
                file: file_path,
                reason: format!("key {} not found", PARQUET_METADATA_KEY),
            })?;
        let json = meta_value
            .value
            .as_ref()
            .with_context(|| InvalidParquetSnafu {
                file: file_path,
                reason: format!("No value for key {}", PARQUET_METADATA_KEY),
            })?;
        let region_metadata = match region_metadata {
            Some(region_metadata) => region_metadata,
            None => Arc::new(
                store_api::metadata::RegionMetadata::from_json(json)
                    .context(InvalidMetadataSnafu)?,
            ),
        };
        // Keep the previous JSON-byte floor and charge the decoded structures as well.
        let region_metadata_weight = region_metadata.estimated_size().max(json.len());
        let parquet_metadata = Arc::new(strip_region_metadata_from_parquet(parquet_metadata));
        let parquet_metadata_size = parquet_meta_size(&parquet_metadata);

        Ok(Self {
            parquet_metadata,
            parquet_metadata_size,
            region_metadata,
            region_metadata_weight,
            page_index_policy,
        })
    }

    pub(crate) fn parquet_metadata(&self) -> Arc<ParquetMetaData> {
        self.parquet_metadata.clone()
    }

    /// Returns the immutable parquet metadata size computed when it was decoded.
    pub(crate) fn parquet_metadata_size(&self) -> usize {
        self.parquet_metadata_size
    }

    pub(crate) fn region_metadata(&self) -> RegionMetadataRef {
        self.region_metadata.clone()
    }

    fn satisfies_page_index_policy(&self, requested: PageIndexPolicy) -> bool {
        satisfies_page_index_policy(self.page_index_policy, requested)
    }
}

fn satisfies_page_index_policy(cached: PageIndexPolicy, requested: PageIndexPolicy) -> bool {
    match requested {
        PageIndexPolicy::Skip => true,
        PageIndexPolicy::Optional => cached != PageIndexPolicy::Skip,
        PageIndexPolicy::Required => cached == PageIndexPolicy::Required,
    }
}

fn infer_loaded_page_index_policy(parquet_metadata: &ParquetMetaData) -> PageIndexPolicy {
    if parquet_metadata.offset_index().is_some() {
        PageIndexPolicy::Optional
    } else {
        PageIndexPolicy::Skip
    }
}

fn strip_region_metadata_from_parquet(parquet_metadata: ParquetMetaData) -> ParquetMetaData {
    let file_metadata = parquet_metadata.file_metadata();
    let filtered_key_values = file_metadata.key_value_metadata().and_then(|key_values| {
        let filtered = key_values
            .iter()
            .filter(|kv| kv.key != PARQUET_METADATA_KEY)
            .cloned()
            .collect::<Vec<_>>();
        (!filtered.is_empty()).then_some(filtered)
    });
    let stripped_file_metadata = FileMetaData::new(
        file_metadata.version(),
        file_metadata.num_rows(),
        file_metadata.created_by().map(ToString::to_string),
        filtered_key_values,
        file_metadata.schema_descr_ptr(),
        file_metadata.column_orders().cloned(),
    );

    let mut builder = parquet_metadata.into_builder();
    let row_groups = builder.take_row_groups();
    let offset_index = builder.take_offset_index();

    parquet::file::metadata::ParquetMetaDataBuilder::new(stripped_file_metadata)
        .set_row_groups(row_groups)
        .set_offset_index(offset_index)
        .build()
}

fn removal_cause_str(cause: RemovalCause) -> &'static str {
    match cause {
        RemovalCause::Expired => "expired",
        RemovalCause::Explicit => "explicit",
        RemovalCause::Replaced => "replaced",
        RemovalCause::Size => "size",
    }
}

#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct PrefilterRowSelector {
    row_count: usize,
    skip: bool,
}

// `parquet::arrow::arrow_reader::RowSelector` does not implement `Hash`, but
// prefilter cache keys must hash the upstream row-selection snapshot. Keep a
// local hashable mirror of the two fields that define selector semantics.
// TODO(yingwen): Remove this mirror if upstream `RowSelector` implements `Hash`.
impl From<&RowSelector> for PrefilterRowSelector {
    fn from(selector: &RowSelector) -> Self {
        Self {
            row_count: selector.row_count,
            skip: selector.skip,
        }
    }
}

/// Key for a cached prefilter result.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
pub(crate) struct PrefilterKey {
    file_id: FileId,
    row_group_idx: u32,
    row_selection: Option<Arc<Vec<PrefilterRowSelector>>>,
    schema_version: u64,
    filter_exprs: SmallVec<[String; 1]>,
    mem_usage: usize,
}

impl PrefilterKey {
    pub(crate) fn row_selection_snapshot(
        row_selection: Option<&RowSelection>,
    ) -> Option<Arc<Vec<PrefilterRowSelector>>> {
        row_selection.map(|selection| {
            Arc::new(
                selection
                    .iter()
                    .map(PrefilterRowSelector::from)
                    .collect::<Vec<_>>(),
            )
        })
    }

    pub(crate) fn new(
        file_id: FileId,
        row_group_idx: u32,
        row_selection: Option<Arc<Vec<PrefilterRowSelector>>>,
        schema_version: u64,
        filter_exprs: SmallVec<[String; 1]>,
    ) -> Self {
        let row_selection_bytes = row_selection
            .as_ref()
            .map(|selection| selection.len() * mem::size_of::<PrefilterRowSelector>())
            .unwrap_or(0);
        let spilled_expr_bytes = if filter_exprs.spilled() {
            filter_exprs.capacity() * mem::size_of::<String>()
        } else {
            0
        };
        let expr_bytes = filter_exprs.iter().map(|s| s.capacity()).sum::<usize>();

        Self {
            file_id,
            row_group_idx,
            row_selection,
            schema_version,
            filter_exprs,
            mem_usage: mem::size_of::<Self>()
                + row_selection_bytes
                + spilled_expr_bytes
                + expr_bytes,
        }
    }

    fn mem_usage(&self) -> usize {
        self.mem_usage
    }
}

type PrefilterResultCache = Cache<PrefilterKey, Tracked<Arc<BooleanBuffer>>>;

fn new_prefilter_result_cache(
    capacity: u64,
    keys: Arc<FileKeys<PrefilterKey>>,
) -> PrefilterResultCache {
    Cache::builder()
        .max_capacity(capacity)
        .weigher(|k, v: &Tracked<Arc<BooleanBuffer>>| prefilter_result_cache_weight(k, &v.1))
        .eviction_listener(move |k, v, cause| {
            keys.remove(k.file_id, &*k, v.0);
            let size = prefilter_result_cache_weight(&k, &v.1);
            CACHE_BYTES
                .with_label_values(&[PREFILTER_RESULT_TYPE])
                .sub(size.into());
            CACHE_EVICTION
                .with_label_values(&[PREFILTER_RESULT_TYPE, removal_cause_str(cause)])
                .inc();
        })
        .build()
}

fn prefilter_result_cache_weight(k: &PrefilterKey, v: &Arc<BooleanBuffer>) -> u32 {
    (k.mem_usage() + mem::size_of::<Tracked<BooleanBuffer>>() + v.values().len()) as u32
}

/// Cache strategies that may only enable a subset of caches.
#[derive(Clone)]
pub enum CacheStrategy {
    /// Strategy for normal operations.
    /// Doesn't disable any cache.
    EnableAll(CacheManagerRef),
    /// Strategy for compaction.
    /// Disables some caches during compaction to avoid affecting queries.
    /// Enables the write cache so that the compaction can read files cached
    /// in the write cache and write the compacted files back to the write cache.
    Compaction(CacheManagerRef),
    /// Do not use any cache.
    Disabled,
}

impl CacheStrategy {
    /// Returns the runtime for CPU-bound SST metadata work for this request.
    pub(crate) fn sst_meta_runtime(&self) -> Runtime {
        match self {
            CacheStrategy::Compaction(_) => common_runtime::compact_runtime(),
            CacheStrategy::EnableAll(_) | CacheStrategy::Disabled => {
                common_runtime::global_runtime()
            }
        }
    }

    /// Returns whether the SST metadata cache is enabled for this strategy.
    pub(crate) fn sst_meta_cache_enabled(&self) -> bool {
        match self {
            CacheStrategy::EnableAll(cache_manager) | CacheStrategy::Compaction(cache_manager) => {
                cache_manager.sst_meta_cache_enabled()
            }
            CacheStrategy::Disabled => false,
        }
    }

    /// Gets fused SST metadata with cache metrics tracking.
    pub(crate) async fn get_sst_meta_data(
        &self,
        file_id: RegionFileId,
        metrics: &mut MetadataCacheMetrics,
        page_index_policy: PageIndexPolicy,
    ) -> Option<Arc<CachedSstMeta>> {
        match self {
            CacheStrategy::EnableAll(cache_manager) | CacheStrategy::Compaction(cache_manager) => {
                cache_manager
                    .get_sst_meta_data(
                        file_id,
                        metrics,
                        page_index_policy,
                        &self.sst_meta_runtime(),
                    )
                    .await
            }
            CacheStrategy::Disabled => {
                metrics.cache_miss += 1;
                None
            }
        }
    }

    /// Calls [CacheManager::get_sst_meta_data_from_mem_cache()].
    pub(crate) fn get_sst_meta_data_from_mem_cache(
        &self,
        file_id: RegionFileId,
        page_index_policy: PageIndexPolicy,
    ) -> Option<Arc<CachedSstMeta>> {
        match self {
            CacheStrategy::EnableAll(cache_manager) | CacheStrategy::Compaction(cache_manager) => {
                cache_manager.get_sst_meta_data_from_mem_cache(file_id, page_index_policy)
            }
            CacheStrategy::Disabled => None,
        }
    }

    /// Calls [CacheManager::get_parquet_meta_data_from_mem_cache()].
    pub fn get_parquet_meta_data_from_mem_cache(
        &self,
        file_id: RegionFileId,
    ) -> Option<Arc<ParquetMetaData>> {
        self.get_sst_meta_data_from_mem_cache(file_id, PageIndexPolicy::Skip)
            .map(|metadata| metadata.parquet_metadata())
    }

    /// Puts compact and decoded forms of SST metadata into the shared cache capacity.
    pub(crate) fn put_prepared_sst_meta(
        &self,
        file_id: RegionFileId,
        metadata: PreparedSstMeta,
        retain_decoded: bool,
    ) {
        match self {
            CacheStrategy::EnableAll(cache_manager) | CacheStrategy::Compaction(cache_manager) => {
                cache_manager.put_prepared_sst_meta(file_id, metadata, retain_decoded);
            }
            CacheStrategy::Disabled => {}
        }
    }

    /// Calls [CacheManager::put_parquet_meta_data()].
    pub fn put_parquet_meta_data(
        &self,
        file_id: RegionFileId,
        metadata: Arc<ParquetMetaData>,
        region_metadata: Option<RegionMetadataRef>,
    ) {
        match self {
            CacheStrategy::EnableAll(cache_manager) | CacheStrategy::Compaction(cache_manager) => {
                cache_manager.put_parquet_meta_data(file_id, metadata, region_metadata);
            }
            CacheStrategy::Disabled => {}
        }
    }

    /// Calls [CacheManager::get_prefilter_result()].
    /// It returns None if the strategy is [CacheStrategy::Compaction] or [CacheStrategy::Disabled].
    pub(crate) fn get_prefilter_result(&self, key: &PrefilterKey) -> Option<Arc<BooleanBuffer>> {
        match self {
            CacheStrategy::EnableAll(cache_manager) => cache_manager.get_prefilter_result(key),
            CacheStrategy::Compaction(_) | CacheStrategy::Disabled => None,
        }
    }

    /// Calls [CacheManager::put_prefilter_result()].
    /// It does nothing if the strategy isn't [CacheStrategy::EnableAll].
    pub(crate) fn put_prefilter_result(&self, key: PrefilterKey, result: Arc<BooleanBuffer>) {
        if let CacheStrategy::EnableAll(cache_manager) = self {
            cache_manager.put_prefilter_result(key, result);
        }
    }

    /// Calls [CacheManager::remove_parquet_meta_data()].
    pub fn remove_parquet_meta_data(&self, file_id: RegionFileId) {
        match self {
            CacheStrategy::EnableAll(cache_manager) => {
                cache_manager.remove_parquet_meta_data(file_id);
            }
            CacheStrategy::Compaction(cache_manager) => {
                cache_manager.remove_parquet_meta_data(file_id);
            }
            CacheStrategy::Disabled => {}
        }
    }

    /// Calls [CacheManager::get_repeated_vector()].
    /// It returns None if the strategy is [CacheStrategy::Compaction] or [CacheStrategy::Disabled].
    pub fn get_repeated_vector(
        &self,
        data_type: &ConcreteDataType,
        value: &Value,
    ) -> Option<VectorRef> {
        match self {
            CacheStrategy::EnableAll(cache_manager) => {
                cache_manager.get_repeated_vector(data_type, value)
            }
            CacheStrategy::Compaction(_) | CacheStrategy::Disabled => None,
        }
    }

    /// Calls [CacheManager::put_repeated_vector()].
    /// It does nothing if the strategy isn't [CacheStrategy::EnableAll].
    pub fn put_repeated_vector(&self, value: Value, vector: VectorRef) {
        if let CacheStrategy::EnableAll(cache_manager) = self {
            cache_manager.put_repeated_vector(value, vector);
        }
    }

    /// Calls [CacheManager::get_page_ranges()].
    /// It returns None if the strategy is [CacheStrategy::Compaction] or [CacheStrategy::Disabled].
    pub fn get_page_ranges(
        &self,
        file_id: FileId,
        row_group_idx: usize,
        ranges: &[Range<u64>],
    ) -> Option<PageRangeLookup> {
        match self {
            CacheStrategy::EnableAll(cache_manager) => {
                cache_manager.get_page_ranges(file_id, row_group_idx, ranges)
            }
            CacheStrategy::Compaction(_) | CacheStrategy::Disabled => None,
        }
    }

    /// Calls [CacheManager::put_page_ranges()].
    /// It does nothing if the strategy isn't [CacheStrategy::EnableAll].
    pub fn put_page_ranges(
        &self,
        file_id: FileId,
        row_group_idx: usize,
        ranges: &[Range<u64>],
        pages: &[Bytes],
    ) {
        if let CacheStrategy::EnableAll(cache_manager) = self {
            cache_manager.put_page_ranges(file_id, row_group_idx, ranges, pages);
        }
    }

    /// Calls [CacheManager::evict_puffin_cache()].
    pub async fn evict_puffin_cache(&self, file_id: RegionIndexId) {
        match self {
            CacheStrategy::EnableAll(cache_manager) => {
                cache_manager.evict_puffin_cache(file_id).await
            }
            CacheStrategy::Compaction(cache_manager) => {
                cache_manager.evict_puffin_cache(file_id).await
            }
            CacheStrategy::Disabled => {}
        }
    }

    /// Calls [CacheManager::get_selector_result()].
    /// It returns None if the strategy is [CacheStrategy::Compaction] or [CacheStrategy::Disabled].
    pub fn get_selector_result(
        &self,
        selector_key: &SelectorResultKey,
    ) -> Option<Arc<SelectorResultValue>> {
        match self {
            CacheStrategy::EnableAll(cache_manager) => {
                cache_manager.get_selector_result(selector_key)
            }
            CacheStrategy::Compaction(_) | CacheStrategy::Disabled => None,
        }
    }

    /// Calls [CacheManager::put_selector_result()].
    /// It does nothing if the strategy isn't [CacheStrategy::EnableAll].
    pub fn put_selector_result(
        &self,
        selector_key: SelectorResultKey,
        result: Arc<SelectorResultValue>,
    ) {
        if let CacheStrategy::EnableAll(cache_manager) = self {
            cache_manager.put_selector_result(selector_key, result);
        }
    }

    /// Calls [CacheManager::get_range_result()].
    /// It returns None if the strategy is [CacheStrategy::Compaction] or [CacheStrategy::Disabled].
    #[allow(dead_code)]
    pub(crate) fn get_range_result(
        &self,
        key: &RangeScanCacheKey,
    ) -> Option<Arc<RangeScanCacheValue>> {
        match self {
            CacheStrategy::EnableAll(cache_manager) => cache_manager.get_range_result(key),
            CacheStrategy::Compaction(_) | CacheStrategy::Disabled => None,
        }
    }

    /// Calls [CacheManager::put_range_result()].
    /// It does nothing if the strategy isn't [CacheStrategy::EnableAll].
    pub(crate) fn put_range_result(
        &self,
        key: RangeScanCacheKey,
        result: Arc<RangeScanCacheValue>,
    ) {
        if let CacheStrategy::EnableAll(cache_manager) = self {
            cache_manager.put_range_result(key, result);
        }
    }

    /// Returns true if the range result cache is enabled.
    pub(crate) fn has_range_result_cache(&self) -> bool {
        match self {
            CacheStrategy::EnableAll(cache_manager) => cache_manager.has_range_result_cache(),
            CacheStrategy::Compaction(_) | CacheStrategy::Disabled => false,
        }
    }

    pub(crate) fn range_result_memory_limiter(&self) -> Option<&Arc<RangeResultMemoryLimiter>> {
        match self {
            CacheStrategy::EnableAll(cache_manager) => {
                Some(cache_manager.range_result_memory_limiter())
            }
            CacheStrategy::Compaction(_) | CacheStrategy::Disabled => None,
        }
    }

    pub(crate) fn range_result_cache_size(&self) -> Option<usize> {
        match self {
            CacheStrategy::EnableAll(cache_manager) => {
                Some(cache_manager.range_result_cache_size())
            }
            CacheStrategy::Compaction(_) | CacheStrategy::Disabled => None,
        }
    }

    /// Calls [CacheManager::write_cache()].
    /// It returns None if the strategy is [CacheStrategy::Disabled].
    pub fn write_cache(&self) -> Option<&WriteCacheRef> {
        match self {
            CacheStrategy::EnableAll(cache_manager) => cache_manager.write_cache(),
            CacheStrategy::Compaction(cache_manager) => cache_manager.write_cache(),
            CacheStrategy::Disabled => None,
        }
    }

    /// Calls [CacheManager::index_cache()].
    /// It returns None if the strategy is [CacheStrategy::Compaction] or [CacheStrategy::Disabled].
    pub fn inverted_index_cache(&self) -> Option<&InvertedIndexCacheRef> {
        match self {
            CacheStrategy::EnableAll(cache_manager) => cache_manager.inverted_index_cache(),
            CacheStrategy::Compaction(_) | CacheStrategy::Disabled => None,
        }
    }

    /// Calls [CacheManager::bloom_filter_index_cache()].
    /// It returns None if the strategy is [CacheStrategy::Compaction] or [CacheStrategy::Disabled].
    pub fn bloom_filter_index_cache(&self) -> Option<&BloomFilterIndexCacheRef> {
        match self {
            CacheStrategy::EnableAll(cache_manager) => cache_manager.bloom_filter_index_cache(),
            CacheStrategy::Compaction(_) | CacheStrategy::Disabled => None,
        }
    }

    /// Calls [CacheManager::puffin_metadata_cache()].
    /// It returns None if the strategy is [CacheStrategy::Compaction] or [CacheStrategy::Disabled].
    pub fn puffin_metadata_cache(&self) -> Option<&PuffinMetadataCacheRef> {
        match self {
            CacheStrategy::EnableAll(cache_manager) => cache_manager.puffin_metadata_cache(),
            CacheStrategy::Compaction(_) | CacheStrategy::Disabled => None,
        }
    }

    /// Calls [CacheManager::index_result_cache()].
    /// It returns None if the strategy is [CacheStrategy::Compaction] or [CacheStrategy::Disabled].
    pub fn index_result_cache(&self) -> Option<&IndexResultCache> {
        match self {
            CacheStrategy::EnableAll(cache_manager) => cache_manager.index_result_cache(),
            CacheStrategy::Compaction(_) | CacheStrategy::Disabled => None,
        }
    }

    /// Triggers download if the strategy is [CacheStrategy::EnableAll] and write cache is available.
    pub fn maybe_download_background(
        &self,
        index_key: IndexKey,
        remote_path: String,
        remote_store: ObjectStore,
        file_size: u64,
    ) {
        if let CacheStrategy::EnableAll(cache_manager) = self
            && let Some(write_cache) = cache_manager.write_cache()
        {
            write_cache.file_cache().maybe_download_background(
                index_key,
                remote_path,
                remote_store,
                file_size,
            );
        }
    }
}

/// Manages cached data for the engine.
///
/// All caches are disabled by default.
#[derive(Default)]
pub struct CacheManager {
    /// Cache for compact, authoritative SST metadata.
    sst_meta_cache: Option<SstMetaCache>,
    /// Cache for decoded SST metadata, used only as an acceleration tier.
    sst_decoded_meta_cache: Option<SstDecodedMetaCache>,
    /// Cache for vectors.
    vector_cache: Option<VectorCache>,
    /// Cache for SST byte ranges.
    page_cache: Option<Arc<PageRangeCache>>,
    /// A Cache for writing files to object stores.
    write_cache: Option<WriteCacheRef>,
    /// Cache for inverted index.
    inverted_index_cache: Option<InvertedIndexCacheRef>,
    /// Cache for bloom filter index.
    bloom_filter_index_cache: Option<BloomFilterIndexCacheRef>,
    /// Puffin metadata cache.
    puffin_metadata_cache: Option<PuffinMetadataCacheRef>,
    /// Cache for time series selectors.
    selector_result_cache: Option<SelectorResultCache>,
    /// Cache for range scan outputs in flat format.
    range_result_cache: Option<RangeResultCache>,
    /// Configured capacity for range scan outputs in flat format.
    range_result_cache_size: u64,
    /// Shared memory limiter for async range-result cache tasks.
    range_result_memory_limiter: Arc<RangeResultMemoryLimiter>,
    /// Cache for index result.
    index_result_cache: Option<IndexResultCache>,
    /// Cache for prefilter result.
    prefilter_result_cache: Option<PrefilterResultCache>,
    selector_result_keys: Arc<FileKeys<SelectorResultKey>>,
    /// Keys are shared by all files of a range, which can be many.
    range_result_keys: Arc<FileKeys<Arc<RangeScanCacheKey>>>,
    prefilter_result_keys: Arc<FileKeys<PrefilterKey>>,
}

pub type CacheManagerRef = Arc<CacheManager>;

impl CacheManager {
    /// Returns a builder to build the cache.
    pub fn builder() -> CacheManagerBuilder {
        CacheManagerBuilder::default()
    }

    /// Gets fused SST metadata with metrics tracking.
    /// Tries in-memory cache first, then file cache, updating metrics accordingly.
    pub(crate) async fn get_sst_meta_data(
        &self,
        file_id: RegionFileId,
        metrics: &mut MetadataCacheMetrics,
        page_index_policy: PageIndexPolicy,
        runtime: &Runtime,
    ) -> Option<Arc<CachedSstMeta>> {
        let cache_key = SstMetaKey(file_id.region_id(), file_id.file_id());
        let compact = self
            .get_compact_sst_meta(&cache_key)
            .filter(|metadata| metadata.satisfies_page_index_policy(page_index_policy));
        let decoded = self
            .get_decoded_sst_meta(&cache_key)
            .filter(|metadata| metadata.satisfies_page_index_policy(page_index_policy));

        if let Some(compact) = compact {
            CACHE_HIT.with_label_values(&[SST_META_TYPE]).inc();
            metrics.mem_cache_hit += 1;
            if let Some(decoded) = decoded {
                CACHE_HIT.with_label_values(&[SST_META_DECODED_TYPE]).inc();
                return Some(decoded);
            }

            CACHE_MISS.with_label_values(&[SST_META_DECODED_TYPE]).inc();
            match compact.decode(runtime).await {
                Ok(decoded) => {
                    self.put_sst_meta_data(file_id, decoded.clone());
                    return Some(decoded);
                }
                Err(err) => {
                    warn!(err; "Failed to decode compact SST metadata, region_id: {}, file_id: {}", file_id.region_id(), file_id.file_id());
                    self.remove_parquet_meta_data(file_id);
                }
            }
        } else if let Some(decoded) = decoded {
            // Metadata produced by a new SST writer can enter the decoded tier before its compact
            // representation is prepared.
            CACHE_HIT.with_label_values(&[SST_META_TYPE]).inc();
            CACHE_HIT.with_label_values(&[SST_META_DECODED_TYPE]).inc();
            metrics.mem_cache_hit += 1;
            return Some(decoded);
        } else {
            CACHE_MISS.with_label_values(&[SST_META_TYPE]).inc();
        }

        let key = IndexKey::new(file_id.region_id(), file_id.file_id(), FileType::Parquet);
        if let Some(write_cache) = &self.write_cache {
            let file_cache = write_cache.file_cache();
            if self.sst_meta_cache_enabled() {
                if let Some(metadata) = file_cache
                    .get_sst_meta_data(key, metrics, page_index_policy, runtime)
                    .await
                {
                    metrics.file_cache_hit += 1;
                    let decoded = metadata.decoded();
                    match metadata {
                        SstMetaPreparation::Prepared(metadata) => {
                            self.put_prepared_sst_meta(file_id, metadata, true);
                        }
                        SstMetaPreparation::DecodedOnly { encoding_error, .. } => {
                            warn!(
                                encoding_error;
                                "Failed to encode file-cached SST metadata for memory cache, region_id: {}, file_id: {}",
                                file_id.region_id(),
                                file_id.file_id()
                            );
                        }
                    }
                    return Some(decoded);
                }
            } else if let Some(decoded) = file_cache
                .get_decoded_sst_meta_data(key, metrics, page_index_policy, runtime)
                .await
            {
                metrics.file_cache_hit += 1;
                return Some(decoded);
            }
        }

        metrics.cache_miss += 1;
        None
    }

    /// Gets cached fused SST metadata from in-memory cache.
    /// This method does not perform I/O.
    pub(crate) fn get_sst_meta_data_from_mem_cache(
        &self,
        file_id: RegionFileId,
        page_index_policy: PageIndexPolicy,
    ) -> Option<Arc<CachedSstMeta>> {
        let key = SstMetaKey(file_id.region_id(), file_id.file_id());
        let value = self
            .get_decoded_sst_meta(&key)
            .filter(|metadata| metadata.satisfies_page_index_policy(page_index_policy));
        update_hit_miss(value, SST_META_DECODED_TYPE)
    }

    /// Gets cached [ParquetMetaData] from in-memory cache.
    /// This method does not perform I/O.
    pub fn get_parquet_meta_data_from_mem_cache(
        &self,
        file_id: RegionFileId,
    ) -> Option<Arc<ParquetMetaData>> {
        self.get_sst_meta_data_from_mem_cache(file_id, PageIndexPolicy::Skip)
            .map(|metadata| metadata.parquet_metadata())
    }

    /// Puts fused SST metadata into the cache.
    pub(crate) fn put_sst_meta_data(&self, file_id: RegionFileId, metadata: Arc<CachedSstMeta>) {
        if let Some(cache) = &self.sst_decoded_meta_cache {
            let key = SstMetaKey(file_id.region_id(), file_id.file_id());
            CACHE_BYTES
                .with_label_values(&[SST_META_DECODED_TYPE])
                .add(decoded_meta_cache_weight(&key, &metadata).into());
            cache.insert(key, metadata);
        }
    }

    /// Puts a compact metadata entry and optionally retains its decoded acceleration entry.
    pub(crate) fn put_prepared_sst_meta(
        &self,
        file_id: RegionFileId,
        metadata: PreparedSstMeta,
        retain_decoded: bool,
    ) {
        let key = SstMetaKey(file_id.region_id(), file_id.file_id());
        if let Some(cache) = &self.sst_meta_cache {
            CACHE_BYTES
                .with_label_values(&[SST_META_TYPE])
                .add(meta_cache_weight(&key, &metadata.compact).into());
            cache.insert(key.clone(), metadata.compact);
        }
        if retain_decoded {
            self.put_sst_meta_data(file_id, metadata.decoded);
        }
    }

    fn get_compact_sst_meta(&self, key: &SstMetaKey) -> Option<Arc<CompactSstMeta>> {
        self.sst_meta_cache.as_ref()?.get(key)
    }

    fn get_decoded_sst_meta(&self, key: &SstMetaKey) -> Option<Arc<CachedSstMeta>> {
        self.sst_decoded_meta_cache.as_ref()?.get(key)
    }

    /// Gets and decodes only the compact tier without promoting into the decoded cache.
    ///
    /// This is used by startup preloading so inspecting already-cached metadata cannot consume the
    /// decoded reservation and prematurely stop compact preloading.
    pub(crate) async fn get_compact_sst_meta_data(
        &self,
        file_id: RegionFileId,
        page_index_policy: PageIndexPolicy,
    ) -> Option<Arc<CachedSstMeta>> {
        let key = SstMetaKey(file_id.region_id(), file_id.file_id());
        let compact = self
            .get_compact_sst_meta(&key)
            .filter(|metadata| metadata.satisfies_page_index_policy(page_index_policy))?;
        match compact.decode(&common_runtime::global_runtime()).await {
            Ok(metadata) => Some(metadata),
            Err(err) => {
                warn!(err; "Failed to decode compact SST metadata, region_id: {}, file_id: {}", file_id.region_id(), file_id.file_id());
                self.remove_parquet_meta_data(file_id);
                None
            }
        }
    }

    /// Puts [ParquetMetaData] into the cache.
    pub fn put_parquet_meta_data(
        &self,
        file_id: RegionFileId,
        metadata: Arc<ParquetMetaData>,
        region_metadata: Option<RegionMetadataRef>,
    ) {
        if self.sst_decoded_meta_cache.is_some() {
            let file_path = format!(
                "region_id={}, file_id={}",
                file_id.region_id(),
                file_id.file_id()
            );
            match CachedSstMeta::try_new_with_region_metadata(
                &file_path,
                Arc::unwrap_or_clone(metadata),
                region_metadata,
            ) {
                Ok(metadata) => self.put_sst_meta_data(file_id, Arc::new(metadata)),
                Err(err) => warn!(
                    err; "Failed to decode region metadata while caching parquet metadata, region_id: {}, file_id: {}",
                    file_id.region_id(),
                    file_id.file_id()
                ),
            }
        }
    }

    /// Removes [ParquetMetaData] from the cache.
    pub fn remove_parquet_meta_data(&self, file_id: RegionFileId) {
        let key = SstMetaKey(file_id.region_id(), file_id.file_id());
        if let Some(cache) = &self.sst_meta_cache {
            cache.remove(&key);
        }
        if let Some(cache) = &self.sst_decoded_meta_cache {
            cache.remove(&key);
        }
    }

    /// Removes in-memory entries of an SST that is no longer part of any version.
    ///
    /// Entries of a removed file can never be read again, but TinyLFU admission
    /// keeps them if they were hot, so they would block admission of the
    /// compaction outputs that replaced them.
    pub(crate) fn remove_file_entries(&self, index_id: RegionIndexId) {
        self.remove_parquet_meta_data(index_id.file_id);
        if let Some(cache) = &self.puffin_metadata_cache {
            cache.remove(&index_id.to_string());
        }
        let file_id = index_id.file_id();
        if let Some(cache) = &self.page_cache {
            cache.invalidate_file(file_id);
        }
        if let Some(cache) = &self.selector_result_cache {
            for key in self.selector_result_keys.take(file_id) {
                cache.invalidate(&key);
            }
        }
        if let Some(cache) = &self.range_result_cache {
            for key in self.range_result_keys.take(file_id) {
                cache.invalidate(key.as_ref());
            }
        }
        if let Some(cache) = &self.prefilter_result_cache {
            for key in self.prefilter_result_keys.take(file_id) {
                cache.invalidate(&key);
            }
        }
        if let Some(cache) = &self.inverted_index_cache {
            cache.invalidate_file(file_id);
        }
        if let Some(cache) = &self.bloom_filter_index_cache {
            cache.invalidate_file(file_id);
        }
        if let Some(cache) = &self.index_result_cache {
            cache.invalidate_file(file_id);
        }
    }

    /// Returns whether the authoritative SST metadata tier has reached its reservation.
    pub(crate) fn sst_meta_cache_is_full(&self) -> bool {
        let Some(cache) = &self.sst_meta_cache else {
            return true;
        };
        cache
            .policy()
            .max_capacity()
            .is_some_and(|capacity| cache.weighted_size() >= capacity)
    }

    /// Returns true if the in-memory SST meta cache is enabled.
    pub(crate) fn sst_meta_cache_enabled(&self) -> bool {
        self.sst_meta_cache.is_some()
    }

    /// Gets a vector with repeated value for specific `key`.
    pub fn get_repeated_vector(
        &self,
        data_type: &ConcreteDataType,
        value: &Value,
    ) -> Option<VectorRef> {
        self.vector_cache.as_ref().and_then(|vector_cache| {
            let value = vector_cache.get(&(data_type.clone(), value.clone()));
            update_hit_miss(value, VECTOR_TYPE)
        })
    }

    /// Puts a vector with repeated value into the cache.
    pub fn put_repeated_vector(&self, value: Value, vector: VectorRef) {
        if let Some(cache) = &self.vector_cache {
            let key = (vector.data_type(), value);
            CACHE_BYTES
                .with_label_values(&[VECTOR_TYPE])
                .add(vector_cache_weight(&key, &vector).into());
            cache.insert(key, vector);
        }
    }

    /// Gets cached byte fragments for the requested ranges.
    pub fn get_page_ranges(
        &self,
        file_id: FileId,
        row_group_idx: usize,
        ranges: &[Range<u64>],
    ) -> Option<PageRangeLookup> {
        self.page_cache.as_ref().map(|page_cache| {
            let lookup = page_cache.lookup(file_id, row_group_idx, ranges);
            if lookup.cached_bytes > 0 {
                CACHE_HIT.with_label_values(&[PAGE_TYPE]).inc();
            }
            if !lookup.missing_ranges.is_empty() {
                CACHE_MISS.with_label_values(&[PAGE_TYPE]).inc();
            }
            lookup
        })
    }

    /// Puts byte fragments into the page cache.
    pub fn put_page_ranges(
        &self,
        file_id: FileId,
        row_group_idx: usize,
        ranges: &[Range<u64>],
        pages: &[Bytes],
    ) {
        if let Some(cache) = &self.page_cache {
            cache.insert_ranges(file_id, row_group_idx, ranges, pages);
        }
    }

    /// Evicts every puffin-related cache entry for the given file.
    pub async fn evict_puffin_cache(&self, file_id: RegionIndexId) {
        if let Some(cache) = &self.bloom_filter_index_cache {
            cache.invalidate_file(file_id.file_id());
        }

        if let Some(cache) = &self.inverted_index_cache {
            cache.invalidate_file(file_id.file_id());
        }

        if let Some(cache) = &self.index_result_cache {
            cache.invalidate_file(file_id.file_id());
        }

        if let Some(cache) = &self.puffin_metadata_cache {
            cache.remove(&file_id.to_string());
        }

        if let Some(write_cache) = &self.write_cache {
            write_cache
                .remove(IndexKey::new(
                    file_id.region_id(),
                    file_id.file_id(),
                    FileType::Puffin(file_id.version),
                ))
                .await;
        }
    }

    /// Gets result of for the selector.
    pub fn get_selector_result(
        &self,
        selector_key: &SelectorResultKey,
    ) -> Option<Arc<SelectorResultValue>> {
        self.selector_result_cache
            .as_ref()
            .and_then(|selector_result_cache| selector_result_cache.get(selector_key))
            .map(|(_, value)| value)
    }

    /// Puts result of the selector into the cache.
    pub fn put_selector_result(
        &self,
        selector_key: SelectorResultKey,
        result: Arc<SelectorResultValue>,
    ) {
        if let Some(cache) = &self.selector_result_cache {
            CACHE_BYTES
                .with_label_values(&[SELECTOR_RESULT_TYPE])
                .add(selector_result_cache_weight(&selector_key, &result).into());
            insert_tracked(cache, selector_key, result, |generation| {
                self.selector_result_keys
                    .add(selector_key.file_id, selector_key, generation)
            });
        }
    }

    /// Gets cached result for range scan.
    #[allow(dead_code)]
    pub(crate) fn get_range_result(
        &self,
        key: &RangeScanCacheKey,
    ) -> Option<Arc<RangeScanCacheValue>> {
        self.range_result_cache.as_ref().and_then(|cache| {
            update_hit_miss(cache.get(key).map(|(_, value)| value), RANGE_RESULT_TYPE)
        })
    }

    /// Puts range scan result into cache.
    pub(crate) fn put_range_result(
        &self,
        key: RangeScanCacheKey,
        result: Arc<RangeScanCacheValue>,
    ) {
        if let Some(cache) = &self.range_result_cache {
            CACHE_BYTES
                .with_label_values(&[RANGE_RESULT_TYPE])
                .add(range_result_cache_weight(&key, &result).into());
            let shared_key = Arc::new(key.clone());
            insert_tracked(cache, key, result, |generation| {
                for file_id in shared_key.file_ids() {
                    self.range_result_keys
                        .add(file_id, shared_key.clone(), generation);
                }
            });
        }
    }

    /// Returns true if the range result cache is enabled.
    pub(crate) fn has_range_result_cache(&self) -> bool {
        self.range_result_cache.is_some()
    }

    pub(crate) fn range_result_memory_limiter(&self) -> &Arc<RangeResultMemoryLimiter> {
        &self.range_result_memory_limiter
    }

    pub(crate) fn range_result_cache_size(&self) -> usize {
        self.range_result_cache_size as usize
    }

    /// Gets the write cache.
    pub(crate) fn write_cache(&self) -> Option<&WriteCacheRef> {
        self.write_cache.as_ref()
    }

    pub(crate) fn inverted_index_cache(&self) -> Option<&InvertedIndexCacheRef> {
        self.inverted_index_cache.as_ref()
    }

    pub(crate) fn bloom_filter_index_cache(&self) -> Option<&BloomFilterIndexCacheRef> {
        self.bloom_filter_index_cache.as_ref()
    }

    pub(crate) fn puffin_metadata_cache(&self) -> Option<&PuffinMetadataCacheRef> {
        self.puffin_metadata_cache.as_ref()
    }

    pub(crate) fn index_result_cache(&self) -> Option<&IndexResultCache> {
        self.index_result_cache.as_ref()
    }

    pub(crate) fn get_prefilter_result(&self, key: &PrefilterKey) -> Option<Arc<BooleanBuffer>> {
        self.prefilter_result_cache.as_ref().and_then(|cache| {
            update_hit_miss(
                cache.get(key).map(|(_, value)| value),
                PREFILTER_RESULT_TYPE,
            )
        })
    }

    pub(crate) fn put_prefilter_result(&self, key: PrefilterKey, result: Arc<BooleanBuffer>) {
        if let Some(cache) = &self.prefilter_result_cache {
            CACHE_BYTES
                .with_label_values(&[PREFILTER_RESULT_TYPE])
                .add(prefilter_result_cache_weight(&key, &result).into());
            let registered = key.clone();
            insert_tracked(cache, key, result, |generation| {
                self.prefilter_result_keys
                    .add(registered.file_id, registered, generation)
            });
        }
    }
}

/// Increases selector cache miss metrics.
pub fn selector_result_cache_miss() {
    CACHE_MISS.with_label_values(&[SELECTOR_RESULT_TYPE]).inc()
}

/// Increases selector cache hit metrics.
pub fn selector_result_cache_hit() {
    CACHE_HIT.with_label_values(&[SELECTOR_RESULT_TYPE]).inc()
}

/// Builder to construct a [CacheManager].
#[derive(Default)]
pub struct CacheManagerBuilder {
    sst_meta_cache_size: u64,
    vector_cache_size: u64,
    page_cache_size: u64,
    index_metadata_size: u64,
    index_content_size: u64,
    index_content_page_size: u64,
    index_result_cache_size: u64,
    prefilter_result_cache_size: u64,
    puffin_metadata_size: u64,
    write_cache: Option<WriteCacheRef>,
    selector_result_cache_size: u64,
    range_result_cache_size: u64,
}

impl CacheManagerBuilder {
    /// Sets meta cache size.
    pub fn sst_meta_cache_size(mut self, bytes: u64) -> Self {
        self.sst_meta_cache_size = bytes;
        self
    }

    /// Sets vector cache size.
    pub fn vector_cache_size(mut self, bytes: u64) -> Self {
        self.vector_cache_size = bytes;
        self
    }

    /// Sets page cache size.
    pub fn page_cache_size(mut self, bytes: u64) -> Self {
        self.page_cache_size = bytes;
        self
    }

    /// Sets write cache.
    pub fn write_cache(mut self, cache: Option<WriteCacheRef>) -> Self {
        self.write_cache = cache;
        self
    }

    /// Sets cache size for index metadata.
    pub fn index_metadata_size(mut self, bytes: u64) -> Self {
        self.index_metadata_size = bytes;
        self
    }

    /// Sets cache size for index content.
    pub fn index_content_size(mut self, bytes: u64) -> Self {
        self.index_content_size = bytes;
        self
    }

    /// Sets page size for index content.
    pub fn index_content_page_size(mut self, bytes: u64) -> Self {
        self.index_content_page_size = bytes;
        self
    }

    /// Sets cache size for index result.
    pub fn index_result_cache_size(mut self, bytes: u64) -> Self {
        self.index_result_cache_size = bytes;
        self
    }

    /// Sets cache size for prefilter result.
    pub fn prefilter_result_cache_size(mut self, bytes: u64) -> Self {
        self.prefilter_result_cache_size = bytes;
        self
    }

    /// Sets cache size for puffin metadata.
    pub fn puffin_metadata_size(mut self, bytes: u64) -> Self {
        self.puffin_metadata_size = bytes;
        self
    }

    /// Sets selector result cache size.
    pub fn selector_result_cache_size(mut self, bytes: u64) -> Self {
        self.selector_result_cache_size = bytes;
        self
    }

    /// Sets range result cache size.
    pub fn range_result_cache_size(mut self, bytes: u64) -> Self {
        self.range_result_cache_size = bytes;
        self
    }

    /// Builds the [CacheManager].
    pub fn build(self) -> CacheManager {
        // Reserve half the configured capacity for the compact authoritative tier. This prevents
        // decoded acceleration entries from evicting metadata that would require storage I/O to
        // recover. Both reservations together equal the configured limit.
        let compact_meta_capacity = self.sst_meta_cache_size.div_ceil(2);
        let decoded_meta_capacity = self.sst_meta_cache_size / 2;
        let sst_meta_cache = (compact_meta_capacity != 0).then(|| {
            Cache::builder()
                .max_capacity(compact_meta_capacity)
                .weigher(meta_cache_weight)
                .eviction_listener(|k, v, cause| {
                    let size = meta_cache_weight(&k, &v);
                    CACHE_BYTES
                        .with_label_values(&[SST_META_TYPE])
                        .sub(size.into());
                    CACHE_EVICTION
                        .with_label_values(&[SST_META_TYPE, removal_cause_str(cause)])
                        .inc();
                })
                .build()
        });
        let sst_decoded_meta_cache = (decoded_meta_capacity != 0).then(|| {
            Cache::builder()
                .max_capacity(decoded_meta_capacity)
                .weigher(decoded_meta_cache_weight)
                .eviction_listener(|k, v, cause| {
                    let size = decoded_meta_cache_weight(&k, &v);
                    CACHE_BYTES
                        .with_label_values(&[SST_META_DECODED_TYPE])
                        .sub(size.into());
                    CACHE_EVICTION
                        .with_label_values(&[SST_META_DECODED_TYPE, removal_cause_str(cause)])
                        .inc();
                })
                .build()
        });
        let vector_cache = (self.vector_cache_size != 0).then(|| {
            Cache::builder()
                .max_capacity(self.vector_cache_size)
                .weigher(vector_cache_weight)
                .eviction_listener(|k, v, cause| {
                    let size = vector_cache_weight(&k, &v);
                    CACHE_BYTES
                        .with_label_values(&[VECTOR_TYPE])
                        .sub(size.into());
                    CACHE_EVICTION
                        .with_label_values(&[VECTOR_TYPE, removal_cause_str(cause)])
                        .inc();
                })
                .build()
        });
        let page_cache =
            (self.page_cache_size != 0).then(|| PageRangeCache::new(self.page_cache_size));
        let inverted_index_cache = InvertedIndexCache::new(
            self.index_metadata_size,
            self.index_content_size,
            self.index_content_page_size,
        );
        // TODO(ruihang): check if it's ok to reuse the same param with inverted index
        let bloom_filter_index_cache = BloomFilterIndexCache::new(
            self.index_metadata_size,
            self.index_content_size,
            self.index_content_page_size,
        );
        let index_result_cache = (self.index_result_cache_size != 0)
            .then(|| IndexResultCache::new(self.index_result_cache_size));
        let prefilter_result_keys = Arc::new(FileKeys::default());
        let prefilter_result_cache = (self.prefilter_result_cache_size != 0).then(|| {
            new_prefilter_result_cache(
                self.prefilter_result_cache_size,
                prefilter_result_keys.clone(),
            )
        });
        let puffin_metadata_cache =
            PuffinMetadataCache::new(self.puffin_metadata_size, &CACHE_BYTES);
        let selector_result_keys = Arc::new(FileKeys::default());
        let selector_result_cache = (self.selector_result_cache_size != 0).then(|| {
            let keys = selector_result_keys.clone();
            Cache::builder()
                .max_capacity(self.selector_result_cache_size)
                .weigher(|k, v: &Tracked<Arc<SelectorResultValue>>| {
                    selector_result_cache_weight(k, &v.1)
                })
                .eviction_listener(move |k, v, cause| {
                    keys.remove(k.file_id, &*k, v.0);
                    let size = selector_result_cache_weight(&k, &v.1);
                    CACHE_BYTES
                        .with_label_values(&[SELECTOR_RESULT_TYPE])
                        .sub(size.into());
                    CACHE_EVICTION
                        .with_label_values(&[SELECTOR_RESULT_TYPE, removal_cause_str(cause)])
                        .inc();
                })
                .build()
        });
        let range_result_keys = Arc::new(FileKeys::default());
        let range_result_cache = (self.range_result_cache_size != 0).then(|| {
            let keys = range_result_keys.clone();
            Cache::builder()
                .max_capacity(self.range_result_cache_size)
                .weigher(|k, v: &Tracked<Arc<RangeScanCacheValue>>| {
                    range_result_cache_weight(k, &v.1)
                })
                .eviction_listener(move |k, v, cause| {
                    for file_id in k.file_ids() {
                        keys.remove(file_id, &k, v.0);
                    }
                    let size = range_result_cache_weight(&k, &v.1);
                    CACHE_BYTES
                        .with_label_values(&[RANGE_RESULT_TYPE])
                        .sub(size.into());
                    CACHE_EVICTION
                        .with_label_values(&[RANGE_RESULT_TYPE, removal_cause_str(cause)])
                        .inc();
                })
                .build()
        });
        CacheManager {
            sst_meta_cache,
            sst_decoded_meta_cache,
            vector_cache,
            page_cache,
            write_cache: self.write_cache,
            inverted_index_cache: Some(Arc::new(inverted_index_cache)),
            bloom_filter_index_cache: Some(Arc::new(bloom_filter_index_cache)),
            puffin_metadata_cache: Some(Arc::new(puffin_metadata_cache)),
            selector_result_cache,
            range_result_cache,
            range_result_cache_size: self.range_result_cache_size,
            range_result_memory_limiter: Arc::new(RangeResultMemoryLimiter::new(
                self.range_result_cache_size as usize,
                RANGE_RESULT_CONCAT_MEMORY_PERMIT.as_bytes() as usize,
            )),
            index_result_cache,
            prefilter_result_cache,
            selector_result_keys,
            range_result_keys,
            prefilter_result_keys,
        }
    }
}

fn meta_cache_weight(k: &SstMetaKey, v: &Arc<CompactSstMeta>) -> u32 {
    // We ignore the size of `Arc`. Region metadata is already present in the compressed Parquet
    // stream and is materialized only in the decoded acceleration tier.
    let size = k.estimated_size() + mem::size_of::<CompactSstMeta>() + v.encoded_metadata.len();
    u32::try_from(size).unwrap_or(u32::MAX)
}

fn decoded_meta_cache_weight(k: &SstMetaKey, v: &Arc<CachedSstMeta>) -> u32 {
    let size = k.estimated_size() + v.parquet_metadata_size + v.region_metadata_weight;
    u32::try_from(size).unwrap_or(u32::MAX)
}

fn vector_cache_weight(_k: &(ConcreteDataType, Value), v: &VectorRef) -> u32 {
    // We ignore the heap size of `Value`.
    (mem::size_of::<ConcreteDataType>() + mem::size_of::<Value>() + v.memory_size()) as u32
}

fn page_cache_weight(k: &PageFragmentKey, v: &Bytes) -> u32 {
    (k.estimated_size() + mem::size_of::<Tracked<Bytes>>() + v.len()) as u32
}

fn selector_result_cache_weight(k: &SelectorResultKey, v: &Arc<SelectorResultValue>) -> u32 {
    (mem::size_of_val(k) + mem::size_of::<u64>() + v.estimated_size()) as u32
}

fn range_result_cache_weight(k: &RangeScanCacheKey, v: &Arc<RangeScanCacheValue>) -> u32 {
    (k.estimated_size() + mem::size_of::<u64>() + v.estimated_size()) as u32
}

/// Updates cache hit/miss metrics.
fn update_hit_miss<T>(value: Option<T>, cache_type: &str) -> Option<T> {
    if value.is_some() {
        CACHE_HIT.with_label_values(&[cache_type]).inc();
    } else {
        CACHE_MISS.with_label_values(&[cache_type]).inc();
    }
    value
}

/// Cache key (region id, file id) for SST meta.
#[derive(Debug, Clone, PartialEq, Eq, Hash)]
struct SstMetaKey(RegionId, FileId);

impl SstMetaKey {
    /// Returns memory used by the key (estimated).
    fn estimated_size(&self) -> usize {
        mem::size_of::<Self>()
    }
}

/// Cache key for one byte fragment in an SST row group.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct PageFragmentKey {
    /// Id of the SST file.
    file_id: FileId,
    /// Index of the row group.
    row_group_idx: usize,
    /// Start offset of the cached byte fragment.
    start: u64,
    /// End offset of the cached byte fragment.
    end: u64,
}

impl PageFragmentKey {
    fn new(file_id: FileId, row_group_idx: usize, range: &Range<u64>) -> PageFragmentKey {
        PageFragmentKey {
            file_id,
            row_group_idx,
            start: range.start,
            end: range.end,
        }
    }

    /// Returns memory used by the key (estimated).
    fn estimated_size(&self) -> usize {
        mem::size_of::<Self>()
    }
}

/// One cached byte fragment that overlaps a requested range.
#[derive(Clone)]
pub struct PageRangePart {
    /// Range covered by `bytes`.
    pub range: Range<u64>,
    /// Bytes for `range`.
    pub bytes: Bytes,
}

/// Result of looking up request ranges in the page range cache.
pub struct PageRangeLookup {
    /// Cached fragments grouped by the original requested range index.
    pub cached_parts: Vec<Vec<PageRangePart>>,
    /// Ranges that are not covered by cached fragments and need fetching.
    pub missing_ranges: Vec<Range<u64>>,
    /// Number of cached fragments used.
    pub cached_range_count: usize,
    /// Number of requested bytes served from cached fragments.
    pub cached_bytes: u64,
}

impl PageRangeLookup {
    pub fn is_fully_cached(&self) -> bool {
        self.missing_ranges.is_empty()
    }
}

/// Fragment range -> (key, insert generation of the cached bytes).
type PageFragmentRangeIndex = BTreeMap<(u64, u64), (PageFragmentKey, u64)>;
/// File id -> row group index -> cached fragments. Grouping by file lets a purged
/// file drop all of its fragments without scanning the whole index.
type PageFragmentIndex = HashMap<FileId, HashMap<usize, PageFragmentRangeIndex>>;

/// Byte-fragment cache for Parquet row-group reads.
pub struct PageRangeCache {
    cache: Cache<PageFragmentKey, Tracked<Bytes>>,
    index: RwLock<PageFragmentIndex>,
}

impl PageRangeCache {
    fn new(capacity: u64) -> Arc<PageRangeCache> {
        Arc::new_cyclic(|weak_cache: &std::sync::Weak<PageRangeCache>| {
            let cache = Cache::builder()
                .max_capacity(capacity)
                .weigher(|k, v: &Tracked<Bytes>| page_cache_weight(k, &v.1))
                .eviction_listener({
                    let weak_cache = weak_cache.clone();
                    move |k, v, cause| {
                        let size = page_cache_weight(&k, &v.1);
                        CACHE_BYTES.with_label_values(&[PAGE_TYPE]).sub(size.into());
                        CACHE_EVICTION
                            .with_label_values(&[PAGE_TYPE, removal_cause_str(cause)])
                            .inc();

                        if let Some(cache) = weak_cache.upgrade() {
                            cache.remove_index_entry(*k, v.0);
                        }
                    }
                })
                .build();

            PageRangeCache {
                cache,
                index: RwLock::new(HashMap::new()),
            }
        })
    }

    fn lookup(
        &self,
        file_id: FileId,
        row_group_idx: usize,
        ranges: &[Range<u64>],
    ) -> PageRangeLookup {
        let mut cached_parts = Vec::with_capacity(ranges.len());
        let mut missing_ranges = Vec::new();
        let mut cached_range_count = 0;
        let mut cached_bytes = 0;

        for range in ranges {
            if range.start >= range.end {
                cached_parts.push(Vec::new());
                continue;
            }

            let mut parts = Vec::new();
            // An indexed fragment may be missing from the cache while its insert is
            // still in flight or its removal notification is pending. The removal
            // listener of that generation drops the index entry.
            let candidates = self.find_index_candidates(file_id, row_group_idx, range);

            for fragment_key in candidates {
                if let Some((_, bytes)) = self.cache.get(&fragment_key) {
                    let part_start = range.start.max(fragment_key.start);
                    let part_end = range.end.min(fragment_key.end);
                    let slice_start = (part_start - fragment_key.start) as usize;
                    let slice_end = (part_end - fragment_key.start) as usize;
                    parts.push(PageRangePart {
                        range: part_start..part_end,
                        bytes: bytes.slice(slice_start..slice_end),
                    });
                }
            }

            let mut cursor = range.start;
            let mut compacted_parts: Vec<PageRangePart> = Vec::with_capacity(parts.len());
            for part in parts {
                if part.range.end <= cursor {
                    continue;
                }

                let part = if part.range.start < cursor {
                    let offset = (cursor - part.range.start) as usize;
                    PageRangePart {
                        range: cursor..part.range.end,
                        bytes: part.bytes.slice(offset..),
                    }
                } else {
                    part
                };

                if cursor < part.range.start {
                    missing_ranges.push(cursor..part.range.start);
                }
                cached_bytes += part.range.end - part.range.start;
                cached_range_count += 1;
                cursor = part.range.end;
                compacted_parts.push(part);

                if cursor >= range.end {
                    break;
                }
            }

            if cursor < range.end {
                missing_ranges.push(cursor..range.end);
            }
            cached_parts.push(compacted_parts);
        }

        PageRangeLookup {
            cached_parts,
            missing_ranges,
            cached_range_count,
            cached_bytes,
        }
    }

    fn insert_ranges(
        &self,
        file_id: FileId,
        row_group_idx: usize,
        ranges: &[Range<u64>],
        pages: &[Bytes],
    ) {
        for (range, bytes) in ranges.iter().zip(pages) {
            if range.start >= range.end || bytes.len() as u64 != range.end - range.start {
                continue;
            }

            let key = PageFragmentKey::new(file_id, row_group_idx, range);
            let bytes = Bytes::copy_from_slice(bytes.as_ref());
            let size = page_cache_weight(&key, &bytes);
            CACHE_BYTES.with_label_values(&[PAGE_TYPE]).add(size.into());
            insert_tracked(&self.cache, key, bytes, |generation| {
                self.register_fragment(key, generation)
            });
        }
    }

    fn register_fragment(&self, key: PageFragmentKey, generation: u64) {
        self.index
            .write()
            .unwrap()
            .entry(key.file_id)
            .or_default()
            .entry(key.row_group_idx)
            .or_default()
            .insert((key.start, key.end), (key, generation));
    }

    /// Removes all fragments of `file_id`.
    fn invalidate_file(&self, file_id: FileId) {
        let removed = self.index.write().unwrap().remove(&file_id);
        for key in removed
            .into_iter()
            .flat_map(|row_groups| row_groups.into_values())
            .flat_map(|ranges| ranges.into_values())
        {
            self.cache.invalidate(&key.0);
        }
    }

    fn find_index_candidates(
        &self,
        file_id: FileId,
        row_group_idx: usize,
        range: &Range<u64>,
    ) -> Vec<PageFragmentKey> {
        let index = self.index.read().unwrap();
        index
            .get(&file_id)
            .and_then(|row_groups| row_groups.get(&row_group_idx))
            .map(|ranges| {
                ranges
                    .range(..(range.end, 0))
                    .filter_map(|(_, (fragment_key, _))| {
                        (fragment_key.end > range.start).then_some(*fragment_key)
                    })
                    .collect()
            })
            .unwrap_or_default()
    }

    /// Removes the index entry of `key` if it belongs to the insert `generation`.
    fn remove_index_entry(&self, key: PageFragmentKey, generation: u64) {
        let mut index = self.index.write().unwrap();
        let Some(row_groups) = index.get_mut(&key.file_id) else {
            return;
        };
        let Some(ranges) = row_groups.get_mut(&key.row_group_idx) else {
            return;
        };

        let removed =
            ranges
                .get(&(key.start, key.end))
                .is_some_and(|(current, current_generation)| {
                    current == &key && generation == *current_generation
                });
        if removed {
            ranges.remove(&(key.start, key.end));
        }
        if ranges.is_empty() {
            row_groups.remove(&key.row_group_idx);
        }
        if row_groups.is_empty() {
            index.remove(&key.file_id);
        }
    }
}

/// Cache key for time series row selector result.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct SelectorResultKey {
    /// Id of the SST file.
    pub file_id: FileId,
    /// Index of the row group.
    pub row_group_idx: usize,
    /// Time series row selector.
    pub selector: TimeSeriesRowSelector,
}

/// Result stored in the selector result cache.
pub enum SelectorResult {
    /// Batches in the primary key format.
    PrimaryKey(Vec<Batch>),
    /// Record batches in the flat format.
    Flat(Vec<RecordBatch>),
}

/// Cached result for time series row selector.
pub struct SelectorResultValue {
    /// Batches of rows selected by the selector.
    pub result: SelectorResult,
    /// The read columns of rows.
    pub read_cols: ParquetReadColumns,
    /// JSON2 target types used by flat-format reads.
    ///
    /// JSON2 projection is query-driven; the same parquet columns can produce
    /// different cached batches under different type hints.
    pub json_target_types: JsonTargetTypes,
}

impl SelectorResultValue {
    /// Creates a new selector result value with primary key format.
    pub fn new(result: Vec<Batch>, read_cols: ParquetReadColumns) -> SelectorResultValue {
        SelectorResultValue {
            result: SelectorResult::PrimaryKey(result),
            read_cols,
            json_target_types: Arc::default(),
        }
    }

    /// Creates a new selector result value with flat format.
    pub fn new_flat(
        result: Vec<RecordBatch>,
        read_cols: ParquetReadColumns,
        json_target_types: JsonTargetTypes,
    ) -> SelectorResultValue {
        SelectorResultValue {
            result: SelectorResult::Flat(result),
            read_cols,
            json_target_types,
        }
    }

    /// Returns memory used by the value (estimated).
    fn estimated_size(&self) -> usize {
        let result_size: usize = match &self.result {
            SelectorResult::PrimaryKey(batches) => {
                batches.iter().map(|batch| batch.memory_size()).sum()
            }
            SelectorResult::Flat(batches) => batches.iter().map(record_batch_estimated_size).sum(),
        };
        result_size
            + self.json_target_types.len() * (size_of::<ColumnId>() + size_of::<JsonNativeType>())
    }
}

/// Maps (region id, file id) to fused SST metadata.
type SstMetaCache = Cache<SstMetaKey, Arc<CompactSstMeta>>;
/// Maps (region id, file id) to decoded SST metadata retained as an acceleration tier.
type SstDecodedMetaCache = Cache<SstMetaKey, Arc<CachedSstMeta>>;
/// Maps [Value] to a vector that holds this value repeatedly.
///
/// e.g. `"hello" => ["hello", "hello", "hello"]`
type VectorCache = Cache<(ConcreteDataType, Value), VectorRef>;
/// Maps (file id, row group id, time series row selector) to [SelectorResultValue].
type SelectorResultCache = Cache<SelectorResultKey, Tracked<Arc<SelectorResultValue>>>;
/// Maps partition-range scan key to cached flat batches.
type RangeResultCache = Cache<RangeScanCacheKey, Tracked<Arc<RangeScanCacheValue>>>;

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::SemanticType;
    use api::v1::index::{BloomFilterMeta, InvertedIndexMetas};
    use datatypes::schema::ColumnSchema;
    use datatypes::vectors::Int64Vector;
    use parquet::file::page_index::offset_index::{OffsetIndexMetaData, PageLocation};
    use puffin::file_metadata::FileMetadata;
    use store_api::metadata::{ColumnMetadata, RegionMetadata, RegionMetadataBuilder};
    use store_api::storage::ColumnId;

    use super::*;
    use crate::cache::index::bloom_filter_index::Tag;
    use crate::cache::index::result_cache::PredicateKey;
    use crate::cache::test_util::{
        parquet_meta, sst_parquet_meta, sst_parquet_meta_with_region_metadata,
    };
    use crate::read::range_cache::{
        RangeScanCacheKey, RangeScanCacheValue, ScanRequestFingerprintBuilder,
    };
    use crate::read::read_columns::ReadColumns;
    use crate::sst::parquet::row_selection::RowGroupSelection;

    #[tokio::test]
    async fn test_disable_cache() {
        let cache = CacheManager::default();
        assert!(cache.sst_meta_cache.is_none());
        assert!(cache.sst_decoded_meta_cache.is_none());
        assert!(cache.vector_cache.is_none());
        assert!(cache.page_cache.is_none());

        let region_id = RegionId::new(1, 1);
        let file_id = RegionFileId::new(region_id, FileId::random());
        let metadata = parquet_meta();
        let mut metrics = MetadataCacheMetrics::default();
        cache.put_parquet_meta_data(file_id, metadata, None);
        assert!(
            cache
                .get_sst_meta_data(
                    file_id,
                    &mut metrics,
                    Default::default(),
                    &common_runtime::global_runtime()
                )
                .await
                .is_none()
        );

        let value = Value::Int64(10);
        let vector: VectorRef = Arc::new(Int64Vector::from_slice([10, 10, 10, 10]));
        cache.put_repeated_vector(value.clone(), vector.clone());
        assert!(
            cache
                .get_repeated_vector(&ConcreteDataType::int64_datatype(), &value)
                .is_none()
        );

        cache.put_page_ranges(
            file_id.file_id(),
            1,
            &[Range { start: 0, end: 5 }],
            &[Bytes::from_static(b"abcde")],
        );
        assert!(
            cache
                .get_page_ranges(file_id.file_id(), 1, &[Range { start: 0, end: 5 }])
                .is_none()
        );

        assert!(cache.write_cache().is_none());
    }

    #[test]
    fn test_sst_meta_cache_splits_capacity() {
        let cache = CacheManager::builder().sst_meta_cache_size(101).build();

        assert_eq!(
            Some(51),
            cache
                .sst_meta_cache
                .as_ref()
                .unwrap()
                .policy()
                .max_capacity()
        );
        assert_eq!(
            Some(50),
            cache
                .sst_decoded_meta_cache
                .as_ref()
                .unwrap()
                .policy()
                .max_capacity()
        );
    }

    #[tokio::test]
    async fn test_parquet_meta_cache() {
        let cache = CacheManager::builder().sst_meta_cache_size(2000).build();
        let mut metrics = MetadataCacheMetrics::default();
        let region_id = RegionId::new(1, 1);
        let file_id = RegionFileId::new(region_id, FileId::random());
        assert!(
            cache
                .get_sst_meta_data(
                    file_id,
                    &mut metrics,
                    Default::default(),
                    &common_runtime::global_runtime()
                )
                .await
                .is_none()
        );
        let (metadata, region_metadata) = sst_parquet_meta();
        cache.put_parquet_meta_data(file_id, metadata, None);
        let cached = cache
            .get_sst_meta_data(
                file_id,
                &mut metrics,
                Default::default(),
                &common_runtime::global_runtime(),
            )
            .await
            .unwrap();
        assert_eq!(region_metadata, cached.region_metadata());
        assert!(
            cached
                .parquet_metadata()
                .file_metadata()
                .key_value_metadata()
                .is_none_or(|key_values| {
                    key_values
                        .iter()
                        .all(|key_value| key_value.key != PARQUET_METADATA_KEY)
                })
        );
        cache.remove_parquet_meta_data(file_id);
        assert!(
            cache
                .get_sst_meta_data(
                    file_id,
                    &mut metrics,
                    Default::default(),
                    &common_runtime::global_runtime()
                )
                .await
                .is_none()
        );
    }

    #[tokio::test]
    async fn test_parquet_meta_cache_with_provided_region_metadata() {
        let cache = CacheManager::builder().sst_meta_cache_size(2000).build();
        let mut metrics = MetadataCacheMetrics::default();
        let region_id = RegionId::new(1, 1);
        let file_id = RegionFileId::new(region_id, FileId::random());
        let (metadata, region_metadata) = sst_parquet_meta();

        cache.put_parquet_meta_data(file_id, metadata, Some(region_metadata.clone()));

        let cached = cache
            .get_sst_meta_data(
                file_id,
                &mut metrics,
                Default::default(),
                &common_runtime::global_runtime(),
            )
            .await
            .unwrap();
        assert!(Arc::ptr_eq(&region_metadata, &cached.region_metadata()));
    }

    #[tokio::test]
    async fn test_compact_sst_meta_round_trip() {
        let cache = CacheManager::builder()
            .sst_meta_cache_size(1024 * 1024)
            .build();
        let region_id = RegionId::new(1, 1);
        let file_id = RegionFileId::new(region_id, FileId::random());
        let (metadata, expected_region_metadata) = sst_parquet_meta();
        let metadata = Arc::unwrap_or_clone(metadata);
        let offset_indexes = metadata
            .row_groups()
            .iter()
            .map(|row_group| {
                row_group
                    .columns()
                    .iter()
                    .map(|_| OffsetIndexMetaData {
                        page_locations: vec![PageLocation {
                            offset: 42,
                            compressed_page_size: 128,
                            first_row_index: 0,
                        }],
                        unencoded_byte_array_data_bytes: None,
                    })
                    .collect()
            })
            .collect();
        let metadata = metadata
            .into_builder()
            .set_offset_index(Some(offset_indexes))
            .build();
        let expected_rows = metadata.file_metadata().num_rows();
        let prepared = prepare_sst_meta(
            "test.parquet",
            metadata,
            None,
            PageIndexPolicy::Required,
            &common_runtime::global_runtime(),
        )
        .await
        .unwrap();
        let SstMetaPreparation::Prepared(prepared) = prepared else {
            panic!("valid metadata should produce a compact cache entry");
        };

        // Retain only the compact form so the lookup must exercise decompression and decoding.
        cache.put_prepared_sst_meta(file_id, prepared, false);
        let mut metrics = MetadataCacheMetrics::default();
        let cached = cache
            .get_sst_meta_data(
                file_id,
                &mut metrics,
                PageIndexPolicy::Required,
                &common_runtime::global_runtime(),
            )
            .await
            .unwrap();

        assert_eq!(1, metrics.mem_cache_hit);
        assert_eq!(0, metrics.cache_miss);
        assert_eq!(
            expected_rows,
            cached.parquet_metadata.file_metadata().num_rows()
        );
        assert!(cached.parquet_metadata.offset_index().is_some());
        assert_eq!(expected_region_metadata, cached.region_metadata());
        assert!(
            cached
                .parquet_metadata
                .file_metadata()
                .key_value_metadata()
                .is_none_or(|key_values| key_values
                    .iter()
                    .all(|key_value| key_value.key != PARQUET_METADATA_KEY))
        );
    }

    #[test]
    fn test_cache_encoding_failure_preserves_decoded_metadata() {
        let (metadata, expected_region_metadata) = sst_parquet_meta();
        let expected_rows = metadata.file_metadata().num_rows();
        let cache_encoding: Result<(Bytes, usize)> = Err(UnexpectedSnafu {
            reason: "injected compact metadata encoding failure",
        }
        .build());

        let preparation = finish_sst_meta_preparation(
            "test.parquet",
            Arc::unwrap_or_clone(metadata),
            None,
            PageIndexPolicy::Skip,
            cache_encoding,
        )
        .unwrap();
        let SstMetaPreparation::DecodedOnly {
            decoded,
            encoding_error,
        } = preparation
        else {
            panic!("cache encoding failure should return decoded-only metadata");
        };

        assert_eq!(
            expected_rows,
            decoded.parquet_metadata.file_metadata().num_rows()
        );
        assert_eq!(expected_region_metadata, decoded.region_metadata());
        assert!(
            encoding_error
                .to_string()
                .contains("injected compact metadata encoding failure")
        );
    }

    #[tokio::test]
    async fn test_parquet_meta_cache_respects_page_index_policy() {
        let cache = CacheManager::builder().sst_meta_cache_size(2000).build();
        let region_id = RegionId::new(1, 1);
        let file_id = RegionFileId::new(region_id, FileId::random());
        let (metadata, _) = sst_parquet_meta();

        let skip_metadata = Arc::new(
            CachedSstMeta::try_new_with_page_index_policy(
                "test.parquet",
                Arc::unwrap_or_clone(metadata.clone()),
                None,
                PageIndexPolicy::Skip,
            )
            .unwrap(),
        );
        cache.put_sst_meta_data(file_id, skip_metadata);

        let mut metrics = MetadataCacheMetrics::default();
        assert!(
            cache
                .get_sst_meta_data(
                    file_id,
                    &mut metrics,
                    PageIndexPolicy::Optional,
                    &common_runtime::global_runtime()
                )
                .await
                .is_none()
        );
        assert_eq!(1, metrics.cache_miss);

        let optional_metadata = Arc::new(
            CachedSstMeta::try_new_with_page_index_policy(
                "test.parquet",
                Arc::unwrap_or_clone(metadata),
                None,
                PageIndexPolicy::Optional,
            )
            .unwrap(),
        );
        cache.put_sst_meta_data(file_id, optional_metadata);

        let mut metrics = MetadataCacheMetrics::default();
        assert!(
            cache
                .get_sst_meta_data(
                    file_id,
                    &mut metrics,
                    PageIndexPolicy::Optional,
                    &common_runtime::global_runtime()
                )
                .await
                .is_some()
        );
        assert_eq!(1, metrics.mem_cache_hit);

        let mut metrics = MetadataCacheMetrics::default();
        assert!(
            cache
                .get_sst_meta_data(
                    file_id,
                    &mut metrics,
                    PageIndexPolicy::Skip,
                    &common_runtime::global_runtime()
                )
                .await
                .is_some()
        );
        assert_eq!(1, metrics.mem_cache_hit);
    }

    #[test]
    fn test_decoded_meta_cache_weight_accounts_for_region_metadata() {
        let region_metadata = Arc::new(wide_region_metadata(128));
        let json_len = region_metadata.to_json().unwrap().len();
        let metadata = sst_parquet_meta_with_region_metadata(region_metadata.clone());
        let cached = Arc::new(
            CachedSstMeta::try_new("test.parquet", Arc::unwrap_or_clone(metadata)).unwrap(),
        );
        let key = SstMetaKey(region_metadata.region_id, FileId::random());

        assert!(cached.region_metadata_weight > json_len);
        assert_eq!(
            decoded_meta_cache_weight(&key, &cached) as usize,
            key.estimated_size() + cached.parquet_metadata_size + cached.region_metadata_weight
        );
        assert_eq!(
            cached.parquet_metadata_size,
            parquet_meta_size(&cached.parquet_metadata)
        );
    }

    #[test]
    fn test_decoded_meta_cache_weight_saturates_on_overflow() {
        let region_metadata = Arc::new(wide_region_metadata(1));
        let metadata = sst_parquet_meta_with_region_metadata(region_metadata.clone());
        let mut cached =
            CachedSstMeta::try_new("test.parquet", Arc::unwrap_or_clone(metadata)).unwrap();
        cached.region_metadata_weight = u32::MAX as usize + 1;
        let cached = Arc::new(cached);
        let key = SstMetaKey(region_metadata.region_id, FileId::random());

        assert_eq!(u32::MAX, decoded_meta_cache_weight(&key, &cached));
    }

    #[test]
    fn test_repeated_vector_cache() {
        let cache = CacheManager::builder().vector_cache_size(4096).build();
        let value = Value::Int64(10);
        assert!(
            cache
                .get_repeated_vector(&ConcreteDataType::int64_datatype(), &value)
                .is_none()
        );
        let vector: VectorRef = Arc::new(Int64Vector::from_slice([10, 10, 10, 10]));
        cache.put_repeated_vector(value.clone(), vector.clone());
        let cached = cache
            .get_repeated_vector(&ConcreteDataType::int64_datatype(), &value)
            .unwrap();
        assert_eq!(vector, cached);
    }

    #[test]
    fn test_page_cache() {
        let cache = CacheManager::builder().page_cache_size(1000).build();
        let file_id = FileId::random();
        let uncached = 0..10;
        assert_eq!(
            vec![0..10],
            cache
                .get_page_ranges(file_id, 0, std::slice::from_ref(&uncached))
                .unwrap()
                .missing_ranges
        );

        let cached = 100..500;
        cache.put_page_ranges(
            file_id,
            0,
            std::slice::from_ref(&cached),
            &[Bytes::from(vec![7; 400])],
        );

        let subrange = 200..300;
        let lookup = cache
            .get_page_ranges(file_id, 0, std::slice::from_ref(&subrange))
            .unwrap();
        assert!(lookup.is_fully_cached());
        assert_eq!(100, lookup.cached_bytes);
        assert_eq!(1, lookup.cached_parts.len());
        assert_eq!(200..300, lookup.cached_parts[0][0].range);
        assert_eq!(100, lookup.cached_parts[0][0].bytes.len());

        let overlapping = 400..600;
        let lookup = cache
            .get_page_ranges(file_id, 0, std::slice::from_ref(&overlapping))
            .unwrap();
        assert!(!lookup.is_fully_cached());
        assert_eq!(100, lookup.cached_bytes);
        assert_eq!(vec![500..600], lookup.missing_ranges);
        assert_eq!(400..500, lookup.cached_parts[0][0].range);
    }

    #[test]
    fn test_page_cache_detaches_fragment_bytes() {
        let cache = PageRangeCache::new(1000);
        let file_id = FileId::random();
        let backing = Bytes::from(vec![1; 1024]);
        let page = backing.slice(512..522);
        let page_ptr = page.as_ptr();
        let range = 0..10;

        cache.insert_ranges(
            file_id,
            0,
            std::slice::from_ref(&range),
            std::slice::from_ref(&page),
        );

        let lookup = cache.lookup(file_id, 0, std::slice::from_ref(&range));
        assert!(lookup.is_fully_cached());
        assert_eq!(1, lookup.cached_parts[0].len());
        assert_eq!(&page[..], &lookup.cached_parts[0][0].bytes[..]);
        assert_ne!(page_ptr, lookup.cached_parts[0][0].bytes.as_ptr());
    }

    #[test]
    fn test_page_cache_replaces_fragment() {
        let cache = PageRangeCache::new(1000);
        let file_id = FileId::random();
        let range = 0..10;

        cache.insert_ranges(
            file_id,
            0,
            std::slice::from_ref(&range),
            &[Bytes::from(vec![1; 10])],
        );
        cache.insert_ranges(
            file_id,
            0,
            std::slice::from_ref(&range),
            &[Bytes::from(vec![2; 10])],
        );
        cache.cache.run_pending_tasks();
        assert_eq!(
            vec![PageFragmentKey::new(file_id, 0, &range)],
            cache.find_index_candidates(file_id, 0, &range)
        );

        let lookup = cache.lookup(file_id, 0, std::slice::from_ref(&range));
        assert!(lookup.is_fully_cached());
        assert_eq!(&vec![2; 10][..], &lookup.cached_parts[0][0].bytes[..]);
    }

    #[test]
    fn test_page_cache_retains_disjoint_inserts_for_same_row_group() {
        let cache = PageRangeCache::new(1000);
        let file_id = FileId::random();
        let range1 = 0..10;
        let range2 = 20..30;

        cache.insert_ranges(
            file_id,
            0,
            std::slice::from_ref(&range1),
            &[Bytes::from(vec![1; 10])],
        );
        cache.insert_ranges(
            file_id,
            0,
            std::slice::from_ref(&range2),
            &[Bytes::from(vec![2; 10])],
        );

        let lookup = cache.lookup(file_id, 0, &[range1, range2]);
        assert!(lookup.is_fully_cached());
        assert_eq!(2, lookup.cached_range_count);
        assert_eq!(&vec![1; 10][..], &lookup.cached_parts[0][0].bytes[..]);
        assert_eq!(&vec![2; 10][..], &lookup.cached_parts[1][0].bytes[..]);
    }

    #[test]
    fn test_page_cache_fragment_eviction() {
        let file_id = FileId::random();
        let range = 0..10;
        let key = PageFragmentKey::new(file_id, 0, &range);
        let page = Bytes::from(vec![1; 10]);
        let cache = PageRangeCache::new(page_cache_weight(&key, &page) as u64);

        cache.insert_ranges(
            file_id,
            0,
            std::slice::from_ref(&range),
            &[Bytes::from(vec![1; 10])],
        );
        assert!(
            cache
                .lookup(file_id, 0, std::slice::from_ref(&range))
                .is_fully_cached()
        );

        cache.cache.invalidate(&key);
        cache.cache.run_pending_tasks();
        assert!(cache.find_index_candidates(file_id, 0, &range).is_empty());

        let lookup = cache.lookup(file_id, 0, std::slice::from_ref(&range));
        assert!(!lookup.is_fully_cached());
        assert_eq!(vec![0..10], lookup.missing_ranges);
    }

    #[test]
    fn test_page_cache_rejects_oversized_fragment() {
        let cache = PageRangeCache::new(1);
        let file_id = FileId::random();
        let range = 0..10;

        cache.insert_ranges(
            file_id,
            0,
            std::slice::from_ref(&range),
            &[Bytes::from(vec![1; 10])],
        );
        cache.cache.run_pending_tasks();
        assert!(cache.find_index_candidates(file_id, 0, &range).is_empty());

        let lookup = cache.lookup(file_id, 0, std::slice::from_ref(&range));
        assert!(!lookup.is_fully_cached());
        assert_eq!(vec![0..10], lookup.missing_ranges);
    }

    #[test]
    fn test_page_cache_lookup_during_insert_keeps_index_entry() {
        let cache = PageRangeCache::new(1024);
        let file_id = FileId::random();
        let range = 0..10;
        let key = PageFragmentKey::new(file_id, 0, &range);

        // The lookup runs after the fragment is indexed but before its bytes are published.
        insert_tracked(&cache.cache, key, Bytes::from(vec![1; 10]), |generation| {
            cache.register_fragment(key, generation);
            let lookup = cache.lookup(file_id, 0, std::slice::from_ref(&range));
            assert!(!lookup.is_fully_cached());
        });
        assert!(
            cache
                .lookup(file_id, 0, std::slice::from_ref(&range))
                .is_fully_cached()
        );

        cache.invalidate_file(file_id);
        assert!(!cache.cache.contains_key(&key));
    }

    #[test]
    fn test_selector_result_cache() {
        let cache = CacheManager::builder()
            .selector_result_cache_size(1000)
            .build();
        let file_id = FileId::random();
        let key = SelectorResultKey {
            file_id,
            row_group_idx: 0,
            selector: TimeSeriesRowSelector::LastRow { after_merge: false },
        };
        assert!(cache.get_selector_result(&key).is_none());
        let result = Arc::new(SelectorResultValue::new(
            Vec::new(),
            ParquetReadColumns::from_deduped(Vec::new()),
        ));
        cache.put_selector_result(key, result);
        assert!(cache.get_selector_result(&key).is_some());
    }

    #[test]
    fn test_prefilter_result_cache() {
        let disabled = CacheManager::builder().build();
        let file_id = FileId::random();
        let key = PrefilterKey::new(
            file_id,
            0,
            None,
            1,
            SmallVec::from_vec(vec!["tag_0 IN ([a])".to_string()]),
        );
        let selection = Arc::new(BooleanBuffer::new_set(3));

        disabled.put_prefilter_result(key.clone(), selection.clone());
        assert!(disabled.get_prefilter_result(&key).is_none());

        let cache = Arc::new(
            CacheManager::builder()
                .prefilter_result_cache_size(1000)
                .build(),
        );
        assert!(cache.get_prefilter_result(&key).is_none());
        cache.put_prefilter_result(key.clone(), selection.clone());
        assert_eq!(
            cache.get_prefilter_result(&key).unwrap().as_ref(),
            selection.as_ref()
        );

        let enable_all = CacheStrategy::EnableAll(cache.clone());
        assert!(enable_all.get_prefilter_result(&key).is_some());

        let compaction = CacheStrategy::Compaction(cache.clone());
        assert!(compaction.get_prefilter_result(&key).is_none());
        compaction.put_prefilter_result(key.clone(), selection.clone());
        assert!(cache.get_prefilter_result(&key).is_some());

        let disabled_strategy = CacheStrategy::Disabled;
        assert!(disabled_strategy.get_prefilter_result(&key).is_none());
        disabled_strategy.put_prefilter_result(key.clone(), selection);
        assert!(cache.get_prefilter_result(&key).is_some());
    }

    #[test]
    fn test_prefilter_key_distinguishes_dimensions() {
        let file_id = FileId::random();
        let row_selection = RowSelection::from(vec![RowSelector::skip(1), RowSelector::select(3)]);
        let other_row_selection =
            RowSelection::from(vec![RowSelector::skip(2), RowSelector::select(2)]);
        let row_selection = PrefilterKey::row_selection_snapshot(Some(&row_selection));
        let other_row_selection = PrefilterKey::row_selection_snapshot(Some(&other_row_selection));
        let base = PrefilterKey::new(
            file_id,
            0,
            row_selection.clone(),
            1,
            SmallVec::from_vec(vec!["tag_0 IN ([a])".to_string()]),
        );

        assert_ne!(
            base,
            PrefilterKey::new(
                FileId::random(),
                0,
                row_selection.clone(),
                1,
                SmallVec::from_vec(vec!["tag_0 IN ([a])".to_string()])
            )
        );
        assert_ne!(
            base,
            PrefilterKey::new(
                file_id,
                1,
                row_selection.clone(),
                1,
                SmallVec::from_vec(vec!["tag_0 IN ([a])".to_string()])
            )
        );
        assert_ne!(
            base,
            PrefilterKey::new(
                file_id,
                0,
                other_row_selection,
                1,
                SmallVec::from_vec(vec!["tag_0 IN ([a])".to_string()])
            )
        );
        assert_ne!(
            base,
            PrefilterKey::new(
                file_id,
                0,
                row_selection.clone(),
                1,
                SmallVec::from_vec(vec!["tag_0 IN ([b])".to_string()])
            )
        );
        assert_ne!(
            base,
            PrefilterKey::new(
                file_id,
                0,
                row_selection.clone(),
                2,
                SmallVec::from_vec(vec!["tag_0 IN ([a])".to_string()])
            )
        );
        let pk_group = PrefilterKey::new(
            file_id,
            0,
            row_selection,
            1,
            SmallVec::from_vec(vec![
                "tag_0 IN ([a])".to_string(),
                "tag_1 IN ([x])".to_string(),
            ]),
        );
        assert_ne!(base, pk_group);
    }

    #[test]
    fn test_range_result_cache() {
        let cache = Arc::new(
            CacheManager::builder()
                .range_result_cache_size(1024 * 1024)
                .build(),
        );

        let key = RangeScanCacheKey {
            region_id: RegionId::new(1, 1),
            row_groups: vec![(FileId::random(), 0)],
            scan: ScanRequestFingerprintBuilder {
                read_columns: ReadColumns::new(std::iter::empty()),
                read_column_types: vec![],
                filters: vec!["tag_0 = 1".to_string()],
                time_filters: vec![],
                series_row_selector: None,
                append_mode: false,
                filter_deleted: true,
                merge_mode: crate::region::options::MergeMode::LastRow,
                sequence_range: None,
                partition_expr_version: 0,
            }
            .build(),
        };
        let value = Arc::new(RangeScanCacheValue::new(Vec::new(), 0));

        assert!(cache.get_range_result(&key).is_none());
        cache.put_range_result(key.clone(), value.clone());
        assert!(cache.get_range_result(&key).is_some());

        let enable_all = CacheStrategy::EnableAll(cache.clone());
        assert!(enable_all.get_range_result(&key).is_some());

        let compaction = CacheStrategy::Compaction(cache.clone());
        assert!(compaction.get_range_result(&key).is_none());
        compaction.put_range_result(key.clone(), value.clone());
        assert!(cache.get_range_result(&key).is_some());

        let disabled = CacheStrategy::Disabled;
        assert!(disabled.get_range_result(&key).is_none());
        disabled.put_range_result(key.clone(), value);
        assert!(cache.get_range_result(&key).is_some());
    }

    #[test]
    fn test_remove_file_entries_drops_only_purged_file() {
        let cache = CacheManager::builder()
            .page_cache_size(4096)
            .selector_result_cache_size(4096)
            .range_result_cache_size(1024 * 1024)
            .prefilter_result_cache_size(4096)
            .puffin_metadata_size(4096)
            .build();
        let region_id = RegionId::new(1, 1);
        let purged = FileId::random();
        let live = FileId::random();
        // Puffin metadata is keyed by index version.
        let purged_index = RegionIndexId::new(RegionFileId::new(region_id, purged), 1);
        let live_index = RegionIndexId::new(RegionFileId::new(region_id, live), 1);
        let puffin_metadata_cache = cache.puffin_metadata_cache().unwrap().clone();
        for index_id in [purged_index, live_index] {
            puffin_metadata_cache.put_metadata(
                index_id.to_string(),
                Arc::new(puffin::file_metadata::FileMetadata {
                    blobs: Vec::new(),
                    properties: std::collections::HashMap::new(),
                }),
            );
        }
        let page = 0..8;
        let selector_key = |file_id| SelectorResultKey {
            file_id,
            row_group_idx: 0,
            selector: TimeSeriesRowSelector::LastRow { after_merge: false },
        };
        let prefilter_key =
            |file_id| PrefilterKey::new(file_id, 0, None, 1, SmallVec::from_vec(vec![]));
        let range_key = |files: Vec<FileId>| RangeScanCacheKey {
            region_id,
            row_groups: files.into_iter().map(|file_id| (file_id, 0)).collect(),
            scan: ScanRequestFingerprintBuilder {
                read_columns: ReadColumns::new(std::iter::empty()),
                read_column_types: vec![],
                filters: vec!["tag_0 = 1".to_string()],
                time_filters: vec![],
                series_row_selector: None,
                append_mode: false,
                filter_deleted: true,
                merge_mode: crate::region::options::MergeMode::LastRow,
                sequence_range: None,
                partition_expr_version: 0,
            }
            .build(),
        };
        let mut files = vec![purged, live];
        files.sort_unstable_by(|a, b| a.as_bytes().cmp(b.as_bytes()));
        let shared_range = range_key(files);
        for file_id in [purged, live] {
            for row_group in [0, 1] {
                cache.put_page_ranges(
                    file_id,
                    row_group,
                    std::slice::from_ref(&page),
                    &[Bytes::from(vec![1; 8])],
                );
            }
            cache.put_selector_result(
                selector_key(file_id),
                Arc::new(SelectorResultValue::new(
                    Vec::new(),
                    ParquetReadColumns::from_deduped(Vec::new()),
                )),
            );
            cache.put_prefilter_result(prefilter_key(file_id), Arc::new(BooleanBuffer::new_set(1)));
            cache.put_range_result(
                range_key(vec![file_id]),
                Arc::new(RangeScanCacheValue::new(Vec::new(), 0)),
            );
        }
        cache.put_range_result(
            shared_range.clone(),
            Arc::new(RangeScanCacheValue::new(Vec::new(), 0)),
        );

        cache.remove_file_entries(purged_index);

        let page_cached = |file_id, row_group| {
            cache
                .get_page_ranges(file_id, row_group, std::slice::from_ref(&page))
                .unwrap()
                .is_fully_cached()
        };
        for row_group in [0, 1] {
            assert!(!page_cached(purged, row_group));
            assert!(page_cached(live, row_group));
        }
        assert!(cache.get_selector_result(&selector_key(purged)).is_none());
        assert!(cache.get_selector_result(&selector_key(live)).is_some());
        assert!(cache.get_prefilter_result(&prefilter_key(purged)).is_none());
        assert!(cache.get_prefilter_result(&prefilter_key(live)).is_some());
        assert!(cache.get_range_result(&range_key(vec![purged])).is_none());
        assert!(cache.get_range_result(&range_key(vec![live])).is_some());
        // A range that also covers a live file is useless once one of its files is gone.
        assert!(cache.get_range_result(&shared_range).is_none());
        assert!(
            puffin_metadata_cache
                .get_metadata(&purged_index.to_string())
                .is_none()
        );
        assert!(
            puffin_metadata_cache
                .get_metadata(&live_index.to_string())
                .is_some()
        );
    }

    #[test]
    fn test_range_result_cache_size_configures_limiter() {
        let cache_size = 3 * 1024_u64;
        let cache = CacheManager::builder()
            .range_result_cache_size(cache_size)
            .build();

        assert_eq!(cache.range_result_cache_size(), cache_size as usize);
        assert_eq!(
            cache.range_result_memory_limiter().permit_bytes(),
            RANGE_RESULT_CONCAT_MEMORY_PERMIT.as_bytes() as usize
        );
        assert_eq!(
            cache.range_result_memory_limiter().available_permits(),
            (cache_size as usize).div_ceil(RANGE_RESULT_CONCAT_MEMORY_PERMIT.as_bytes() as usize)
        );
    }

    #[tokio::test]
    async fn range_result_memory_limiter_rejects_oversized_request() {
        let limiter = RangeResultMemoryLimiter::new(2 * 1024, 1024);
        assert_eq!(limiter.available_permits(), 2);

        let err = limiter.acquire(10 * 1024).await.unwrap_err();
        assert!(
            err.to_string().contains("exceeds limiter capacity"),
            "unexpected error: {err}"
        );
        assert_eq!(limiter.available_permits(), 2);
    }

    #[tokio::test]
    async fn range_result_memory_limiter_allows_request_up_to_capacity() {
        let limiter = RangeResultMemoryLimiter::new(2 * 1024, 1024);
        let permit = limiter.acquire(2 * 1024).await.unwrap();
        assert_eq!(limiter.available_permits(), 0);
        drop(permit);
        assert_eq!(limiter.available_permits(), 2);
    }

    #[tokio::test]
    async fn test_evict_puffin_cache_clears_all_entries() {
        use std::collections::{BTreeMap, HashMap};

        let cache = CacheManager::builder()
            .index_metadata_size(128)
            .index_content_size(128)
            .index_content_page_size(64)
            .index_result_cache_size(128)
            .puffin_metadata_size(128)
            .build();
        let cache = Arc::new(cache);

        let region_id = RegionId::new(1, 1);
        let index_id = RegionIndexId::new(RegionFileId::new(region_id, FileId::random()), 0);
        let column_id: ColumnId = 1;

        let bloom_cache = cache.bloom_filter_index_cache().unwrap().clone();
        let inverted_cache = cache.inverted_index_cache().unwrap().clone();
        let result_cache = cache.index_result_cache().unwrap();
        let puffin_metadata_cache = cache.puffin_metadata_cache().unwrap().clone();

        let bloom_key = (
            index_id.file_id(),
            index_id.version,
            column_id,
            Tag::Skipping,
        );
        bloom_cache.put_metadata(bloom_key, Arc::new(BloomFilterMeta::default()));
        inverted_cache.put_metadata(
            (index_id.file_id(), index_id.version),
            Arc::new(InvertedIndexMetas::default()),
        );
        let predicate = PredicateKey::new_bloom(Arc::new(BTreeMap::new()));
        let selection = Arc::new(RowGroupSelection::default());
        result_cache.put(predicate.clone(), index_id.file_id(), selection);
        let file_id_str = index_id.to_string();
        let metadata = Arc::new(FileMetadata {
            blobs: Vec::new(),
            properties: HashMap::new(),
        });
        puffin_metadata_cache.put_metadata(file_id_str.clone(), metadata);

        assert!(bloom_cache.get_metadata(bloom_key).is_some());
        assert!(
            inverted_cache
                .get_metadata((index_id.file_id(), index_id.version))
                .is_some()
        );
        assert!(result_cache.get(&predicate, index_id.file_id()).is_some());
        assert!(puffin_metadata_cache.get_metadata(&file_id_str).is_some());

        cache.evict_puffin_cache(index_id).await;

        assert!(bloom_cache.get_metadata(bloom_key).is_none());
        assert!(
            inverted_cache
                .get_metadata((index_id.file_id(), index_id.version))
                .is_none()
        );
        assert!(result_cache.get(&predicate, index_id.file_id()).is_none());
        assert!(puffin_metadata_cache.get_metadata(&file_id_str).is_none());

        // Refill caches and evict via CacheStrategy to ensure delegation works.
        bloom_cache.put_metadata(bloom_key, Arc::new(BloomFilterMeta::default()));
        inverted_cache.put_metadata(
            (index_id.file_id(), index_id.version),
            Arc::new(InvertedIndexMetas::default()),
        );
        result_cache.put(
            predicate.clone(),
            index_id.file_id(),
            Arc::new(RowGroupSelection::default()),
        );
        puffin_metadata_cache.put_metadata(
            file_id_str.clone(),
            Arc::new(FileMetadata {
                blobs: Vec::new(),
                properties: HashMap::new(),
            }),
        );

        let strategy = CacheStrategy::EnableAll(cache.clone());
        strategy.evict_puffin_cache(index_id).await;

        assert!(bloom_cache.get_metadata(bloom_key).is_none());
        assert!(
            inverted_cache
                .get_metadata((index_id.file_id(), index_id.version))
                .is_none()
        );
        assert!(result_cache.get(&predicate, index_id.file_id()).is_none());
        assert!(puffin_metadata_cache.get_metadata(&file_id_str).is_none());
    }

    fn wide_region_metadata(column_count: u32) -> RegionMetadata {
        let region_id = RegionId::new(1024, 7);
        let mut builder = RegionMetadataBuilder::new(region_id);
        let mut primary_key = Vec::new();

        for column_id in 0..column_count {
            let semantic_type = if column_id < 32 {
                primary_key.push(column_id);
                SemanticType::Tag
            } else {
                SemanticType::Field
            };
            let mut column_schema = ColumnSchema::new(
                format!("wide_column_{column_id}"),
                ConcreteDataType::string_datatype(),
                true,
            );
            column_schema
                .mut_metadata()
                .insert(format!("cache_key_{column_id}"), "cache_value".repeat(4));
            builder.push_column_metadata(ColumnMetadata {
                column_schema,
                semantic_type,
                column_id,
            });
        }

        builder.push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                "ts",
                ConcreteDataType::timestamp_millisecond_datatype(),
                false,
            ),
            semantic_type: SemanticType::Timestamp,
            column_id: column_count,
        });
        builder.primary_key(primary_key);

        builder.build().unwrap()
    }
}
