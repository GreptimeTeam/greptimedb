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

//! Vector index applier for performing KNN search on SST files.

use std::sync::Arc;
use std::time::Instant;

use common_base::range_read::RangeReader;
use common_telemetry::{debug, warn};
use datatypes::schema::VectorDistanceMetric as DtVectorDistanceMetric;
use index::vector::distance_metric_to_usearch;
use object_store::ObjectStore;
use puffin::puffin_manager::cache::PuffinMetadataCacheRef;
use puffin::puffin_manager::{PuffinManager, PuffinReader};
use roaring::RoaringBitmap;
use snafu::ResultExt;
use store_api::region_request::PathType;
use store_api::storage::{ColumnId, VectorDistanceMetric, VectorIndexEngineType};

use crate::access_layer::{RegionFilePathFactory, WriteCachePathProvider};
use crate::cache::file_cache::{FileCacheRef, FileType, IndexKey};
use crate::cache::index::vector_index::{
    CachedVectorIndex, VectorIndexCacheKey, VectorIndexCacheRef,
};
use crate::error::{
    ApplyVectorIndexSnafu, MetadataSnafu, PuffinBuildReaderSnafu, PuffinReadBlobSnafu, Result,
};
use crate::metrics::{CACHE_BYTES, CACHE_HIT, CACHE_MISS, INDEX_APPLY_ELAPSED};
use crate::sst::file::{RegionFileId, RegionIndexId};
use crate::sst::index::TYPE_VECTOR_INDEX;
use crate::sst::index::puffin_manager::{BlobReader, PuffinManagerFactory};
use crate::sst::index::vector_index::creator::VectorIndexConfig;
use crate::sst::index::vector_index::{INDEX_BLOB_TYPE, engine};

/// Result of a vector KNN search.
#[derive(Debug, Clone)]
pub struct VectorSearchResult {
    /// The row offsets of the matching rows in the SST file.
    /// These are the original row offsets (not HNSW keys).
    pub row_offsets: Vec<u64>,
    /// The distances to the query vector.
    #[allow(dead_code)]
    pub distances: Vec<f32>,
}

/// Metrics for tracking vector index apply operations.
#[derive(Default, Clone)]
pub struct VectorIndexApplyMetrics {
    /// Total time spent applying the index.
    pub apply_elapsed: std::time::Duration,
    /// Number of blob cache misses.
    pub blob_cache_miss: usize,
    /// Total size of blobs read (in bytes).
    pub blob_read_bytes: u64,
    /// Number of vectors searched.
    pub vectors_searched: usize,
}

impl std::fmt::Debug for VectorIndexApplyMetrics {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        if self.is_empty() {
            return write!(f, "{{}}");
        }
        write!(f, "{{")?;
        write!(f, "\"apply_elapsed\":\"{:?}\"", self.apply_elapsed)?;
        if self.blob_cache_miss > 0 {
            write!(f, ", \"blob_cache_miss\":{}", self.blob_cache_miss)?;
        }
        if self.blob_read_bytes > 0 {
            write!(f, ", \"blob_read_bytes\":{}", self.blob_read_bytes)?;
        }
        if self.vectors_searched > 0 {
            write!(f, ", \"vectors_searched\":{}", self.vectors_searched)?;
        }
        write!(f, "}}")
    }
}

impl VectorIndexApplyMetrics {
    /// Returns true if the metrics are empty.
    pub fn is_empty(&self) -> bool {
        self.apply_elapsed.is_zero()
    }

    /// Merges another metrics into this one.
    #[allow(dead_code)]
    pub fn merge_from(&mut self, other: &Self) {
        self.apply_elapsed += other.apply_elapsed;
        self.blob_cache_miss += other.blob_cache_miss;
        self.blob_read_bytes += other.blob_read_bytes;
        self.vectors_searched += other.vectors_searched;
    }
}

pub(crate) type VectorIndexApplierRef = Arc<VectorIndexApplier>;

/// `VectorIndexApplier` applies vector KNN search to SST files.
pub struct VectorIndexApplier {
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

    /// Cache for loaded vector indexes.
    vector_index_cache: Option<VectorIndexCacheRef>,

    /// The column ID of the vector column to search.
    column_id: ColumnId,

    /// The query vector.
    query_vector: Vec<f32>,

    /// The number of results to return (k in KNN).
    #[allow(dead_code)]
    k: usize,

    /// The distance metric.
    metric: VectorDistanceMetric,

    /// Vector dimension.
    dim: usize,
}

impl VectorIndexApplier {
    /// Creates a new `VectorIndexApplier`.
    #[allow(clippy::too_many_arguments)]
    pub fn new(
        table_dir: String,
        path_type: PathType,
        object_store: ObjectStore,
        puffin_manager_factory: PuffinManagerFactory,
        column_id: ColumnId,
        query_vector: Vec<f32>,
        k: usize,
        metric: VectorDistanceMetric,
    ) -> Self {
        let dim = query_vector.len();
        Self {
            table_dir,
            path_type,
            object_store,
            file_cache: None,
            puffin_manager_factory,
            puffin_metadata_cache: None,
            vector_index_cache: None,
            column_id,
            query_vector,
            k,
            metric,
            dim,
        }
    }

    pub fn with_file_cache(mut self, file_cache: Option<FileCacheRef>) -> Self {
        self.file_cache = file_cache;
        self
    }

    /// Returns the number of results to return (k in KNN).
    #[allow(dead_code)]
    pub fn k(&self) -> usize {
        self.k
    }

    /// Returns the query vector.
    #[allow(dead_code)]
    pub fn query_vector(&self) -> &[f32] {
        &self.query_vector
    }

    /// Returns the column ID of the vector column to search.
    #[allow(dead_code)]
    pub fn column_id(&self) -> ColumnId {
        self.column_id
    }

    /// Returns the distance metric.
    #[allow(dead_code)]
    pub fn metric(&self) -> VectorDistanceMetric {
        self.metric
    }

    pub fn with_puffin_metadata_cache(
        mut self,
        puffin_metadata_cache: Option<PuffinMetadataCacheRef>,
    ) -> Self {
        self.puffin_metadata_cache = puffin_metadata_cache;
        self
    }

    pub fn with_vector_index_cache(
        mut self,
        vector_index_cache: Option<VectorIndexCacheRef>,
    ) -> Self {
        self.vector_index_cache = vector_index_cache;
        self
    }

    /// Applies vector KNN search to the provided SST file with a custom k value.
    ///
    /// This is useful for over-fetching when post-filtering is needed.
    pub async fn apply_with_k(
        &self,
        file_id: RegionFileId,
        file_size_hint: Option<u64>,
        mut metrics: Option<&mut VectorIndexApplyMetrics>,
        k: usize,
    ) -> Result<VectorSearchResult> {
        let apply_start = Instant::now();

        // Try to get cached index or load from blob
        let cached_index = self
            .get_or_load_index(file_id, file_size_hint, metrics.as_deref_mut())
            .await?;

        // Handle the case where no index exists for this column
        let Some(cached_index) = cached_index else {
            return Ok(VectorSearchResult {
                row_offsets: vec![],
                distances: vec![],
            });
        };

        if let Some(m) = &mut metrics {
            m.vectors_searched = cached_index.size();
        }

        // Perform KNN search
        let matches = cached_index
            .engine
            .search(&self.query_vector, k)
            .map_err(|e| {
                ApplyVectorIndexSnafu {
                    reason: format!("Failed to search vector index: {}", e),
                }
                .build()
            })?;
        debug!(
            "Vector search completed: index_size={}, k={}, matches={}",
            cached_index.size(),
            k,
            matches.keys.len()
        );

        // Convert HNSW keys to row offsets
        // HNSW keys are sequential (0, 1, 2...) but row offsets have gaps due to NULLs
        let mut row_offsets = Vec::with_capacity(matches.keys.len());
        let mut distances = Vec::with_capacity(matches.distances.len());

        for (hnsw_key, distance) in matches.keys.iter().zip(matches.distances.iter()) {
            let row_offset = hnsw_key_to_row_offset(*hnsw_key, &cached_index.null_bitmap);
            row_offsets.push(row_offset);
            distances.push(*distance);
        }

        if let Some(m) = &mut metrics {
            m.apply_elapsed = apply_start.elapsed();
        }

        INDEX_APPLY_ELAPSED
            .with_label_values(&[TYPE_VECTOR_INDEX])
            .observe(apply_start.elapsed().as_secs_f64());

        Ok(VectorSearchResult {
            row_offsets,
            distances,
        })
    }

    /// Gets the cached index or loads it from the blob.
    /// Returns None if the index blob doesn't exist for this column.
    async fn get_or_load_index(
        &self,
        file_id: RegionFileId,
        file_size_hint: Option<u64>,
        metrics: Option<&mut VectorIndexApplyMetrics>,
    ) -> Result<Option<Arc<CachedVectorIndex>>> {
        let cache_key = VectorIndexCacheKey::new(file_id.file_id(), self.column_id);

        // If cache is available, try to get from cache first
        if let Some(cache) = &self.vector_index_cache {
            if let Some(cached) = cache.get(&cache_key) {
                CACHE_HIT.with_label_values(&[TYPE_VECTOR_INDEX]).inc();
                return Ok(Some(cached));
            }
            // Cache exists but key not found - this is a real cache miss
            CACHE_MISS.with_label_values(&[TYPE_VECTOR_INDEX]).inc();
        }
        // Note: when vector_index_cache is None, we don't count it as a cache miss
        // because the cache is not enabled

        // Read blob data (async)
        let blob_data = match self.read_blob_data(file_id, file_size_hint, metrics).await {
            Ok(data) => data,
            Err(e) => {
                // Blob not found means no index for this column
                if is_blob_not_found(&e) {
                    return Ok(None);
                }
                return Err(e);
            }
        };

        // Parse and load the index (sync)
        let cached_index = self.parse_and_load_index(&blob_data)?;
        let cached_index = Arc::new(cached_index);

        // Store in cache if available
        // TODO(vector-index): Consider using moka::future::Cache with try_get_with for atomic
        // get-or-insert to avoid loading the same index multiple times when concurrent requests
        // arrive for the same key. Current implementation may load the same index multiple times
        // but doesn't cause data inconsistency.
        if let Some(cache) = &self.vector_index_cache {
            CACHE_BYTES
                .with_label_values(&[TYPE_VECTOR_INDEX])
                .add(cached_index.size_bytes as i64);
            cache.insert(cache_key, cached_index.clone());
        }

        Ok(Some(cached_index))
    }

    /// Reads the blob data from file cache or remote.
    async fn read_blob_data(
        &self,
        file_id: RegionFileId,
        file_size_hint: Option<u64>,
        mut metrics: Option<&mut VectorIndexApplyMetrics>,
    ) -> Result<bytes::Bytes> {
        let reader = self
            .blob_reader(file_id, file_size_hint, metrics.as_deref_mut())
            .await?;

        let blob_size = reader
            .metadata()
            .await
            .context(MetadataSnafu)?
            .content_length;
        let data = reader.read(0..blob_size).await.context(MetadataSnafu)?;

        // Update blob_read_bytes metrics
        if let Some(m) = metrics.as_mut() {
            m.blob_read_bytes += data.len() as u64;
        }

        Ok(data)
    }

    /// Parses blob data and loads the vector index.
    ///
    /// Blob format v1:
    /// - 1 byte: version (must be 1)
    /// - 1 byte: engine type (u8)
    /// - 4 bytes: dimension (u32, little-endian)
    /// - 1 byte: metric (u8)
    /// - 2 bytes: connectivity/M (u16, little-endian)
    /// - 2 bytes: expansion_add/ef_construction (u16, little-endian)
    /// - 2 bytes: expansion_search/ef_search (u16, little-endian)
    /// - 8 bytes: total_rows (u64, little-endian)
    /// - 8 bytes: indexed_rows (u64, little-endian)
    /// - 4 bytes: NULL bitmap length (u32, little-endian)
    /// - N bytes: NULL bitmap (serialized RoaringBitmap)
    /// - Remaining: vector index data
    fn parse_and_load_index(&self, blob_data: &[u8]) -> Result<CachedVectorIndex> {
        // Header size: version(1) + engine(1) + dim(4) + metric(1) +
        //              connectivity(2) + expansion_add(2) + expansion_search(2) +
        //              total_rows(8) + indexed_rows(8) + bitmap_len(4) = 33 bytes
        const HEADER_SIZE: usize = 33;

        if blob_data.len() < HEADER_SIZE {
            return ApplyVectorIndexSnafu {
                reason: format!(
                    "Blob data too small to contain header: {} < {}",
                    blob_data.len(),
                    HEADER_SIZE
                ),
            }
            .fail();
        }

        // Read version
        let version = blob_data[0];
        if version != 1 {
            return ApplyVectorIndexSnafu {
                reason: format!("Unsupported vector index blob version: {}", version),
            }
            .fail();
        }

        // Read engine type
        let engine_type_byte = blob_data[1];
        let engine_type =
            VectorIndexEngineType::try_from_u8(engine_type_byte).ok_or_else(|| {
                ApplyVectorIndexSnafu {
                    reason: format!("Unknown vector index engine type: {}", engine_type_byte),
                }
                .build()
            })?;

        // Read dimension
        let stored_dim =
            u32::from_le_bytes([blob_data[2], blob_data[3], blob_data[4], blob_data[5]]) as usize;

        // Read metric
        let metric_byte = blob_data[6];
        let stored_metric = DtVectorDistanceMetric::try_from_u8(metric_byte).ok_or_else(|| {
            ApplyVectorIndexSnafu {
                reason: format!("Unknown distance metric: {}", metric_byte),
            }
            .build()
        })?;

        // Read HNSW parameters (for observability, not used in loading)
        let connectivity = u16::from_le_bytes([blob_data[7], blob_data[8]]) as usize;
        let expansion_add = u16::from_le_bytes([blob_data[9], blob_data[10]]) as usize;
        let expansion_search = u16::from_le_bytes([blob_data[11], blob_data[12]]) as usize;

        // Read row statistics (for observability)
        let total_rows = u64::from_le_bytes([
            blob_data[13],
            blob_data[14],
            blob_data[15],
            blob_data[16],
            blob_data[17],
            blob_data[18],
            blob_data[19],
            blob_data[20],
        ]);
        let indexed_rows = u64::from_le_bytes([
            blob_data[21],
            blob_data[22],
            blob_data[23],
            blob_data[24],
            blob_data[25],
            blob_data[26],
            blob_data[27],
            blob_data[28],
        ]);

        common_telemetry::debug!(
            "Vector index header: dim={}, metric={:?}, connectivity={}, \
             expansion_add={}, expansion_search={}, total_rows={}, indexed_rows={}",
            stored_dim,
            stored_metric,
            connectivity,
            expansion_add,
            expansion_search,
            total_rows,
            indexed_rows
        );

        // Validate dimension matches query vector
        if stored_dim != self.dim {
            return ApplyVectorIndexSnafu {
                reason: format!(
                    "Vector dimension mismatch: index has {}, query has {}",
                    stored_dim, self.dim
                ),
            }
            .fail();
        }

        // Validate metric matches
        if stored_metric != self.metric {
            return ApplyVectorIndexSnafu {
                reason: format!(
                    "Distance metric mismatch: index has {:?}, query has {:?}",
                    stored_metric, self.metric
                ),
            }
            .fail();
        }

        // Read NULL bitmap length
        let null_bitmap_len =
            u32::from_le_bytes([blob_data[29], blob_data[30], blob_data[31], blob_data[32]])
                as usize;

        if blob_data.len() < HEADER_SIZE + null_bitmap_len {
            return ApplyVectorIndexSnafu {
                reason: format!(
                    "Blob data too small to contain NULL bitmap: {} < {}",
                    blob_data.len(),
                    HEADER_SIZE + null_bitmap_len
                ),
            }
            .fail();
        }

        let null_bitmap_data = &blob_data[HEADER_SIZE..HEADER_SIZE + null_bitmap_len];
        let null_bitmap = RoaringBitmap::deserialize_from(null_bitmap_data).map_err(|e| {
            ApplyVectorIndexSnafu {
                reason: format!("Failed to deserialize NULL bitmap: {}", e),
            }
            .build()
        })?;

        let index_data = &blob_data[HEADER_SIZE + null_bitmap_len..];

        // Create config for loading the engine (HNSW params from header)
        let config = VectorIndexConfig {
            engine: engine_type,
            dim: stored_dim,
            metric: distance_metric_to_usearch(stored_metric),
            distance_metric: stored_metric,
            connectivity,
            expansion_add,
            expansion_search,
        };

        // Load the vector index engine
        let engine_instance = engine::load_engine(engine_type, &config, index_data)?;

        Ok(CachedVectorIndex::new(engine_instance, null_bitmap))
    }

    /// Creates a blob reader from the cached or remote index file.
    async fn blob_reader(
        &self,
        file_id: RegionFileId,
        file_size_hint: Option<u64>,
        metrics: Option<&mut VectorIndexApplyMetrics>,
    ) -> Result<BlobReader> {
        let reader = match self.cached_blob_reader(file_id, file_size_hint).await {
            Ok(Some(reader)) => reader,
            other => {
                if let Some(m) = metrics {
                    m.blob_cache_miss += 1;
                }
                if let Err(err) = other {
                    warn!(err; "An unexpected error occurred while reading the cached index file. Fallback to remote index file.");
                }
                self.remote_blob_reader(file_id, file_size_hint).await?
            }
        };

        Ok(reader)
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

        let index_key = IndexKey::new(file_id.region_id(), file_id.file_id(), FileType::Puffin(0));
        if file_cache.get(index_key).await.is_none() {
            return Ok(None);
        }

        let puffin_manager = self.puffin_manager_factory.build(
            file_cache.local_store(),
            WriteCachePathProvider::new(file_cache.clone()),
        );
        let blob_name = Self::column_blob_name(self.column_id);
        let index_id = RegionIndexId::new(file_id, 0);

        let reader = puffin_manager
            .reader(&index_id)
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

    fn column_blob_name(column_id: ColumnId) -> String {
        format!("{}-{}", INDEX_BLOB_TYPE, column_id)
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
                self.object_store.clone(),
                RegionFilePathFactory::new(self.table_dir.clone(), self.path_type),
            )
            .with_puffin_metadata_cache(self.puffin_metadata_cache.clone());

        let blob_name = Self::column_blob_name(self.column_id);
        let index_id = RegionIndexId::new(file_id, 0);

        puffin_manager
            .reader(&index_id)
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
}

/// Converts an HNSW key to a row offset, accounting for NULL values.
///
/// HNSW keys are sequential (0, 1, 2...) but row offsets may have gaps
/// where NULL values exist. The null_bitmap tracks which row offsets are NULL.
fn hnsw_key_to_row_offset(hnsw_key: u64, null_bitmap: &RoaringBitmap) -> u64 {
    // Count how many NULLs exist before this HNSW key position
    // Row offset = HNSW key + number of NULLs before this position
    let row_offset = hnsw_key;
    let mut nulls_before = 0u64;

    // The null bitmap contains row offsets of NULL values
    // We need to find how many NULL row offsets are <= row_offset + nulls_before
    loop {
        let new_nulls_before = null_bitmap.rank((row_offset + nulls_before) as u32);
        if new_nulls_before == nulls_before {
            break;
        }
        nulls_before = new_nulls_before;
    }

    row_offset + nulls_before
}

/// Checks if the error indicates a blob or puffin file was not found.
fn is_blob_not_found(err: &crate::error::Error) -> bool {
    matches!(
        err,
        crate::error::Error::PuffinReadBlob {
            source: puffin::error::Error::BlobNotFound { .. },
            ..
        }
    ) || err.is_object_not_found()
}

#[cfg(test)]
mod tests {
    use datatypes::schema::VectorDistanceMetric as DtVectorDistanceMetric;
    use futures::io::Cursor;
    use index::vector::{VectorDistanceMetric, distance_metric_to_usearch};
    use object_store::services::Memory;
    use puffin::puffin_manager::{PuffinManager, PuffinWriter, PutOptions};
    use store_api::region_request::PathType;
    use store_api::storage::{FileId, VectorIndexEngineType};

    use super::*;
    use crate::access_layer::RegionFilePathFactory;
    use crate::sst::index::puffin_manager::PuffinManagerFactory;
    use crate::sst::index::vector_index::creator::VectorIndexConfig;
    use crate::sst::index::vector_index::{INDEX_BLOB_TYPE, engine};

    #[test]
    fn test_hnsw_key_to_row_offset_no_nulls() {
        let bitmap = RoaringBitmap::new();
        assert_eq!(hnsw_key_to_row_offset(0, &bitmap), 0);
        assert_eq!(hnsw_key_to_row_offset(1, &bitmap), 1);
        assert_eq!(hnsw_key_to_row_offset(5, &bitmap), 5);
    }

    #[test]
    fn test_hnsw_key_to_row_offset_with_nulls() {
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(1);
        bitmap.insert(3);

        assert_eq!(hnsw_key_to_row_offset(0, &bitmap), 0);
        assert_eq!(hnsw_key_to_row_offset(1, &bitmap), 2);
        assert_eq!(hnsw_key_to_row_offset(2, &bitmap), 4);
    }

    #[test]
    fn test_hnsw_key_to_row_offset_consecutive_nulls() {
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(0);
        bitmap.insert(1);
        bitmap.insert(2);

        assert_eq!(hnsw_key_to_row_offset(0, &bitmap), 3);
        assert_eq!(hnsw_key_to_row_offset(1, &bitmap), 4);
    }

    fn create_vector_index_blob(
        vectors: &[Vec<f32>],
        null_positions: &[u32],
        config: &VectorIndexConfig,
    ) -> Vec<u8> {
        let mut engine_instance = engine::create_engine(config.engine, config).unwrap();
        engine_instance.reserve(vectors.len()).unwrap();
        for (i, v) in vectors.iter().enumerate() {
            engine_instance.add(i as u64, v).unwrap();
        }

        let mut null_bitmap = RoaringBitmap::new();
        for &pos in null_positions {
            null_bitmap.insert(pos);
        }

        let mut null_bitmap_bytes = Vec::new();
        null_bitmap.serialize_into(&mut null_bitmap_bytes).unwrap();

        let index_size = engine_instance.serialized_length();
        let mut index_bytes = vec![0u8; index_size];
        engine_instance.save_to_buffer(&mut index_bytes).unwrap();

        let total_rows = vectors.len() as u64 + null_positions.len() as u64;
        let indexed_rows = vectors.len() as u64;

        const HEADER_SIZE: usize = 33;
        let total_size = HEADER_SIZE + null_bitmap_bytes.len() + index_bytes.len();
        let mut blob_data = Vec::with_capacity(total_size);

        blob_data.push(1u8);
        blob_data.push(config.engine.as_u8());
        blob_data.extend_from_slice(&(config.dim as u32).to_le_bytes());
        blob_data.push(config.distance_metric.as_u8());
        blob_data.extend_from_slice(&(config.connectivity as u16).to_le_bytes());
        blob_data.extend_from_slice(&(config.expansion_add as u16).to_le_bytes());
        blob_data.extend_from_slice(&(config.expansion_search as u16).to_le_bytes());
        blob_data.extend_from_slice(&total_rows.to_le_bytes());
        blob_data.extend_from_slice(&indexed_rows.to_le_bytes());
        blob_data.extend_from_slice(&(null_bitmap_bytes.len() as u32).to_le_bytes());
        blob_data.extend_from_slice(&null_bitmap_bytes);
        blob_data.extend_from_slice(&index_bytes);

        blob_data
    }

    fn test_config(dim: usize) -> VectorIndexConfig {
        VectorIndexConfig {
            engine: VectorIndexEngineType::Usearch,
            dim,
            metric: distance_metric_to_usearch(VectorDistanceMetric::L2sq),
            distance_metric: VectorDistanceMetric::L2sq,
            connectivity: 16,
            expansion_add: 128,
            expansion_search: 64,
        }
    }

    async fn write_blob_to_puffin(
        puffin_manager_factory: &PuffinManagerFactory,
        object_store: &ObjectStore,
        table_dir: &str,
        index_id: RegionIndexId,
        column_id: ColumnId,
        blob_data: Vec<u8>,
    ) {
        let puffin_manager = puffin_manager_factory.build(
            object_store.clone(),
            RegionFilePathFactory::new(table_dir.to_string(), PathType::Bare),
        );
        let mut writer = puffin_manager.writer(&index_id).await.unwrap();
        let blob_name = format!("{}-{}", INDEX_BLOB_TYPE, column_id);
        writer
            .put_blob(
                &blob_name,
                Cursor::new(blob_data),
                PutOptions::default(),
                Default::default(),
            )
            .await
            .unwrap();
        writer.finish().await.unwrap();
    }

    #[tokio::test]
    async fn test_apply_with_k_basic() {
        let (_dir, puffin_manager_factory) =
            PuffinManagerFactory::new_for_test_async("test_apply_with_k_basic_").await;
        let object_store = ObjectStore::new(Memory::default()).unwrap().finish();
        let table_dir = "test_table";
        let file_id = RegionFileId::new(0.into(), FileId::random());
        let index_id = RegionIndexId::new(file_id, 0);
        let column_id = 1;

        let vectors = vec![
            vec![1.0f32, 0.0, 0.0, 0.0],
            vec![0.0f32, 1.0, 0.0, 0.0],
            vec![0.0f32, 0.0, 1.0, 0.0],
            vec![0.0f32, 0.0, 0.0, 1.0],
        ];
        let config = test_config(4);
        let blob_data = create_vector_index_blob(&vectors, &[], &config);
        write_blob_to_puffin(
            &puffin_manager_factory,
            &object_store,
            table_dir,
            index_id,
            column_id,
            blob_data,
        )
        .await;

        let applier = VectorIndexApplier::new(
            table_dir.to_string(),
            PathType::Bare,
            object_store,
            puffin_manager_factory,
            column_id,
            vec![1.0f32, 0.0, 0.0, 0.0],
            2,
            DtVectorDistanceMetric::L2sq,
        );

        let mut metrics = VectorIndexApplyMetrics::default();
        let result = applier
            .apply_with_k(file_id, None, Some(&mut metrics), 2)
            .await
            .unwrap();

        assert_eq!(result.row_offsets.len(), 2);
        assert_eq!(result.row_offsets[0], 0);
        assert!(result.distances[0] < 0.01);
        assert!(metrics.apply_elapsed.as_nanos() > 0);
        assert_eq!(metrics.vectors_searched, 4);
    }

    #[tokio::test]
    async fn test_apply_with_k_with_nulls() {
        let (_dir, puffin_manager_factory) =
            PuffinManagerFactory::new_for_test_async("test_apply_with_k_with_nulls_").await;
        let object_store = ObjectStore::new(Memory::default()).unwrap().finish();
        let table_dir = "test_table";
        let file_id = RegionFileId::new(0.into(), FileId::random());
        let index_id = RegionIndexId::new(file_id, 0);
        let column_id = 1;

        // Row layout: [vec0, NULL, vec1, NULL, NULL, vec2]
        let vectors = vec![
            vec![1.0f32, 0.0, 0.0, 0.0], // row 0
            vec![0.0f32, 1.0, 0.0, 0.0], // row 2
            vec![0.0f32, 0.0, 1.0, 0.0], // row 5
        ];
        let null_positions = vec![1, 3, 4];
        let config = test_config(4);
        let blob_data = create_vector_index_blob(&vectors, &null_positions, &config);
        write_blob_to_puffin(
            &puffin_manager_factory,
            &object_store,
            table_dir,
            index_id,
            column_id,
            blob_data,
        )
        .await;

        let applier = VectorIndexApplier::new(
            table_dir.to_string(),
            PathType::Bare,
            object_store,
            puffin_manager_factory,
            column_id,
            vec![0.0f32, 1.0, 0.0, 0.0],
            3,
            DtVectorDistanceMetric::L2sq,
        );

        let result = applier.apply_with_k(file_id, None, None, 3).await.unwrap();

        assert_eq!(result.row_offsets.len(), 3);
        assert_eq!(result.row_offsets[0], 2);
        assert!(result.distances[0] < 0.01);
        assert!(result.row_offsets.contains(&0));
        assert!(result.row_offsets.contains(&5));
    }

    #[tokio::test]
    async fn test_apply_with_k_different_k_values() {
        let (_dir, puffin_manager_factory) =
            PuffinManagerFactory::new_for_test_async("test_apply_with_k_different_k_").await;
        let object_store = ObjectStore::new(Memory::default()).unwrap().finish();
        let table_dir = "test_table";
        let file_id = RegionFileId::new(0.into(), FileId::random());
        let index_id = RegionIndexId::new(file_id, 0);
        let column_id = 1;

        let mut vectors = Vec::new();
        for i in 0..10 {
            let mut v = vec![0.0f32; 4];
            v[i % 4] = 1.0;
            v[(i + 1) % 4] = (i as f32) * 0.1;
            vectors.push(v);
        }
        let config = test_config(4);
        let blob_data = create_vector_index_blob(&vectors, &[], &config);
        write_blob_to_puffin(
            &puffin_manager_factory,
            &object_store,
            table_dir,
            index_id,
            column_id,
            blob_data,
        )
        .await;

        let applier = VectorIndexApplier::new(
            table_dir.to_string(),
            PathType::Bare,
            object_store,
            puffin_manager_factory,
            column_id,
            vec![1.0f32, 0.0, 0.0, 0.0],
            5,
            DtVectorDistanceMetric::L2sq,
        );

        assert_eq!(
            applier
                .apply_with_k(file_id, None, None, 1)
                .await
                .unwrap()
                .row_offsets
                .len(),
            1
        );
        assert_eq!(
            applier
                .apply_with_k(file_id, None, None, 5)
                .await
                .unwrap()
                .row_offsets
                .len(),
            5
        );
        assert_eq!(
            applier
                .apply_with_k(file_id, None, None, 10)
                .await
                .unwrap()
                .row_offsets
                .len(),
            10
        );
        assert_eq!(
            applier
                .apply_with_k(file_id, None, None, 100)
                .await
                .unwrap()
                .row_offsets
                .len(),
            10
        );
    }

    #[tokio::test]
    async fn test_apply_with_k_blob_not_found() {
        let (_dir, puffin_manager_factory) =
            PuffinManagerFactory::new_for_test_async("test_apply_with_k_blob_not_found_").await;
        let object_store = ObjectStore::new(Memory::default()).unwrap().finish();
        let table_dir = "test_table";
        let file_id = RegionFileId::new(0.into(), FileId::random());
        let index_id = RegionIndexId::new(file_id, 0);

        let vectors = vec![vec![1.0f32, 0.0, 0.0, 0.0]];
        let config = test_config(4);
        let blob_data = create_vector_index_blob(&vectors, &[], &config);
        write_blob_to_puffin(
            &puffin_manager_factory,
            &object_store,
            table_dir,
            index_id,
            1, // column 1 has index
            blob_data,
        )
        .await;

        // Query column 999 which has no index
        let applier = VectorIndexApplier::new(
            table_dir.to_string(),
            PathType::Bare,
            object_store,
            puffin_manager_factory,
            999,
            vec![1.0f32, 0.0, 0.0, 0.0],
            5,
            DtVectorDistanceMetric::L2sq,
        );

        let result = applier.apply_with_k(file_id, None, None, 5).await.unwrap();
        assert!(result.row_offsets.is_empty());
    }

    #[tokio::test]
    async fn test_apply_with_k_distance_ordering() {
        let (_dir, puffin_manager_factory) =
            PuffinManagerFactory::new_for_test_async("test_apply_with_k_distance_ordering_").await;
        let object_store = ObjectStore::new(Memory::default()).unwrap().finish();
        let table_dir = "test_table";
        let file_id = RegionFileId::new(0.into(), FileId::random());
        let index_id = RegionIndexId::new(file_id, 0);
        let column_id = 1;

        let vectors = vec![
            vec![1.0f32, 0.0, 0.0, 0.0],
            vec![0.9f32, 0.1, 0.0, 0.0],
            vec![0.7f32, 0.3, 0.0, 0.0],
            vec![0.5f32, 0.5, 0.0, 0.0],
            vec![0.0f32, 1.0, 0.0, 0.0],
        ];
        let config = test_config(4);
        let blob_data = create_vector_index_blob(&vectors, &[], &config);
        write_blob_to_puffin(
            &puffin_manager_factory,
            &object_store,
            table_dir,
            index_id,
            column_id,
            blob_data,
        )
        .await;

        let applier = VectorIndexApplier::new(
            table_dir.to_string(),
            PathType::Bare,
            object_store,
            puffin_manager_factory,
            column_id,
            vec![1.0f32, 0.0, 0.0, 0.0],
            5,
            DtVectorDistanceMetric::L2sq,
        );

        let result = applier.apply_with_k(file_id, None, None, 5).await.unwrap();

        for i in 1..result.distances.len() {
            assert!(result.distances[i] >= result.distances[i - 1]);
        }
        assert_eq!(result.row_offsets[0], 0);
        assert!(result.distances[0] < 0.01);
    }

    #[tokio::test]
    async fn test_apply_with_k_cosine_metric() {
        let (_dir, puffin_manager_factory) =
            PuffinManagerFactory::new_for_test_async("test_apply_with_k_cosine_").await;
        let object_store = ObjectStore::new(Memory::default()).unwrap().finish();
        let table_dir = "test_table";
        let file_id = RegionFileId::new(0.into(), FileId::random());
        let index_id = RegionIndexId::new(file_id, 0);
        let column_id = 1;

        let vectors = vec![
            vec![1.0f32, 0.0, 0.0, 0.0],
            vec![2.0f32, 0.0, 0.0, 0.0],
            vec![0.0f32, 1.0, 0.0, 0.0],
            vec![-1.0f32, 0.0, 0.0, 0.0],
        ];
        let config = VectorIndexConfig {
            engine: VectorIndexEngineType::Usearch,
            dim: 4,
            metric: distance_metric_to_usearch(VectorDistanceMetric::Cosine),
            distance_metric: VectorDistanceMetric::Cosine,
            connectivity: 16,
            expansion_add: 128,
            expansion_search: 64,
        };
        let blob_data = create_vector_index_blob(&vectors, &[], &config);
        write_blob_to_puffin(
            &puffin_manager_factory,
            &object_store,
            table_dir,
            index_id,
            column_id,
            blob_data,
        )
        .await;

        let applier = VectorIndexApplier::new(
            table_dir.to_string(),
            PathType::Bare,
            object_store,
            puffin_manager_factory,
            column_id,
            vec![1.0f32, 0.0, 0.0, 0.0],
            2,
            DtVectorDistanceMetric::Cosine,
        );

        let result = applier.apply_with_k(file_id, None, None, 2).await.unwrap();

        assert_eq!(result.row_offsets.len(), 2);
        assert!(result.row_offsets.contains(&0) || result.row_offsets.contains(&1));
    }
}
