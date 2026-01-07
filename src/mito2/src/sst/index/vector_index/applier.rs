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

//! Vector index applier for KNN search.

use std::sync::Arc;

use common_base::range_read::RangeReader;
use common_telemetry::warn;
use index::vector::distance_metric_to_usearch;
use puffin::puffin_manager::cache::PuffinMetadataCacheRef;
use puffin::puffin_manager::{PuffinManager, PuffinReader};
use roaring::RoaringBitmap;
use snafu::ResultExt;
use store_api::storage::{ColumnId, VectorDistanceMetric};

use crate::access_layer::{RegionFilePathFactory, WriteCachePathProvider};
use crate::cache::file_cache::{FileCacheRef, FileType, IndexKey};
use crate::cache::index::vector_index::{
    CachedVectorIndex, VectorIndexCacheKey, VectorIndexCacheRef,
};
use crate::error::{ApplyVectorIndexSnafu, PuffinBuildReaderSnafu, PuffinReadBlobSnafu, Result};
use crate::sst::file::RegionIndexId;
use crate::sst::index::puffin_manager::{BlobReader, PuffinManagerFactory};
use crate::sst::index::trigger_index_background_download;
use crate::sst::index::vector_index::creator::VectorIndexConfig;
use crate::sst::index::vector_index::format::VectorIndexBlobHeader;
use crate::sst::index::vector_index::{INDEX_BLOB_TYPE, engine};

/// Result of applying vector index.
#[derive(Debug)]
pub struct VectorIndexApplyOutput {
    /// Row offsets in the SST file.
    pub row_offsets: Vec<u64>,
}

/// Vector index applier.
pub struct VectorIndexApplier {
    table_dir: String,
    path_type: store_api::region_request::PathType,
    object_store: object_store::ObjectStore,
    puffin_manager_factory: PuffinManagerFactory,
    file_cache: Option<FileCacheRef>,
    puffin_metadata_cache: Option<PuffinMetadataCacheRef>,
    vector_index_cache: Option<VectorIndexCacheRef>,
    column_id: ColumnId,
    query_vector: Vec<f32>,
    metric: VectorDistanceMetric,
}

pub type VectorIndexApplierRef = Arc<VectorIndexApplier>;

impl VectorIndexApplier {
    pub fn new(
        table_dir: String,
        path_type: store_api::region_request::PathType,
        object_store: object_store::ObjectStore,
        puffin_manager_factory: PuffinManagerFactory,
        column_id: ColumnId,
        query_vector: Vec<f32>,
        metric: VectorDistanceMetric,
    ) -> Self {
        Self {
            table_dir,
            path_type,
            object_store,
            puffin_manager_factory,
            file_cache: None,
            puffin_metadata_cache: None,
            vector_index_cache: None,
            column_id,
            query_vector,
            metric,
        }
    }

    pub fn with_file_cache(mut self, file_cache: Option<FileCacheRef>) -> Self {
        self.file_cache = file_cache;
        self
    }

    pub fn with_puffin_metadata_cache(
        mut self,
        puffin_metadata_cache: Option<PuffinMetadataCacheRef>,
    ) -> Self {
        self.puffin_metadata_cache = puffin_metadata_cache;
        self
    }

    pub fn with_vector_index_cache(mut self, cache: Option<VectorIndexCacheRef>) -> Self {
        self.vector_index_cache = cache;
        self
    }

    /// Applies vector index to the file and returns candidates.
    ///
    /// This method loads the vector index blob (from cache or remote), runs
    /// a KNN search against the indexed vectors, and maps the HNSW keys back
    /// to row offsets in the SST file. It returns only row offsets; callers
    /// are responsible for any higher-level ordering or limit enforcement.
    pub async fn apply_with_k(
        &self,
        file_id: RegionIndexId,
        file_size_hint: Option<u64>,
        k: usize,
    ) -> Result<VectorIndexApplyOutput> {
        if k == 0 {
            return Ok(VectorIndexApplyOutput {
                row_offsets: Vec::new(),
            });
        }

        let index = self.load_or_read_index(file_id, file_size_hint).await?;
        let Some(index) = index else {
            return Ok(VectorIndexApplyOutput {
                row_offsets: Vec::new(),
            });
        };

        if self.query_vector.len() != index.dimensions as usize {
            return ApplyVectorIndexSnafu {
                reason: format!(
                    "Query vector dimension {} does not match index dimension {}",
                    self.query_vector.len(),
                    index.dimensions
                ),
            }
            .fail();
        }
        if self.metric != index.metric {
            return ApplyVectorIndexSnafu {
                reason: format!(
                    "Query metric {} does not match index metric {}",
                    self.metric, index.metric
                ),
            }
            .fail();
        }
        if index.indexed_rows == 0 {
            return Ok(VectorIndexApplyOutput {
                row_offsets: Vec::new(),
            });
        }

        let matches = index
            .engine
            .search(&self.query_vector, k.min(index.indexed_rows as usize))
            .map_err(|e| {
                ApplyVectorIndexSnafu {
                    reason: e.to_string(),
                }
                .build()
            })?;

        let row_offsets = map_hnsw_keys_to_row_offsets(
            &index.null_bitmap,
            index.total_rows,
            index.indexed_rows,
            matches.keys,
        )?;

        Ok(VectorIndexApplyOutput { row_offsets })
    }

    async fn load_or_read_index(
        &self,
        file_id: RegionIndexId,
        file_size_hint: Option<u64>,
    ) -> Result<Option<Arc<CachedVectorIndex>>> {
        let cache_key =
            VectorIndexCacheKey::new(file_id.file_id(), file_id.version, self.column_id);
        if let Some(cache) = &self.vector_index_cache
            && let Some(cached) = cache.get(&cache_key)
        {
            return Ok(Some(cached));
        }

        let reader = match self.cached_blob_reader(file_id, file_size_hint).await {
            Ok(Some(reader)) => reader,
            Ok(None) => self.remote_blob_reader(file_id, file_size_hint).await?,
            Err(err) => {
                if is_blob_not_found(&err) {
                    self.remote_blob_reader(file_id, file_size_hint).await?
                } else {
                    warn!(err; "Failed to read cached vector index blob, fallback to remote");
                    self.remote_blob_reader(file_id, file_size_hint).await?
                }
            }
        };

        let blob_data = read_all_blob(reader, file_size_hint).await?;
        if blob_data.is_empty() {
            return Ok(None);
        }

        let cached = Arc::new(parse_vector_index_blob(&blob_data)?);
        if let Some(cache) = &self.vector_index_cache {
            cache.insert(cache_key, cached.clone());
        }

        Ok(Some(cached))
    }

    async fn cached_blob_reader(
        &self,
        file_id: RegionIndexId,
        file_size_hint: Option<u64>,
    ) -> Result<Option<BlobReader>> {
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
        }

        let puffin_manager = self.puffin_manager_factory.build(
            file_cache.local_store(),
            WriteCachePathProvider::new(file_cache.clone()),
        );
        let blob_name = column_blob_name(self.column_id);

        let reader = puffin_manager
            .reader(&file_id)
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

    async fn remote_blob_reader(
        &self,
        file_id: RegionIndexId,
        file_size_hint: Option<u64>,
    ) -> Result<BlobReader> {
        let path_factory = RegionFilePathFactory::new(self.table_dir.clone(), self.path_type);

        trigger_index_background_download(
            self.file_cache.as_ref(),
            &file_id,
            file_size_hint,
            &path_factory,
            &self.object_store,
        );

        let puffin_manager = self
            .puffin_manager_factory
            .build(self.object_store.clone(), path_factory)
            .with_puffin_metadata_cache(self.puffin_metadata_cache.clone());

        let blob_name = column_blob_name(self.column_id);

        puffin_manager
            .reader(&file_id)
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

fn column_blob_name(column_id: ColumnId) -> String {
    format!("{INDEX_BLOB_TYPE}-{}", column_id)
}

fn is_blob_not_found(err: &crate::error::Error) -> bool {
    matches!(
        err,
        crate::error::Error::PuffinReadBlob {
            source: puffin::error::Error::BlobNotFound { .. },
            ..
        }
    )
}

async fn read_all_blob(reader: BlobReader, file_size_hint: Option<u64>) -> Result<Vec<u8>> {
    let metadata = reader.metadata().await.map_err(|e| {
        ApplyVectorIndexSnafu {
            reason: format!("Failed to read vector index metadata: {}", e),
        }
        .build()
    })?;
    if let Some(limit) = file_size_hint
        && metadata.content_length > limit
    {
        return ApplyVectorIndexSnafu {
            reason: format!(
                "Vector index blob size {} exceeds file size hint {}",
                metadata.content_length, limit
            ),
        }
        .fail();
    }
    let bytes = reader.read(0..metadata.content_length).await.map_err(|e| {
        ApplyVectorIndexSnafu {
            reason: format!("Failed to read vector index data: {}", e),
        }
        .build()
    })?;
    Ok(bytes.to_vec())
}

fn parse_vector_index_blob(data: &[u8]) -> Result<CachedVectorIndex> {
    let (header, mut offset) = VectorIndexBlobHeader::decode(data).map_err(|e| {
        ApplyVectorIndexSnafu {
            reason: e.to_string(),
        }
        .build()
    })?;
    let null_bitmap_len = header.null_bitmap_len as usize;

    if data.len() < offset + null_bitmap_len {
        return ApplyVectorIndexSnafu {
            reason: "Vector index blob truncated while reading null bitmap".to_string(),
        }
        .fail();
    }

    let null_bitmap_bytes = &data[offset..offset + null_bitmap_len];
    offset += null_bitmap_len;
    let null_bitmap = RoaringBitmap::deserialize_from(null_bitmap_bytes).map_err(|e| {
        ApplyVectorIndexSnafu {
            reason: format!("Failed to deserialize null bitmap: {}", e),
        }
        .build()
    })?;

    let index_bytes = &data[offset..];
    let config = VectorIndexConfig {
        engine: header.engine_type,
        dim: header.dim as usize,
        metric: distance_metric_to_usearch(header.metric),
        distance_metric: header.metric,
        connectivity: header.connectivity as usize,
        expansion_add: header.expansion_add as usize,
        expansion_search: header.expansion_search as usize,
    };
    let engine = engine::load_engine(header.engine_type, &config, index_bytes).map_err(|e| {
        ApplyVectorIndexSnafu {
            reason: e.to_string(),
        }
        .build()
    })?;

    Ok(CachedVectorIndex::new(
        engine,
        null_bitmap,
        header.dim,
        header.metric,
        header.total_rows,
        header.indexed_rows,
    ))
}

fn map_hnsw_keys_to_row_offsets(
    null_bitmap: &RoaringBitmap,
    total_rows: u64,
    indexed_rows: u64,
    keys: Vec<u64>,
) -> Result<Vec<u64>> {
    if total_rows == 0 {
        return Ok(Vec::new());
    }
    let total_rows_u32 = u32::try_from(total_rows).map_err(|_| {
        ApplyVectorIndexSnafu {
            reason: format!("Total rows {} exceeds u32::MAX", total_rows),
        }
        .build()
    })?;

    let mut row_offsets = Vec::with_capacity(keys.len());
    for key in keys {
        let offset = hnsw_key_to_row_offset(null_bitmap, total_rows_u32, indexed_rows, key)?;
        row_offsets.push(offset as u64);
    }
    Ok(row_offsets)
}

fn hnsw_key_to_row_offset(
    null_bitmap: &RoaringBitmap,
    total_rows: u32,
    indexed_rows: u64,
    key: u64,
) -> Result<u32> {
    if total_rows == 0 {
        return ApplyVectorIndexSnafu {
            reason: "Total rows is zero".to_string(),
        }
        .fail();
    }
    if key >= indexed_rows {
        return ApplyVectorIndexSnafu {
            reason: format!("HNSW key {} exceeds indexed rows {}", key, indexed_rows),
        }
        .fail();
    }

    if null_bitmap.is_empty() {
        return Ok(key as u32);
    }

    let mut left: u32 = 0;
    let mut right: u32 = total_rows - 1;
    while left <= right {
        let mid = left + (right - left) / 2;
        let nulls_before = null_bitmap.rank(mid);
        let non_nulls = (mid as u64 + 1).saturating_sub(nulls_before);
        if non_nulls > key {
            if mid == 0 {
                break;
            }
            right = mid - 1;
        } else {
            left = mid + 1;
        }
    }

    if left >= total_rows {
        return ApplyVectorIndexSnafu {
            reason: "Failed to map HNSW key to row offset".to_string(),
        }
        .fail();
    }

    Ok(left)
}

#[cfg(test)]
mod tests {
    use store_api::storage::VectorIndexEngineType;

    use super::*;

    #[test]
    fn test_hnsw_key_to_row_offset_with_nulls() {
        let mut bitmap = RoaringBitmap::new();
        bitmap.insert(1);
        bitmap.insert(3);

        assert_eq!(hnsw_key_to_row_offset(&bitmap, 6, 4, 0).unwrap(), 0);
        assert_eq!(hnsw_key_to_row_offset(&bitmap, 6, 4, 1).unwrap(), 2);
        assert_eq!(hnsw_key_to_row_offset(&bitmap, 6, 4, 2).unwrap(), 4);
    }

    #[test]
    fn test_hnsw_key_to_row_offset_without_nulls() {
        let bitmap = RoaringBitmap::new();
        assert_eq!(hnsw_key_to_row_offset(&bitmap, 4, 4, 3).unwrap(), 3);
    }

    #[test]
    fn test_hnsw_key_to_row_offset_out_of_range() {
        let bitmap = RoaringBitmap::new();
        assert!(hnsw_key_to_row_offset(&bitmap, 4, 4, 4).is_err());
    }

    #[test]
    fn test_map_hnsw_keys_to_row_offsets_multiple_keys() {
        let bitmap = RoaringBitmap::new();
        let offsets = map_hnsw_keys_to_row_offsets(&bitmap, 4, 4, vec![0, 2, 3]).unwrap();
        assert_eq!(offsets, vec![0, 2, 3]);
    }

    #[test]
    fn test_parse_vector_index_blob_roundtrip() {
        let config = VectorIndexConfig {
            engine: VectorIndexEngineType::Usearch,
            dim: 2,
            metric: distance_metric_to_usearch(VectorDistanceMetric::L2sq),
            distance_metric: VectorDistanceMetric::L2sq,
            connectivity: 16,
            expansion_add: 128,
            expansion_search: 64,
        };
        let mut engine = engine::create_engine(config.engine, &config).unwrap();
        engine.add(0, &[0.0, 1.0]).unwrap();
        let index_size = engine.serialized_length();
        let mut index_bytes = vec![0u8; index_size];
        engine.save_to_buffer(&mut index_bytes).unwrap();

        let null_bitmap = RoaringBitmap::new();
        let mut null_bitmap_bytes = Vec::new();
        null_bitmap.serialize_into(&mut null_bitmap_bytes).unwrap();

        let total_rows: u64 = 1;
        let indexed_rows: u64 = 1;
        let header = VectorIndexBlobHeader::new(
            config.engine,
            config.dim as u32,
            VectorDistanceMetric::L2sq,
            config.connectivity as u16,
            config.expansion_add as u16,
            config.expansion_search as u16,
            total_rows,
            indexed_rows,
            null_bitmap_bytes.len() as u32,
        )
        .unwrap();
        let mut blob = Vec::new();
        header.encode_into(&mut blob);
        blob.extend_from_slice(&null_bitmap_bytes);
        blob.extend_from_slice(&index_bytes);

        let parsed = parse_vector_index_blob(&blob).unwrap();
        assert_eq!(parsed.dimensions, 2);
        assert_eq!(parsed.metric, VectorDistanceMetric::L2sq);
        assert_eq!(parsed.total_rows, total_rows);
        assert_eq!(parsed.indexed_rows, indexed_rows);
        assert_eq!(parsed.null_bitmap.len(), 0);
    }

    #[test]
    fn test_parse_vector_index_blob_invalid_version() {
        let mut blob = vec![0u8; 33];
        blob[0] = 2;
        assert!(parse_vector_index_blob(&blob).is_err());
    }

    #[test]
    fn test_parse_vector_index_blob_truncated_null_bitmap() {
        let mut blob = vec![0u8; 33];
        blob[0] = 1;
        blob[1] = VectorIndexEngineType::Usearch.as_u8();
        blob[2..6].copy_from_slice(&(2u32).to_le_bytes());
        blob[6] = VectorDistanceMetric::L2sq.as_u8();
        blob[7..9].copy_from_slice(&(16u16).to_le_bytes());
        blob[9..11].copy_from_slice(&(128u16).to_le_bytes());
        blob[11..13].copy_from_slice(&(64u16).to_le_bytes());
        blob[13..21].copy_from_slice(&(1u64).to_le_bytes());
        blob[21..29].copy_from_slice(&(1u64).to_le_bytes());
        blob[29..33].copy_from_slice(&(4u32).to_le_bytes());
        assert!(parse_vector_index_blob(&blob).is_err());
    }
}
