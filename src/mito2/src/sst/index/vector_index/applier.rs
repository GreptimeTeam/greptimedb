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
use index::vector::VectorDistanceMetric;
use index::vector::apply::HnswVectorIndexApplier;
use puffin::puffin_manager::cache::PuffinMetadataCacheRef;
use puffin::puffin_manager::{PuffinManager, PuffinReader};
use snafu::ResultExt;
use store_api::storage::ColumnId;

use crate::access_layer::{RegionFilePathFactory, WriteCachePathProvider};
use crate::cache::file_cache::{FileCacheRef, FileType, IndexKey};
use crate::cache::index::vector_index::{
    CachedVectorIndex, VectorIndexCacheKey, VectorIndexCacheRef,
};
use crate::error::{ApplyVectorIndexSnafu, PuffinBuildReaderSnafu, PuffinReadBlobSnafu, Result};
use crate::sst::file::RegionIndexId;
use crate::sst::index::puffin_manager::{BlobReader, PuffinManagerFactory};
use crate::sst::index::trigger_index_background_download;
use crate::sst::index::vector_index::INDEX_BLOB_TYPE;

/// Result of applying vector index.
#[derive(Debug)]
pub struct VectorIndexApplyOutput {
    /// Row offsets in the SST file.
    pub row_offsets: Vec<u64>,
    /// Distances to the query vector.
    /// TODO(dennis): Not used yet, reserved for future use.
    #[allow(dead_code)]
    pub distances: Vec<f32>,
}

/// Vector index applier for KNN search against SST blobs.
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
                distances: Vec::new(),
            });
        }

        let cached = self.load_or_read_index(file_id, file_size_hint).await?;
        let Some(cached) = cached else {
            return Ok(VectorIndexApplyOutput {
                row_offsets: Vec::new(),
                distances: Vec::new(),
            });
        };

        let applier = cached.applier();
        if self.query_vector.len() != applier.dimensions() as usize {
            return ApplyVectorIndexSnafu {
                reason: format!(
                    "Query vector dimension {} does not match index dimension {}",
                    self.query_vector.len(),
                    applier.dimensions()
                ),
            }
            .fail();
        }
        if self.metric != applier.metric() {
            return ApplyVectorIndexSnafu {
                reason: format!(
                    "Query metric {} does not match index metric {}",
                    self.metric,
                    applier.metric()
                ),
            }
            .fail();
        }

        let output = applier.search(&self.query_vector, k).map_err(|e| {
            ApplyVectorIndexSnafu {
                reason: e.to_string(),
            }
            .build()
        })?;

        Ok(VectorIndexApplyOutput {
            row_offsets: output.row_offsets,
            distances: output.distances,
        })
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

        let applier = HnswVectorIndexApplier::from_blob(&blob_data).map_err(|e| {
            ApplyVectorIndexSnafu {
                reason: e.to_string(),
            }
            .build()
        })?;

        let cached = Arc::new(CachedVectorIndex::new(Box::new(applier)));
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

#[cfg(test)]
mod tests {
    use common_test_util::temp_dir::TempDir;
    use futures::io::Cursor;
    use greptime_proto::v1::index::{VectorIndexMeta, VectorIndexStats};
    use index::vector::engine::{self, VectorIndexConfig};
    use index::vector::format::{distance_metric_to_proto, engine_type_to_proto};
    use object_store::ObjectStore;
    use object_store::services::Memory;
    use prost::Message;
    use puffin::puffin_manager::PuffinWriter;
    use roaring::RoaringBitmap;
    use store_api::region_request::PathType;
    use store_api::storage::{ColumnId, FileId, VectorIndexEngineType};

    use super::*;
    use crate::access_layer::RegionFilePathFactory;
    use crate::sst::file::RegionFileId;
    use crate::sst::index::puffin_manager::PuffinManagerFactory;

    async fn build_applier_with_blob(
        blob: Vec<u8>,
        column_id: ColumnId,
        query_vector: Vec<f32>,
        metric: VectorDistanceMetric,
    ) -> (TempDir, VectorIndexApplier, RegionIndexId, u64) {
        let (dir, puffin_manager_factory) =
            PuffinManagerFactory::new_for_test_async("test_vector_index_applier_").await;
        let object_store = ObjectStore::new(Memory::default()).unwrap().finish();
        let file_id = RegionFileId::new(0.into(), FileId::random());
        let index_id = RegionIndexId::new(file_id, 0);
        let table_dir = "table_dir".to_string();

        let puffin_manager = puffin_manager_factory.build(
            object_store.clone(),
            RegionFilePathFactory::new(table_dir.clone(), PathType::Bare),
        );
        let mut writer = puffin_manager.writer(&index_id).await.unwrap();
        let blob_name = column_blob_name(column_id);
        let _bytes_written = writer
            .put_blob(
                blob_name.as_str(),
                Cursor::new(blob),
                Default::default(),
                Default::default(),
            )
            .await
            .unwrap();
        let file_size = writer.finish().await.unwrap();

        let applier = VectorIndexApplier::new(
            table_dir,
            PathType::Bare,
            object_store,
            puffin_manager_factory,
            column_id,
            query_vector,
            metric,
        );

        (dir, applier, index_id, file_size)
    }

    fn build_blob_with_vectors(
        config: &VectorIndexConfig,
        vectors: Vec<(u64, Vec<f32>)>,
        null_bitmap: &RoaringBitmap,
        total_rows: u64,
        indexed_rows: u64,
    ) -> Vec<u8> {
        let mut engine = engine::create_engine(config.engine, config).unwrap();
        for (key, vector) in vectors {
            engine.add(key, &vector).unwrap();
        }
        let index_size = engine.serialized_length();
        let mut index_bytes = vec![0u8; index_size];
        engine.save_to_buffer(&mut index_bytes).unwrap();

        let mut null_bitmap_bytes = Vec::new();
        null_bitmap.serialize_into(&mut null_bitmap_bytes).unwrap();

        let null_count = total_rows - indexed_rows;
        let meta = VectorIndexMeta {
            engine: engine_type_to_proto(config.engine).into(),
            dim: config.dim as u32,
            metric: distance_metric_to_proto(config.distance_metric).into(),
            connectivity: config.connectivity as u32,
            expansion_add: config.expansion_add as u32,
            expansion_search: config.expansion_search as u32,
            null_bitmap_size: null_bitmap_bytes.len() as u32,
            index_size: index_bytes.len() as u64,
            stats: Some(VectorIndexStats {
                total_row_count: total_rows,
                indexed_row_count: indexed_rows,
                null_count,
            }),
        };
        let meta_bytes = meta.encode_to_vec();
        let meta_size = meta_bytes.len() as u32;

        // Build blob: null_bitmap | index_data | meta | meta_size
        let mut blob = Vec::new();
        blob.extend_from_slice(&null_bitmap_bytes);
        blob.extend_from_slice(&index_bytes);
        blob.extend_from_slice(&meta_bytes);
        blob.extend_from_slice(&meta_size.to_le_bytes());
        blob
    }

    fn test_config() -> VectorIndexConfig {
        VectorIndexConfig {
            engine: VectorIndexEngineType::Usearch,
            dim: 2,
            distance_metric: VectorDistanceMetric::L2sq,
            connectivity: 16,
            expansion_add: 128,
            expansion_search: 64,
        }
    }

    #[tokio::test]
    async fn test_apply_with_k_returns_offsets() {
        let config = test_config();
        let mut null_bitmap = RoaringBitmap::new();
        null_bitmap.insert(1);
        let blob = build_blob_with_vectors(
            &config,
            vec![(0, vec![1.0, 0.0]), (1, vec![0.0, 1.0])],
            &null_bitmap,
            3,
            2,
        );
        let (_dir, applier, index_id, size_bytes) =
            build_applier_with_blob(blob, 1, vec![1.0, 0.0], VectorDistanceMetric::L2sq).await;
        let output = applier
            .apply_with_k(index_id, Some(size_bytes), 2)
            .await
            .unwrap();
        assert_eq!(output.row_offsets, vec![0, 2]);
    }

    #[tokio::test]
    async fn test_apply_with_k_dimension_mismatch() {
        let config = test_config();
        let null_bitmap = RoaringBitmap::new();
        let blob = build_blob_with_vectors(&config, vec![(0, vec![1.0, 0.0])], &null_bitmap, 1, 1);
        let (_dir, applier, index_id, size_bytes) =
            build_applier_with_blob(blob, 1, vec![1.0, 0.0, 0.0], VectorDistanceMetric::L2sq).await;
        let res = applier.apply_with_k(index_id, Some(size_bytes), 1).await;
        assert!(res.is_err());
    }

    #[tokio::test]
    async fn test_apply_with_k_empty_blob() {
        let config = VectorIndexConfig {
            engine: VectorIndexEngineType::Usearch,
            dim: 1,
            distance_metric: VectorDistanceMetric::L2sq,
            connectivity: 16,
            expansion_add: 128,
            expansion_search: 64,
        };
        let null_bitmap = RoaringBitmap::new();
        let blob = build_blob_with_vectors(&config, Vec::new(), &null_bitmap, 0, 0);
        let (_dir, applier, index_id, size_bytes) =
            build_applier_with_blob(blob, 1, vec![1.0], VectorDistanceMetric::L2sq).await;
        let output = applier
            .apply_with_k(index_id, Some(size_bytes), 1)
            .await
            .unwrap();
        assert!(output.row_offsets.is_empty());
    }
}
