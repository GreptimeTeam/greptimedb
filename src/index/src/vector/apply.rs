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

//! Vector index application trait and implementation.

use greptime_proto::v1::index::VectorIndexMeta;
use prost::Message;
use roaring::RoaringBitmap;
use snafu::ResultExt;
use store_api::storage::VectorIndexEngine;

use super::engine::{self, VectorIndexConfig};
use super::error::{
    BlobTooSmallSnafu, BlobTruncatedSnafu, DecodeProtoSnafu, DeserializeBitmapSnafu, EngineSnafu,
    InvalidBlobSnafu, KeyMappingSnafu, Result, UnknownDistanceMetricSnafu, UnknownEngineTypeSnafu,
};
use super::format::{META_SIZE_LEN, distance_metric_from_proto, engine_type_from_proto};
use super::{VectorDistanceMetric, distance_metric_to_usearch};

/// Output of a vector index search.
#[derive(Debug, Clone)]
pub struct VectorSearchOutput {
    /// Row offsets in the original data.
    pub row_offsets: Vec<u64>,
    /// Distances to query vector.
    pub distances: Vec<f32>,
}

/// `IndexApplier` is for searching a vector index.
pub trait IndexApplier: Send + Sync {
    /// Searches for k nearest neighbors.
    fn search(&self, query: &[f32], k: usize) -> Result<VectorSearchOutput>;

    /// Returns the memory usage.
    fn memory_usage(&self) -> usize;

    /// Returns the dimension of vectors in this index.
    fn dimensions(&self) -> u32;

    /// Returns the distance metric used for this index.
    fn metric(&self) -> VectorDistanceMetric;

    /// Returns the total number of rows (including NULLs).
    fn total_rows(&self) -> u64;

    /// Returns the number of indexed rows (excluding NULLs).
    fn indexed_rows(&self) -> u64;
}

/// HNSW-based vector index applier.
pub struct HnswVectorIndexApplier {
    /// The loaded vector index engine.
    engine: Box<dyn VectorIndexEngine>,
    /// Bitmap tracking which row offsets have NULL vectors.
    null_bitmap: RoaringBitmap,
    /// The dimension of vectors in this index.
    dimensions: u32,
    /// The distance metric used for this index.
    metric: VectorDistanceMetric,
    /// Total number of rows (including NULLs).
    total_rows: u64,
    /// Number of indexed rows (excluding NULLs).
    indexed_rows: u64,
}

impl HnswVectorIndexApplier {
    /// Creates a new HNSW vector index applier from blob data.
    ///
    /// Blob layout:
    /// ```text
    /// +----------------------+
    /// |  Null Bitmap         |  <- RoaringBitmap serialized
    /// +----------------------+
    /// |  Index Data          |  <- engine-specific serialized
    /// +----------------------+
    /// |  Meta (protobuf)     |  <- VectorIndexMeta encoded
    /// +----------------------+
    /// |  Meta Size (4B)      |  <- u32 LE
    /// +----------------------+
    /// ```
    pub fn from_blob(data: &[u8]) -> Result<Self> {
        if data.len() < META_SIZE_LEN {
            return BlobTooSmallSnafu {
                min_size: META_SIZE_LEN,
                actual_size: data.len(),
            }
            .fail();
        }

        // Read meta size from the end
        let blob_len = data.len();
        let meta_size =
            u32::from_le_bytes(data[blob_len - META_SIZE_LEN..].try_into().unwrap()) as usize;

        if blob_len < META_SIZE_LEN + meta_size {
            return BlobTruncatedSnafu {
                reason: "blob truncated while reading meta".to_string(),
            }
            .fail();
        }

        // Read and decode meta
        let meta_start = blob_len - META_SIZE_LEN - meta_size;
        let meta = VectorIndexMeta::decode(&data[meta_start..blob_len - META_SIZE_LEN])
            .context(DecodeProtoSnafu)?;

        let null_bitmap_size = meta.null_bitmap_size as usize;
        let index_size = meta.index_size as usize;

        if null_bitmap_size + index_size > meta_start {
            return InvalidBlobSnafu {
                reason: "invalid size fields".to_string(),
            }
            .fail();
        }

        // Read null bitmap
        let null_bitmap = RoaringBitmap::deserialize_from(&data[..null_bitmap_size])
            .context(DeserializeBitmapSnafu)?;

        // Read index data
        let index_start = null_bitmap_size;
        let index_end = index_start + index_size;
        let index_bytes = &data[index_start..index_end];

        // Convert proto enums to Rust types
        let engine_proto = meta.engine.try_into().map_err(|_| {
            UnknownEngineTypeSnafu {
                engine_type: meta.engine,
            }
            .build()
        })?;
        let engine_type = engine_type_from_proto(engine_proto).ok_or_else(|| {
            UnknownEngineTypeSnafu {
                engine_type: meta.engine,
            }
            .build()
        })?;

        let metric_proto = meta.metric.try_into().map_err(|_| {
            UnknownDistanceMetricSnafu {
                metric: meta.metric,
            }
            .build()
        })?;
        let distance_metric = distance_metric_from_proto(metric_proto).ok_or_else(|| {
            UnknownDistanceMetricSnafu {
                metric: meta.metric,
            }
            .build()
        })?;

        let config = VectorIndexConfig {
            engine: engine_type,
            dim: meta.dim as usize,
            metric: distance_metric_to_usearch(distance_metric),
            distance_metric,
            connectivity: meta.connectivity as usize,
            expansion_add: meta.expansion_add as usize,
            expansion_search: meta.expansion_search as usize,
        };

        let engine = engine::load_engine(engine_type, &config, index_bytes)?;

        let stats = meta.stats.unwrap_or_default();

        Ok(Self {
            engine,
            null_bitmap,
            dimensions: meta.dim,
            metric: distance_metric,
            total_rows: stats.total_row_count,
            indexed_rows: stats.indexed_row_count,
        })
    }

    /// Maps HNSW keys from search results to row offsets in the original data.
    fn map_keys_to_row_offsets(&self, keys: Vec<u64>) -> Result<Vec<u64>> {
        if self.total_rows == 0 {
            return Ok(Vec::new());
        }
        let total_rows_u32 = u32::try_from(self.total_rows).map_err(|_| {
            KeyMappingSnafu {
                reason: format!("Total rows {} exceeds u32::MAX", self.total_rows),
            }
            .build()
        })?;

        let mut row_offsets = Vec::with_capacity(keys.len());
        for key in keys {
            let offset = self.hnsw_key_to_row_offset(total_rows_u32, key)?;
            row_offsets.push(offset as u64);
        }
        Ok(row_offsets)
    }

    /// Converts a single HNSW key to its corresponding row offset.
    fn hnsw_key_to_row_offset(&self, total_rows: u32, key: u64) -> Result<u32> {
        if total_rows == 0 {
            return KeyMappingSnafu {
                reason: "Total rows is zero".to_string(),
            }
            .fail();
        }
        if key >= self.indexed_rows {
            return KeyMappingSnafu {
                reason: format!(
                    "HNSW key {} exceeds indexed rows {}",
                    key, self.indexed_rows
                ),
            }
            .fail();
        }

        // Fast path: no NULLs means key == row offset
        if self.null_bitmap.is_empty() {
            return Ok(key as u32);
        }

        // Binary search to find the row offset
        let mut left: u32 = 0;
        let mut right: u32 = total_rows - 1;
        while left <= right {
            let mid = left + (right - left) / 2;
            let nulls_before = self.null_bitmap.rank(mid);
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
            return KeyMappingSnafu {
                reason: "Failed to map HNSW key to row offset".to_string(),
            }
            .fail();
        }

        Ok(left)
    }
}

impl IndexApplier for HnswVectorIndexApplier {
    fn search(&self, query: &[f32], k: usize) -> Result<VectorSearchOutput> {
        if self.indexed_rows == 0 || k == 0 {
            return Ok(VectorSearchOutput {
                row_offsets: Vec::new(),
                distances: Vec::new(),
            });
        }

        let actual_k = k.min(self.indexed_rows as usize);
        let matches = self.engine.search(query, actual_k).map_err(|e| {
            EngineSnafu {
                reason: e.to_string(),
            }
            .build()
        })?;

        let row_offsets = self.map_keys_to_row_offsets(matches.keys)?;

        Ok(VectorSearchOutput {
            row_offsets,
            distances: matches.distances,
        })
    }

    fn memory_usage(&self) -> usize {
        self.engine.memory_usage()
            + self.null_bitmap.serialized_size()
            + std::mem::size_of::<Self>()
    }

    fn dimensions(&self) -> u32 {
        self.dimensions
    }

    fn metric(&self) -> VectorDistanceMetric {
        self.metric
    }

    fn total_rows(&self) -> u64 {
        self.total_rows
    }

    fn indexed_rows(&self) -> u64 {
        self.indexed_rows
    }
}

#[cfg(test)]
mod tests {
    use greptime_proto::v1::index::{VectorIndexMeta, VectorIndexStats};
    use prost::Message;
    use roaring::RoaringBitmap;
    use store_api::storage::VectorIndexEngineType;
    use usearch::MetricKind;

    use super::*;
    use crate::vector::engine::VectorIndexConfig;
    use crate::vector::format::{distance_metric_to_proto, engine_type_to_proto};

    fn build_test_blob(
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
            metric: MetricKind::L2sq,
            distance_metric: VectorDistanceMetric::L2sq,
            connectivity: 16,
            expansion_add: 128,
            expansion_search: 64,
        }
    }

    #[test]
    fn test_hnsw_vector_index_applier_search() {
        let config = test_config();
        let mut null_bitmap = RoaringBitmap::new();
        null_bitmap.insert(1);

        let blob = build_test_blob(
            &config,
            vec![(0, vec![1.0, 0.0]), (1, vec![0.0, 1.0])],
            &null_bitmap,
            3,
            2,
        );

        let applier = HnswVectorIndexApplier::from_blob(&blob).unwrap();
        assert_eq!(applier.dimensions(), 2);
        assert_eq!(applier.metric(), VectorDistanceMetric::L2sq);
        assert_eq!(applier.total_rows(), 3);
        assert_eq!(applier.indexed_rows(), 2);

        let output = applier.search(&[1.0, 0.0], 2).unwrap();
        assert_eq!(output.row_offsets.len(), 2);
        // First result should be row 0 (closest to [1.0, 0.0])
        assert_eq!(output.row_offsets[0], 0);
        // Second result should be row 2 (maps from HNSW key 1)
        assert_eq!(output.row_offsets[1], 2);
    }

    #[test]
    fn test_hnsw_vector_index_applier_empty() {
        let config = test_config();
        let null_bitmap = RoaringBitmap::new();

        let blob = build_test_blob(&config, vec![], &null_bitmap, 0, 0);

        let applier = HnswVectorIndexApplier::from_blob(&blob).unwrap();
        let output = applier.search(&[1.0, 0.0], 1).unwrap();
        assert!(output.row_offsets.is_empty());
    }

    #[test]
    fn test_hnsw_vector_index_applier_k_zero() {
        let config = test_config();
        let null_bitmap = RoaringBitmap::new();

        let blob = build_test_blob(&config, vec![(0, vec![1.0, 0.0])], &null_bitmap, 1, 1);

        let applier = HnswVectorIndexApplier::from_blob(&blob).unwrap();
        let output = applier.search(&[1.0, 0.0], 0).unwrap();
        assert!(output.row_offsets.is_empty());
    }
}
