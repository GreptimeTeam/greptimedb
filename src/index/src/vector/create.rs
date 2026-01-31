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

//! Vector index creation trait and implementation.

use async_trait::async_trait;
use common_error::ext::BoxedError;
use greptime_proto::v1::index::{VectorIndexMeta, VectorIndexStats};
use prost::Message;
use puffin::puffin_manager::{PuffinWriter, PutOptions};
use roaring::RoaringBitmap;
use snafu::ResultExt;
use store_api::storage::{VectorIndexEngine, VectorIndexEngineType};
use tokio::io::AsyncWriteExt;
use tokio_util::compat::TokioAsyncReadCompatExt;

use super::engine::{self, VectorIndexConfig};
use super::error::{
    EngineSnafu, ExternalSnafu, FinishSnafu, Result, RowCountOverflowSnafu, SerializeBitmapSnafu,
};
use super::format::{META_SIZE_LEN, distance_metric_to_proto, engine_type_to_proto};

/// The buffer size for the pipe used to send index data to the puffin blob.
const PIPE_BUFFER_SIZE_FOR_SENDING_BLOB: usize = 8192;

/// `IndexCreator` is for creating a vector index.
#[async_trait]
pub trait IndexCreator: Send {
    /// Pushes a vector to the index.
    fn push_vector(&mut self, vector: &[f32]) -> Result<()>;

    /// Records a NULL vector.
    fn push_null(&mut self) -> Result<()>;

    /// Records multiple NULL vectors.
    fn push_nulls(&mut self, n: usize) -> Result<()>;

    /// Finalizes the index and writes to puffin.
    /// Returns bytes written.
    async fn finish(
        &mut self,
        puffin_writer: &mut (impl PuffinWriter + Send),
        blob_key: &str,
        put_options: PutOptions,
    ) -> Result<u64>;

    /// Aborts the creation.
    async fn abort(&mut self) -> Result<()>;

    /// Returns the memory usage.
    fn memory_usage(&self) -> usize;

    /// Returns the number of indexed vectors.
    fn size(&self) -> usize;
}

/// HNSW-based vector index creator.
pub struct HnswVectorIndexCreator {
    /// The vector index engine (e.g., USearch HNSW).
    engine: Box<dyn VectorIndexEngine>,
    /// Configuration for this index.
    config: VectorIndexConfig,
    /// Bitmap tracking which row offsets have NULL vectors.
    null_bitmap: RoaringBitmap,
    /// Current row offset (including NULLs).
    current_row_offset: u64,
    /// Next HNSW key to assign (only for non-NULL vectors).
    next_hnsw_key: u64,
    /// Memory usage estimation.
    memory_usage_bytes: usize,
    /// Whether the index has been finished.
    finished: bool,
}

impl HnswVectorIndexCreator {
    /// Creates a new HNSW vector index creator.
    pub fn new(config: VectorIndexConfig) -> Result<Self> {
        let engine_instance = engine::create_engine(config.engine, &config)?;

        Ok(Self {
            engine: engine_instance,
            config,
            null_bitmap: RoaringBitmap::new(),
            current_row_offset: 0,
            next_hnsw_key: 0,
            memory_usage_bytes: 0,
            finished: false,
        })
    }

    /// Reserves capacity for the expected number of vectors.
    #[allow(dead_code)]
    pub fn reserve(&mut self, capacity: usize) -> Result<()> {
        self.engine.reserve(capacity).map_err(|e| {
            EngineSnafu {
                reason: format!("Failed to reserve capacity: {}", e),
            }
            .build()
        })
    }

    /// Returns the engine type.
    pub fn engine_type(&self) -> VectorIndexEngineType {
        self.config.engine
    }

    /// Returns the configuration.
    pub fn config(&self) -> &VectorIndexConfig {
        &self.config
    }

    /// Returns the null bitmap.
    pub fn null_bitmap(&self) -> &RoaringBitmap {
        &self.null_bitmap
    }

    /// Returns the current row offset (total rows including NULLs).
    pub fn current_row_offset(&self) -> u64 {
        self.current_row_offset
    }

    /// Returns the next HNSW key (total indexed rows excluding NULLs).
    pub fn next_hnsw_key(&self) -> u64 {
        self.next_hnsw_key
    }

    /// Builds the blob data for the vector index.
    fn build_blob(&self) -> Result<Vec<u8>> {
        // Serialize null bitmap
        let mut null_bitmap_bytes = Vec::new();
        self.null_bitmap
            .serialize_into(&mut null_bitmap_bytes)
            .context(SerializeBitmapSnafu)?;

        // Serialize the vector index
        let index_size = self.engine.serialized_length();
        let mut index_bytes = vec![0u8; index_size];
        self.engine.save_to_buffer(&mut index_bytes).map_err(|e| {
            EngineSnafu {
                reason: format!("Failed to serialize index: {}", e),
            }
            .build()
        })?;

        // Build VectorIndexMeta
        let total_rows = self.current_row_offset;
        let indexed_rows = self.next_hnsw_key;
        let null_count = total_rows - indexed_rows;

        let meta = VectorIndexMeta {
            engine: engine_type_to_proto(self.config.engine).into(),
            dim: self.config.dim as u32,
            metric: distance_metric_to_proto(self.config.distance_metric).into(),
            connectivity: self.config.connectivity as u32,
            expansion_add: self.config.expansion_add as u32,
            expansion_search: self.config.expansion_search as u32,
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
        let total_size =
            null_bitmap_bytes.len() + index_bytes.len() + meta_bytes.len() + META_SIZE_LEN;
        let mut blob_data = Vec::with_capacity(total_size);
        blob_data.extend_from_slice(&null_bitmap_bytes);
        blob_data.extend_from_slice(&index_bytes);
        blob_data.extend_from_slice(&meta_bytes);
        blob_data.extend_from_slice(&meta_size.to_le_bytes());

        Ok(blob_data)
    }
}

#[async_trait]
impl IndexCreator for HnswVectorIndexCreator {
    fn push_vector(&mut self, vector: &[f32]) -> Result<()> {
        if self.current_row_offset >= u32::MAX as u64 {
            return RowCountOverflowSnafu {
                current: self.current_row_offset,
                increment: 1u64,
            }
            .fail();
        }

        let key = self.next_hnsw_key;
        self.engine.add(key, vector).map_err(|e| {
            EngineSnafu {
                reason: e.to_string(),
            }
            .build()
        })?;
        self.next_hnsw_key += 1;
        self.current_row_offset += 1;
        self.memory_usage_bytes = self.engine.memory_usage();
        Ok(())
    }

    fn push_null(&mut self) -> Result<()> {
        if self.current_row_offset >= u32::MAX as u64 {
            return RowCountOverflowSnafu {
                current: self.current_row_offset,
                increment: 1u64,
            }
            .fail();
        }

        self.null_bitmap.insert(self.current_row_offset as u32);
        self.current_row_offset += 1;
        Ok(())
    }

    fn push_nulls(&mut self, n: usize) -> Result<()> {
        let increment = n as u64;
        if self.current_row_offset.saturating_add(increment) > u32::MAX as u64 {
            return RowCountOverflowSnafu {
                current: self.current_row_offset,
                increment,
            }
            .fail();
        }

        let start = self.current_row_offset as u32;
        let end = start + n as u32;
        self.null_bitmap.insert_range(start..end);
        self.current_row_offset += increment;
        Ok(())
    }

    async fn finish(
        &mut self,
        puffin_writer: &mut (impl PuffinWriter + Send),
        blob_key: &str,
        put_options: PutOptions,
    ) -> Result<u64> {
        if self.finished {
            return FinishSnafu {
                reason: "Index already finished".to_string(),
            }
            .fail();
        }
        self.finished = true;

        let blob_data = self.build_blob()?;

        // Write to puffin using a pipe
        let (tx, rx) = tokio::io::duplex(PIPE_BUFFER_SIZE_FOR_SENDING_BLOB);

        // Writer task writes the blob data to the pipe
        let write_index = async move {
            let mut writer = tx;
            writer.write_all(&blob_data).await?;
            writer.shutdown().await?;
            Ok::<(), std::io::Error>(())
        };

        let (index_write_result, puffin_add_blob) = futures::join!(
            write_index,
            puffin_writer.put_blob(blob_key, rx.compat(), put_options, Default::default())
        );

        // Handle errors
        let puffin_result = puffin_add_blob
            .map_err(BoxedError::new)
            .context(ExternalSnafu);
        let write_result = index_write_result.map_err(|e| {
            FinishSnafu {
                reason: format!("Failed to write blob data: {}", e),
            }
            .build()
        });

        match (puffin_result, write_result) {
            (Err(e), _) => Err(e),
            (_, Err(e)) => Err(e),
            (Ok(written_bytes), Ok(_)) => Ok(written_bytes),
        }
    }

    async fn abort(&mut self) -> Result<()> {
        self.finished = true;
        Ok(())
    }

    fn memory_usage(&self) -> usize {
        self.memory_usage_bytes + self.null_bitmap.serialized_size()
    }

    fn size(&self) -> usize {
        self.engine.size()
    }
}

#[cfg(test)]
mod tests {
    use store_api::storage::VectorIndexEngineType;
    use usearch::MetricKind;

    use super::*;
    use crate::vector::VectorDistanceMetric;

    fn test_config() -> VectorIndexConfig {
        VectorIndexConfig {
            engine: VectorIndexEngineType::Usearch,
            dim: 4,
            metric: MetricKind::L2sq,
            distance_metric: VectorDistanceMetric::L2sq,
            connectivity: 16,
            expansion_add: 128,
            expansion_search: 64,
        }
    }

    #[test]
    fn test_hnsw_vector_index_creator() {
        let config = test_config();
        let mut creator = HnswVectorIndexCreator::new(config).unwrap();

        creator.reserve(10).unwrap();

        // Add some vectors
        let v1 = vec![1.0f32, 0.0, 0.0, 0.0];
        let v2 = vec![0.0f32, 1.0, 0.0, 0.0];

        creator.push_vector(&v1).unwrap();
        creator.push_null().unwrap();
        creator.push_vector(&v2).unwrap();

        assert_eq!(creator.size(), 2); // 2 vectors (excluding NULL)
        assert_eq!(creator.current_row_offset(), 3); // 3 rows total
        assert!(creator.null_bitmap().contains(1)); // Row 1 is NULL
    }

    #[test]
    fn test_hnsw_vector_index_creator_build_blob() {
        let config = test_config();
        let mut creator = HnswVectorIndexCreator::new(config).unwrap();

        creator.reserve(10).unwrap();

        // Add vectors
        creator.push_vector(&[1.0, 0.0, 0.0, 0.0]).unwrap();
        creator.push_null().unwrap();
        creator.push_vector(&[0.0, 1.0, 0.0, 0.0]).unwrap();

        let blob = creator.build_blob().unwrap();
        assert!(!blob.is_empty());

        // Verify blob can be parsed using the applier
        use crate::vector::apply::{HnswVectorIndexApplier, IndexApplier};
        let applier = HnswVectorIndexApplier::from_blob(&blob).unwrap();
        assert_eq!(applier.dimensions(), 4);
        assert_eq!(applier.total_rows(), 3);
        assert_eq!(applier.indexed_rows(), 2);
    }
}
