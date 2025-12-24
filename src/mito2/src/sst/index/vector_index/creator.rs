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

//! Vector index creator using pluggable vector index engines.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use common_telemetry::warn;
use datatypes::data_type::ConcreteDataType;
use datatypes::prelude::ValueRef;
use index::vector::{VectorDistanceMetric, VectorIndexOptions, distance_metric_to_usearch};
use puffin::puffin_manager::{PuffinWriter, PutOptions};
use roaring::RoaringBitmap;
use snafu::{ResultExt, ensure};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{ColumnId, FileId, VectorIndexEngine, VectorIndexEngineType};
use tokio_util::compat::TokioAsyncReadCompatExt;
use usearch::MetricKind;

use crate::error::{
    BiErrorsSnafu, OperateAbortedIndexSnafu, PuffinAddBlobSnafu, Result, VectorIndexBuildSnafu,
    VectorIndexFinishSnafu,
};
use crate::metrics::{INDEX_CREATE_BYTES_TOTAL, INDEX_CREATE_ROWS_TOTAL};
use crate::read::Batch;
use crate::sst::index::TYPE_VECTOR_INDEX;
use crate::sst::index::intermediate::{
    IntermediateLocation, IntermediateManager, TempFileProvider,
};
use crate::sst::index::puffin_manager::SstPuffinWriter;
use crate::sst::index::statistics::{ByteCount, RowCount, Statistics};
use crate::sst::index::vector_index::util::bytes_to_f32_slice;
use crate::sst::index::vector_index::{INDEX_BLOB_TYPE, engine};

/// The buffer size for the pipe used to send index data to the puffin blob.
const PIPE_BUFFER_SIZE_FOR_SENDING_BLOB: usize = 8192;

/// Configuration for a single column's vector index.
#[derive(Debug, Clone)]
pub struct VectorIndexConfig {
    /// The vector index engine type.
    pub engine: VectorIndexEngineType,
    /// The dimension of vectors in this column.
    pub dim: usize,
    /// The distance metric to use (e.g., L2, Cosine, IP) - usearch format.
    pub metric: MetricKind,
    /// The original distance metric (for serialization).
    pub distance_metric: VectorDistanceMetric,
    /// HNSW connectivity parameter (M in the paper).
    /// Higher values give better recall but use more memory.
    pub connectivity: usize,
    /// Expansion factor during index construction (ef_construction).
    pub expansion_add: usize,
    /// Expansion factor during search (ef_search).
    pub expansion_search: usize,
}

impl VectorIndexConfig {
    /// Creates a new vector index config from VectorIndexOptions.
    pub fn new(dim: usize, options: &VectorIndexOptions) -> Self {
        Self {
            engine: options.engine,
            dim,
            metric: distance_metric_to_usearch(options.metric),
            distance_metric: options.metric,
            connectivity: options.connectivity as usize,
            expansion_add: options.expansion_add as usize,
            expansion_search: options.expansion_search as usize,
        }
    }
}

/// Creator for a single column's vector index.
struct VectorIndexCreator {
    /// The vector index engine (e.g., USearch HNSW).
    engine: Box<dyn VectorIndexEngine>,
    /// Configuration for this index.
    config: VectorIndexConfig,
    /// Bitmap tracking which row offsets have NULL vectors.
    /// HNSW keys are sequential (0, 1, 2...) but row offsets may have gaps due to NULLs.
    null_bitmap: RoaringBitmap,
    /// Current row offset (including NULLs).
    current_row_offset: u64,
    /// Next HNSW key to assign (only for non-NULL vectors).
    next_hnsw_key: u64,
    /// Memory usage estimation.
    memory_usage: usize,
}

impl VectorIndexCreator {
    /// Creates a new vector index creator.
    fn new(config: VectorIndexConfig) -> Result<Self> {
        let engine_instance = engine::create_engine(config.engine, &config)?;

        Ok(Self {
            engine: engine_instance,
            config,
            null_bitmap: RoaringBitmap::new(),
            current_row_offset: 0,
            next_hnsw_key: 0,
            memory_usage: 0,
        })
    }

    /// Reserves capacity for the expected number of vectors.
    #[allow(dead_code)]
    fn reserve(&mut self, capacity: usize) -> Result<()> {
        self.engine.reserve(capacity).map_err(|e| {
            VectorIndexBuildSnafu {
                reason: format!("Failed to reserve capacity: {}", e),
            }
            .build()
        })
    }

    /// Adds a vector to the index.
    /// Returns the HNSW key assigned to this vector.
    fn add_vector(&mut self, vector: &[f32]) -> Result<u64> {
        let key = self.next_hnsw_key;
        self.engine.add(key, vector).map_err(|e| {
            VectorIndexBuildSnafu {
                reason: e.to_string(),
            }
            .build()
        })?;
        self.next_hnsw_key += 1;
        self.current_row_offset += 1;
        self.memory_usage = self.engine.memory_usage();
        Ok(key)
    }

    /// Records a NULL vector at the current row offset.
    fn add_null(&mut self) {
        self.null_bitmap.insert(self.current_row_offset as u32);
        self.current_row_offset += 1;
    }

    /// Returns the serialized size of the index.
    fn serialized_length(&self) -> usize {
        self.engine.serialized_length()
    }

    /// Serializes the index to a buffer.
    fn save_to_buffer(&self, buffer: &mut [u8]) -> Result<()> {
        self.engine.save_to_buffer(buffer).map_err(|e| {
            VectorIndexFinishSnafu {
                reason: format!("Failed to serialize index: {}", e),
            }
            .build()
        })
    }

    /// Returns the memory usage of this creator.
    fn memory_usage(&self) -> usize {
        self.memory_usage + self.null_bitmap.serialized_size()
    }

    /// Returns the number of vectors in the index (excluding NULLs).
    fn size(&self) -> usize {
        self.engine.size()
    }

    /// Returns the engine type.
    fn engine_type(&self) -> VectorIndexEngineType {
        self.config.engine
    }

    /// Returns the distance metric.
    fn metric(&self) -> VectorDistanceMetric {
        self.config.distance_metric
    }
}

/// The indexer for vector indexes across multiple columns.
pub struct VectorIndexer {
    /// Per-column vector index creators.
    creators: HashMap<ColumnId, VectorIndexCreator>,
    /// Provider for intermediate files.
    temp_file_provider: Arc<TempFileProvider>,
    /// Whether the indexing process has been aborted.
    aborted: bool,
    /// Statistics for this indexer.
    stats: Statistics,
    /// Global memory usage tracker.
    #[allow(dead_code)]
    global_memory_usage: Arc<AtomicUsize>,
    /// Region metadata for column lookups.
    #[allow(dead_code)]
    metadata: RegionMetadataRef,
    /// Memory usage threshold.
    memory_usage_threshold: Option<usize>,
}

impl VectorIndexer {
    /// Creates a new vector indexer.
    ///
    /// Returns `None` if there are no vector columns that need indexing.
    pub fn new(
        sst_file_id: FileId,
        metadata: &RegionMetadataRef,
        intermediate_manager: IntermediateManager,
        memory_usage_threshold: Option<usize>,
        vector_index_options: &HashMap<ColumnId, VectorIndexOptions>,
    ) -> Result<Option<Self>> {
        let mut creators = HashMap::new();

        let temp_file_provider = Arc::new(TempFileProvider::new(
            IntermediateLocation::new(&metadata.region_id, &sst_file_id),
            intermediate_manager,
        ));
        let global_memory_usage = Arc::new(AtomicUsize::new(0));

        // Find all vector columns that have vector index enabled
        for column in &metadata.column_metadatas {
            // Check if this column has vector index options configured
            let Some(options) = vector_index_options.get(&column.column_id) else {
                continue;
            };

            // Verify the column is a vector type
            let ConcreteDataType::Vector(vector_type) = &column.column_schema.data_type else {
                continue;
            };

            let config = VectorIndexConfig::new(vector_type.dim as usize, options);
            let creator = VectorIndexCreator::new(config)?;
            creators.insert(column.column_id, creator);
        }

        if creators.is_empty() {
            return Ok(None);
        }

        let indexer = Self {
            creators,
            temp_file_provider,
            aborted: false,
            stats: Statistics::new(TYPE_VECTOR_INDEX),
            global_memory_usage,
            metadata: metadata.clone(),
            memory_usage_threshold,
        };

        Ok(Some(indexer))
    }

    /// Updates index with a batch of rows.
    /// Garbage will be cleaned up if failed to update.
    pub async fn update(&mut self, batch: &mut Batch) -> Result<()> {
        ensure!(!self.aborted, OperateAbortedIndexSnafu);

        if self.creators.is_empty() {
            return Ok(());
        }

        if let Err(update_err) = self.do_update(batch).await {
            // Clean up garbage if failed to update
            if let Err(err) = self.do_cleanup().await {
                if cfg!(any(test, feature = "test")) {
                    panic!("Failed to clean up vector index creator, err: {err:?}");
                } else {
                    warn!(err; "Failed to clean up vector index creator");
                }
            }
            return Err(update_err);
        }

        Ok(())
    }

    /// Internal update implementation.
    async fn do_update(&mut self, batch: &mut Batch) -> Result<()> {
        let mut guard = self.stats.record_update();
        let n = batch.num_rows();
        guard.inc_row_count(n);

        for (col_id, creator) in &mut self.creators {
            let Some(values) = batch.field_col_value(*col_id) else {
                continue;
            };

            // Process each row in the batch
            for i in 0..n {
                let value = values.data.get_ref(i);
                if value.is_null() {
                    creator.add_null();
                } else {
                    // Extract the vector bytes and convert to f32 slice
                    if let ValueRef::Binary(bytes) = value {
                        let floats = bytes_to_f32_slice(bytes);
                        if floats.len() != creator.config.dim {
                            return VectorIndexBuildSnafu {
                                reason: format!(
                                    "Vector dimension mismatch: expected {}, got {}",
                                    creator.config.dim,
                                    floats.len()
                                ),
                            }
                            .fail();
                        }
                        creator.add_vector(&floats)?;
                    } else {
                        creator.add_null();
                    }
                }
            }

            // Check memory limit - abort index creation if exceeded
            if let Some(threshold) = self.memory_usage_threshold {
                let current_usage = creator.memory_usage();
                if current_usage > threshold {
                    warn!(
                        "Vector index memory usage {} exceeds threshold {}, aborting index creation",
                        current_usage, threshold
                    );
                    return VectorIndexBuildSnafu {
                        reason: format!(
                            "Memory usage {} exceeds threshold {}",
                            current_usage, threshold
                        ),
                    }
                    .fail();
                }
            }
        }

        Ok(())
    }

    /// Finishes index creation and writes to puffin.
    /// Returns the number of rows and bytes written.
    pub async fn finish(
        &mut self,
        puffin_writer: &mut SstPuffinWriter,
    ) -> Result<(RowCount, ByteCount)> {
        ensure!(!self.aborted, OperateAbortedIndexSnafu);

        if self.stats.row_count() == 0 {
            // No IO is performed, no garbage to clean up
            return Ok((0, 0));
        }

        let finish_res = self.do_finish(puffin_writer).await;
        // Clean up garbage no matter finish successfully or not
        if let Err(err) = self.do_cleanup().await {
            if cfg!(any(test, feature = "test")) {
                panic!("Failed to clean up vector index creator, err: {err:?}");
            } else {
                warn!(err; "Failed to clean up vector index creator");
            }
        }

        // Report metrics on successful finish
        if finish_res.is_ok() {
            INDEX_CREATE_ROWS_TOTAL
                .with_label_values(&[TYPE_VECTOR_INDEX])
                .inc_by(self.stats.row_count() as u64);
            INDEX_CREATE_BYTES_TOTAL
                .with_label_values(&[TYPE_VECTOR_INDEX])
                .inc_by(self.stats.byte_count());
        }

        finish_res.map(|_| (self.stats.row_count(), self.stats.byte_count()))
    }

    /// Internal finish implementation.
    async fn do_finish(&mut self, puffin_writer: &mut SstPuffinWriter) -> Result<()> {
        let mut guard = self.stats.record_finish();

        for (id, creator) in &mut self.creators {
            if creator.size() == 0 {
                // No vectors to index
                continue;
            }

            let written_bytes = Self::do_finish_single_creator(*id, creator, puffin_writer).await?;
            guard.inc_byte_count(written_bytes);
        }

        Ok(())
    }

    /// Finishes a single column's vector index.
    ///
    /// The blob format v1:
    /// ```text
    /// +------------------+
    /// | Version          | 1 byte (u8, = 1)
    /// +------------------+
    /// | Engine type      | 1 byte (u8, engine identifier)
    /// +------------------+
    /// | Dimension        | 4 bytes (u32, little-endian)
    /// +------------------+
    /// | Metric           | 1 byte (u8, distance metric)
    /// +------------------+
    /// | NULL bitmap len  | 4 bytes (u32, little-endian)
    /// +------------------+
    /// | NULL bitmap      | variable length (serialized RoaringBitmap)
    /// +------------------+
    /// | Vector index     | variable length (engine-specific serialized format)
    /// +------------------+
    /// ```
    async fn do_finish_single_creator(
        col_id: ColumnId,
        creator: &mut VectorIndexCreator,
        puffin_writer: &mut SstPuffinWriter,
    ) -> Result<ByteCount> {
        // Serialize the NULL bitmap
        let mut null_bitmap_bytes = Vec::new();
        creator
            .null_bitmap
            .serialize_into(&mut null_bitmap_bytes)
            .map_err(|e| {
                VectorIndexFinishSnafu {
                    reason: format!("Failed to serialize NULL bitmap: {}", e),
                }
                .build()
            })?;

        // Serialize the vector index
        let index_size = creator.serialized_length();
        let mut index_bytes = vec![0u8; index_size];
        creator.save_to_buffer(&mut index_bytes)?;

        // Header size: version(1) + engine(1) + dim(4) + metric(1) +
        //              connectivity(2) + expansion_add(2) + expansion_search(2) +
        //              total_rows(8) + indexed_rows(8) + bitmap_len(4) = 33 bytes
        let header_size = 33;
        let total_size = header_size + null_bitmap_bytes.len() + index_bytes.len();
        let mut blob_data = Vec::with_capacity(total_size);

        // Write version (1 byte)
        blob_data.push(1u8);
        // Write engine type (1 byte)
        blob_data.push(creator.engine_type().as_u8());
        // Write dimension (4 bytes, little-endian)
        blob_data.extend_from_slice(&(creator.config.dim as u32).to_le_bytes());
        // Write metric (1 byte)
        blob_data.push(creator.metric().as_u8());
        // Write connectivity/M (2 bytes, little-endian)
        blob_data.extend_from_slice(&(creator.config.connectivity as u16).to_le_bytes());
        // Write expansion_add/ef_construction (2 bytes, little-endian)
        blob_data.extend_from_slice(&(creator.config.expansion_add as u16).to_le_bytes());
        // Write expansion_search/ef_search (2 bytes, little-endian)
        blob_data.extend_from_slice(&(creator.config.expansion_search as u16).to_le_bytes());
        // Write total_rows (8 bytes, little-endian)
        blob_data.extend_from_slice(&creator.current_row_offset.to_le_bytes());
        // Write indexed_rows (8 bytes, little-endian)
        blob_data.extend_from_slice(&creator.next_hnsw_key.to_le_bytes());
        // Write NULL bitmap length (4 bytes, little-endian)
        blob_data.extend_from_slice(&(null_bitmap_bytes.len() as u32).to_le_bytes());
        // Write NULL bitmap
        blob_data.extend_from_slice(&null_bitmap_bytes);
        // Write vector index
        blob_data.extend_from_slice(&index_bytes);

        // Create blob name following the same pattern as bloom filter
        let blob_name = format!("{}-{}", INDEX_BLOB_TYPE, col_id);

        // Write to puffin using a pipe
        let (tx, rx) = tokio::io::duplex(PIPE_BUFFER_SIZE_FOR_SENDING_BLOB);

        // Writer task writes the blob data to the pipe
        let write_index = async move {
            use tokio::io::AsyncWriteExt;
            let mut writer = tx;
            writer.write_all(&blob_data).await?;
            writer.shutdown().await?;
            Ok::<(), std::io::Error>(())
        };

        let (index_write_result, puffin_add_blob) = futures::join!(
            write_index,
            puffin_writer.put_blob(
                &blob_name,
                rx.compat(),
                PutOptions::default(),
                Default::default()
            )
        );

        match (
            puffin_add_blob.context(PuffinAddBlobSnafu),
            index_write_result.map_err(|e| {
                VectorIndexFinishSnafu {
                    reason: format!("Failed to write blob data: {}", e),
                }
                .build()
            }),
        ) {
            (Err(e1), Err(e2)) => BiErrorsSnafu {
                first: Box::new(e1),
                second: Box::new(e2),
            }
            .fail()?,

            (Ok(_), e @ Err(_)) => e?,
            (e @ Err(_), Ok(_)) => e.map(|_| ())?,
            (Ok(written_bytes), Ok(_)) => {
                return Ok(written_bytes);
            }
        }

        Ok(0)
    }

    /// Aborts index creation and cleans up garbage.
    pub async fn abort(&mut self) -> Result<()> {
        if self.aborted {
            return Ok(());
        }
        self.aborted = true;
        self.do_cleanup().await
    }

    /// Cleans up temporary files.
    async fn do_cleanup(&mut self) -> Result<()> {
        let mut _guard = self.stats.record_cleanup();
        self.creators.clear();
        self.temp_file_provider.cleanup().await
    }

    /// Returns the memory usage of the indexer.
    pub fn memory_usage(&self) -> usize {
        self.creators.values().map(|c| c.memory_usage()).sum()
    }

    /// Returns the column IDs being indexed.
    pub fn column_ids(&self) -> impl Iterator<Item = ColumnId> + '_ {
        self.creators.keys().copied()
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_vector_index_creator() {
        let options = VectorIndexOptions::default();
        let config = VectorIndexConfig::new(4, &options);
        let mut creator = VectorIndexCreator::new(config).unwrap();

        creator.reserve(10).unwrap();

        // Add some vectors
        let v1 = vec![1.0f32, 0.0, 0.0, 0.0];
        let v2 = vec![0.0f32, 1.0, 0.0, 0.0];

        creator.add_vector(&v1).unwrap();
        creator.add_null();
        creator.add_vector(&v2).unwrap();

        assert_eq!(creator.size(), 2); // 2 vectors (excluding NULL)
        assert_eq!(creator.current_row_offset, 3); // 3 rows total
        assert!(creator.null_bitmap.contains(1)); // Row 1 is NULL
    }

    #[test]
    fn test_vector_index_creator_serialization() {
        let options = VectorIndexOptions::default();
        let config = VectorIndexConfig::new(4, &options);
        let mut creator = VectorIndexCreator::new(config).unwrap();

        creator.reserve(10).unwrap();

        // Add vectors
        let vectors = vec![
            vec![1.0f32, 0.0, 0.0, 0.0],
            vec![0.0f32, 1.0, 0.0, 0.0],
            vec![0.0f32, 0.0, 1.0, 0.0],
        ];

        for v in &vectors {
            creator.add_vector(v).unwrap();
        }

        // Test serialization
        let size = creator.serialized_length();
        assert!(size > 0);

        let mut buffer = vec![0u8; size];
        creator.save_to_buffer(&mut buffer).unwrap();

        // Verify buffer is not empty and starts with some data
        assert!(!buffer.iter().all(|&b| b == 0));
    }

    #[test]
    fn test_vector_index_creator_null_bitmap_serialization() {
        let options = VectorIndexOptions::default();
        let config = VectorIndexConfig::new(4, &options);
        let mut creator = VectorIndexCreator::new(config).unwrap();

        creator.reserve(10).unwrap();

        // Add pattern: vector, null, vector, null, null, vector
        creator.add_vector(&[1.0, 0.0, 0.0, 0.0]).unwrap();
        creator.add_null();
        creator.add_vector(&[0.0, 1.0, 0.0, 0.0]).unwrap();
        creator.add_null();
        creator.add_null();
        creator.add_vector(&[0.0, 0.0, 1.0, 0.0]).unwrap();

        assert_eq!(creator.size(), 3); // 3 vectors
        assert_eq!(creator.current_row_offset, 6); // 6 rows total
        assert!(!creator.null_bitmap.contains(0));
        assert!(creator.null_bitmap.contains(1));
        assert!(!creator.null_bitmap.contains(2));
        assert!(creator.null_bitmap.contains(3));
        assert!(creator.null_bitmap.contains(4));
        assert!(!creator.null_bitmap.contains(5));

        // Test NULL bitmap serialization
        let mut bitmap_bytes = Vec::new();
        creator
            .null_bitmap
            .serialize_into(&mut bitmap_bytes)
            .unwrap();

        // Deserialize and verify
        let restored = RoaringBitmap::deserialize_from(&bitmap_bytes[..]).unwrap();
        assert_eq!(restored.len(), 3); // 3 NULLs
        assert!(restored.contains(1));
        assert!(restored.contains(3));
        assert!(restored.contains(4));
    }

    #[test]
    fn test_vector_index_config() {
        use index::vector::VectorDistanceMetric;

        let options = VectorIndexOptions {
            engine: VectorIndexEngineType::default(),
            metric: VectorDistanceMetric::Cosine,
            connectivity: 32,
            expansion_add: 256,
            expansion_search: 128,
        };
        let config = VectorIndexConfig::new(128, &options);
        assert_eq!(config.engine, VectorIndexEngineType::Usearch);
        assert_eq!(config.dim, 128);
        assert_eq!(config.metric, MetricKind::Cos);
        assert_eq!(config.connectivity, 32);
        assert_eq!(config.expansion_add, 256);
        assert_eq!(config.expansion_search, 128);
    }

    #[test]
    fn test_vector_index_header_format() {
        use index::vector::VectorDistanceMetric;

        // Create config with specific HNSW parameters
        let options = VectorIndexOptions {
            engine: VectorIndexEngineType::Usearch,
            metric: VectorDistanceMetric::L2sq,
            connectivity: 24,
            expansion_add: 200,
            expansion_search: 100,
        };
        let config = VectorIndexConfig::new(4, &options);
        let mut creator = VectorIndexCreator::new(config).unwrap();

        creator.reserve(10).unwrap();

        // Add pattern: vector, null, vector, null, vector
        creator.add_vector(&[1.0, 0.0, 0.0, 0.0]).unwrap();
        creator.add_null();
        creator.add_vector(&[0.0, 1.0, 0.0, 0.0]).unwrap();
        creator.add_null();
        creator.add_vector(&[0.0, 0.0, 1.0, 0.0]).unwrap();

        // Verify counts
        assert_eq!(creator.current_row_offset, 5); // total_rows
        assert_eq!(creator.next_hnsw_key, 3); // indexed_rows

        // Build blob data manually (simulating write_to_puffin header writing)
        let mut null_bitmap_bytes = Vec::new();
        creator
            .null_bitmap
            .serialize_into(&mut null_bitmap_bytes)
            .unwrap();

        let index_size = creator.serialized_length();
        let mut index_bytes = vec![0u8; index_size];
        creator.save_to_buffer(&mut index_bytes).unwrap();

        // Header: 33 bytes
        let header_size = 33;
        let total_size = header_size + null_bitmap_bytes.len() + index_bytes.len();
        let mut blob_data = Vec::with_capacity(total_size);

        // Write header fields
        blob_data.push(1u8); // version
        blob_data.push(creator.engine_type().as_u8()); // engine type
        blob_data.extend_from_slice(&(creator.config.dim as u32).to_le_bytes()); // dimension
        blob_data.push(creator.metric().as_u8()); // metric
        blob_data.extend_from_slice(&(creator.config.connectivity as u16).to_le_bytes());
        blob_data.extend_from_slice(&(creator.config.expansion_add as u16).to_le_bytes());
        blob_data.extend_from_slice(&(creator.config.expansion_search as u16).to_le_bytes());
        blob_data.extend_from_slice(&creator.current_row_offset.to_le_bytes()); // total_rows
        blob_data.extend_from_slice(&creator.next_hnsw_key.to_le_bytes()); // indexed_rows
        blob_data.extend_from_slice(&(null_bitmap_bytes.len() as u32).to_le_bytes());
        blob_data.extend_from_slice(&null_bitmap_bytes);
        blob_data.extend_from_slice(&index_bytes);

        // Verify header size
        assert_eq!(blob_data.len(), total_size);

        // Parse header and verify values
        assert_eq!(blob_data[0], 1); // version
        assert_eq!(blob_data[1], VectorIndexEngineType::Usearch.as_u8()); // engine

        let dim = u32::from_le_bytes([blob_data[2], blob_data[3], blob_data[4], blob_data[5]]);
        assert_eq!(dim, 4);

        let metric = blob_data[6];
        assert_eq!(
            metric,
            datatypes::schema::VectorDistanceMetric::L2sq.as_u8()
        );

        let connectivity = u16::from_le_bytes([blob_data[7], blob_data[8]]);
        assert_eq!(connectivity, 24);

        let expansion_add = u16::from_le_bytes([blob_data[9], blob_data[10]]);
        assert_eq!(expansion_add, 200);

        let expansion_search = u16::from_le_bytes([blob_data[11], blob_data[12]]);
        assert_eq!(expansion_search, 100);

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
        assert_eq!(total_rows, 5);

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
        assert_eq!(indexed_rows, 3);

        let null_bitmap_len =
            u32::from_le_bytes([blob_data[29], blob_data[30], blob_data[31], blob_data[32]]);
        assert_eq!(null_bitmap_len as usize, null_bitmap_bytes.len());

        // Verify null bitmap can be deserialized
        let null_bitmap_data = &blob_data[header_size..header_size + null_bitmap_len as usize];
        let restored_bitmap = RoaringBitmap::deserialize_from(null_bitmap_data).unwrap();
        assert_eq!(restored_bitmap.len(), 2); // 2 nulls
        assert!(restored_bitmap.contains(1));
        assert!(restored_bitmap.contains(3));
    }
}
