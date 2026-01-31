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

//! Vector indexer for managing vector indexes across multiple columns.

use std::collections::HashMap;
use std::sync::Arc;
use std::sync::atomic::AtomicUsize;

use common_telemetry::warn;
use datatypes::arrow::array::{Array, BinaryArray};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::data_type::ConcreteDataType;
use datatypes::prelude::ValueRef;
use index::vector::create::{HnswVectorIndexCreator, IndexCreator};
use index::vector::engine::VectorIndexConfig;
use index::vector::util::bytes_to_f32_slice;
use index::vector::{VectorIndexOptions, distance_metric_to_usearch};
use puffin::puffin_manager::PutOptions;
use snafu::ensure;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{ColumnId, FileId};

use crate::error::{
    OperateAbortedIndexSnafu, Result, VectorIndexBuildSnafu, VectorIndexFinishSnafu,
};
use crate::metrics::{INDEX_CREATE_BYTES_TOTAL, INDEX_CREATE_ROWS_TOTAL};
use crate::read::Batch;
use crate::sst::index::TYPE_VECTOR_INDEX;
use crate::sst::index::intermediate::{
    IntermediateLocation, IntermediateManager, TempFileProvider,
};
use crate::sst::index::puffin_manager::SstPuffinWriter;
use crate::sst::index::statistics::{ByteCount, RowCount, Statistics};
use crate::sst::index::vector_index::INDEX_BLOB_TYPE;

/// Helper to create VectorIndexConfig from VectorIndexOptions.
fn create_config(dim: usize, options: &VectorIndexOptions) -> VectorIndexConfig {
    VectorIndexConfig {
        engine: options.engine,
        dim,
        metric: distance_metric_to_usearch(options.metric),
        distance_metric: options.metric,
        connectivity: options.connectivity as usize,
        expansion_add: options.expansion_add as usize,
        expansion_search: options.expansion_search as usize,
    }
}

/// The indexer for vector indexes across multiple columns.
pub struct VectorIndexer {
    /// Per-column vector index creators.
    creators: HashMap<ColumnId, HnswVectorIndexCreator>,
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

            let config = create_config(vector_type.dim as usize, options);
            let creator = HnswVectorIndexCreator::new(config).map_err(|e| {
                VectorIndexBuildSnafu {
                    reason: e.to_string(),
                }
                .build()
            })?;
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

    /// Updates index with a flat format `RecordBatch`.
    /// Garbage will be cleaned up if failed to update.
    pub async fn update_flat(&mut self, batch: &RecordBatch) -> Result<()> {
        ensure!(!self.aborted, OperateAbortedIndexSnafu);

        if self.creators.is_empty() || batch.num_rows() == 0 {
            return Ok(());
        }

        if let Err(update_err) = self.do_update_flat(batch).await {
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
                creator.push_nulls(n).map_err(|e| {
                    VectorIndexBuildSnafu {
                        reason: e.to_string(),
                    }
                    .build()
                })?;
                continue;
            };

            // Process each row in the batch
            for i in 0..n {
                let value = values.data.get_ref(i);
                if value.is_null() {
                    creator.push_null().map_err(|e| {
                        VectorIndexBuildSnafu {
                            reason: e.to_string(),
                        }
                        .build()
                    })?;
                } else {
                    // Extract the vector bytes and convert to f32 slice
                    if let ValueRef::Binary(bytes) = value {
                        let floats = bytes_to_f32_slice(bytes);
                        if floats.len() != creator.config().dim {
                            return VectorIndexBuildSnafu {
                                reason: format!(
                                    "Vector dimension mismatch: expected {}, got {}",
                                    creator.config().dim,
                                    floats.len()
                                ),
                            }
                            .fail();
                        }
                        creator.push_vector(&floats).map_err(|e| {
                            VectorIndexBuildSnafu {
                                reason: e.to_string(),
                            }
                            .build()
                        })?;
                    } else {
                        creator.push_null().map_err(|e| {
                            VectorIndexBuildSnafu {
                                reason: e.to_string(),
                            }
                            .build()
                        })?;
                    }
                }
            }

            // Check memory limit - abort index creation if exceeded
            if let Some(threshold) = self.memory_usage_threshold {
                let current_usage = creator.memory_usage();
                if current_usage > threshold {
                    warn!(
                        "Vector index memory usage {} exceeds threshold {}, aborting index creation, region_id: {}",
                        current_usage, threshold, self.metadata.region_id
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

    /// Internal flat update implementation.
    async fn do_update_flat(&mut self, batch: &RecordBatch) -> Result<()> {
        let mut guard = self.stats.record_update();
        let n = batch.num_rows();
        guard.inc_row_count(n);

        for (col_id, creator) in &mut self.creators {
            // This should never happen: creator exists but column not in metadata
            let column_meta = self.metadata.column_by_id(*col_id).ok_or_else(|| {
                VectorIndexBuildSnafu {
                    reason: format!(
                        "Column {} not found in region metadata, this is a bug",
                        col_id
                    ),
                }
                .build()
            })?;

            let column_name = &column_meta.column_schema.name;
            // Column not in batch is normal for flat format - treat as NULLs
            let Some(column_array) = batch.column_by_name(column_name) else {
                creator.push_nulls(n).map_err(|e| {
                    VectorIndexBuildSnafu {
                        reason: e.to_string(),
                    }
                    .build()
                })?;
                continue;
            };

            // Vector type must be stored as binary array
            let binary_array = column_array
                .as_any()
                .downcast_ref::<BinaryArray>()
                .ok_or_else(|| {
                    VectorIndexBuildSnafu {
                        reason: format!(
                            "Column {} is not a binary array, got {:?}",
                            column_name,
                            column_array.data_type()
                        ),
                    }
                    .build()
                })?;

            for i in 0..n {
                if !binary_array.is_valid(i) {
                    creator.push_null().map_err(|e| {
                        VectorIndexBuildSnafu {
                            reason: e.to_string(),
                        }
                        .build()
                    })?;
                } else {
                    let bytes = binary_array.value(i);
                    let floats = bytes_to_f32_slice(bytes);
                    if floats.len() != creator.config().dim {
                        return VectorIndexBuildSnafu {
                            reason: format!(
                                "Vector dimension mismatch: expected {}, got {}",
                                creator.config().dim,
                                floats.len()
                            ),
                        }
                        .fail();
                    }
                    creator.push_vector(&floats).map_err(|e| {
                        VectorIndexBuildSnafu {
                            reason: e.to_string(),
                        }
                        .build()
                    })?;
                }
            }

            if let Some(threshold) = self.memory_usage_threshold {
                let current_usage = creator.memory_usage();
                if current_usage > threshold {
                    warn!(
                        "Vector index memory usage {} exceeds threshold {}, aborting index creation, region_id: {}",
                        current_usage, threshold, self.metadata.region_id
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

    /// Finishes a single column's vector index using the VectorIndexCreator trait.
    async fn do_finish_single_creator(
        col_id: ColumnId,
        creator: &mut HnswVectorIndexCreator,
        puffin_writer: &mut SstPuffinWriter,
    ) -> Result<ByteCount> {
        // Create blob name following the same pattern as bloom filter
        let blob_name = format!("{}-{}", INDEX_BLOB_TYPE, col_id);

        // Use the trait's finish method which handles blob building and puffin writing
        let written_bytes = creator
            .finish(puffin_writer, &blob_name, PutOptions::default())
            .await
            .map_err(|e| {
                VectorIndexFinishSnafu {
                    reason: e.to_string(),
                }
                .build()
            })?;

        Ok(written_bytes)
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
    use std::collections::HashMap;
    use std::sync::Arc;

    use api::v1::SemanticType;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use datatypes::value::ValueRef;
    use datatypes::vectors::{TimestampMillisecondVector, UInt8Vector, UInt64Vector};
    use mito_codec::row_converter::{DensePrimaryKeyCodec, PrimaryKeyCodecExt, SortField};
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder, RegionMetadataRef};
    use store_api::storage::{ColumnId, FileId, RegionId};

    use super::*;
    use crate::read::BatchColumn;

    fn mock_region_metadata_with_vector() -> RegionMetadataRef {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("tag", ConcreteDataType::int64_datatype(), true),
                semantic_type: SemanticType::Tag,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("vec", ConcreteDataType::vector_datatype(2), true),
                semantic_type: SemanticType::Field,
                column_id: 3,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "field_u64",
                    ConcreteDataType::uint64_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Field,
                column_id: 4,
            })
            .primary_key(vec![2]);

        Arc::new(builder.build().unwrap())
    }

    fn new_batch_missing_vector_column(column_id: ColumnId, rows: usize) -> Batch {
        let fields = vec![(0, SortField::new(ConcreteDataType::int64_datatype()))];
        let codec = DensePrimaryKeyCodec::with_fields(fields);
        let primary_key = codec.encode([ValueRef::Int64(1)].into_iter()).unwrap();

        let field = BatchColumn {
            column_id,
            data: Arc::new(UInt64Vector::from_iter_values(0..rows as u64)),
        };

        Batch::new(
            primary_key,
            Arc::new(TimestampMillisecondVector::from_values(
                (0..rows).map(|i| i as i64).collect::<Vec<_>>(),
            )),
            Arc::new(UInt64Vector::from_iter_values(std::iter::repeat_n(0, rows))),
            Arc::new(UInt8Vector::from_iter_values(std::iter::repeat_n(1, rows))),
            vec![field],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_vector_index_missing_column_as_nulls() {
        let tempdir = common_test_util::temp_dir::create_temp_dir(
            "test_vector_index_missing_column_as_nulls_",
        );
        let intm_mgr = IntermediateManager::init_fs(tempdir.path().to_string_lossy())
            .await
            .unwrap();
        let region_metadata = mock_region_metadata_with_vector();

        let mut vector_index_options = HashMap::new();
        vector_index_options.insert(3, VectorIndexOptions::default());

        let mut indexer = VectorIndexer::new(
            FileId::random(),
            &region_metadata,
            intm_mgr,
            None,
            &vector_index_options,
        )
        .unwrap()
        .unwrap();

        let mut batch = new_batch_missing_vector_column(4, 3);
        indexer.update(&mut batch).await.unwrap();

        let creator = indexer.creators.get(&3).unwrap();
        assert_eq!(creator.size(), 0);
        assert_eq!(creator.current_row_offset(), 3);
        assert_eq!(creator.null_bitmap().len(), 3);
        for idx in 0..3 {
            assert!(creator.null_bitmap().contains(idx as u32));
        }
    }
}
