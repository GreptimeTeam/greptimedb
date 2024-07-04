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

mod indexer;
pub(crate) mod intermediate;
pub(crate) mod inverted_index;
pub(crate) mod puffin_manager;
mod statistics;
mod store;

use std::num::NonZeroUsize;

use common_telemetry::{debug, warn};
use puffin::puffin_manager::PuffinManager;
use puffin_manager::{SstPuffinManager, SstPuffinWriter};
use statistics::{ByteCount, RowCount};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{ColumnId, RegionId};

use crate::access_layer::OperationType;
use crate::config::InvertedIndexConfig;
use crate::metrics::INDEX_CREATE_MEMORY_USAGE;
use crate::read::Batch;
use crate::region::options::IndexOptions;
use crate::sst::file::FileId;
use crate::sst::index::intermediate::IntermediateManager;
use crate::sst::index::inverted_index::creator::SstIndexCreator as InvertedIndexer;

#[derive(Debug, Clone, Default)]
pub struct IndexOutput {
    pub file_size: u64,
    pub inverted_index: InvertedIndexOutput,
}

#[derive(Debug, Clone, Default)]
pub struct InvertedIndexOutput {
    pub available: bool,
    pub index_size: ByteCount,
    pub row_count: RowCount,
    pub columns: Vec<ColumnId>,
}

/// The index creator that hides the error handling details.
#[derive(Default)]
pub struct Indexer {
    file_id: FileId,
    region_id: RegionId,
    last_memory_usage: usize,

    inverted_indexer: Option<InvertedIndexer>,
    puffin_writer: Option<SstPuffinWriter>,
}

impl Indexer {
    /// Updates the index with the given batch.
    pub async fn update(&mut self, batch: &Batch) {
        self.do_update(batch).await;

        let memory_usage = self.memory_usage();
        INDEX_CREATE_MEMORY_USAGE.add(memory_usage as i64 - self.last_memory_usage as i64);
        self.last_memory_usage = memory_usage;
    }

    /// Finalizes the index creation.
    pub async fn finish(&mut self) -> IndexOutput {
        INDEX_CREATE_MEMORY_USAGE.sub(self.last_memory_usage as i64);
        self.last_memory_usage = 0;

        self.do_finish().await
    }

    /// Aborts the index creation.
    pub async fn abort(&mut self) {
        INDEX_CREATE_MEMORY_USAGE.sub(self.last_memory_usage as i64);
        self.last_memory_usage = 0;

        self.do_abort().await;
    }

    fn memory_usage(&self) -> usize {
        self.inverted_indexer
            .as_ref()
            .map_or(0, |creator| creator.memory_usage())
    }
}

pub(crate) struct IndexerBuilder<'a> {
    pub(crate) op_type: OperationType,
    pub(crate) file_id: FileId,
    pub(crate) file_path: String,
    pub(crate) metadata: &'a RegionMetadataRef,
    pub(crate) row_group_size: usize,
    pub(crate) puffin_manager: SstPuffinManager,
    pub(crate) intermediate_manager: IntermediateManager,
    pub(crate) index_options: IndexOptions,
    pub(crate) inverted_index_config: InvertedIndexConfig,
}

impl<'a> IndexerBuilder<'a> {
    /// Sanity check for arguments and create a new [Indexer] if arguments are valid.
    pub(crate) async fn build(self) -> Indexer {
        let mut indexer = Indexer {
            file_id: self.file_id,
            region_id: self.metadata.region_id,
            last_memory_usage: 0,

            ..Default::default()
        };

        indexer.inverted_indexer = self.build_inverted_indexer();
        if indexer.inverted_indexer.is_none() {
            indexer.abort().await;
            return Indexer::default();
        }

        indexer.puffin_writer = self.build_puffin_writer().await;
        if indexer.puffin_writer.is_none() {
            indexer.abort().await;
            return Indexer::default();
        }

        indexer
    }

    fn build_inverted_indexer(&self) -> Option<InvertedIndexer> {
        let create = match self.op_type {
            OperationType::Flush => self.inverted_index_config.create_on_flush.auto(),
            OperationType::Compact => self.inverted_index_config.create_on_compaction.auto(),
        };

        if !create {
            debug!(
                "Skip creating inverted index due to config, region_id: {}, file_id: {}",
                self.metadata.region_id, self.file_id,
            );
            return None;
        }

        if self.metadata.primary_key.is_empty() {
            debug!(
                "No tag columns, skip creating index, region_id: {}, file_id: {}",
                self.metadata.region_id, self.file_id,
            );
            return None;
        }

        let Some(mut segment_row_count) =
            NonZeroUsize::new(self.index_options.inverted_index.segment_row_count)
        else {
            warn!(
                "Segment row count is 0, skip creating index, region_id: {}, file_id: {}",
                self.metadata.region_id, self.file_id,
            );
            return None;
        };

        let Some(row_group_size) = NonZeroUsize::new(self.row_group_size) else {
            warn!(
                "Row group size is 0, skip creating index, region_id: {}, file_id: {}",
                self.metadata.region_id, self.file_id,
            );
            return None;
        };

        // if segment row count not aligned with row group size, adjust it to be aligned.
        if row_group_size.get() % segment_row_count.get() != 0 {
            segment_row_count = row_group_size;
        }

        let mem_threshold = self
            .inverted_index_config
            .mem_threshold_on_create
            .map(|t| t.as_bytes() as usize);

        let indexer = InvertedIndexer::new(
            self.file_id,
            self.metadata,
            self.intermediate_manager.clone(),
            mem_threshold,
            segment_row_count,
            self.inverted_index_config.compress,
        )
        .with_ignore_column_ids(&self.index_options.inverted_index.ignore_column_ids);

        Some(indexer)
    }

    async fn build_puffin_writer(&self) -> Option<SstPuffinWriter> {
        let err = match self.puffin_manager.writer(&self.file_path).await {
            Ok(writer) => return Some(writer),
            Err(err) => err,
        };

        if cfg!(any(test, feature = "test")) {
            panic!(
                "Failed to create puffin writer, region_id: {}, file_id: {}, err: {}",
                self.metadata.region_id, self.file_id, err
            );
        } else {
            warn!(
                err; "Failed to create puffin writer, region_id: {}, file_id: {}",
                self.metadata.region_id, self.file_id,
            );
        }

        None
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::SemanticType;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use object_store::services::Memory;
    use object_store::ObjectStore;
    use puffin_manager::PuffinManagerFactory;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};

    use super::*;
    use crate::config::Mode;

    fn mock_region_metadata() -> RegionMetadataRef {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 2));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("a", ConcreteDataType::int64_datatype(), false),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("b", ConcreteDataType::float64_datatype(), false),
                semantic_type: SemanticType::Field,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "c",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 3,
            })
            .primary_key(vec![1]);

        Arc::new(builder.build().unwrap())
    }

    fn no_tag_region_metadata() -> RegionMetadataRef {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 2));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("a", ConcreteDataType::int64_datatype(), false),
                semantic_type: SemanticType::Field,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("b", ConcreteDataType::float64_datatype(), false),
                semantic_type: SemanticType::Field,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "c",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 3,
            });

        Arc::new(builder.build().unwrap())
    }

    fn mock_object_store() -> ObjectStore {
        ObjectStore::new(Memory::default()).unwrap().finish()
    }

    fn mock_intm_mgr() -> IntermediateManager {
        IntermediateManager::new(mock_object_store())
    }

    #[tokio::test]
    async fn test_build_indexer_basic() {
        let (_d, factory) =
            PuffinManagerFactory::new_for_test_async("test_build_indexer_basic_").await;
        let store = mock_object_store();
        let puffin_manager = factory.build(store);
        let metadata = mock_region_metadata();
        let indexer = IndexerBuilder {
            op_type: OperationType::Flush,
            file_id: FileId::random(),
            file_path: "test".to_string(),
            metadata: &metadata,
            row_group_size: 1024,
            puffin_manager,
            intermediate_manager: mock_intm_mgr(),
            index_options: IndexOptions::default(),
            inverted_index_config: InvertedIndexConfig::default(),
        }
        .build()
        .await;

        assert!(indexer.inverted_indexer.is_some());
    }

    #[tokio::test]
    async fn test_build_indexer_disable_create() {
        let (_d, factory) =
            PuffinManagerFactory::new_for_test_async("test_build_indexer_disable_create_").await;
        let store = mock_object_store();
        let puffin_manager = factory.build(store);
        let metadata = mock_region_metadata();
        let indexer = IndexerBuilder {
            op_type: OperationType::Flush,
            file_id: FileId::random(),
            file_path: "test".to_string(),
            metadata: &metadata,
            row_group_size: 1024,
            puffin_manager,
            intermediate_manager: mock_intm_mgr(),
            index_options: IndexOptions::default(),
            inverted_index_config: InvertedIndexConfig {
                create_on_flush: Mode::Disable,
                ..Default::default()
            },
        }
        .build()
        .await;

        assert!(indexer.inverted_indexer.is_none());
    }

    #[tokio::test]
    async fn test_build_indexer_no_tag() {
        let (_d, factory) =
            PuffinManagerFactory::new_for_test_async("test_build_indexer_no_tag_").await;
        let store = mock_object_store();
        let puffin_manager = factory.build(store);
        let metadata = no_tag_region_metadata();
        let indexer = IndexerBuilder {
            op_type: OperationType::Flush,
            file_id: FileId::random(),
            file_path: "test".to_string(),
            metadata: &metadata,
            row_group_size: 1024,
            puffin_manager,
            intermediate_manager: mock_intm_mgr(),
            index_options: IndexOptions::default(),
            inverted_index_config: InvertedIndexConfig::default(),
        }
        .build()
        .await;

        assert!(indexer.inverted_indexer.is_none());
    }

    #[tokio::test]
    async fn test_build_indexer_zero_row_group() {
        let (_d, factory) =
            PuffinManagerFactory::new_for_test_async("test_build_indexer_zero_row_group_").await;
        let store = mock_object_store();
        let puffin_manager = factory.build(store);
        let metadata = mock_region_metadata();
        let indexer = IndexerBuilder {
            op_type: OperationType::Flush,
            file_id: FileId::random(),
            file_path: "test".to_string(),
            metadata: &metadata,
            row_group_size: 0,
            puffin_manager,
            intermediate_manager: mock_intm_mgr(),
            index_options: IndexOptions::default(),
            inverted_index_config: InvertedIndexConfig::default(),
        }
        .build()
        .await;

        assert!(indexer.inverted_indexer.is_none());
    }
}
