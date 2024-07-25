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

pub(crate) mod fulltext_index;
mod indexer;
pub(crate) mod intermediate;
pub(crate) mod inverted_index;
pub(crate) mod puffin_manager;
mod statistics;
mod store;

use std::num::NonZeroUsize;

use common_telemetry::{debug, warn};
use puffin_manager::SstPuffinManager;
use statistics::{ByteCount, RowCount};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{ColumnId, RegionId};

use crate::access_layer::OperationType;
use crate::config::{FulltextIndexConfig, InvertedIndexConfig};
use crate::metrics::INDEX_CREATE_MEMORY_USAGE;
use crate::read::Batch;
use crate::region::options::IndexOptions;
use crate::sst::file::FileId;
use crate::sst::index::fulltext_index::creator::FulltextIndexer;
use crate::sst::index::intermediate::IntermediateManager;
use crate::sst::index::inverted_index::creator::InvertedIndexer;

pub(crate) const TYPE_INVERTED_INDEX: &str = "inverted_index";
pub(crate) const TYPE_FULLTEXT_INDEX: &str = "fulltext_index";

/// Output of the index creation.
#[derive(Debug, Clone, Default)]
pub struct IndexOutput {
    /// Size of the file.
    pub file_size: u64,
    /// Inverted index output.
    pub inverted_index: InvertedIndexOutput,
    /// Fulltext index output.
    pub fulltext_index: FulltextIndexOutput,
}

/// Output of the inverted index creation.
#[derive(Debug, Clone, Default)]
pub struct InvertedIndexOutput {
    /// Size of the index.
    pub index_size: ByteCount,
    /// Number of rows in the index.
    pub row_count: RowCount,
    /// Available columns in the index.
    pub columns: Vec<ColumnId>,
}

/// Output of the fulltext index creation.
#[derive(Debug, Clone, Default)]
pub struct FulltextIndexOutput {
    /// Size of the index.
    pub index_size: ByteCount,
    /// Number of rows in the index.
    pub row_count: RowCount,
    /// Available columns in the index.
    pub columns: Vec<ColumnId>,
}

impl InvertedIndexOutput {
    pub fn is_available(&self) -> bool {
        self.index_size > 0
    }
}

impl FulltextIndexOutput {
    pub fn is_available(&self) -> bool {
        self.index_size > 0
    }
}

/// The index creator that hides the error handling details.
#[derive(Default)]
pub struct Indexer {
    file_id: FileId,
    file_path: String,
    region_id: RegionId,

    puffin_manager: Option<SstPuffinManager>,
    inverted_indexer: Option<InvertedIndexer>,
    last_mem_inverted_index: usize,
    fulltext_indexer: Option<FulltextIndexer>,
    last_mem_fulltext_index: usize,
}

impl Indexer {
    /// Updates the index with the given batch.
    pub async fn update(&mut self, batch: &Batch) {
        self.do_update(batch).await;

        self.flush_mem_metrics();
    }

    /// Finalizes the index creation.
    pub async fn finish(&mut self) -> IndexOutput {
        let output = self.do_finish().await;

        self.flush_mem_metrics();
        output
    }

    /// Aborts the index creation.
    pub async fn abort(&mut self) {
        self.do_abort().await;

        self.flush_mem_metrics();
    }

    fn flush_mem_metrics(&mut self) {
        let inverted_mem = self
            .inverted_indexer
            .as_ref()
            .map_or(0, |creator| creator.memory_usage());
        INDEX_CREATE_MEMORY_USAGE
            .with_label_values(&[TYPE_INVERTED_INDEX])
            .add(inverted_mem as i64 - self.last_mem_inverted_index as i64);
        self.last_mem_inverted_index = inverted_mem;

        let fulltext_mem = self
            .fulltext_indexer
            .as_ref()
            .map_or(0, |creator| creator.memory_usage());
        INDEX_CREATE_MEMORY_USAGE
            .with_label_values(&[TYPE_FULLTEXT_INDEX])
            .add(fulltext_mem as i64 - self.last_mem_fulltext_index as i64);
        self.last_mem_fulltext_index = fulltext_mem;
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
    pub(crate) fulltext_index_config: FulltextIndexConfig,
}

impl<'a> IndexerBuilder<'a> {
    /// Sanity check for arguments and create a new [Indexer] if arguments are valid.
    pub(crate) async fn build(self) -> Indexer {
        let mut indexer = Indexer {
            file_id: self.file_id,
            file_path: self.file_path.clone(),
            region_id: self.metadata.region_id,

            ..Default::default()
        };

        indexer.inverted_indexer = self.build_inverted_indexer();
        indexer.fulltext_indexer = self.build_fulltext_indexer().await;
        if indexer.inverted_indexer.is_none() && indexer.fulltext_indexer.is_none() {
            indexer.abort().await;
            return Indexer::default();
        }

        indexer.puffin_manager = Some(self.puffin_manager);
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

        let indexer = InvertedIndexer::new(
            self.file_id,
            self.metadata,
            self.intermediate_manager.clone(),
            self.inverted_index_config.mem_threshold_on_create(),
            segment_row_count,
            &self.index_options.inverted_index.ignore_column_ids,
        );

        Some(indexer)
    }

    async fn build_fulltext_indexer(&self) -> Option<FulltextIndexer> {
        let create = match self.op_type {
            OperationType::Flush => self.fulltext_index_config.create_on_flush.auto(),
            OperationType::Compact => self.fulltext_index_config.create_on_compaction.auto(),
        };

        if !create {
            debug!(
                "Skip creating full-text index due to config, region_id: {}, file_id: {}",
                self.metadata.region_id, self.file_id,
            );
            return None;
        }

        let mem_limit = self.fulltext_index_config.mem_threshold_on_create();
        let creator = FulltextIndexer::new(
            &self.metadata.region_id,
            &self.file_id,
            &self.intermediate_manager,
            self.metadata,
            self.fulltext_index_config.compress,
            mem_limit,
        )
        .await;

        let err = match creator {
            Ok(creator) => {
                if creator.is_none() {
                    debug!(
                        "Skip creating full-text index due to no columns require indexing, region_id: {}, file_id: {}",
                        self.metadata.region_id, self.file_id,
                    );
                }
                return creator;
            }
            Err(err) => err,
        };

        if cfg!(any(test, feature = "test")) {
            panic!(
                "Failed to create full-text indexer, region_id: {}, file_id: {}, err: {}",
                self.metadata.region_id, self.file_id, err
            );
        } else {
            warn!(
                err; "Failed to create full-text indexer, region_id: {}, file_id: {}",
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
    use datatypes::schema::{ColumnSchema, FulltextOptions};
    use object_store::services::Memory;
    use object_store::ObjectStore;
    use puffin_manager::PuffinManagerFactory;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};

    use super::*;
    use crate::config::{FulltextIndexConfig, Mode};

    struct MetaConfig {
        with_tag: bool,
        with_fulltext: bool,
    }

    fn mock_region_metadata(
        MetaConfig {
            with_tag,
            with_fulltext,
        }: MetaConfig,
    ) -> RegionMetadataRef {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 2));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("a", ConcreteDataType::int64_datatype(), false),
                semantic_type: if with_tag {
                    SemanticType::Tag
                } else {
                    SemanticType::Field
                },
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

        if with_tag {
            builder.primary_key(vec![1]);
        }

        if with_fulltext {
            let column_schema =
                ColumnSchema::new("text", ConcreteDataType::string_datatype(), true)
                    .with_fulltext_options(FulltextOptions {
                        enable: true,
                        ..Default::default()
                    })
                    .unwrap();

            let column = ColumnMetadata {
                column_schema,
                semantic_type: SemanticType::Field,
                column_id: 4,
            };

            builder.push_column_metadata(column);
        }

        Arc::new(builder.build().unwrap())
    }

    fn mock_object_store() -> ObjectStore {
        ObjectStore::new(Memory::default()).unwrap().finish()
    }

    async fn mock_intm_mgr(path: impl AsRef<str>) -> IntermediateManager {
        IntermediateManager::init_fs(path).await.unwrap()
    }

    #[tokio::test]
    async fn test_build_indexer_basic() {
        let (dir, factory) =
            PuffinManagerFactory::new_for_test_async("test_build_indexer_basic_").await;
        let intm_manager = mock_intm_mgr(dir.path().to_string_lossy()).await;

        let metadata = mock_region_metadata(MetaConfig {
            with_tag: true,
            with_fulltext: true,
        });
        let indexer = IndexerBuilder {
            op_type: OperationType::Flush,
            file_id: FileId::random(),
            file_path: "test".to_string(),
            metadata: &metadata,
            row_group_size: 1024,
            puffin_manager: factory.build(mock_object_store()),
            intermediate_manager: intm_manager,
            index_options: IndexOptions::default(),
            inverted_index_config: InvertedIndexConfig::default(),
            fulltext_index_config: FulltextIndexConfig::default(),
        }
        .build()
        .await;

        assert!(indexer.inverted_indexer.is_some());
        assert!(indexer.fulltext_indexer.is_some());
    }

    #[tokio::test]
    async fn test_build_indexer_disable_create() {
        let (dir, factory) =
            PuffinManagerFactory::new_for_test_async("test_build_indexer_disable_create_").await;
        let intm_manager = mock_intm_mgr(dir.path().to_string_lossy()).await;

        let metadata = mock_region_metadata(MetaConfig {
            with_tag: true,
            with_fulltext: true,
        });
        let indexer = IndexerBuilder {
            op_type: OperationType::Flush,
            file_id: FileId::random(),
            file_path: "test".to_string(),
            metadata: &metadata,
            row_group_size: 1024,
            puffin_manager: factory.build(mock_object_store()),
            intermediate_manager: intm_manager.clone(),
            index_options: IndexOptions::default(),
            inverted_index_config: InvertedIndexConfig {
                create_on_flush: Mode::Disable,
                ..Default::default()
            },
            fulltext_index_config: FulltextIndexConfig::default(),
        }
        .build()
        .await;

        assert!(indexer.inverted_indexer.is_none());
        assert!(indexer.fulltext_indexer.is_some());

        let indexer = IndexerBuilder {
            op_type: OperationType::Compact,
            file_id: FileId::random(),
            file_path: "test".to_string(),
            metadata: &metadata,
            row_group_size: 1024,
            puffin_manager: factory.build(mock_object_store()),
            intermediate_manager: intm_manager,
            index_options: IndexOptions::default(),
            inverted_index_config: InvertedIndexConfig::default(),
            fulltext_index_config: FulltextIndexConfig {
                create_on_compaction: Mode::Disable,
                ..Default::default()
            },
        }
        .build()
        .await;

        assert!(indexer.inverted_indexer.is_some());
        assert!(indexer.fulltext_indexer.is_none());
    }

    #[tokio::test]
    async fn test_build_indexer_no_required() {
        let (dir, factory) =
            PuffinManagerFactory::new_for_test_async("test_build_indexer_no_required_").await;
        let intm_manager = mock_intm_mgr(dir.path().to_string_lossy()).await;

        let metadata = mock_region_metadata(MetaConfig {
            with_tag: false,
            with_fulltext: true,
        });
        let indexer = IndexerBuilder {
            op_type: OperationType::Flush,
            file_id: FileId::random(),
            file_path: "test".to_string(),
            metadata: &metadata,
            row_group_size: 1024,
            puffin_manager: factory.build(mock_object_store()),
            intermediate_manager: intm_manager.clone(),
            index_options: IndexOptions::default(),
            inverted_index_config: InvertedIndexConfig::default(),
            fulltext_index_config: FulltextIndexConfig::default(),
        }
        .build()
        .await;

        assert!(indexer.inverted_indexer.is_none());
        assert!(indexer.fulltext_indexer.is_some());

        let metadata = mock_region_metadata(MetaConfig {
            with_tag: true,
            with_fulltext: false,
        });
        let indexer = IndexerBuilder {
            op_type: OperationType::Flush,
            file_id: FileId::random(),
            file_path: "test".to_string(),
            metadata: &metadata,
            row_group_size: 1024,
            puffin_manager: factory.build(mock_object_store()),
            intermediate_manager: intm_manager,
            index_options: IndexOptions::default(),
            inverted_index_config: InvertedIndexConfig::default(),
            fulltext_index_config: FulltextIndexConfig::default(),
        }
        .build()
        .await;

        assert!(indexer.inverted_indexer.is_some());
        assert!(indexer.fulltext_indexer.is_none());
    }

    #[tokio::test]
    async fn test_build_indexer_zero_row_group() {
        let (dir, factory) =
            PuffinManagerFactory::new_for_test_async("test_build_indexer_zero_row_group_").await;
        let intm_manager = mock_intm_mgr(dir.path().to_string_lossy()).await;

        let metadata = mock_region_metadata(MetaConfig {
            with_tag: true,
            with_fulltext: true,
        });
        let indexer = IndexerBuilder {
            op_type: OperationType::Flush,
            file_id: FileId::random(),
            file_path: "test".to_string(),
            metadata: &metadata,
            row_group_size: 0,
            puffin_manager: factory.build(mock_object_store()),
            intermediate_manager: intm_manager,
            index_options: IndexOptions::default(),
            inverted_index_config: InvertedIndexConfig::default(),
            fulltext_index_config: FulltextIndexConfig::default(),
        }
        .build()
        .await;

        assert!(indexer.inverted_indexer.is_none());
    }
}
