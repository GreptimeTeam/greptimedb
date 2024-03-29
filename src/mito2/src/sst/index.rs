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

pub(crate) mod applier;
mod codec;
pub(crate) mod creator;
pub(crate) mod intermediate;
mod store;

use std::num::NonZeroUsize;

use common_telemetry::{debug, warn};
use creator::SstIndexCreator;
use object_store::ObjectStore;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::RegionId;

use crate::metrics::INDEX_CREATE_MEMORY_USAGE;
use crate::read::Batch;
use crate::region::options::IndexOptions;
use crate::sst::file::FileId;
use crate::sst::index::intermediate::IntermediateManager;

const INDEX_BLOB_TYPE: &str = "greptime-inverted-index-v1";

/// The index creator that hides the error handling details.
#[derive(Default)]
pub struct Indexer {
    file_id: FileId,
    region_id: RegionId,
    inner: Option<SstIndexCreator>,
    last_memory_usage: usize,
}

impl Indexer {
    /// Update the index with the given batch.
    pub async fn update(&mut self, batch: &Batch) {
        if let Some(creator) = self.inner.as_mut() {
            if let Err(err) = creator.update(batch).await {
                if cfg!(any(test, feature = "test")) {
                    panic!(
                        "Failed to update index, region_id: {}, file_id: {}, err: {}",
                        self.region_id, self.file_id, err
                    );
                } else {
                    warn!(
                        err; "Failed to update index, skip creating index, region_id: {}, file_id: {}",
                        self.region_id, self.file_id,
                    );
                }

                // Skip index creation if error occurs.
                self.inner = None;
            }
        }

        if let Some(creator) = self.inner.as_ref() {
            let memory_usage = creator.memory_usage();
            INDEX_CREATE_MEMORY_USAGE.add(memory_usage as i64 - self.last_memory_usage as i64);
            self.last_memory_usage = memory_usage;
        } else {
            INDEX_CREATE_MEMORY_USAGE.sub(self.last_memory_usage as i64);
            self.last_memory_usage = 0;
        }
    }

    /// Finish the index creation.
    /// Returns the number of bytes written if success or None if failed.
    pub async fn finish(&mut self) -> Option<usize> {
        if let Some(mut creator) = self.inner.take() {
            match creator.finish().await {
                Ok((row_count, byte_count)) => {
                    debug!(
                        "Create index successfully, region_id: {}, file_id: {}, bytes: {}, rows: {}",
                        self.region_id, self.file_id, byte_count, row_count
                    );

                    INDEX_CREATE_MEMORY_USAGE.sub(self.last_memory_usage as i64);
                    self.last_memory_usage = 0;
                    return Some(byte_count);
                }
                Err(err) => {
                    if cfg!(any(test, feature = "test")) {
                        panic!(
                            "Failed to create index, region_id: {}, file_id: {}, err: {}",
                            self.region_id, self.file_id, err
                        );
                    } else {
                        warn!(
                            err; "Failed to create index, region_id: {}, file_id: {}",
                            self.region_id, self.file_id,
                        );
                    }
                }
            }
        }

        INDEX_CREATE_MEMORY_USAGE.sub(self.last_memory_usage as i64);
        self.last_memory_usage = 0;
        None
    }

    /// Abort the index creation.
    pub async fn abort(&mut self) {
        if let Some(mut creator) = self.inner.take() {
            if let Err(err) = creator.abort().await {
                if cfg!(any(test, feature = "test")) {
                    panic!(
                        "Failed to abort index, region_id: {}, file_id: {}, err: {}",
                        self.region_id, self.file_id, err
                    );
                } else {
                    warn!(
                        err; "Failed to abort index, region_id: {}, file_id: {}",
                        self.region_id, self.file_id,
                    );
                }
            }
        }
        INDEX_CREATE_MEMORY_USAGE.sub(self.last_memory_usage as i64);
        self.last_memory_usage = 0;
    }
}

pub(crate) struct IndexerBuilder<'a> {
    pub(crate) create_inverted_index: bool,
    pub(crate) mem_threshold_index_create: Option<usize>,
    pub(crate) write_buffer_size: Option<usize>,
    pub(crate) file_id: FileId,
    pub(crate) file_path: String,
    pub(crate) metadata: &'a RegionMetadataRef,
    pub(crate) row_group_size: usize,
    pub(crate) object_store: ObjectStore,
    pub(crate) intermediate_manager: IntermediateManager,
    pub(crate) index_options: IndexOptions,
}

impl<'a> IndexerBuilder<'a> {
    /// Sanity check for arguments and create a new [Indexer]
    /// with inner [SstIndexCreator] if arguments are valid.
    pub(crate) fn build(self) -> Indexer {
        if !self.create_inverted_index {
            debug!(
                "Skip creating index due to request, region_id: {}, file_id: {}",
                self.metadata.region_id, self.file_id,
            );
            return Indexer::default();
        }

        if self.metadata.primary_key.is_empty() {
            debug!(
                "No tag columns, skip creating index, region_id: {}, file_id: {}",
                self.metadata.region_id, self.file_id,
            );
            return Indexer::default();
        }

        let Some(mut segment_row_count) =
            NonZeroUsize::new(self.index_options.inverted_index.segment_row_count)
        else {
            warn!(
                "Segment row count is 0, skip creating index, region_id: {}, file_id: {}",
                self.metadata.region_id, self.file_id,
            );
            return Indexer::default();
        };

        let Some(row_group_size) = NonZeroUsize::new(self.row_group_size) else {
            warn!(
                "Row group size is 0, skip creating index, region_id: {}, file_id: {}",
                self.metadata.region_id, self.file_id,
            );
            return Indexer::default();
        };

        // if segment row count not aligned with row group size, adjust it to be aligned.
        if row_group_size.get() % segment_row_count.get() != 0 {
            segment_row_count = row_group_size;
        }

        let creator = SstIndexCreator::new(
            self.file_path,
            self.file_id,
            self.metadata,
            self.object_store,
            self.intermediate_manager,
            self.mem_threshold_index_create,
            segment_row_count,
        )
        .with_buffer_size(self.write_buffer_size)
        .with_ignore_column_ids(
            self.index_options
                .inverted_index
                .ignore_column_ids
                .iter()
                .map(|i| i.to_string())
                .collect(),
        );

        Indexer {
            file_id: self.file_id,
            region_id: self.metadata.region_id,
            inner: Some(creator),
            last_memory_usage: 0,
        }
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::SemanticType;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::schema::ColumnSchema;
    use object_store::services::Memory;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};

    use super::*;

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

    #[test]
    fn test_build_indexer_basic() {
        let metadata = mock_region_metadata();
        let indexer = IndexerBuilder {
            create_inverted_index: true,
            mem_threshold_index_create: Some(1024),
            write_buffer_size: None,
            file_id: FileId::random(),
            file_path: "test".to_string(),
            metadata: &metadata,
            row_group_size: 1024,
            object_store: mock_object_store(),
            intermediate_manager: mock_intm_mgr(),
            index_options: IndexOptions::default(),
        }
        .build();

        assert!(indexer.inner.is_some());
    }

    #[test]
    fn test_build_indexer_disable_create() {
        let metadata = mock_region_metadata();
        let indexer = IndexerBuilder {
            create_inverted_index: false,
            mem_threshold_index_create: Some(1024),
            write_buffer_size: None,
            file_id: FileId::random(),
            file_path: "test".to_string(),
            metadata: &metadata,
            row_group_size: 1024,
            object_store: mock_object_store(),
            intermediate_manager: mock_intm_mgr(),
            index_options: IndexOptions::default(),
        }
        .build();

        assert!(indexer.inner.is_none());
    }

    #[test]
    fn test_build_indexer_no_tag() {
        let metadata = no_tag_region_metadata();
        let indexer = IndexerBuilder {
            create_inverted_index: true,
            mem_threshold_index_create: Some(1024),
            write_buffer_size: None,
            file_id: FileId::random(),
            file_path: "test".to_string(),
            metadata: &metadata,
            row_group_size: 1024,
            object_store: mock_object_store(),
            intermediate_manager: mock_intm_mgr(),
            index_options: IndexOptions::default(),
        }
        .build();

        assert!(indexer.inner.is_none());
    }

    #[test]
    fn test_build_indexer_zero_row_group() {
        let metadata = mock_region_metadata();
        let indexer = IndexerBuilder {
            create_inverted_index: true,
            mem_threshold_index_create: Some(1024),
            write_buffer_size: None,
            file_id: FileId::random(),
            file_path: "test".to_string(),
            metadata: &metadata,
            row_group_size: 0,
            object_store: mock_object_store(),
            intermediate_manager: mock_intm_mgr(),
            index_options: IndexOptions::default(),
        }
        .build();

        assert!(indexer.inner.is_none());
    }
}
