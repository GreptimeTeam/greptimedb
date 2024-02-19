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

//! Implementation of the merge tree.

use std::sync::Arc;

use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;
use table::predicate::Predicate;

use crate::error::Result;
use crate::memtable::merge_tree::metrics::WriteMetrics;
use crate::memtable::merge_tree::MergeTreeConfig;
use crate::memtable::{BoxedBatchIterator, KeyValues};
use crate::row_converter::{McmpRowCodec, SortField};

/// The merge tree.
pub struct MergeTree {
    /// Config of the tree.
    config: MergeTreeConfig,
    /// Metadata of the region.
    pub(crate) metadata: RegionMetadataRef,
    /// Primary key codec.
    row_codec: Arc<McmpRowCodec>,
}

impl MergeTree {
    /// Creates a new merge tree.
    pub fn new(metadata: RegionMetadataRef, config: &MergeTreeConfig) -> MergeTree {
        let row_codec = McmpRowCodec::new(
            metadata
                .primary_key_columns()
                .map(|c| SortField::new(c.column_schema.data_type.clone()))
                .collect(),
        );

        MergeTree {
            config: config.clone(),
            metadata,
            row_codec: Arc::new(row_codec),
        }
    }

    // TODO(yingwen): The size computed from values is inaccurate.
    /// Write key-values into the tree.
    ///
    /// # Panics
    /// Panics if the tree is immutable (frozen).
    pub fn write(&self, _kvs: &KeyValues, _metrics: &mut WriteMetrics) -> Result<()> {
        todo!()
    }

    /// Scans the tree.
    pub fn scan(
        &self,
        _projection: Option<&[ColumnId]>,
        _predicate: Option<Predicate>,
    ) -> Result<BoxedBatchIterator> {
        todo!()
    }

    /// Returns true if the tree is empty.
    pub fn is_empty(&self) -> bool {
        todo!()
    }

    /// Marks the tree as immutable.
    ///
    /// Once the tree becomes immutable, callers should not write to it again.
    pub fn freeze(&self) -> Result<()> {
        todo!()
    }

    /// Forks an immutable tree. Returns a mutable tree that inherits the index
    /// of this tree.
    pub fn fork(&self, _metadata: RegionMetadataRef) -> MergeTree {
        todo!()
    }

    /// Returns the memory size shared by forked trees.
    pub fn shared_memory_size(&self) -> usize {
        todo!()
    }
}
