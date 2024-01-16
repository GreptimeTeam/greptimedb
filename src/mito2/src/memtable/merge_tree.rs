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

//! Memtable implementation based on a merge tree:
//! - Flushes mutable parts to immutable parts
//! - Merges small immutable parts into a big immutable part

mod tree;

use std::fmt;
use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};
use std::sync::Arc;

use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;
use table::predicate::Predicate;

use crate::error::Result;
use crate::flush::WriteBufferManagerRef;
use crate::memtable::merge_tree::tree::{MergeTree, MergeTreePtr};
use crate::memtable::{
    AllocTracker, BoxedBatchIterator, KeyValues, Memtable, MemtableBuilder, MemtableId,
    MemtableRef, MemtableStats,
};

/// Config for the merge tree memtable.
#[derive(Debug, Default, Clone)]
pub struct MergeTreeConfig {}

/// Memtable based on a merge tree.
pub struct MergeTreeMemtable {
    id: MemtableId,
    tree: MergeTreePtr,
    alloc_tracker: AllocTracker,
    max_timestamp: AtomicI64,
    min_timestamp: AtomicI64,
}

impl MergeTreeMemtable {
    /// Returns a new memtable.
    pub fn new(
        id: MemtableId,
        metadata: RegionMetadataRef,
        write_buffer_manager: Option<WriteBufferManagerRef>,
        _config: &MergeTreeConfig,
    ) -> Self {
        Self {
            id,
            tree: Arc::new(MergeTree::new(metadata)),
            alloc_tracker: AllocTracker::new(write_buffer_manager),
            max_timestamp: AtomicI64::new(i64::MIN),
            min_timestamp: AtomicI64::new(i64::MAX),
        }
    }
}

impl fmt::Debug for MergeTreeMemtable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("MergeTreeMemtable")
            .field("id", &self.id)
            .finish()
    }
}

impl Memtable for MergeTreeMemtable {
    fn id(&self) -> MemtableId {
        todo!()
    }

    fn write(&self, _kvs: &KeyValues) -> Result<()> {
        todo!()
    }

    fn iter(
        &self,
        _projection: Option<&[ColumnId]>,
        _predicate: Option<Predicate>,
    ) -> BoxedBatchIterator {
        todo!()
    }

    fn is_empty(&self) -> bool {
        todo!()
    }

    fn mark_immutable(&self) {
        todo!()
    }

    fn stats(&self) -> MemtableStats {
        let estimated_bytes = self.alloc_tracker.bytes_allocated();

        if estimated_bytes == 0 {
            // no rows ever written
            return MemtableStats {
                estimated_bytes,
                time_range: None,
            };
        }

        let ts_type = self
            .tree
            .metadata
            .time_index_column()
            .column_schema
            .data_type
            .clone()
            .as_timestamp()
            .expect("Timestamp column must have timestamp type");
        let max_timestamp = ts_type.create_timestamp(self.max_timestamp.load(Ordering::Relaxed));
        let min_timestamp = ts_type.create_timestamp(self.min_timestamp.load(Ordering::Relaxed));
        MemtableStats {
            estimated_bytes,
            time_range: Some((min_timestamp, max_timestamp)),
        }
    }
}

/// Builder to build a [MergeTreeMemtable].
#[derive(Debug, Default)]
pub struct MergeTreeMemtableBuilder {
    id: AtomicU32,
    write_buffer_manager: Option<WriteBufferManagerRef>,
    config: MergeTreeConfig,
}

impl MergeTreeMemtableBuilder {
    /// Creates a new builder with specific `write_buffer_manager`.
    pub fn new(write_buffer_manager: Option<WriteBufferManagerRef>) -> Self {
        Self {
            id: AtomicU32::new(0),
            write_buffer_manager,
            config: MergeTreeConfig::default(),
        }
    }
}

impl MemtableBuilder for MergeTreeMemtableBuilder {
    fn build(&self, metadata: &RegionMetadataRef) -> MemtableRef {
        let id = self.id.fetch_add(1, Ordering::Relaxed);
        Arc::new(MergeTreeMemtable::new(
            id,
            metadata.clone(),
            self.write_buffer_manager.clone(),
            &self.config,
        ))
    }
}
