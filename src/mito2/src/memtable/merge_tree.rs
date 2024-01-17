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

mod mutable;
mod tree;

use std::fmt;
use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};
use std::sync::Arc;

use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;
use table::predicate::Predicate;

use crate::error::Result;
use crate::flush::WriteBufferManagerRef;
use crate::memtable::merge_tree::mutable::WriteMetrics;
use crate::memtable::merge_tree::tree::{MergeTree, MergeTreeRef};
use crate::memtable::{
    AllocTracker, BoxedBatchIterator, KeyValues, Memtable, MemtableBuilder, MemtableId,
    MemtableRef, MemtableStats,
};

/// Config for the merge tree memtable.
#[derive(Debug, Clone)]
pub struct MergeTreeConfig {
    /// Enable dictionary.
    enable_dict: bool,
    /// Number of keys in a dictionary.
    dict_key_num: usize,
    /// Max number of dictionaries.
    max_dict_num: usize,
}

impl Default for MergeTreeConfig {
    fn default() -> Self {
        Self {
            enable_dict: true,
            dict_key_num: 50_000,
            max_dict_num: 16,
        }
    }
}

/// Memtable based on a merge tree.
pub struct MergeTreeMemtable {
    id: MemtableId,
    tree: MergeTreeRef,
    alloc_tracker: AllocTracker,
    max_timestamp: AtomicI64,
    min_timestamp: AtomicI64,
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

    fn write(&self, kvs: &KeyValues) -> Result<()> {
        // TODO(yingwen): Validate schema while inserting rows.

        let mut metrics = WriteMetrics::default();
        let res = self.tree.write(kvs, &mut metrics);

        self.update_stats(&metrics);

        res
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

impl MergeTreeMemtable {
    /// Returns a new memtable.
    pub fn new(
        id: MemtableId,
        metadata: RegionMetadataRef,
        write_buffer_manager: Option<WriteBufferManagerRef>,
        config: &MergeTreeConfig,
    ) -> Self {
        Self {
            id,
            tree: Arc::new(MergeTree::new(metadata, config)),
            alloc_tracker: AllocTracker::new(write_buffer_manager),
            max_timestamp: AtomicI64::new(i64::MIN),
            min_timestamp: AtomicI64::new(i64::MAX),
        }
    }

    /// Updates stats of the memtable.
    fn update_stats(&self, metrics: &WriteMetrics) {
        self.alloc_tracker
            .on_allocation(metrics.key_bytes + metrics.value_bytes);

        loop {
            let current_min = self.min_timestamp.load(Ordering::Relaxed);
            if metrics.min_ts >= current_min {
                break;
            }

            let Err(updated) = self.min_timestamp.compare_exchange(
                current_min,
                metrics.min_ts,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) else {
                break;
            };

            if updated == metrics.min_ts {
                break;
            }
        }

        loop {
            let current_max = self.max_timestamp.load(Ordering::Relaxed);
            if metrics.max_ts <= current_max {
                break;
            }

            let Err(updated) = self.max_timestamp.compare_exchange(
                current_max,
                metrics.max_ts,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) else {
                break;
            };

            if updated == metrics.max_ts {
                break;
            }
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
