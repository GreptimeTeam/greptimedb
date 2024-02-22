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

//! Memtable implementation based on a merge tree.

mod data;
mod dict;
mod metrics;
mod partition;
mod shard;
mod shard_builder;
mod tree;

use std::fmt;
use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};
use std::sync::Arc;

use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;
use table::predicate::Predicate;

use crate::error::Result;
use crate::flush::WriteBufferManagerRef;
use crate::memtable::merge_tree::metrics::WriteMetrics;
use crate::memtable::merge_tree::tree::MergeTree;
use crate::memtable::{
    AllocTracker, BoxedBatchIterator, KeyValues, Memtable, MemtableBuilder, MemtableId,
    MemtableRef, MemtableStats,
};

/// Id of a shard, only unique inside a partition.
type ShardId = u32;
/// Index of a primary key in a shard.
type PkIndex = u16;
/// Id of a primary key inside a tree.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
struct PkId {
    shard_id: ShardId,
    pk_index: PkIndex,
}

/// Config for the merge tree memtable.
#[derive(Debug, Clone)]
pub struct MergeTreeConfig {
    /// Max keys in an index shard.
    pub index_max_keys_per_shard: usize,
    /// Number of rows to freeze a data part.
    pub data_freeze_threshold: usize,
}

impl Default for MergeTreeConfig {
    fn default() -> Self {
        Self {
            index_max_keys_per_shard: 8192,
            data_freeze_threshold: 102400,
        }
    }
}

/// Memtable based on a merge tree.
pub struct MergeTreeMemtable {
    id: MemtableId,
    tree: MergeTree,
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
        self.id
    }

    fn write(&self, kvs: &KeyValues) -> Result<()> {
        // TODO(yingwen): Validate schema while inserting rows.

        let mut metrics = WriteMetrics::default();
        let mut pk_buffer = Vec::new();
        let res = self.tree.write(kvs, &mut pk_buffer, &mut metrics);

        self.update_stats(&metrics);

        res
    }

    fn iter(
        &self,
        _projection: Option<&[ColumnId]>,
        _predicate: Option<Predicate>,
    ) -> BoxedBatchIterator {
        // FIXME(yingwen): Change return value to `Result<BoxedBatchIterator>`.
        todo!()
    }

    fn is_empty(&self) -> bool {
        self.tree.is_empty()
    }

    fn mark_immutable(&self) {
        self.alloc_tracker.done_allocating();
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
        Self::with_tree(id, MergeTree::new(metadata, config), write_buffer_manager)
    }

    /// Creates a mutable memtable from the tree.
    ///
    /// It also adds the bytes used by shared parts (e.g. index) to the memory usage.
    fn with_tree(
        id: MemtableId,
        tree: MergeTree,
        write_buffer_manager: Option<WriteBufferManagerRef>,
    ) -> Self {
        let alloc_tracker = AllocTracker::new(write_buffer_manager);
        // Track space allocated by the tree.
        let allocated = tree.shared_memory_size();
        // Here we still add the bytes of shared parts to the tracker as the old memtable
        // will release its tracker soon.
        alloc_tracker.on_allocation(allocated);

        Self {
            id,
            tree,
            alloc_tracker,
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

#[cfg(test)]
mod tests {
    use common_time::Timestamp;

    use super::*;
    use crate::test_util::memtable_util;

    #[test]
    fn test_memtable_sorted_input() {
        write_sorted_input(true);
        write_sorted_input(false);
    }

    fn write_sorted_input(has_pk: bool) {
        let metadata = if has_pk {
            memtable_util::metadata_with_primary_key(vec![1, 0], true)
        } else {
            memtable_util::metadata_with_primary_key(vec![], false)
        };
        let timestamps = (0..100).collect::<Vec<_>>();
        let kvs =
            memtable_util::build_key_values(&metadata, "hello".to_string(), 42, &timestamps, 1);
        let memtable = MergeTreeMemtable::new(1, metadata, None, &MergeTreeConfig::default());
        memtable.write(&kvs).unwrap();

        // TODO(yingwen): Test iter.

        let stats = memtable.stats();
        assert!(stats.bytes_allocated() > 0);
        assert_eq!(
            Some((
                Timestamp::new_millisecond(0),
                Timestamp::new_millisecond(99)
            )),
            stats.time_range()
        );
    }

    #[test]
    fn test_memtable_unsorted_input() {
        write_iter_unsorted_input(true);
        write_iter_unsorted_input(false);
    }

    fn write_iter_unsorted_input(has_pk: bool) {
        let metadata = if has_pk {
            memtable_util::metadata_with_primary_key(vec![1, 0], true)
        } else {
            memtable_util::metadata_with_primary_key(vec![], false)
        };
        let memtable =
            MergeTreeMemtable::new(1, metadata.clone(), None, &MergeTreeConfig::default());

        let kvs = memtable_util::build_key_values(
            &metadata,
            "hello".to_string(),
            0,
            &[1, 3, 7, 5, 6],
            0, // sequence 0, 1, 2, 3, 4
        );
        memtable.write(&kvs).unwrap();

        let kvs = memtable_util::build_key_values(
            &metadata,
            "hello".to_string(),
            0,
            &[5, 2, 4, 0, 7],
            5, // sequence 5, 6, 7, 8, 9
        );
        memtable.write(&kvs).unwrap();

        // TODO(yingwen): Test iter.

        let stats = memtable.stats();
        assert!(stats.bytes_allocated() > 0);
        assert_eq!(
            Some((Timestamp::new_millisecond(0), Timestamp::new_millisecond(7))),
            stats.time_range()
        );
    }
}
