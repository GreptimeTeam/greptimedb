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

mod data;
mod index;
// TODO(yingwen): Remove this mod.
mod mutable;
mod tree;

use std::fmt;
use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};
use std::sync::Arc;

use common_base::readable_size::ReadableSize;
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

/// Id of a shard.
pub(crate) type ShardId = u32;
/// Index of a primary key in a shard.
pub(crate) type PkIndex = u16;
/// Id of a primary key.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) struct PkId {
    pub(crate) shard_id: ShardId,
    pub(crate) pk_index: PkIndex,
}

/// Config for the merge tree memtable.
#[derive(Debug, Clone)]
pub struct MergeTreeConfig {
    /// Max keys in an index shard.
    index_max_keys_per_shard: usize,
    /// Freeze threshold of data parts.
    freeze_threshold: usize,
}

impl Default for MergeTreeConfig {
    fn default() -> Self {
        Self {
            // TODO(yingwen): Use 4096 or find a proper value.
            index_max_keys_per_shard: 8192,
            freeze_threshold: 4096,
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
        self.id
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
        projection: Option<&[ColumnId]>,
        predicate: Option<Predicate>,
    ) -> Result<BoxedBatchIterator> {
        self.tree.scan(projection, predicate)
    }

    fn is_empty(&self) -> bool {
        self.tree.is_empty()
    }

    fn freeze(&self) -> Result<()> {
        self.alloc_tracker.done_allocating();

        self.tree.freeze()?;

        Ok(())
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

    fn fork(&self, id: MemtableId, metadata: &RegionMetadataRef) -> MemtableRef {
        let tree = self.tree.fork(metadata.clone());

        Arc::new(MergeTreeMemtable::with_tree(
            id,
            tree,
            self.alloc_tracker.write_buffer_manager(),
        ))
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
            tree: Arc::new(tree),
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
    use std::collections::BTreeSet;

    use common_time::Timestamp;
    use datatypes::scalars::ScalarVector;
    use datatypes::vectors::{Int64Vector, TimestampMillisecondVector};

    use super::*;
    use crate::test_util::memtable_util;

    #[test]
    fn test_memtable_sorted_input() {
        write_iter_sorted_input(true);
        write_iter_sorted_input(false);
    }

    fn write_iter_sorted_input(has_pk: bool) {
        let metadata = if has_pk {
            memtable_util::metadata_for_test()
        } else {
            memtable_util::metadata_with_primary_key(vec![])
        };
        let kvs = memtable_util::build_key_values(&metadata, "hello".to_string(), 42, 100);
        let memtable = MergeTreeMemtable::new(1, metadata, None, &MergeTreeConfig::default());
        memtable.write(&kvs).unwrap();

        let expected_ts = kvs
            .iter()
            .map(|kv| kv.timestamp().as_timestamp().unwrap().unwrap().value())
            .collect::<BTreeSet<_>>();

        let iter = memtable.iter(None, None).unwrap();
        let read = iter
            .flat_map(|batch| {
                batch
                    .unwrap()
                    .timestamps()
                    .as_any()
                    .downcast_ref::<TimestampMillisecondVector>()
                    .unwrap()
                    .iter_data()
                    .collect::<Vec<_>>()
                    .into_iter()
            })
            .map(|v| v.unwrap().0.value())
            .collect::<BTreeSet<_>>();
        assert_eq!(expected_ts, read);

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
            memtable_util::metadata_for_test()
        } else {
            memtable_util::metadata_with_primary_key(vec![])
        };
        let memtable =
            MergeTreeMemtable::new(1, metadata.clone(), None, &MergeTreeConfig::default());

        let kvs = memtable_util::build_key_values_with_ts_seq(
            &metadata,
            "hello".to_string(),
            0,
            [1, 3, 7, 5, 6].into_iter(),
            0, // sequence 0, 1, 2, 3, 4
        );
        memtable.write(&kvs).unwrap();

        let kvs = memtable_util::build_key_values_with_ts_seq(
            &metadata,
            "hello".to_string(),
            0,
            [5, 2, 4, 0, 7].into_iter(),
            5, // sequence 5, 6, 7, 8, 9
        );
        memtable.write(&kvs).unwrap();

        let iter = memtable.iter(None, None).unwrap();
        let read = iter
            .flat_map(|batch| {
                batch
                    .unwrap()
                    .timestamps()
                    .as_any()
                    .downcast_ref::<TimestampMillisecondVector>()
                    .unwrap()
                    .iter_data()
                    .collect::<Vec<_>>()
                    .into_iter()
            })
            .map(|v| v.unwrap().0.value())
            .collect::<Vec<_>>();
        assert_eq!(vec![0, 1, 2, 3, 4, 5, 6, 7], read);

        let iter = memtable.iter(None, None).unwrap();
        let read = iter
            .flat_map(|batch| {
                batch
                    .unwrap()
                    .sequences()
                    .iter_data()
                    .collect::<Vec<_>>()
                    .into_iter()
            })
            .map(|v| v.unwrap())
            .collect::<Vec<_>>();
        assert_eq!(vec![8, 0, 6, 1, 7, 5, 4, 9], read);

        let stats = memtable.stats();
        assert!(stats.bytes_allocated() > 0);
        assert_eq!(
            Some((Timestamp::new_millisecond(0), Timestamp::new_millisecond(7))),
            stats.time_range()
        );
    }

    #[test]
    fn test_memtable_projection() {
        write_iter_projection(true);
        write_iter_projection(false);
    }

    fn write_iter_projection(has_pk: bool) {
        let metadata = if has_pk {
            memtable_util::metadata_for_test()
        } else {
            memtable_util::metadata_with_primary_key(vec![])
        };
        let memtable = MergeTreeMemtableBuilder::new(None).build(&metadata);

        let kvs = memtable_util::build_key_values(&metadata, "hello".to_string(), 10, 100);
        memtable.write(&kvs).unwrap();
        let iter = memtable.iter(Some(&[3]), None).unwrap();

        let mut v0_all = vec![];
        for res in iter {
            let batch = res.unwrap();
            assert_eq!(1, batch.fields().len());
            let v0 = batch
                .fields()
                .first()
                .unwrap()
                .data
                .as_any()
                .downcast_ref::<Int64Vector>()
                .unwrap();
            v0_all.extend(v0.iter_data().map(|v| v.unwrap()));
        }
        assert_eq!((0..100i64).collect::<Vec<_>>(), v0_all);
    }
}
