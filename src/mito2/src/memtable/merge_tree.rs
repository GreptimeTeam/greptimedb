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

pub(crate) mod data;
mod dedup;
mod dict;
mod merger;
mod metrics;
mod partition;
mod shard;
mod shard_builder;
mod tree;

use std::fmt;
use std::sync::atomic::{AtomicI64, Ordering};
use std::sync::Arc;

use common_base::readable_size::ReadableSize;
use serde::{Deserialize, Serialize};
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

/// Use `1/DICTIONARY_SIZE_FACTOR` of OS memory as dictionary size.
const DICTIONARY_SIZE_FACTOR: u64 = 8;

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
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct MergeTreeConfig {
    /// Max keys in an index shard.
    pub index_max_keys_per_shard: usize,
    /// Number of rows to freeze a data part.
    pub data_freeze_threshold: usize,
    /// Whether to delete duplicates rows.
    pub dedup: bool,
    /// Total bytes of dictionary to keep in fork.
    pub fork_dictionary_bytes: ReadableSize,
}

impl Default for MergeTreeConfig {
    fn default() -> Self {
        let mut fork_dictionary_bytes = ReadableSize::gb(1);
        if let Some(sys_memory) = common_config::utils::get_sys_total_memory() {
            let adjust_dictionary_bytes =
                std::cmp::min(sys_memory / DICTIONARY_SIZE_FACTOR, fork_dictionary_bytes);
            if adjust_dictionary_bytes.0 > 0 {
                fork_dictionary_bytes = adjust_dictionary_bytes;
            }
        }

        Self {
            index_max_keys_per_shard: 8192,
            data_freeze_threshold: 32768,
            dedup: true,
            fork_dictionary_bytes,
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
        // Ensures the memtable always updates stats.
        let res = self.tree.write(kvs, &mut pk_buffer, &mut metrics);

        self.update_stats(&metrics);

        res
    }

    fn iter(
        &self,
        projection: Option<&[ColumnId]>,
        predicate: Option<Predicate>,
    ) -> Result<BoxedBatchIterator> {
        self.tree.read(projection, predicate)
    }

    fn is_empty(&self) -> bool {
        self.tree.is_empty()
    }

    fn freeze(&self) -> Result<()> {
        self.alloc_tracker.done_allocating();

        self.tree.freeze()
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

        let memtable = MergeTreeMemtable::with_tree(id, tree);
        Arc::new(memtable)
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
        Self::with_tree(
            id,
            MergeTree::new(metadata, config, write_buffer_manager.clone()),
        )
    }

    /// Creates a mutable memtable from the tree.
    ///
    /// It also adds the bytes used by shared parts (e.g. index) to the memory usage.
    fn with_tree(id: MemtableId, tree: MergeTree) -> Self {
        let alloc_tracker = AllocTracker::new(tree.write_buffer_manager());

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
        // Only let the tracker tracks value bytes.
        self.alloc_tracker.on_allocation(metrics.value_bytes);

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
    config: MergeTreeConfig,
    write_buffer_manager: Option<WriteBufferManagerRef>,
}

impl MergeTreeMemtableBuilder {
    /// Creates a new builder with specific `write_buffer_manager`.
    pub fn new(
        config: MergeTreeConfig,
        write_buffer_manager: Option<WriteBufferManagerRef>,
    ) -> Self {
        Self {
            config,
            write_buffer_manager,
        }
    }
}

impl MemtableBuilder for MergeTreeMemtableBuilder {
    fn build(&self, id: MemtableId, metadata: &RegionMetadataRef) -> MemtableRef {
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
            memtable_util::metadata_with_primary_key(vec![1, 0], true)
        } else {
            memtable_util::metadata_with_primary_key(vec![], false)
        };
        let timestamps = (0..100).collect::<Vec<_>>();
        let kvs =
            memtable_util::build_key_values(&metadata, "hello".to_string(), 42, &timestamps, 1);
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
            memtable_util::metadata_with_primary_key(vec![1, 0], true)
        } else {
            memtable_util::metadata_with_primary_key(vec![], false)
        };
        // Try to build a memtable via the builder.
        let memtable =
            MergeTreeMemtableBuilder::new(MergeTreeConfig::default(), None).build(1, &metadata);

        let expect = (0..100).collect::<Vec<_>>();
        let kvs = memtable_util::build_key_values(&metadata, "hello".to_string(), 10, &expect, 1);
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
        assert_eq!(expect, v0_all);
    }

    #[test]
    fn test_write_iter_multi_keys() {
        write_iter_multi_keys(1, 100);
        write_iter_multi_keys(2, 100);
        write_iter_multi_keys(4, 100);
        write_iter_multi_keys(8, 5);
        write_iter_multi_keys(2, 10);
    }

    fn write_iter_multi_keys(max_keys: usize, freeze_threshold: usize) {
        let metadata = memtable_util::metadata_with_primary_key(vec![1, 0], true);
        let memtable = MergeTreeMemtable::new(
            1,
            metadata.clone(),
            None,
            &MergeTreeConfig {
                index_max_keys_per_shard: max_keys,
                data_freeze_threshold: freeze_threshold,
                ..Default::default()
            },
        );

        let mut data = Vec::new();
        // 4 partitions, each partition 4 pks.
        for i in 0..4 {
            for j in 0..4 {
                // key: i, a{j}
                let timestamps = [11, 13, 1, 5, 3, 7, 9];
                let key = format!("a{j}");
                let kvs =
                    memtable_util::build_key_values(&metadata, key.clone(), i, &timestamps, 0);
                memtable.write(&kvs).unwrap();
                for ts in timestamps {
                    data.push((i, key.clone(), ts));
                }
            }
            for j in 0..4 {
                // key: i, a{j}
                let timestamps = [10, 2, 4, 8, 6];
                let key = format!("a{j}");
                let kvs =
                    memtable_util::build_key_values(&metadata, key.clone(), i, &timestamps, 200);
                memtable.write(&kvs).unwrap();
                for ts in timestamps {
                    data.push((i, key.clone(), ts));
                }
            }
        }
        data.sort_unstable();

        let expect = data.into_iter().map(|x| x.2).collect::<Vec<_>>();
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
        assert_eq!(expect, read);
    }
}
