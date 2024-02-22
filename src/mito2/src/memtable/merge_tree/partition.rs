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

//! Partition of a merge tree.
//!
//! We only support partitioning the tree by pre-defined internal columns.

use std::collections::HashSet;
use std::sync::{Arc, RwLock};

use common_recordbatch::filter::SimpleFilterEvaluator;
use store_api::metadata::RegionMetadataRef;
use store_api::metric_engine_consts::DATA_SCHEMA_TABLE_ID_COLUMN_NAME;
use store_api::storage::ColumnId;

use crate::error::Result;
use crate::memtable::key_values::KeyValue;
use crate::memtable::merge_tree::data::{DataBuffer, DataParts, DATA_INIT_CAP};
use crate::memtable::merge_tree::metrics::WriteMetrics;
use crate::memtable::merge_tree::shard::Shard;
use crate::memtable::merge_tree::shard_builder::ShardBuilder;
use crate::memtable::merge_tree::{MergeTreeConfig, PkId, ShardId};

/// Key of a partition.
pub type PartitionKey = u32;

/// A tree partition.
pub struct Partition {
    inner: RwLock<Inner>,
}

impl Partition {
    /// Creates a new partition.
    pub fn new(metadata: RegionMetadataRef, config: &MergeTreeConfig) -> Self {
        let shard_builder = ShardBuilder::new(metadata.clone(), config);

        Partition {
            inner: RwLock::new(Inner::new(metadata, shard_builder)),
        }
    }

    /// Writes to the partition with a primary key.
    pub fn write_with_key(
        &self,
        primary_key: &[u8],
        key_value: KeyValue,
        metrics: &mut WriteMetrics,
    ) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        // Now we ensure one key only exists in one shard.
        if let Some(pk_id) = inner.find_key_in_shards(primary_key) {
            // Key already in shards.
            inner.write_to_shard(pk_id, key_value);
            return Ok(());
        }

        if inner.shard_builder.should_freeze() {
            inner.freeze_active_shard()?;
        }

        // Write to the shard builder.
        inner
            .shard_builder
            .write_with_key(primary_key, key_value, metrics);
        inner.num_rows += 1;

        Ok(())
    }

    /// Writes to the partition without a primary key.
    pub fn write_no_key(&self, key_value: KeyValue) {
        let mut inner = self.inner.write().unwrap();
        // If no primary key, always write to the first shard.
        debug_assert!(!inner.shards.is_empty());
        debug_assert_eq!(1, inner.active_shard_id);

        // A dummy pk id.
        let pk_id = PkId {
            shard_id: 0,
            pk_index: 0,
        };
        inner.shards[0].write_key_value(pk_id, key_value);
        inner.num_rows += 1;
    }

    /// Scans data in the partition.
    pub fn scan(
        &self,
        _projection: HashSet<ColumnId>,
        _filters: Vec<SimpleFilterEvaluator>,
    ) -> Result<PartitionReader> {
        unimplemented!()
    }

    /// Freezes the partition.
    pub fn freeze(&self) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        inner.freeze_active_shard()?;
        Ok(())
    }

    /// Forks the partition.
    pub fn fork(&self, metadata: &RegionMetadataRef, config: &MergeTreeConfig) -> Partition {
        let inner = self.inner.read().unwrap();
        // TODO(yingwen): TTL or evict shards.
        let shard_builder = ShardBuilder::new(metadata.clone(), config);
        let shards = inner
            .shards
            .iter()
            .map(|shard| shard.fork(metadata.clone()))
            .collect();

        Partition {
            inner: RwLock::new(Inner {
                metadata: metadata.clone(),
                shard_builder,
                active_shard_id: inner.active_shard_id,
                shards,
                num_rows: 0,
            }),
        }
    }

    /// Returns true if the partition has data.
    pub fn has_data(&self) -> bool {
        let inner = self.inner.read().unwrap();
        inner.num_rows > 0
    }

    /// Returns shared memory size of the partition.
    pub fn shared_memory_size(&self) -> usize {
        let inner = self.inner.read().unwrap();
        inner
            .shards
            .iter()
            .map(|shard| shard.shared_memory_size())
            .sum()
    }

    /// Get partition key from the key value.
    pub(crate) fn get_partition_key(key_value: &KeyValue, is_partitioned: bool) -> PartitionKey {
        if !is_partitioned {
            return PartitionKey::default();
        }

        let Some(value) = key_value.primary_keys().next() else {
            return PartitionKey::default();
        };

        value.as_u32().unwrap().unwrap()
    }

    /// Returns true if the region can be partitioned.
    pub(crate) fn has_multi_partitions(metadata: &RegionMetadataRef) -> bool {
        metadata
            .primary_key_columns()
            .next()
            .map(|meta| meta.column_schema.name == DATA_SCHEMA_TABLE_ID_COLUMN_NAME)
            .unwrap_or(false)
    }

    /// Returns true if this is a partition column.
    pub(crate) fn is_partition_column(name: &str) -> bool {
        name == DATA_SCHEMA_TABLE_ID_COLUMN_NAME
    }
}

/// Reader to scan rows in a partition.
///
/// It can merge rows from multiple shards.
pub struct PartitionReader {}

pub type PartitionRef = Arc<Partition>;

/// Inner struct of the partition.
///
/// A key only exists in one shard.
struct Inner {
    metadata: RegionMetadataRef,
    /// Shard whose dictionary is active.
    shard_builder: ShardBuilder,
    active_shard_id: ShardId,
    /// Shards with frozen dictionary.
    shards: Vec<Shard>,
    num_rows: usize,
}

impl Inner {
    fn new(metadata: RegionMetadataRef, shard_builder: ShardBuilder) -> Self {
        let mut inner = Self {
            metadata,
            shard_builder,
            active_shard_id: 0,
            shards: Vec::new(),
            num_rows: 0,
        };

        if inner.metadata.primary_key.is_empty() {
            let data_parts = DataParts::new(
                DataBuffer::with_capacity(inner.metadata.clone(), DATA_INIT_CAP),
                Vec::new(),
            );
            inner.shards.push(Shard::new(0, None, data_parts));
            inner.active_shard_id = 1;
        }

        inner
    }

    fn find_key_in_shards(&self, primary_key: &[u8]) -> Option<PkId> {
        for shard in &self.shards {
            if let Some(pkid) = shard.find_id_by_key(primary_key) {
                return Some(pkid);
            }
        }

        None
    }

    fn write_to_shard(&mut self, pk_id: PkId, key_value: KeyValue) {
        for shard in &mut self.shards {
            if shard.shard_id == pk_id.shard_id {
                shard.write_key_value(pk_id, key_value);
                self.num_rows += 1;
                return;
            }
        }
    }

    fn freeze_active_shard(&mut self) -> Result<()> {
        if let Some(shard) = self
            .shard_builder
            .finish(self.active_shard_id, self.metadata.clone())?
        {
            self.active_shard_id += 1;
            self.shards.push(shard);
        }
        Ok(())
    }
}
