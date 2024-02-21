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

use std::collections::BTreeMap;
use std::sync::{Arc, RwLock};

use api::v1::OpType;
use common_time::Timestamp;
use snafu::ensure;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;
use table::predicate::Predicate;

use crate::error::{PrimaryKeyLengthMismatchSnafu, Result};
use crate::memtable::key_values::KeyValue;
use crate::memtable::merge_tree::metrics::WriteMetrics;
use crate::memtable::merge_tree::partition::{Partition, PartitionKey, PartitionRef};
use crate::memtable::merge_tree::MergeTreeConfig;
use crate::memtable::{BoxedBatchIterator, KeyValues};
use crate::row_converter::{McmpRowCodec, RowCodec, SortField};

/// The merge tree.
pub struct MergeTree {
    /// Config of the tree.
    config: MergeTreeConfig,
    /// Metadata of the region.
    pub(crate) metadata: RegionMetadataRef,
    /// Primary key codec.
    row_codec: Arc<McmpRowCodec>,
    /// Partitions in the tree.
    partitions: RwLock<BTreeMap<PartitionKey, PartitionRef>>,
    /// Whether the tree has multiple partitions.
    is_partitioned: bool,
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
        let is_partitioned = Partition::has_multi_partitions(&metadata);

        MergeTree {
            config: config.clone(),
            metadata,
            row_codec: Arc::new(row_codec),
            partitions: Default::default(),
            is_partitioned,
        }
    }

    // TODO(yingwen): The size computed from values is inaccurate.
    /// Write key-values into the tree.
    ///
    /// # Panics
    /// Panics if the tree is immutable (frozen).
    pub fn write(
        &self,
        kvs: &KeyValues,
        pk_buffer: &mut Vec<u8>,
        metrics: &mut WriteMetrics,
    ) -> Result<()> {
        let has_pk = !self.metadata.primary_key.is_empty();

        for kv in kvs.iter() {
            ensure!(
                kv.num_primary_keys() == self.row_codec.num_fields(),
                PrimaryKeyLengthMismatchSnafu {
                    expect: self.row_codec.num_fields(),
                    actual: kv.num_primary_keys(),
                }
            );
            // Safety: timestamp of kv must be both present and a valid timestamp value.
            let ts = kv.timestamp().as_timestamp().unwrap().unwrap().value();
            metrics.min_ts = metrics.min_ts.min(ts);
            metrics.max_ts = metrics.max_ts.max(ts);
            metrics.value_bytes += kv.fields().map(|v| v.data_size()).sum::<usize>();

            if !has_pk {
                // No primary key.
                self.write_no_key(kv, metrics)?;
                continue;
            }

            // Encode primary key.
            pk_buffer.clear();
            self.row_codec.encode_to_vec(kv.primary_keys(), pk_buffer)?;

            // Write rows with primary keys.
            self.write_with_key(&pk_buffer, kv, metrics)?;
        }

        metrics.value_bytes +=
            kvs.num_rows() * (std::mem::size_of::<Timestamp>() + std::mem::size_of::<OpType>());

        Ok(())
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

    fn write_with_key(
        &self,
        primary_key: &[u8],
        key_value: KeyValue,
        metrics: &mut WriteMetrics,
    ) -> Result<()> {
        let partition_key = Partition::get_partition_key(&key_value, self.is_partitioned);
        let partition = self.get_or_create_partition(partition_key);

        partition.write_with_key(primary_key, key_value, metrics)
    }

    fn write_no_key(&self, key_value: KeyValue, metrics: &mut WriteMetrics) -> Result<()> {
        let partition_key = Partition::get_partition_key(&key_value, self.is_partitioned);
        let partition = self.get_or_create_partition(partition_key);

        partition.write_on_key(key_value, metrics)
    }

    fn get_or_create_partition(&self, partition_key: PartitionKey) -> PartitionRef {
        unimplemented!()
    }
}
