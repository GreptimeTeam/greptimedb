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

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::{Arc, RwLock};

use api::v1::OpType;
use common_recordbatch::filter::SimpleFilterEvaluator;
use common_time::Timestamp;
use datafusion_common::ScalarValue;
use snafu::ensure;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;
use table::predicate::Predicate;

use crate::error::{PrimaryKeyLengthMismatchSnafu, Result};
use crate::flush::WriteBufferManagerRef;
use crate::memtable::key_values::KeyValue;
use crate::memtable::merge_tree::metrics::WriteMetrics;
use crate::memtable::merge_tree::partition::{
    Partition, PartitionKey, PartitionReader, PartitionRef, ReadPartitionContext,
};
use crate::memtable::merge_tree::MergeTreeConfig;
use crate::memtable::{BoxedBatchIterator, KeyValues};
use crate::read::Batch;
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
    /// Manager to report size of the tree.
    write_buffer_manager: Option<WriteBufferManagerRef>,
}

impl MergeTree {
    /// Creates a new merge tree.
    pub fn new(
        metadata: RegionMetadataRef,
        config: &MergeTreeConfig,
        write_buffer_manager: Option<WriteBufferManagerRef>,
    ) -> MergeTree {
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
            write_buffer_manager,
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
                self.write_no_key(kv);
                continue;
            }

            // Encode primary key.
            pk_buffer.clear();
            self.row_codec.encode_to_vec(kv.primary_keys(), pk_buffer)?;

            // Write rows with primary keys.
            self.write_with_key(pk_buffer, kv, metrics)?;
        }

        metrics.value_bytes +=
            kvs.num_rows() * (std::mem::size_of::<Timestamp>() + std::mem::size_of::<OpType>());

        Ok(())
    }

    /// Scans the tree.
    pub fn read(
        &self,
        projection: Option<&[ColumnId]>,
        predicate: Option<Predicate>,
    ) -> Result<BoxedBatchIterator> {
        // Creates the projection set.
        let projection: HashSet<_> = if let Some(projection) = projection {
            projection.iter().copied().collect()
        } else {
            self.metadata.field_columns().map(|c| c.column_id).collect()
        };

        let filters = predicate
            .map(|p| {
                p.exprs()
                    .iter()
                    .filter_map(|f| SimpleFilterEvaluator::try_new(f.df_expr()))
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let partitions = self.prune_partitions(&filters);

        let mut iter = TreeIter {
            partitions,
            current_reader: None,
        };
        let context = ReadPartitionContext::new(
            self.metadata.clone(),
            self.row_codec.clone(),
            projection,
            filters,
        );
        iter.fetch_next_partition(context)?;

        Ok(Box::new(iter))
    }

    /// Returns true if the tree is empty.
    ///
    /// A tree is empty if no partition has data.
    pub fn is_empty(&self) -> bool {
        let partitions = self.partitions.read().unwrap();
        partitions.values().all(|part| !part.has_data())
    }

    /// Marks the tree as immutable.
    ///
    /// Once the tree becomes immutable, callers should not write to it again.
    pub fn freeze(&self) -> Result<()> {
        let partitions = self.partitions.read().unwrap();
        for partition in partitions.values() {
            partition.freeze()?;
        }
        Ok(())
    }

    /// Forks an immutable tree. Returns a mutable tree that inherits the index
    /// of this tree.
    pub fn fork(&self, metadata: RegionMetadataRef) -> MergeTree {
        if self.metadata.schema_version != metadata.schema_version
            || self.metadata.column_metadatas != metadata.column_metadatas
        {
            // The schema has changed, we can't reuse the tree.
            return MergeTree::new(metadata, &self.config, self.write_buffer_manager.clone());
        }

        let mut forked = BTreeMap::new();
        let partitions = self.partitions.read().unwrap();
        for (part_key, part) in partitions.iter() {
            if !part.has_data() {
                continue;
            }

            // Only fork partitions that have data.
            let forked_part = part.fork(&metadata, &self.config, self.write_buffer_manager.clone());
            forked.insert(*part_key, Arc::new(forked_part));
        }

        MergeTree {
            config: self.config.clone(),
            metadata,
            row_codec: self.row_codec.clone(),
            partitions: RwLock::new(forked),
            is_partitioned: self.is_partitioned,
            write_buffer_manager: self.write_buffer_manager.clone(),
        }
    }

    /// Returns the write buffer manager.
    pub(crate) fn write_buffer_manager(&self) -> Option<WriteBufferManagerRef> {
        self.write_buffer_manager.clone()
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

    fn write_no_key(&self, key_value: KeyValue) {
        let partition_key = Partition::get_partition_key(&key_value, self.is_partitioned);
        let partition = self.get_or_create_partition(partition_key);

        partition.write_no_key(key_value)
    }

    fn get_or_create_partition(&self, partition_key: PartitionKey) -> PartitionRef {
        let mut partitions = self.partitions.write().unwrap();
        partitions
            .entry(partition_key)
            .or_insert_with(|| {
                Arc::new(Partition::new(
                    self.metadata.clone(),
                    &self.config,
                    self.write_buffer_manager.clone(),
                ))
            })
            .clone()
    }

    fn prune_partitions(&self, filters: &[SimpleFilterEvaluator]) -> VecDeque<PartitionRef> {
        let partitions = self.partitions.read().unwrap();
        if !self.is_partitioned {
            return partitions.values().cloned().collect();
        }

        let mut pruned = VecDeque::new();
        // Prune partition keys.
        for (key, partition) in partitions.iter() {
            let mut is_needed = true;
            for filter in filters {
                if !Partition::is_partition_column(filter.column_name()) {
                    continue;
                }

                if !filter
                    .evaluate_scalar(&ScalarValue::UInt32(Some(*key)))
                    .unwrap_or(true)
                {
                    is_needed = false;
                }
            }

            if is_needed {
                pruned.push_back(partition.clone());
            }
        }

        pruned
    }
}

struct TreeIter {
    partitions: VecDeque<PartitionRef>,
    current_reader: Option<PartitionReader>,
}

impl Iterator for TreeIter {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.next_batch().transpose()
    }
}

impl TreeIter {
    /// Fetch next partition.
    fn fetch_next_partition(&mut self, mut context: ReadPartitionContext) -> Result<()> {
        while let Some(partition) = self.partitions.pop_front() {
            let part_reader = partition.read(context)?;
            if !part_reader.is_valid() {
                context = part_reader.into_context();
                continue;
            }
            self.current_reader = Some(part_reader);
            break;
        }

        Ok(())
    }

    /// Fetches next batch.
    fn next_batch(&mut self) -> Result<Option<Batch>> {
        let Some(part_reader) = &mut self.current_reader else {
            return Ok(None);
        };

        debug_assert!(part_reader.is_valid());
        let batch = part_reader.convert_current_batch()?;
        part_reader.next()?;
        if part_reader.is_valid() {
            return Ok(Some(batch));
        }

        // Safety: current reader is Some.
        let part_reader = self.current_reader.take().unwrap();
        let context = part_reader.into_context();
        self.fetch_next_partition(context)?;

        Ok(Some(batch))
    }
}
