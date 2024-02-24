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

use api::v1::SemanticType;
use common_recordbatch::filter::SimpleFilterEvaluator;
use store_api::metadata::RegionMetadataRef;
use store_api::metric_engine_consts::DATA_SCHEMA_TABLE_ID_COLUMN_NAME;
use store_api::storage::ColumnId;

use crate::error::Result;
use crate::memtable::key_values::KeyValue;
use crate::memtable::merge_tree::data::{DataBatch, DataParts, DATA_INIT_CAP};
use crate::memtable::merge_tree::metrics::WriteMetrics;
use crate::memtable::merge_tree::shard::Shard;
use crate::memtable::merge_tree::shard_builder::ShardBuilder;
use crate::memtable::merge_tree::{MergeTreeConfig, PkId};
use crate::read::{Batch, BatchBuilder};
use crate::row_converter::{McmpRowCodec, RowCodec};

/// Key of a partition.
pub type PartitionKey = u32;

/// A tree partition.
pub struct Partition {
    inner: RwLock<Inner>,
}

impl Partition {
    /// Creates a new partition.
    pub fn new(metadata: RegionMetadataRef, config: &MergeTreeConfig) -> Self {
        Partition {
            inner: RwLock::new(Inner::new(metadata, config)),
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
        debug_assert_eq!(1, inner.shard_builder.current_shard_id());

        // A dummy pk id.
        let pk_id = PkId {
            shard_id: 0,
            pk_index: 0,
        };
        inner.shards[0].write_with_pk_id(pk_id, key_value);
        inner.num_rows += 1;
    }

    /// Scans data in the partition.
    pub fn read(&self, _context: ReadPartitionContext) -> Result<PartitionReader> {
        unimplemented!()
    }

    /// Freezes the partition.
    pub fn freeze(&self) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        inner.freeze_active_shard()?;
        Ok(())
    }

    /// Forks the partition.
    ///
    /// Must freeze the partition before fork.
    pub fn fork(&self, metadata: &RegionMetadataRef, config: &MergeTreeConfig) -> Partition {
        let inner = self.inner.read().unwrap();
        debug_assert!(inner.shard_builder.is_empty());
        // TODO(yingwen): TTL or evict shards.
        let shard_builder = ShardBuilder::new(
            metadata.clone(),
            config,
            inner.shard_builder.current_shard_id(),
        );
        let shards = inner
            .shards
            .iter()
            .map(|shard| shard.fork(metadata.clone()))
            .collect();

        Partition {
            inner: RwLock::new(Inner {
                metadata: metadata.clone(),
                shard_builder,
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
pub struct PartitionReader {
    projection: HashSet<ColumnId>,
    filters: Vec<SimpleFilterEvaluator>,
    pk_weights: Vec<u16>,
}

impl PartitionReader {
    pub fn is_valid(&self) -> bool {
        unimplemented!()
    }

    pub fn next(&mut self) {
        unimplemented!()
    }

    fn current_key(&self) -> Option<&[u8]> {
        unimplemented!()
    }

    pub fn current_batch(&self) -> Batch {
        unimplemented!()
    }

    pub(crate) fn into_context(self) -> ReadPartitionContext {
        ReadPartitionContext {
            projection: self.projection,
            filters: self.filters,
            pk_weights: self.pk_weights,
        }
    }
}

// TODO(yingwen): Improve performance of key prunning. Now we need to find index and
// then decode and convert each value.
fn prune_primary_key(
    metadata: &RegionMetadataRef,
    predicates: &[SimpleFilterEvaluator],
    codec: &McmpRowCodec,
    pk: &[u8],
) -> bool {
    if predicates.is_empty() {
        return true;
    }

    // no primary key, we simply return true.
    if metadata.primary_key.is_empty() {
        return true;
    }

    let pk_values = match codec.decode(pk) {
        Ok(values) => values,
        Err(e) => {
            common_telemetry::error!(e; "Failed to decode primary key");
            return true;
        }
    };

    // evaluate predicates against primary key values
    let mut result = true;
    for predicate in predicates {
        let Some(column) = metadata.column_by_name(predicate.column_name()) else {
            continue;
        };
        // ignore predicates that are not referencing primary key columns
        if column.semantic_type != SemanticType::Tag {
            continue;
        }
        // index of the column in primary keys.
        // Safety: A tag column is always in primary key.
        let index = metadata.primary_key_index(column.column_id).unwrap();
        // Safety: arrow schema and datatypes are constructed from the same source.
        let scalar_value = pk_values[index]
            .try_to_scalar_value(&column.column_schema.data_type)
            .unwrap();
        result &= predicate.evaluate_scalar(&scalar_value).unwrap_or(true);
    }

    result
}

/// Structs to reuse across readers to avoid allocating for each reader.
pub(crate) struct ReadPartitionContext {
    projection: HashSet<ColumnId>,
    filters: Vec<SimpleFilterEvaluator>,
    /// Buffer to store pk weights.
    pk_weights: Vec<u16>,
}

impl ReadPartitionContext {
    pub(crate) fn new(
        projection: HashSet<ColumnId>,
        filters: Vec<SimpleFilterEvaluator>,
    ) -> ReadPartitionContext {
        ReadPartitionContext {
            projection,
            filters,
            pk_weights: Vec::new(),
        }
    }
}

// TODO(yingwen): Pushdown projection to shard readers.
/// Converts a [DataBatch] to a [Batch].
fn convert_batch(
    metadata: &RegionMetadataRef,
    projection: &HashSet<ColumnId>,
    key: Option<&[u8]>,
    data_batch: &DataBatch,
) -> Result<Batch> {
    let record_batch = data_batch.slice_record_batch();
    let primary_key = key.map(|k| k.to_vec()).unwrap_or_default();
    let mut builder = BatchBuilder::new(primary_key);
    builder
        .timestamps_array(record_batch.column(1).clone())?
        .sequences_array(record_batch.column(2).clone())?
        .op_types_array(record_batch.column(3).clone())?;

    if record_batch.num_columns() <= 4 {
        // No fields.
        return builder.build();
    }

    // Iterate all field columns.
    for (array, field) in record_batch
        .columns()
        .iter()
        .zip(record_batch.schema().fields().iter())
        .skip(4)
    {
        // Safety: metadata should contain all fields.
        let column_id = metadata.column_by_name(field.name()).unwrap().column_id;
        if !projection.contains(&column_id) {
            continue;
        }
        builder.push_field_array(column_id, array.clone())?;
    }

    builder.build()
}

pub type PartitionRef = Arc<Partition>;

/// Inner struct of the partition.
///
/// A key only exists in one shard.
struct Inner {
    metadata: RegionMetadataRef,
    /// Shard whose dictionary is active.
    shard_builder: ShardBuilder,
    /// Shards with frozen dictionary.
    shards: Vec<Shard>,
    num_rows: usize,
}

impl Inner {
    fn new(metadata: RegionMetadataRef, config: &MergeTreeConfig) -> Self {
        let (shards, current_shard_id) = if metadata.primary_key.is_empty() {
            let data_parts = DataParts::new(metadata.clone(), DATA_INIT_CAP);
            (vec![Shard::new(0, None, data_parts)], 1)
        } else {
            (Vec::new(), 0)
        };
        let shard_builder = ShardBuilder::new(metadata.clone(), config, current_shard_id);
        Self {
            metadata,
            shard_builder,
            shards,
            num_rows: 0,
        }
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
                shard.write_with_pk_id(pk_id, key_value);
                self.num_rows += 1;
                return;
            }
        }
    }

    fn freeze_active_shard(&mut self) -> Result<()> {
        if let Some(shard) = self.shard_builder.finish(self.metadata.clone())? {
            self.shards.push(shard);
        }
        Ok(())
    }
}
