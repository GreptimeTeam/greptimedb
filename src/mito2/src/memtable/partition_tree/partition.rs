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

//! Partition of a partition tree.
//!
//! We only support partitioning the tree by pre-defined internal columns.

use std::collections::{HashMap, HashSet};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use api::v1::SemanticType;
use common_recordbatch::filter::SimpleFilterEvaluator;
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::RegionMetadataRef;
use store_api::metric_engine_consts::DATA_SCHEMA_TABLE_ID_COLUMN_NAME;
use store_api::storage::ColumnId;

use crate::error::Result;
use crate::memtable::key_values::KeyValue;
use crate::memtable::partition_tree::data::{DataBatch, DataParts, DATA_INIT_CAP};
use crate::memtable::partition_tree::dedup::DedupReader;
use crate::memtable::partition_tree::shard::{
    BoxedDataBatchSource, Shard, ShardMerger, ShardNode, ShardSource,
};
use crate::memtable::partition_tree::shard_builder::ShardBuilder;
use crate::memtable::partition_tree::{PartitionTreeConfig, PkId};
use crate::memtable::stats::WriteMetrics;
use crate::metrics::PARTITION_TREE_READ_STAGE_ELAPSED;
use crate::read::{Batch, BatchBuilder};
use crate::row_converter::{
    DensePrimaryKeyCodec, PrimaryKeyCodec, PrimaryKeyFilter, PrimaryKeyFilterFactory,
};

/// Key of a partition.
pub type PartitionKey = u32;

/// A tree partition.
pub struct Partition {
    inner: RwLock<Inner>,
    /// Whether to dedup batches.
    dedup: bool,
}

pub type PartitionRef = Arc<Partition>;

impl Partition {
    /// Creates a new partition.
    pub fn new(metadata: RegionMetadataRef, config: &PartitionTreeConfig) -> Self {
        Partition {
            inner: RwLock::new(Inner::new(metadata, config)),
            dedup: config.dedup,
        }
    }

    /// Writes to the partition with a primary key.
    pub fn write_with_key<T: for<'a, 'b> PrimaryKeyCodec<'a, 'b>>(
        &self,
        primary_key: &mut Vec<u8>,
        row_codec: &T,
        key_value: KeyValue,
        re_encode: bool,
        metrics: &mut WriteMetrics,
    ) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        // Freeze the shard builder if needed.
        if inner.shard_builder.should_freeze() {
            inner.freeze_active_shard()?;
        }

        // Finds key in shards, now we ensure one key only exists in one shard.
        if let Some(pk_id) = inner.find_key_in_shards(primary_key) {
            inner.write_to_shard(pk_id, &key_value)?;
            inner.num_rows += 1;
            return Ok(());
        }

        // Key does not yet exist in shard or builder, encode and insert the full primary key.
        if re_encode {
            match row_codec.encoding() {
                PrimaryKeyEncoding::Dense => {
                    // `primary_key` is sparse, re-encode the full primary key.
                    let sparse_key = primary_key.clone();
                    primary_key.clear();
                    {
                        let mut encoder = row_codec.encoder(primary_key);
                        key_value.encode_primary_key(&mut encoder)?;
                    }
                    let pk_id = inner.shard_builder.write_with_key(
                        primary_key,
                        Some(&sparse_key),
                        &key_value,
                        metrics,
                    );
                    inner.pk_to_pk_id.insert(sparse_key, pk_id);
                }
                PrimaryKeyEncoding::Sparse => {
                    // TODO(weny): support sparse primary key.
                    todo!()
                }
            }
        } else {
            // `primary_key` is already the full primary key.
            let pk_id = inner
                .shard_builder
                .write_with_key(primary_key, None, &key_value, metrics);
            inner.pk_to_pk_id.insert(std::mem::take(primary_key), pk_id);
        };

        inner.num_rows += 1;
        Ok(())
    }

    /// Writes to the partition without a primary key.
    pub fn write_no_key(&self, key_value: KeyValue) -> Result<()> {
        let mut inner = self.inner.write().unwrap();
        // If no primary key, always write to the first shard.
        debug_assert!(!inner.shards.is_empty());
        debug_assert_eq!(1, inner.shard_builder.current_shard_id());

        // A dummy pk id.
        let pk_id = PkId {
            shard_id: 0,
            pk_index: 0,
        };
        inner.shards[0].write_with_pk_id(pk_id, &key_value)?;
        inner.num_rows += 1;

        Ok(())
    }

    /// Scans data in the partition.
    pub fn read<T: PrimaryKeyFilterFactory>(
        &self,
        mut context: ReadPartitionContext<T>,
    ) -> Result<PartitionReader<T>> {
        let start = Instant::now();
        let primary_key_filter_factory = context.primary_key_filter_factory.as_ref();
        let (builder_source, shard_reader_builders) = {
            let inner = self.inner.read().unwrap();
            let mut shard_source = Vec::with_capacity(inner.shards.len() + 1);
            let builder_reader = if !inner.shard_builder.is_empty() {
                let builder_reader = inner.shard_builder.read(&mut context.pk_weights)?;
                Some(builder_reader)
            } else {
                None
            };
            for shard in &inner.shards {
                if !shard.is_empty() {
                    let shard_reader_builder = shard.read()?;
                    shard_source.push(shard_reader_builder);
                }
            }
            (builder_reader, shard_source)
        };

        context.metrics.num_shards += shard_reader_builders.len();
        let mut nodes = shard_reader_builders
            .into_iter()
            .map(|builder| {
                Ok(ShardNode::new(ShardSource::Shard(
                    builder.build(
                        primary_key_filter_factory
                            .map(|f| Box::new(f.build()) as Box<dyn PrimaryKeyFilter>),
                    )?,
                )))
            })
            .collect::<Result<Vec<_>>>()?;

        if let Some(builder) = builder_source {
            context.metrics.num_builder += 1;
            // Move the initialization of ShardBuilderReader out of read lock.
            let shard_builder_reader = builder.build(
                Some(&context.pk_weights),
                primary_key_filter_factory
                    .map(|f| Box::new(f.build()) as Box<dyn PrimaryKeyFilter>),
            )?;
            nodes.push(ShardNode::new(ShardSource::Builder(shard_builder_reader)));
        }

        // Creating a shard merger will invoke next so we do it outside the lock.
        let merger = ShardMerger::try_new(nodes)?;
        if self.dedup {
            let source = DedupReader::try_new(merger)?;
            context.metrics.build_partition_reader += start.elapsed();
            PartitionReader::new(context, Box::new(source))
        } else {
            context.metrics.build_partition_reader += start.elapsed();
            PartitionReader::new(context, Box::new(merger))
        }
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
    pub fn fork(&self, metadata: &RegionMetadataRef, config: &PartitionTreeConfig) -> Partition {
        let (shards, shard_builder) = {
            let inner = self.inner.read().unwrap();
            debug_assert!(inner.shard_builder.is_empty());
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

            (shards, shard_builder)
        };
        let pk_to_pk_id = {
            let mut inner = self.inner.write().unwrap();
            std::mem::take(&mut inner.pk_to_pk_id)
        };

        Partition {
            inner: RwLock::new(Inner {
                metadata: metadata.clone(),
                shard_builder,
                shards,
                num_rows: 0,
                pk_to_pk_id,
                frozen: false,
            }),
            dedup: self.dedup,
        }
    }

    /// Returns true if the partition has data.
    pub fn has_data(&self) -> bool {
        let inner = self.inner.read().unwrap();
        inner.num_rows > 0
    }

    /// Gets the stats of the partition.
    pub(crate) fn stats(&self) -> PartitionStats {
        let inner = self.inner.read().unwrap();
        let num_rows = inner.num_rows;
        let shard_num = inner.shards.len();
        let shared_memory_size = inner
            .shards
            .iter()
            .map(|shard| shard.shared_memory_size())
            .sum();
        PartitionStats {
            num_rows,
            shard_num,
            shared_memory_size,
        }
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

pub(crate) struct PartitionStats {
    pub(crate) num_rows: usize,
    pub(crate) shard_num: usize,
    pub(crate) shared_memory_size: usize,
}

#[derive(Default)]
struct PartitionReaderMetrics {
    build_partition_reader: Duration,
    read_source: Duration,
    data_batch_to_batch: Duration,
    num_builder: usize,
    num_shards: usize,
}

/// Reader to scan rows in a partition.
///
/// It can merge rows from multiple shards.
pub struct PartitionReader<T: PrimaryKeyFilterFactory> {
    context: ReadPartitionContext<T>,
    source: BoxedDataBatchSource,
}

impl<T: PrimaryKeyFilterFactory> PartitionReader<T> {
    fn new(context: ReadPartitionContext<T>, source: BoxedDataBatchSource) -> Result<Self> {
        let reader = Self { context, source };

        Ok(reader)
    }

    /// Returns true if the reader is valid.
    pub fn is_valid(&self) -> bool {
        self.source.is_valid()
    }

    /// Advances the reader.
    ///
    /// # Panics
    /// Panics if the reader is invalid.
    pub fn next(&mut self) -> Result<()> {
        self.advance_source()
    }

    /// Converts current data batch into a [Batch].
    ///
    /// # Panics
    /// Panics if the reader is invalid.
    pub fn convert_current_batch(&mut self) -> Result<Batch> {
        let start = Instant::now();
        let data_batch = self.source.current_data_batch();
        let batch = data_batch_to_batch(
            &self.context.metadata,
            &self.context.projection,
            self.source.current_key(),
            data_batch,
        )?;
        self.context.metrics.data_batch_to_batch += start.elapsed();
        Ok(batch)
    }

    pub(crate) fn into_context(self) -> ReadPartitionContext<T> {
        self.context
    }

    fn advance_source(&mut self) -> Result<()> {
        let read_source = Instant::now();
        self.source.next()?;
        self.context.metrics.read_source += read_source.elapsed();
        Ok(())
    }
}

/// Dense primary key filter factory.
pub struct DensePrimaryKeyFilterFactory {
    metadata: RegionMetadataRef,
    filters: Arc<Vec<SimpleFilterEvaluator>>,
    codec: Arc<DensePrimaryKeyCodec>,
}

impl DensePrimaryKeyFilterFactory {
    pub(crate) fn new(
        metadata: RegionMetadataRef,
        filters: Arc<Vec<SimpleFilterEvaluator>>,
        codec: Arc<DensePrimaryKeyCodec>,
    ) -> Self {
        Self {
            metadata,
            filters,
            codec,
        }
    }
}

impl PrimaryKeyFilterFactory for DensePrimaryKeyFilterFactory {
    type Filter = DensePrimaryKeyFilter;

    fn build(&self) -> Self::Filter {
        DensePrimaryKeyFilter::new(
            self.metadata.clone(),
            self.filters.clone(),
            self.codec.clone(),
        )
    }
}

/// Dense primary key filter.
#[derive(Clone)]
pub struct DensePrimaryKeyFilter {
    metadata: RegionMetadataRef,
    filters: Arc<Vec<SimpleFilterEvaluator>>,
    codec: Arc<DensePrimaryKeyCodec>,
    offsets_buf: Vec<usize>,
}

impl DensePrimaryKeyFilter {
    pub(crate) fn new(
        metadata: RegionMetadataRef,
        filters: Arc<Vec<SimpleFilterEvaluator>>,
        codec: Arc<DensePrimaryKeyCodec>,
    ) -> Self {
        Self {
            metadata,
            filters,
            codec,
            offsets_buf: Vec::new(),
        }
    }
}

impl PrimaryKeyFilter for DensePrimaryKeyFilter {
    fn prune_primary_key(&mut self, pk: &[u8]) -> bool {
        if self.filters.is_empty() {
            return true;
        }

        // no primary key, we simply return true.
        if self.metadata.primary_key.is_empty() {
            return true;
        }

        // evaluate filters against primary key values
        let mut result = true;
        self.offsets_buf.clear();
        for filter in &*self.filters {
            if Partition::is_partition_column(filter.column_name()) {
                continue;
            }
            let Some(column) = self.metadata.column_by_name(filter.column_name()) else {
                continue;
            };
            // ignore filters that are not referencing primary key columns
            if column.semantic_type != SemanticType::Tag {
                continue;
            }
            // index of the column in primary keys.
            // Safety: A tag column is always in primary key.
            let index = self.metadata.primary_key_index(column.column_id).unwrap();
            let value = match self.codec.decode_value_at(pk, index, &mut self.offsets_buf) {
                Ok(v) => v,
                Err(e) => {
                    common_telemetry::error!(e; "Failed to decode primary key");
                    return true;
                }
            };

            // TODO(yingwen): `evaluate_scalar()` creates temporary arrays to compare scalars. We
            // can compare the bytes directly without allocation and matching types as we use
            // comparable encoding.
            // Safety: arrow schema and datatypes are constructed from the same source.
            let scalar_value = value
                .try_to_scalar_value(&column.column_schema.data_type)
                .unwrap();
            result &= filter.evaluate_scalar(&scalar_value).unwrap_or(true);
        }

        result
    }
}

/// Structs to reuse across readers to avoid allocating for each reader.
pub(crate) struct ReadPartitionContext<T: PrimaryKeyFilterFactory> {
    metadata: RegionMetadataRef,
    projection: HashSet<ColumnId>,
    primary_key_filter_factory: Option<T>,
    /// Buffer to store pk weights.
    pk_weights: Vec<u16>,
    metrics: PartitionReaderMetrics,
}

impl<T: PrimaryKeyFilterFactory> Drop for ReadPartitionContext<T> {
    fn drop(&mut self) {
        let partition_read_source = self.metrics.read_source.as_secs_f64();
        PARTITION_TREE_READ_STAGE_ELAPSED
            .with_label_values(&["partition_read_source"])
            .observe(partition_read_source);
        let partition_data_batch_to_batch = self.metrics.data_batch_to_batch.as_secs_f64();
        PARTITION_TREE_READ_STAGE_ELAPSED
            .with_label_values(&["partition_data_batch_to_batch"])
            .observe(partition_data_batch_to_batch);

        common_telemetry::debug!(
            "TreeIter partitions metrics, \
            num_builder: {}, \
            num_shards: {}, \
            build_partition_reader: {}s, \
            partition_read_source: {}s, \
            partition_data_batch_to_batch: {}s",
            self.metrics.num_builder,
            self.metrics.num_shards,
            self.metrics.build_partition_reader.as_secs_f64(),
            partition_read_source,
            partition_data_batch_to_batch,
        );
    }
}

impl<T: PrimaryKeyFilterFactory> ReadPartitionContext<T> {
    pub(crate) fn new<F: Fn(&RegionMetadataRef, Vec<SimpleFilterEvaluator>) -> T>(
        metadata: RegionMetadataRef,
        primary_key_filter_factory: F,
        projection: HashSet<ColumnId>,
        filters: Vec<SimpleFilterEvaluator>,
    ) -> ReadPartitionContext<T> {
        let need_prune_key = Self::need_prune_key(&metadata, &filters);
        let primary_key_filter_factory = if need_prune_key && !filters.is_empty() {
            Some(primary_key_filter_factory(&metadata, filters))
        } else {
            None
        };

        ReadPartitionContext {
            metadata,
            primary_key_filter_factory,
            projection,
            pk_weights: Vec::new(),
            metrics: Default::default(),
        }
    }

    /// Does filter contain predicate on primary key columns after pruning the
    /// partition column.
    fn need_prune_key(metadata: &RegionMetadataRef, filters: &[SimpleFilterEvaluator]) -> bool {
        for filter in filters {
            // We already pruned partitions before so we skip the partition column.
            if Partition::is_partition_column(filter.column_name()) {
                continue;
            }
            let Some(column) = metadata.column_by_name(filter.column_name()) else {
                continue;
            };
            if column.semantic_type != SemanticType::Tag {
                continue;
            }

            return true;
        }

        false
    }
}

// TODO(yingwen): Pushdown projection to shard readers.
/// Converts a [DataBatch] to a [Batch].
fn data_batch_to_batch(
    metadata: &RegionMetadataRef,
    projection: &HashSet<ColumnId>,
    key: Option<&[u8]>,
    data_batch: DataBatch,
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
        // TODO(yingwen): Avoid finding column by name. We know the schema of a DataBatch.
        // Safety: metadata should contain all fields.
        let column_id = metadata.column_by_name(field.name()).unwrap().column_id;
        if !projection.contains(&column_id) {
            continue;
        }
        builder.push_field_array(column_id, array.clone())?;
    }

    builder.build()
}

/// Inner struct of the partition.
///
/// A key only exists in one shard.
struct Inner {
    metadata: RegionMetadataRef,
    /// Map to index pk to pk id.
    pk_to_pk_id: HashMap<Vec<u8>, PkId>,
    /// Shard whose dictionary is active.
    shard_builder: ShardBuilder,
    /// Shards with frozen dictionary.
    shards: Vec<Shard>,
    num_rows: usize,
    frozen: bool,
}

impl Inner {
    fn new(metadata: RegionMetadataRef, config: &PartitionTreeConfig) -> Self {
        let (shards, current_shard_id) = if metadata.primary_key.is_empty() {
            let data_parts = DataParts::new(metadata.clone(), DATA_INIT_CAP, config.dedup);
            (
                vec![Shard::new(
                    0,
                    None,
                    data_parts,
                    config.dedup,
                    config.data_freeze_threshold,
                )],
                1,
            )
        } else {
            (Vec::new(), 0)
        };
        let shard_builder = ShardBuilder::new(metadata.clone(), config, current_shard_id);
        Self {
            metadata,
            pk_to_pk_id: HashMap::new(),
            shard_builder,
            shards,
            num_rows: 0,
            frozen: false,
        }
    }

    fn find_key_in_shards(&self, primary_key: &[u8]) -> Option<PkId> {
        assert!(!self.frozen);
        self.pk_to_pk_id.get(primary_key).copied()
    }

    fn write_to_shard(&mut self, pk_id: PkId, key_value: &KeyValue) -> Result<()> {
        if pk_id.shard_id == self.shard_builder.current_shard_id() {
            self.shard_builder.write_with_pk_id(pk_id, key_value);
            return Ok(());
        }

        // Safety: We find the shard by shard id.
        let shard = self
            .shards
            .iter_mut()
            .find(|shard| shard.shard_id == pk_id.shard_id)
            .unwrap();
        shard.write_with_pk_id(pk_id, key_value)?;
        self.num_rows += 1;

        Ok(())
    }

    fn freeze_active_shard(&mut self) -> Result<()> {
        if let Some(shard) = self
            .shard_builder
            .finish(self.metadata.clone(), &mut self.pk_to_pk_id)?
        {
            self.shards.push(shard);
        }
        Ok(())
    }
}
