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

//! Implementation of the partition tree.

use std::collections::{BTreeMap, HashSet, VecDeque};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use api::v1::OpType;
use common_recordbatch::filter::SimpleFilterEvaluator;
use common_time::Timestamp;
use datafusion_common::ScalarValue;
use datatypes::prelude::ValueRef;
use mito_codec::key_values::KeyValue;
use mito_codec::primary_key_filter::is_partition_column;
use mito_codec::row_converter::sparse::{FieldWithId, SparseEncoder};
use mito_codec::row_converter::{PrimaryKeyCodec, SortField};
use snafu::{ResultExt, ensure};
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{ColumnId, SequenceRange};
use table::predicate::Predicate;

use crate::error::{
    EncodeSnafu, EncodeSparsePrimaryKeySnafu, PrimaryKeyLengthMismatchSnafu, Result,
};
use crate::flush::WriteBufferManagerRef;
use crate::memtable::partition_tree::PartitionTreeConfig;
use crate::memtable::partition_tree::partition::{
    Partition, PartitionKey, PartitionReader, PartitionRef, ReadPartitionContext,
};
use crate::memtable::stats::WriteMetrics;
use crate::memtable::{BoxedBatchIterator, KeyValues};
use crate::metrics::{PARTITION_TREE_READ_STAGE_ELAPSED, READ_ROWS_TOTAL, READ_STAGE_ELAPSED};
use crate::read::Batch;
use crate::read::dedup::LastNonNullIter;
use crate::region::options::MergeMode;

/// The partition tree.
pub struct PartitionTree {
    /// Config of the tree.
    config: PartitionTreeConfig,
    /// Metadata of the region.
    pub(crate) metadata: RegionMetadataRef,
    /// Primary key codec.
    row_codec: Arc<dyn PrimaryKeyCodec>,
    /// Partitions in the tree.
    partitions: RwLock<BTreeMap<PartitionKey, PartitionRef>>,
    /// Whether the tree has multiple partitions.
    is_partitioned: bool,
    /// Manager to report size of the tree.
    write_buffer_manager: Option<WriteBufferManagerRef>,
    sparse_encoder: Arc<SparseEncoder>,
}

impl PartitionTree {
    /// Creates a new partition tree.
    pub fn new(
        row_codec: Arc<dyn PrimaryKeyCodec>,
        metadata: RegionMetadataRef,
        config: &PartitionTreeConfig,
        write_buffer_manager: Option<WriteBufferManagerRef>,
    ) -> Self {
        let sparse_encoder = SparseEncoder::new(
            metadata
                .primary_key_columns()
                .map(|c| FieldWithId {
                    field: SortField::new(c.column_schema.data_type.clone()),
                    column_id: c.column_id,
                })
                .collect(),
        );
        let is_partitioned = Partition::has_multi_partitions(&metadata);
        let mut config = config.clone();
        if config.merge_mode == MergeMode::LastNonNull {
            config.dedup = false;
        }

        PartitionTree {
            config,
            metadata,
            row_codec,
            partitions: Default::default(),
            is_partitioned,
            write_buffer_manager,
            sparse_encoder: Arc::new(sparse_encoder),
        }
    }

    fn verify_primary_key_length(&self, kv: &KeyValue) -> Result<()> {
        // The sparse primary key codec does not have a fixed number of fields.
        if let Some(expected_num_fields) = self.row_codec.num_fields() {
            ensure!(
                expected_num_fields == kv.num_primary_keys(),
                PrimaryKeyLengthMismatchSnafu {
                    expect: expected_num_fields,
                    actual: kv.num_primary_keys(),
                }
            );
        }
        // TODO(weny): verify the primary key length for sparse primary key codec.
        Ok(())
    }

    /// Encodes the given key value into a sparse primary key.
    fn encode_sparse_primary_key(&self, kv: &KeyValue, buffer: &mut Vec<u8>) -> Result<()> {
        if kv.primary_key_encoding() == PrimaryKeyEncoding::Sparse {
            // If the primary key encoding is sparse and already encoded in the metric engine,
            // we only need to copy the encoded primary key into the destination buffer.
            let ValueRef::Binary(primary_key) = kv.primary_keys().next().unwrap() else {
                return EncodeSparsePrimaryKeySnafu {
                    reason: "sparse primary key is not binary".to_string(),
                }
                .fail();
            };
            buffer.extend_from_slice(primary_key);
        } else {
            // For compatibility, use the sparse encoder for dense primary key.
            self.sparse_encoder
                .encode_to_vec(kv.primary_keys(), buffer)
                .context(EncodeSnafu)?;
        }
        Ok(())
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
            self.verify_primary_key_length(&kv)?;
            // Safety: timestamp of kv must be both present and a valid timestamp value.
            let ts = kv
                .timestamp()
                .try_into_timestamp()
                .unwrap()
                .unwrap()
                .value();
            metrics.min_ts = metrics.min_ts.min(ts);
            metrics.max_ts = metrics.max_ts.max(ts);
            metrics.value_bytes += kv.fields().map(|v| v.data_size()).sum::<usize>();

            if !has_pk {
                // No primary key.
                self.write_no_key(kv)?;
                continue;
            }

            // Encode primary key.
            pk_buffer.clear();
            if self.is_partitioned {
                self.encode_sparse_primary_key(&kv, pk_buffer)?;
            } else {
                self.row_codec
                    .encode_key_value(&kv, pk_buffer)
                    .context(EncodeSnafu)?;
            }

            // Write rows with
            self.write_with_key(pk_buffer, kv, metrics)?;
        }

        metrics.value_bytes +=
            kvs.num_rows() * (std::mem::size_of::<Timestamp>() + std::mem::size_of::<OpType>());

        Ok(())
    }

    /// Write one key value pair into the tree.
    ///
    /// # Panics
    /// Panics if the tree is immutable (frozen).
    pub fn write_one(
        &self,
        kv: KeyValue,
        pk_buffer: &mut Vec<u8>,
        metrics: &mut WriteMetrics,
    ) -> Result<()> {
        let has_pk = !self.metadata.primary_key.is_empty();

        self.verify_primary_key_length(&kv)?;
        // Safety: timestamp of kv must be both present and a valid timestamp value.
        let ts = kv
            .timestamp()
            .try_into_timestamp()
            .unwrap()
            .unwrap()
            .value();
        metrics.min_ts = metrics.min_ts.min(ts);
        metrics.max_ts = metrics.max_ts.max(ts);
        metrics.value_bytes += kv.fields().map(|v| v.data_size()).sum::<usize>();

        if !has_pk {
            // No primary key.
            return self.write_no_key(kv);
        }

        // Encode primary key.
        pk_buffer.clear();
        if self.is_partitioned {
            self.encode_sparse_primary_key(&kv, pk_buffer)?;
        } else {
            self.row_codec
                .encode_key_value(&kv, pk_buffer)
                .context(EncodeSnafu)?;
        }

        // Write rows with
        self.write_with_key(pk_buffer, kv, metrics)?;

        metrics.value_bytes += std::mem::size_of::<Timestamp>() + std::mem::size_of::<OpType>();

        Ok(())
    }

    /// Scans the tree.
    pub fn read(
        &self,
        projection: Option<&[ColumnId]>,
        predicate: Option<Predicate>,
        sequence: Option<SequenceRange>,
        mem_scan_metrics: Option<crate::memtable::MemScanMetrics>,
    ) -> Result<BoxedBatchIterator> {
        let start = Instant::now();
        // Creates the projection set.
        let projection: HashSet<_> = if let Some(projection) = projection {
            projection.iter().copied().collect()
        } else {
            self.metadata.field_columns().map(|c| c.column_id).collect()
        };

        let filters = predicate
            .map(|predicate| {
                predicate
                    .exprs()
                    .iter()
                    .filter_map(SimpleFilterEvaluator::try_new)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();

        let mut tree_iter_metric = TreeIterMetrics::default();
        let partitions = self.prune_partitions(&filters, &mut tree_iter_metric);

        let mut iter = TreeIter {
            sequence,
            partitions,
            current_reader: None,
            metrics: tree_iter_metric,
            mem_scan_metrics,
        };
        let context = ReadPartitionContext::new(
            self.metadata.clone(),
            self.row_codec.clone(),
            projection,
            Arc::new(filters),
        );
        iter.fetch_next_partition(context)?;

        iter.metrics.iter_elapsed += start.elapsed();

        if self.config.merge_mode == MergeMode::LastNonNull {
            let iter = LastNonNullIter::new(iter);
            Ok(Box::new(iter))
        } else {
            Ok(Box::new(iter))
        }
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
    pub fn fork(&self, metadata: RegionMetadataRef) -> PartitionTree {
        if self.metadata.schema_version != metadata.schema_version
            || self.metadata.column_metadatas != metadata.column_metadatas
        {
            // The schema has changed, we can't reuse the tree.
            return PartitionTree::new(
                self.row_codec.clone(),
                metadata,
                &self.config,
                self.write_buffer_manager.clone(),
            );
        }

        let mut total_shared_size = 0;
        let mut part_infos = {
            let partitions = self.partitions.read().unwrap();
            partitions
                .iter()
                .filter_map(|(part_key, part)| {
                    let stats = part.stats();
                    if stats.num_rows > 0 {
                        // Only fork partitions that have data.
                        total_shared_size += stats.shared_memory_size;
                        Some((*part_key, part.clone(), stats))
                    } else {
                        None
                    }
                })
                .collect::<Vec<_>>()
        };

        // TODO(yingwen): Optimize eviction strategy. Now we evict the whole partition.
        let fork_size = self.config.fork_dictionary_bytes.as_bytes() as usize;
        if total_shared_size > fork_size {
            // Sort partitions by memory size desc.
            part_infos.sort_unstable_by_key(|info| info.2.shared_memory_size);
            while total_shared_size > fork_size {
                let Some(info) = part_infos.pop() else {
                    break;
                };

                common_telemetry::debug!(
                    "Evict partition {} with memory size {}, {} shards",
                    info.0,
                    info.2.shared_memory_size,
                    info.2.shard_num,
                );

                total_shared_size -= info.2.shared_memory_size;
            }
        }

        let mut forked = BTreeMap::new();
        for (part_key, part, _) in part_infos {
            let forked_part = part.fork(&metadata, &self.config);
            forked.insert(part_key, Arc::new(forked_part));
        }

        PartitionTree {
            config: self.config.clone(),
            metadata,
            row_codec: self.row_codec.clone(),
            partitions: RwLock::new(forked),
            is_partitioned: self.is_partitioned,
            write_buffer_manager: self.write_buffer_manager.clone(),
            sparse_encoder: self.sparse_encoder.clone(),
        }
    }

    /// Returns the write buffer manager.
    pub(crate) fn write_buffer_manager(&self) -> Option<WriteBufferManagerRef> {
        self.write_buffer_manager.clone()
    }

    fn write_with_key(
        &self,
        primary_key: &mut Vec<u8>,
        key_value: KeyValue,
        metrics: &mut WriteMetrics,
    ) -> Result<()> {
        let partition_key = Partition::get_partition_key(&key_value, self.is_partitioned);
        let partition = self.get_or_create_partition(partition_key);

        partition.write_with_key(
            primary_key,
            self.row_codec.as_ref(),
            key_value,
            self.is_partitioned, // If tree is partitioned, re-encode is required to get the full primary key.
            metrics,
        )
    }

    fn write_no_key(&self, key_value: KeyValue) -> Result<()> {
        let partition_key = Partition::get_partition_key(&key_value, self.is_partitioned);
        let partition = self.get_or_create_partition(partition_key);

        partition.write_no_key(key_value)
    }

    fn get_or_create_partition(&self, partition_key: PartitionKey) -> PartitionRef {
        let mut partitions = self.partitions.write().unwrap();
        partitions
            .entry(partition_key)
            .or_insert_with(|| Arc::new(Partition::new(self.metadata.clone(), &self.config)))
            .clone()
    }

    fn prune_partitions(
        &self,
        filters: &[SimpleFilterEvaluator],
        metrics: &mut TreeIterMetrics,
    ) -> VecDeque<PartitionRef> {
        let partitions = self.partitions.read().unwrap();
        metrics.partitions_total = partitions.len();
        if !self.is_partitioned {
            return partitions.values().cloned().collect();
        }

        let mut pruned = VecDeque::new();
        // Prune partition keys.
        for (key, partition) in partitions.iter() {
            let mut is_needed = true;
            for filter in filters {
                if !is_partition_column(filter.column_name()) {
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
        metrics.partitions_after_pruning = pruned.len();
        pruned
    }

    /// Returns all series count in all partitions.
    pub(crate) fn series_count(&self) -> usize {
        self.partitions
            .read()
            .unwrap()
            .values()
            .map(|p| p.series_count())
            .sum()
    }
}

#[derive(Default)]
struct TreeIterMetrics {
    iter_elapsed: Duration,
    fetch_partition_elapsed: Duration,
    rows_fetched: usize,
    batches_fetched: usize,
    partitions_total: usize,
    partitions_after_pruning: usize,
}

struct TreeIter {
    /// Optional Sequence number of the current reader which limit results batch to lower than this sequence number.
    sequence: Option<SequenceRange>,
    partitions: VecDeque<PartitionRef>,
    current_reader: Option<PartitionReader>,
    metrics: TreeIterMetrics,
    mem_scan_metrics: Option<crate::memtable::MemScanMetrics>,
}

impl TreeIter {
    fn report_mem_scan_metrics(&mut self) {
        if let Some(mem_scan_metrics) = self.mem_scan_metrics.take() {
            let inner = crate::memtable::MemScanMetricsData {
                total_series: 0, // This is unavailable.
                num_rows: self.metrics.rows_fetched,
                num_batches: self.metrics.batches_fetched,
                scan_cost: self.metrics.iter_elapsed,
            };
            mem_scan_metrics.merge_inner(&inner);
        }
    }
}

impl Drop for TreeIter {
    fn drop(&mut self) {
        // Report MemScanMetrics if not already reported
        self.report_mem_scan_metrics();

        READ_ROWS_TOTAL
            .with_label_values(&["partition_tree_memtable"])
            .inc_by(self.metrics.rows_fetched as u64);
        PARTITION_TREE_READ_STAGE_ELAPSED
            .with_label_values(&["fetch_next_partition"])
            .observe(self.metrics.fetch_partition_elapsed.as_secs_f64());
        let scan_elapsed = self.metrics.iter_elapsed.as_secs_f64();
        READ_STAGE_ELAPSED
            .with_label_values(&["scan_memtable"])
            .observe(scan_elapsed);
        common_telemetry::debug!(
            "TreeIter partitions total: {}, partitions after prune: {}, rows fetched: {}, batches fetched: {}, scan elapsed: {}",
            self.metrics.partitions_total,
            self.metrics.partitions_after_pruning,
            self.metrics.rows_fetched,
            self.metrics.batches_fetched,
            scan_elapsed
        );
    }
}

impl Iterator for TreeIter {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        let start = Instant::now();
        let res = self.next_batch().transpose();
        self.metrics.iter_elapsed += start.elapsed();
        res
    }
}

impl TreeIter {
    /// Fetch next partition.
    fn fetch_next_partition(&mut self, mut context: ReadPartitionContext) -> Result<()> {
        let start = Instant::now();
        while let Some(partition) = self.partitions.pop_front() {
            let part_reader = partition.read(context)?;
            if !part_reader.is_valid() {
                context = part_reader.into_context();
                continue;
            }
            self.current_reader = Some(part_reader);
            break;
        }
        self.metrics.fetch_partition_elapsed += start.elapsed();
        Ok(())
    }

    /// Fetches next batch.
    fn next_batch(&mut self) -> Result<Option<Batch>> {
        let Some(part_reader) = &mut self.current_reader else {
            // Report MemScanMetrics before returning None
            self.report_mem_scan_metrics();
            return Ok(None);
        };

        debug_assert!(part_reader.is_valid());
        let batch = part_reader.convert_current_batch()?;
        part_reader.next()?;
        if part_reader.is_valid() {
            self.metrics.rows_fetched += batch.num_rows();
            self.metrics.batches_fetched += 1;
            let mut batch = batch;
            batch.filter_by_sequence(self.sequence)?;
            return Ok(Some(batch));
        }

        // Safety: current reader is Some.
        let part_reader = self.current_reader.take().unwrap();
        let context = part_reader.into_context();
        self.fetch_next_partition(context)?;

        self.metrics.rows_fetched += batch.num_rows();
        self.metrics.batches_fetched += 1;
        let mut batch = batch;
        batch.filter_by_sequence(self.sequence)?;
        Ok(Some(batch))
    }
}
