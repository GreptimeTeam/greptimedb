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

//! Memtable implementation based on a partition tree.

pub(crate) mod data;
mod dedup;
mod dict;
mod merger;
mod partition;
mod shard;
mod shard_builder;
mod tree;

use std::fmt;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::Arc;

use common_base::readable_size::ReadableSize;
use mito_codec::key_values::KeyValue;
use mito_codec::row_converter::{build_primary_key_codec, PrimaryKeyCodec};
use serde::{Deserialize, Serialize};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{ColumnId, SequenceNumber};
use table::predicate::Predicate;

use crate::error::{Result, UnsupportedOperationSnafu};
use crate::flush::WriteBufferManagerRef;
use crate::memtable::bulk::part::BulkPart;
use crate::memtable::partition_tree::tree::PartitionTree;
use crate::memtable::stats::WriteMetrics;
use crate::memtable::{
    AllocTracker, BoxedBatchIterator, IterBuilder, KeyValues, MemScanMetrics, Memtable,
    MemtableBuilder, MemtableId, MemtableRange, MemtableRangeContext, MemtableRanges, MemtableRef,
    MemtableStats, PredicateGroup,
};
use crate::region::options::MergeMode;

/// Use `1/DICTIONARY_SIZE_FACTOR` of OS memory as dictionary size.
pub(crate) const DICTIONARY_SIZE_FACTOR: u64 = 8;
pub(crate) const DEFAULT_MAX_KEYS_PER_SHARD: usize = 8192;
pub(crate) const DEFAULT_FREEZE_THRESHOLD: usize = 131072;

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

// TODO(yingwen): `fork_dictionary_bytes` is per region option, if we have multiple partition tree
// memtable then we will use a lot memory. We should find a better way to control the
// dictionary size.
/// Config for the partition tree memtable.
#[derive(Debug, Clone, Serialize, Deserialize, PartialEq, Eq)]
#[serde(default)]
pub struct PartitionTreeConfig {
    /// Max keys in an index shard.
    pub index_max_keys_per_shard: usize,
    /// Number of rows to freeze a data part.
    pub data_freeze_threshold: usize,
    /// Whether to delete duplicates rows.
    ///
    /// Skips deserializing as it should be determined by whether the
    /// table is append only.
    #[serde(skip_deserializing)]
    pub dedup: bool,
    /// Total bytes of dictionary to keep in fork.
    pub fork_dictionary_bytes: ReadableSize,
    /// Merge mode of the tree.
    #[serde(skip_deserializing)]
    pub merge_mode: MergeMode,
}

impl Default for PartitionTreeConfig {
    fn default() -> Self {
        let mut fork_dictionary_bytes = ReadableSize::mb(512);
        if let Some(sys_memory) = common_config::utils::get_sys_total_memory() {
            let adjust_dictionary_bytes =
                std::cmp::min(sys_memory / DICTIONARY_SIZE_FACTOR, fork_dictionary_bytes);
            if adjust_dictionary_bytes.0 > 0 {
                fork_dictionary_bytes = adjust_dictionary_bytes;
            }
        }

        Self {
            index_max_keys_per_shard: 8192,
            data_freeze_threshold: 131072,
            dedup: true,
            fork_dictionary_bytes,
            merge_mode: MergeMode::LastRow,
        }
    }
}

/// Memtable based on a partition tree.
pub struct PartitionTreeMemtable {
    id: MemtableId,
    tree: Arc<PartitionTree>,
    alloc_tracker: AllocTracker,
    max_timestamp: AtomicI64,
    min_timestamp: AtomicI64,
    max_sequence: AtomicU64,
    /// Total written rows in memtable. This also includes deleted and duplicated rows.
    num_rows: AtomicUsize,
}

impl fmt::Debug for PartitionTreeMemtable {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PartitionTreeMemtable")
            .field("id", &self.id)
            .finish()
    }
}

impl Memtable for PartitionTreeMemtable {
    fn id(&self) -> MemtableId {
        self.id
    }

    fn write(&self, kvs: &KeyValues) -> Result<()> {
        if kvs.is_empty() {
            return Ok(());
        }

        // TODO(yingwen): Validate schema while inserting rows.

        let mut metrics = WriteMetrics::default();
        let mut pk_buffer = Vec::new();
        // Ensures the memtable always updates stats.
        let res = self.tree.write(kvs, &mut pk_buffer, &mut metrics);

        if res.is_ok() {
            metrics.max_sequence = kvs.max_sequence();
            metrics.num_rows = kvs.num_rows();
            self.update_stats(&metrics);
        }
        res
    }

    fn write_one(&self, key_value: KeyValue) -> Result<()> {
        let mut metrics = WriteMetrics::default();
        let mut pk_buffer = Vec::new();
        // Ensures the memtable always updates stats.
        let res = self.tree.write_one(key_value, &mut pk_buffer, &mut metrics);

        // update max_sequence
        if res.is_ok() {
            metrics.max_sequence = metrics.max_sequence.max(key_value.sequence());
            metrics.num_rows = 1;
            self.update_stats(&metrics);
        }
        res
    }

    fn write_bulk(&self, _part: BulkPart) -> Result<()> {
        UnsupportedOperationSnafu {
            err_msg: "PartitionTreeMemtable does not support write_bulk",
        }
        .fail()
    }

    #[cfg(any(test, feature = "test"))]
    fn iter(
        &self,
        projection: Option<&[ColumnId]>,
        predicate: Option<Predicate>,
        sequence: Option<SequenceNumber>,
    ) -> Result<BoxedBatchIterator> {
        self.tree.read(projection, predicate, sequence, None)
    }

    fn ranges(
        &self,
        projection: Option<&[ColumnId]>,
        predicate: PredicateGroup,
        sequence: Option<SequenceNumber>,
    ) -> Result<MemtableRanges> {
        let projection = projection.map(|ids| ids.to_vec());
        let builder = Box::new(PartitionTreeIterBuilder {
            tree: self.tree.clone(),
            projection,
            predicate: predicate.predicate().cloned(),
            sequence,
        });
        let context = Arc::new(MemtableRangeContext::new(self.id, builder, predicate));

        let stats = self.stats();
        Ok(MemtableRanges {
            ranges: [(0, MemtableRange::new(context, stats.num_rows))].into(),
            stats,
        })
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
                num_rows: 0,
                num_ranges: 0,
                max_sequence: 0,
                series_count: 0,
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
        let series_count = self.tree.series_count();
        MemtableStats {
            estimated_bytes,
            time_range: Some((min_timestamp, max_timestamp)),
            num_rows: self.num_rows.load(Ordering::Relaxed),
            num_ranges: 1,
            max_sequence: self.max_sequence.load(Ordering::Relaxed),
            series_count,
        }
    }

    fn fork(&self, id: MemtableId, metadata: &RegionMetadataRef) -> MemtableRef {
        let tree = self.tree.fork(metadata.clone());

        let memtable = PartitionTreeMemtable::with_tree(id, tree);
        Arc::new(memtable)
    }
}

impl PartitionTreeMemtable {
    /// Returns a new memtable.
    pub fn new(
        id: MemtableId,
        row_codec: Arc<dyn PrimaryKeyCodec>,
        metadata: RegionMetadataRef,
        write_buffer_manager: Option<WriteBufferManagerRef>,
        config: &PartitionTreeConfig,
    ) -> Self {
        Self::with_tree(
            id,
            PartitionTree::new(row_codec, metadata, config, write_buffer_manager.clone()),
        )
    }

    /// Creates a mutable memtable from the tree.
    ///
    /// It also adds the bytes used by shared parts (e.g. index) to the memory usage.
    fn with_tree(id: MemtableId, tree: PartitionTree) -> Self {
        let alloc_tracker = AllocTracker::new(tree.write_buffer_manager());

        Self {
            id,
            tree: Arc::new(tree),
            alloc_tracker,
            max_timestamp: AtomicI64::new(i64::MIN),
            min_timestamp: AtomicI64::new(i64::MAX),
            num_rows: AtomicUsize::new(0),
            max_sequence: AtomicU64::new(0),
        }
    }

    /// Updates stats of the memtable.
    fn update_stats(&self, metrics: &WriteMetrics) {
        // Only let the tracker tracks value bytes.
        self.alloc_tracker.on_allocation(metrics.value_bytes);
        self.max_timestamp
            .fetch_max(metrics.max_ts, Ordering::SeqCst);
        self.min_timestamp
            .fetch_min(metrics.min_ts, Ordering::SeqCst);
        self.num_rows.fetch_add(metrics.num_rows, Ordering::SeqCst);
        self.max_sequence
            .fetch_max(metrics.max_sequence, Ordering::SeqCst);
    }

    #[cfg(any(test, feature = "test"))]
    pub fn iter(
        &self,
        projection: Option<&[ColumnId]>,
        predicate: Option<Predicate>,
        sequence: Option<SequenceNumber>,
    ) -> Result<BoxedBatchIterator> {
        self.tree.read(projection, predicate, sequence, None)
    }
}

/// Builder to build a [PartitionTreeMemtable].
#[derive(Debug, Default)]
pub struct PartitionTreeMemtableBuilder {
    config: PartitionTreeConfig,
    write_buffer_manager: Option<WriteBufferManagerRef>,
}

impl PartitionTreeMemtableBuilder {
    /// Creates a new builder with specific `write_buffer_manager`.
    pub fn new(
        config: PartitionTreeConfig,
        write_buffer_manager: Option<WriteBufferManagerRef>,
    ) -> Self {
        Self {
            config,
            write_buffer_manager,
        }
    }
}

impl MemtableBuilder for PartitionTreeMemtableBuilder {
    fn build(&self, id: MemtableId, metadata: &RegionMetadataRef) -> MemtableRef {
        let codec = build_primary_key_codec(metadata);
        Arc::new(PartitionTreeMemtable::new(
            id,
            codec,
            metadata.clone(),
            self.write_buffer_manager.clone(),
            &self.config,
        ))
    }

    fn use_bulk_insert(&self, _metadata: &RegionMetadataRef) -> bool {
        false
    }
}

struct PartitionTreeIterBuilder {
    tree: Arc<PartitionTree>,
    projection: Option<Vec<ColumnId>>,
    predicate: Option<Predicate>,
    sequence: Option<SequenceNumber>,
}

impl IterBuilder for PartitionTreeIterBuilder {
    fn build(&self, metrics: Option<MemScanMetrics>) -> Result<BoxedBatchIterator> {
        self.tree.read(
            self.projection.as_deref(),
            self.predicate.clone(),
            self.sequence,
            metrics,
        )
    }
}

#[cfg(test)]
mod tests {
    use std::collections::HashMap;
    use std::sync::Arc;

    use api::v1::value::ValueData;
    use api::v1::{Mutation, OpType, Row, Rows, SemanticType};
    use common_time::Timestamp;
    use datafusion_common::Column;
    use datafusion_expr::{BinaryExpr, Expr, Literal, Operator};
    use datatypes::data_type::ConcreteDataType;
    use datatypes::prelude::Vector;
    use datatypes::scalars::ScalarVector;
    use datatypes::schema::ColumnSchema;
    use datatypes::value::Value;
    use datatypes::vectors::{Int64Vector, StringVector};
    use mito_codec::row_converter::DensePrimaryKeyCodec;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::RegionId;

    use super::*;
    use crate::test_util::memtable_util::{
        self, collect_iter_timestamps, region_metadata_to_row_schema,
    };

    #[test]
    fn test_memtable_sorted_input() {
        write_iter_sorted_input(true);
        write_iter_sorted_input(false);
    }

    fn write_iter_sorted_input(has_pk: bool) {
        let metadata = if has_pk {
            Arc::new(memtable_util::metadata_with_primary_key(vec![1, 0], true))
        } else {
            Arc::new(memtable_util::metadata_with_primary_key(vec![], false))
        };
        let timestamps = (0..100).collect::<Vec<_>>();
        let kvs =
            memtable_util::build_key_values(&metadata, "hello".to_string(), 42, &timestamps, 1);
        let codec = Arc::new(DensePrimaryKeyCodec::new(&metadata));
        let memtable = PartitionTreeMemtable::new(
            1,
            codec,
            metadata.clone(),
            None,
            &PartitionTreeConfig::default(),
        );
        memtable.write(&kvs).unwrap();

        let expected_ts = kvs
            .iter()
            .map(|kv| kv.timestamp().as_timestamp().unwrap().unwrap().value())
            .collect::<Vec<_>>();

        let iter = memtable.iter(None, None, None).unwrap();
        let read = collect_iter_timestamps(iter);
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
            Arc::new(memtable_util::metadata_with_primary_key(vec![1, 0], true))
        } else {
            Arc::new(memtable_util::metadata_with_primary_key(vec![], false))
        };
        let codec = Arc::new(DensePrimaryKeyCodec::new(&metadata));
        let memtable = PartitionTreeMemtable::new(
            1,
            codec,
            metadata.clone(),
            None,
            &PartitionTreeConfig::default(),
        );

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

        let iter = memtable.iter(None, None, None).unwrap();
        let read = collect_iter_timestamps(iter);
        assert_eq!(vec![0, 1, 2, 3, 4, 5, 6, 7], read);

        let iter = memtable.iter(None, None, None).unwrap();
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
            Arc::new(memtable_util::metadata_with_primary_key(vec![1, 0], true))
        } else {
            Arc::new(memtable_util::metadata_with_primary_key(vec![], false))
        };
        // Try to build a memtable via the builder.
        let memtable = PartitionTreeMemtableBuilder::new(PartitionTreeConfig::default(), None)
            .build(1, &metadata);

        let expect = (0..100).collect::<Vec<_>>();
        let kvs = memtable_util::build_key_values(&metadata, "hello".to_string(), 10, &expect, 1);
        memtable.write(&kvs).unwrap();
        let iter = memtable.iter(Some(&[3]), None, None).unwrap();

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
        let metadata = Arc::new(memtable_util::metadata_with_primary_key(vec![1, 0], true));
        let codec = Arc::new(DensePrimaryKeyCodec::new(&metadata));
        let memtable = PartitionTreeMemtable::new(
            1,
            codec,
            metadata.clone(),
            None,
            &PartitionTreeConfig {
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
        let iter = memtable.iter(None, None, None).unwrap();
        let read = collect_iter_timestamps(iter);
        assert_eq!(expect, read);
    }

    #[test]
    fn test_memtable_filter() {
        let metadata = Arc::new(memtable_util::metadata_with_primary_key(vec![0, 1], false));
        // Try to build a memtable via the builder.
        let memtable = PartitionTreeMemtableBuilder::new(
            PartitionTreeConfig {
                index_max_keys_per_shard: 40,
                ..Default::default()
            },
            None,
        )
        .build(1, &metadata);

        for i in 0..100 {
            let timestamps: Vec<_> = (0..10).map(|v| i as i64 * 1000 + v).collect();
            let kvs =
                memtable_util::build_key_values(&metadata, "hello".to_string(), i, &timestamps, 1);
            memtable.write(&kvs).unwrap();
        }

        for i in 0..100 {
            let timestamps: Vec<_> = (0..10).map(|v| i as i64 * 1000 + v).collect();
            let expr = Expr::BinaryExpr(BinaryExpr {
                left: Box::new(Expr::Column(Column::from_name("k1"))),
                op: Operator::Eq,
                right: Box::new((i as u32).lit()),
            });
            let iter = memtable
                .iter(None, Some(Predicate::new(vec![expr])), None)
                .unwrap();
            let read = collect_iter_timestamps(iter);
            assert_eq!(timestamps, read);
        }
    }

    #[test]
    fn test_deserialize_config() {
        let config = PartitionTreeConfig {
            dedup: false,
            ..Default::default()
        };
        // Creates a json with dedup = false.
        let json = serde_json::to_string(&config).unwrap();
        let config: PartitionTreeConfig = serde_json::from_str(&json).unwrap();
        assert!(config.dedup);
        assert_eq!(PartitionTreeConfig::default(), config);
    }

    fn metadata_for_metric_engine() -> RegionMetadataRef {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(123, 456));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "__table_id",
                    ConcreteDataType::uint32_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 2147483652,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "__tsid",
                    ConcreteDataType::uint64_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 2147483651,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "test_label",
                    ConcreteDataType::string_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "greptime_timestamp",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 0,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "greptime_value",
                    ConcreteDataType::float64_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Field,
                column_id: 1,
            })
            .primary_key(vec![2147483652, 2147483651, 2]);
        let region_metadata = builder.build().unwrap();
        Arc::new(region_metadata)
    }

    fn build_key_values(
        metadata: RegionMetadataRef,
        labels: &[&str],
        table_id: &[u32],
        ts_id: &[u64],
        ts: &[i64],
        values: &[f64],
        sequence: u64,
    ) -> KeyValues {
        let column_schema = region_metadata_to_row_schema(&metadata);

        let rows = ts
            .iter()
            .zip(table_id.iter())
            .zip(ts_id.iter())
            .zip(labels.iter())
            .zip(values.iter())
            .map(|((((ts, table_id), ts_id), label), val)| Row {
                values: vec![
                    api::v1::Value {
                        value_data: Some(ValueData::U32Value(*table_id)),
                    },
                    api::v1::Value {
                        value_data: Some(ValueData::U64Value(*ts_id)),
                    },
                    api::v1::Value {
                        value_data: Some(ValueData::StringValue(label.to_string())),
                    },
                    api::v1::Value {
                        value_data: Some(ValueData::TimestampMillisecondValue(*ts)),
                    },
                    api::v1::Value {
                        value_data: Some(ValueData::F64Value(*val)),
                    },
                ],
            })
            .collect();
        let mutation = api::v1::Mutation {
            op_type: 1,
            sequence,
            rows: Some(Rows {
                schema: column_schema,
                rows,
            }),
            write_hint: None,
        };
        KeyValues::new(metadata.as_ref(), mutation).unwrap()
    }

    #[test]
    fn test_write_freeze() {
        let metadata = metadata_for_metric_engine();
        let memtable = PartitionTreeMemtableBuilder::new(
            PartitionTreeConfig {
                index_max_keys_per_shard: 40,
                ..Default::default()
            },
            None,
        )
        .build(1, &metadata);

        let codec = DensePrimaryKeyCodec::new(&metadata);

        memtable
            .write(&build_key_values(
                metadata.clone(),
                &["daily", "10min", "daily", "10min"],
                &[1025, 1025, 1025, 1025],
                &[
                    16442255374049317291,
                    5686004715529701024,
                    16442255374049317291,
                    5686004715529701024,
                ],
                &[1712070000000, 1712717731000, 1712761200000, 1712761200000],
                &[0.0, 0.0, 0.0, 0.0],
                1,
            ))
            .unwrap();

        memtable.freeze().unwrap();
        let new_memtable = memtable.fork(2, &metadata);

        new_memtable
            .write(&build_key_values(
                metadata.clone(),
                &["10min"],
                &[1025],
                &[5686004715529701024],
                &[1714643131000],
                &[0.1],
                2,
            ))
            .unwrap();

        let mut reader = new_memtable.iter(None, None, None).unwrap();
        let batch = reader.next().unwrap().unwrap();
        let pk = codec.decode(batch.primary_key()).unwrap().into_dense();
        if let Value::String(s) = &pk[2] {
            assert_eq!("10min", s.as_utf8());
        } else {
            unreachable!()
        }
    }

    fn kv_region_metadata() -> RegionMetadataRef {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(123, 456));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 0,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("k", ConcreteDataType::string_datatype(), false),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("v", ConcreteDataType::string_datatype(), false),
                semantic_type: SemanticType::Field,
                column_id: 2,
            })
            .primary_key(vec![1]);
        let region_metadata = builder.build().unwrap();
        Arc::new(region_metadata)
    }

    fn kv_column_schemas() -> Vec<api::v1::ColumnSchema> {
        vec![
            api::v1::ColumnSchema {
                column_name: "ts".to_string(),
                datatype: api::v1::ColumnDataType::TimestampMillisecond as i32,
                semantic_type: SemanticType::Timestamp as i32,
                ..Default::default()
            },
            api::v1::ColumnSchema {
                column_name: "k".to_string(),
                datatype: api::v1::ColumnDataType::String as i32,
                semantic_type: SemanticType::Tag as i32,
                ..Default::default()
            },
            api::v1::ColumnSchema {
                column_name: "v".to_string(),
                datatype: api::v1::ColumnDataType::String as i32,
                semantic_type: SemanticType::Field as i32,
                ..Default::default()
            },
        ]
    }

    fn key_values<T: AsRef<str>>(
        metadata: &RegionMetadataRef,
        keys: impl Iterator<Item = T>,
    ) -> KeyValues {
        let rows = keys
            .map(|c| Row {
                values: vec![
                    api::v1::Value {
                        value_data: Some(ValueData::TimestampMillisecondValue(0)),
                    },
                    api::v1::Value {
                        value_data: Some(ValueData::StringValue(c.as_ref().to_string())),
                    },
                    api::v1::Value {
                        value_data: Some(ValueData::StringValue(c.as_ref().to_string())),
                    },
                ],
            })
            .collect();
        let mutation = Mutation {
            op_type: OpType::Put as i32,
            sequence: 0,
            rows: Some(Rows {
                schema: kv_column_schemas(),
                rows,
            }),
            write_hint: None,
        };
        KeyValues::new(metadata, mutation).unwrap()
    }

    fn collect_kvs(
        iter: BoxedBatchIterator,
        region_meta: &RegionMetadataRef,
    ) -> HashMap<String, String> {
        let decoder = DensePrimaryKeyCodec::new(region_meta);
        let mut res = HashMap::new();
        for v in iter {
            let batch = v.unwrap();
            let values = decoder.decode(batch.primary_key()).unwrap().into_dense();
            let field_vector = batch.fields()[0]
                .data
                .as_any()
                .downcast_ref::<StringVector>()
                .unwrap();
            for row in 0..batch.num_rows() {
                res.insert(
                    values[0].as_string().unwrap(),
                    field_vector.get(row).as_string().unwrap(),
                );
            }
        }
        res
    }

    #[test]
    fn test_reorder_insert_key_values() {
        let metadata = kv_region_metadata();
        let memtable = PartitionTreeMemtableBuilder::new(PartitionTreeConfig::default(), None)
            .build(1, &metadata);

        memtable
            .write(&key_values(&metadata, ('a'..'h').map(|c| c.to_string())))
            .unwrap();
        memtable.freeze().unwrap();
        assert_eq!(
            collect_kvs(memtable.iter(None, None, None).unwrap(), &metadata),
            ('a'..'h').map(|c| (c.to_string(), c.to_string())).collect()
        );
        let forked = memtable.fork(2, &metadata);

        let keys = ["c", "f", "i", "h", "b", "e", "g"];
        forked.write(&key_values(&metadata, keys.iter())).unwrap();
        forked.freeze().unwrap();
        assert_eq!(
            collect_kvs(forked.iter(None, None, None).unwrap(), &metadata),
            keys.iter()
                .map(|c| (c.to_string(), c.to_string()))
                .collect()
        );

        let forked2 = forked.fork(3, &metadata);

        let keys = ["g", "e", "a", "f", "b", "c", "h"];
        forked2.write(&key_values(&metadata, keys.iter())).unwrap();

        let kvs = collect_kvs(forked2.iter(None, None, None).unwrap(), &metadata);
        let expected = keys
            .iter()
            .map(|c| (c.to_string(), c.to_string()))
            .collect::<HashMap<_, _>>();
        assert_eq!(kvs, expected);
    }
}
