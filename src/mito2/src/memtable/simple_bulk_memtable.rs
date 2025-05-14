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

use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};

use api::v1::OpType;
use datatypes::vectors::Helper;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{ColumnId, SequenceNumber};
use table::predicate::Predicate;

use crate::flush::WriteBufferManagerRef;
use crate::memtable::bulk::part::BulkPart;
use crate::memtable::key_values::KeyValue;
use crate::memtable::stats::WriteMetrics;
use crate::memtable::time_series::{Series, Values};
use crate::memtable::{
    AllocTracker, BoxedBatchIterator, IterBuilder, KeyValues, Memtable, MemtableId, MemtableRange,
    MemtableRangeContext, MemtableRanges, MemtableRef, MemtableStats,
};
use crate::read::dedup::LastNonNullIter;
use crate::read::scan_region::PredicateGroup;
use crate::read::Batch;
use crate::region::options::MergeMode;
use crate::{error, metrics};

pub struct SimpleBulkMemtable {
    id: MemtableId,
    region_metadata: RegionMetadataRef,
    alloc_tracker: AllocTracker,
    max_timestamp: AtomicI64,
    min_timestamp: AtomicI64,
    max_sequence: AtomicU64,
    dedup: bool,
    merge_mode: MergeMode,
    num_rows: AtomicUsize,
    series: RwLock<Series>,
}

impl SimpleBulkMemtable {
    pub(crate) fn new(
        id: MemtableId,
        region_metadata: RegionMetadataRef,
        write_buffer_manager: Option<WriteBufferManagerRef>,
        dedup: bool,
        merge_mode: MergeMode,
    ) -> Self {
        let dedup = if merge_mode == MergeMode::LastNonNull {
            false
        } else {
            dedup
        };
        let series = RwLock::new(Series::new(&region_metadata));

        Self {
            id,
            region_metadata,
            alloc_tracker: AllocTracker::new(write_buffer_manager),
            max_timestamp: AtomicI64::new(i64::MIN),
            min_timestamp: AtomicI64::new(i64::MAX),
            max_sequence: AtomicU64::new(0),
            dedup,
            merge_mode,
            num_rows: AtomicUsize::new(0),
            series,
        }
    }

    fn build_projection(&self, projection: Option<&[ColumnId]>) -> HashSet<ColumnId> {
        if let Some(projection) = projection {
            projection.iter().copied().collect()
        } else {
            self.region_metadata
                .field_columns()
                .map(|c| c.column_id)
                .collect()
        }
    }

    fn create_iter(
        &self,
        projection: Option<&[ColumnId]>,
        sequence: Option<SequenceNumber>,
    ) -> error::Result<BatchIterBuilder> {
        let mut series = self.series.write().unwrap();

        let values = if series.is_empty() {
            None
        } else {
            Some(series.compact(&self.region_metadata)?.clone())
        };

        let projection = self.build_projection(projection);

        Ok(BatchIterBuilder {
            region_metadata: self.region_metadata.clone(),
            values,
            projection,
            dedup: self.dedup,
            sequence,
            merge_mode: self.merge_mode,
        })
    }

    fn write_key_value(&self, kv: KeyValue, stats: &mut WriteMetrics) {
        let ts = kv.timestamp();
        let sequence = kv.sequence();
        let op_type = kv.op_type();
        let mut series = self.series.write().unwrap();
        let size = series.push(ts, sequence, op_type, kv.fields());
        stats.value_bytes += size;
        // safety: timestamp of kv must be both present and a valid timestamp value.
        let ts = kv.timestamp().as_timestamp().unwrap().unwrap().value();
        stats.min_ts = stats.min_ts.min(ts);
        stats.max_ts = stats.max_ts.max(ts);
    }

    /// Updates memtable stats.
    fn update_stats(&self, stats: WriteMetrics) {
        self.alloc_tracker
            .on_allocation(stats.key_bytes + stats.value_bytes);
        self.num_rows.fetch_add(stats.num_rows, Ordering::SeqCst);
        self.max_timestamp.fetch_max(stats.max_ts, Ordering::SeqCst);
        self.min_timestamp.fetch_min(stats.min_ts, Ordering::SeqCst);
        self.max_sequence
            .fetch_max(stats.max_sequence, Ordering::SeqCst);
    }

    #[cfg(test)]
    fn schema(&self) -> &RegionMetadataRef {
        &self.region_metadata
    }
}

impl Debug for SimpleBulkMemtable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimpleBulkMemtable").finish()
    }
}

impl Memtable for SimpleBulkMemtable {
    fn id(&self) -> MemtableId {
        self.id
    }

    fn write(&self, kvs: &KeyValues) -> error::Result<()> {
        let mut stats = WriteMetrics::default();
        let max_sequence = kvs.max_sequence();
        for kv in kvs.iter() {
            self.write_key_value(kv, &mut stats);
        }
        stats.max_sequence = max_sequence;
        stats.num_rows = kvs.num_rows();
        self.update_stats(stats);
        Ok(())
    }

    fn write_one(&self, kv: KeyValue) -> error::Result<()> {
        debug_assert_eq!(0, kv.num_primary_keys());
        let mut stats = WriteMetrics::default();
        self.write_key_value(kv, &mut stats);
        stats.num_rows = 1;
        stats.max_sequence = kv.sequence();
        self.update_stats(stats);
        Ok(())
    }

    fn write_bulk(&self, part: BulkPart) -> error::Result<()> {
        let rb = &part.batch;

        let ts = Helper::try_into_vector(
            rb.column_by_name(&self.region_metadata.time_index_column().column_schema.name)
                .with_context(|| error::InvalidRequestSnafu {
                    region_id: self.region_metadata.region_id,
                    reason: "Timestamp not found",
                })?,
        )
        .context(error::ConvertVectorSnafu)?;

        let sequence = part.sequence;

        let fields: Vec<_> = self
            .region_metadata
            .field_columns()
            .map(|f| {
                let array = rb.column_by_name(&f.column_schema.name).ok_or_else(|| {
                    error::InvalidRequestSnafu {
                        region_id: self.region_metadata.region_id,
                        reason: format!("Column {} not found", f.column_schema.name),
                    }
                    .build()
                })?;
                Helper::try_into_vector(array).context(error::ConvertVectorSnafu)
            })
            .collect::<error::Result<Vec<_>>>()?;

        let mut series = self.series.write().unwrap();
        let extend_timer = metrics::REGION_WORKER_HANDLE_WRITE_ELAPSED
            .with_label_values(&["bulk_extend"])
            .start_timer();
        series.extend(ts, OpType::Put as u8, sequence, fields.into_iter())?;
        extend_timer.observe_duration();

        self.update_stats(WriteMetrics {
            key_bytes: 0,
            value_bytes: part.estimated_size(),
            min_ts: part.min_ts,
            max_ts: part.max_ts,
            num_rows: part.num_rows,
            max_sequence: sequence,
        });
        Ok(())
    }

    fn iter(
        &self,
        projection: Option<&[ColumnId]>,
        _predicate: Option<Predicate>,
        sequence: Option<SequenceNumber>,
    ) -> error::Result<BoxedBatchIterator> {
        let iter = self.create_iter(projection, sequence)?.build()?;

        if self.merge_mode == MergeMode::LastNonNull {
            let iter = LastNonNullIter::new(iter);
            Ok(Box::new(iter))
        } else {
            Ok(Box::new(iter))
        }
    }

    fn ranges(
        &self,
        projection: Option<&[ColumnId]>,
        predicate: PredicateGroup,
        sequence: Option<SequenceNumber>,
    ) -> error::Result<MemtableRanges> {
        let builder = Box::new(self.create_iter(projection, sequence).unwrap());

        let context = Arc::new(MemtableRangeContext::new(self.id, builder, predicate));
        Ok(MemtableRanges {
            ranges: [(0, MemtableRange::new(context))].into(),
            stats: self.stats(),
        })
    }

    fn is_empty(&self) -> bool {
        self.series.read().unwrap().is_empty()
    }

    fn freeze(&self) -> error::Result<()> {
        self.series.write().unwrap().freeze(&self.region_metadata);
        Ok(())
    }

    fn stats(&self) -> MemtableStats {
        let estimated_bytes = self.alloc_tracker.bytes_allocated();
        let num_rows = self.num_rows.load(Ordering::Relaxed);
        if num_rows == 0 {
            // no rows ever written
            return MemtableStats {
                estimated_bytes,
                time_range: None,
                num_rows: 0,
                num_ranges: 0,
                max_sequence: 0,
            };
        }
        let ts_type = self
            .region_metadata
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
            num_rows,
            num_ranges: 1,
            max_sequence: self.max_sequence.load(Ordering::Relaxed),
        }
    }

    fn fork(&self, id: MemtableId, metadata: &RegionMetadataRef) -> MemtableRef {
        Arc::new(Self::new(
            id,
            metadata.clone(),
            self.alloc_tracker.write_buffer_manager(),
            self.dedup,
            self.merge_mode,
        ))
    }
}

#[derive(Clone)]
struct BatchIterBuilder {
    region_metadata: RegionMetadataRef,
    values: Option<Values>,
    projection: HashSet<ColumnId>,
    sequence: Option<SequenceNumber>,
    dedup: bool,
    merge_mode: MergeMode,
}

impl IterBuilder for BatchIterBuilder {
    fn build(&self) -> error::Result<BoxedBatchIterator> {
        let Some(values) = self.values.clone() else {
            return Ok(Box::new(Iter { batch: None }));
        };

        let maybe_batch = values
            .to_batch(&[], &self.region_metadata, &self.projection, self.dedup)
            .and_then(|mut b| {
                b.filter_by_sequence(self.sequence)?;
                Ok(b)
            })
            .map(Some)
            .transpose();

        let iter = Iter { batch: maybe_batch };

        if self.merge_mode == MergeMode::LastNonNull {
            Ok(Box::new(LastNonNullIter::new(iter)))
        } else {
            Ok(Box::new(iter))
        }
    }
}

struct Iter {
    batch: Option<error::Result<Batch>>,
}

impl Iterator for Iter {
    type Item = error::Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        self.batch.take()
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::value::ValueData;
    use api::v1::{Mutation, OpType, Row, Rows, SemanticType};
    use common_recordbatch::DfRecordBatch;
    use common_time::Timestamp;
    use datatypes::arrow::array::{ArrayRef, Float64Array, TimestampMillisecondArray};
    use datatypes::arrow_array::StringArray;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::prelude::{ScalarVector, Vector};
    use datatypes::schema::ColumnSchema;
    use datatypes::value::Value;
    use datatypes::vectors::TimestampMillisecondVector;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::SequenceNumber;

    use super::*;
    use crate::region::options::MergeMode;
    use crate::test_util::column_metadata_to_column_schema;

    fn new_test_metadata() -> RegionMetadataRef {
        let mut builder = RegionMetadataBuilder::new(1.into());
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("f1", ConcreteDataType::float64_datatype(), true),
                semantic_type: SemanticType::Field,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("f2", ConcreteDataType::string_datatype(), true),
                semantic_type: SemanticType::Field,
                column_id: 3,
            });
        Arc::new(builder.build().unwrap())
    }

    fn new_test_memtable(dedup: bool, merge_mode: MergeMode) -> SimpleBulkMemtable {
        SimpleBulkMemtable::new(1, new_test_metadata(), None, dedup, merge_mode)
    }

    fn build_key_values(
        metadata: &RegionMetadataRef,
        sequence: SequenceNumber,
        row_values: &[(i64, f64, String)],
    ) -> KeyValues {
        let column_schemas: Vec<_> = metadata
            .column_metadatas
            .iter()
            .map(column_metadata_to_column_schema)
            .collect();

        let rows: Vec<_> = row_values
            .iter()
            .map(|(ts, f1, f2)| Row {
                values: vec![
                    api::v1::Value {
                        value_data: Some(ValueData::TimestampMillisecondValue(*ts)),
                    },
                    api::v1::Value {
                        value_data: Some(ValueData::F64Value(*f1)),
                    },
                    api::v1::Value {
                        value_data: Some(ValueData::StringValue(f2.clone())),
                    },
                ],
            })
            .collect();
        let mutation = Mutation {
            op_type: OpType::Put as i32,
            sequence,
            rows: Some(Rows {
                schema: column_schemas,
                rows,
            }),
            write_hint: None,
        };
        KeyValues::new(metadata, mutation).unwrap()
    }

    #[test]
    fn test_write_and_iter() {
        let memtable = new_test_memtable(false, MergeMode::LastRow);
        memtable
            .write(&build_key_values(
                &memtable.region_metadata,
                0,
                &[(1, 1.0, "a".to_string())],
            ))
            .unwrap();
        memtable
            .write(&build_key_values(
                &memtable.region_metadata,
                1,
                &[(2, 2.0, "b".to_string())],
            ))
            .unwrap();

        let mut iter = memtable.iter(None, None, None).unwrap();
        let batch = iter.next().unwrap().unwrap();
        assert_eq!(2, batch.num_rows());
        assert_eq!(2, batch.fields().len());
        let ts_v = batch
            .timestamps()
            .as_any()
            .downcast_ref::<TimestampMillisecondVector>()
            .unwrap();
        assert_eq!(Value::Timestamp(Timestamp::new_millisecond(1)), ts_v.get(0));
        assert_eq!(Value::Timestamp(Timestamp::new_millisecond(2)), ts_v.get(1));
    }

    #[test]
    fn test_projection() {
        let memtable = new_test_memtable(false, MergeMode::LastRow);
        memtable
            .write(&build_key_values(
                &memtable.region_metadata,
                0,
                &[(1, 1.0, "a".to_string())],
            ))
            .unwrap();

        let mut iter = memtable.iter(None, None, None).unwrap();
        let batch = iter.next().unwrap().unwrap();
        assert_eq!(1, batch.num_rows());
        assert_eq!(2, batch.fields().len());

        let ts_v = batch
            .timestamps()
            .as_any()
            .downcast_ref::<TimestampMillisecondVector>()
            .unwrap();
        assert_eq!(Value::Timestamp(Timestamp::new_millisecond(1)), ts_v.get(0));

        // Only project column 2 (f1)
        let projection = vec![2];
        let mut iter = memtable.iter(Some(&projection), None, None).unwrap();
        let batch = iter.next().unwrap().unwrap();

        assert_eq!(1, batch.num_rows());
        assert_eq!(1, batch.fields().len()); // only f1
        assert_eq!(2, batch.fields()[0].column_id);
    }

    #[test]
    fn test_dedup() {
        let memtable = new_test_memtable(true, MergeMode::LastRow);
        memtable
            .write(&build_key_values(
                &memtable.region_metadata,
                0,
                &[(1, 1.0, "a".to_string())],
            ))
            .unwrap();
        memtable
            .write(&build_key_values(
                &memtable.region_metadata,
                1,
                &[(1, 2.0, "b".to_string())],
            ))
            .unwrap();
        let mut iter = memtable.iter(None, None, None).unwrap();
        let batch = iter.next().unwrap().unwrap();

        assert_eq!(1, batch.num_rows()); // deduped to 1 row
        assert_eq!(2.0, batch.fields()[0].data.get(0).as_f64_lossy().unwrap()); // last write wins
    }

    #[test]
    fn test_write_one() {
        let memtable = new_test_memtable(false, MergeMode::LastRow);
        let kvs = build_key_values(&memtable.region_metadata, 0, &[(1, 1.0, "a".to_string())]);
        let kv = kvs.iter().next().unwrap();
        memtable.write_one(kv).unwrap();

        let mut iter = memtable.iter(None, None, None).unwrap();
        let batch = iter.next().unwrap().unwrap();
        assert_eq!(1, batch.num_rows());
    }

    #[test]
    fn test_write_bulk() {
        let memtable = new_test_memtable(false, MergeMode::LastRow);
        let arrow_schema = memtable.schema().schema.arrow_schema().clone();
        let arrays = vec![
            Arc::new(TimestampMillisecondArray::from(vec![1, 2])) as ArrayRef,
            Arc::new(Float64Array::from(vec![1.0, 2.0])) as ArrayRef,
            Arc::new(StringArray::from(vec!["a", "b"])) as ArrayRef,
        ];
        let rb = DfRecordBatch::try_new(arrow_schema, arrays).unwrap();

        let part = BulkPart {
            batch: rb,
            sequence: 1,
            min_ts: 1,
            max_ts: 2,
            num_rows: 2,
            timestamp_index: 0,
        };
        memtable.write_bulk(part).unwrap();

        let mut iter = memtable.iter(None, None, None).unwrap();
        let batch = iter.next().unwrap().unwrap();
        assert_eq!(2, batch.num_rows());

        let stats = memtable.stats();
        assert_eq!(1, stats.max_sequence);
        assert_eq!(2, stats.num_rows);
        assert_eq!(
            Some((Timestamp::new_millisecond(1), Timestamp::new_millisecond(2))),
            stats.time_range
        );

        let kvs = build_key_values(&memtable.region_metadata, 2, &[(3, 3.0, "c".to_string())]);
        memtable.write(&kvs).unwrap();
        let mut iter = memtable.iter(None, None, None).unwrap();
        let batch = iter.next().unwrap().unwrap();
        assert_eq!(3, batch.num_rows());
        assert_eq!(
            vec![1, 2, 3],
            batch
                .timestamps()
                .as_any()
                .downcast_ref::<TimestampMillisecondVector>()
                .unwrap()
                .iter_data()
                .map(|t| { t.unwrap().0.value() })
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_is_empty() {
        let memtable = new_test_memtable(false, MergeMode::LastRow);
        assert!(memtable.is_empty());

        memtable
            .write(&build_key_values(
                &memtable.region_metadata,
                0,
                &[(1, 1.0, "a".to_string())],
            ))
            .unwrap();
        assert!(!memtable.is_empty());
    }

    #[test]
    fn test_stats() {
        let memtable = new_test_memtable(false, MergeMode::LastRow);
        let stats = memtable.stats();
        assert_eq!(0, stats.num_rows);
        assert!(stats.time_range.is_none());

        memtable
            .write(&build_key_values(
                &memtable.region_metadata,
                0,
                &[(1, 1.0, "a".to_string())],
            ))
            .unwrap();
        let stats = memtable.stats();
        assert_eq!(1, stats.num_rows);
        assert!(stats.time_range.is_some());
    }

    #[test]
    fn test_fork() {
        let memtable = new_test_memtable(false, MergeMode::LastRow);
        memtable
            .write(&build_key_values(
                &memtable.region_metadata,
                0,
                &[(1, 1.0, "a".to_string())],
            ))
            .unwrap();

        let forked = memtable.fork(2, &memtable.region_metadata);
        assert!(forked.is_empty());
    }

    #[test]
    fn test_sequence_filter() {
        let memtable = new_test_memtable(false, MergeMode::LastRow);
        memtable
            .write(&build_key_values(
                &memtable.region_metadata,
                0,
                &[(1, 1.0, "a".to_string())],
            ))
            .unwrap();
        memtable
            .write(&build_key_values(
                &memtable.region_metadata,
                1,
                &[(2, 2.0, "b".to_string())],
            ))
            .unwrap();

        // Filter with sequence 0 should only return first write
        let mut iter = memtable.iter(None, None, Some(0)).unwrap();
        let batch = iter.next().unwrap().unwrap();
        assert_eq!(1, batch.num_rows());
        assert_eq!(1.0, batch.fields()[0].data.get(0).as_f64_lossy().unwrap());
    }
}
