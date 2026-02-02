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

#[cfg(any(test, feature = "test"))]
mod test_only;

use std::collections::HashSet;
use std::fmt::{Debug, Formatter};
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use api::v1::OpType;
use datatypes::vectors::Helper;
use mito_codec::key_values::KeyValue;
use rayon::prelude::*;
use snafu::{OptionExt, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;

use crate::flush::WriteBufferManagerRef;
use crate::memtable::bulk::part::BulkPart;
use crate::memtable::stats::WriteMetrics;
use crate::memtable::time_series::Series;
use crate::memtable::{
    AllocTracker, BoxedBatchIterator, IterBuilder, KeyValues, MemScanMetrics, Memtable, MemtableId,
    MemtableRange, MemtableRangeContext, MemtableRanges, MemtableRef, MemtableStats, RangesOptions,
};
use crate::metrics::MEMTABLE_ACTIVE_SERIES_COUNT;
use crate::read::Batch;
use crate::read::dedup::LastNonNullIter;
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

impl Drop for SimpleBulkMemtable {
    fn drop(&mut self) {
        MEMTABLE_ACTIVE_SERIES_COUNT.dec();
    }
}

impl SimpleBulkMemtable {
    pub fn new(
        id: MemtableId,
        region_metadata: RegionMetadataRef,
        write_buffer_manager: Option<WriteBufferManagerRef>,
        dedup: bool,
        merge_mode: MergeMode,
    ) -> Self {
        let series = RwLock::new(Series::with_capacity(&region_metadata, 1024, 8192));

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

    fn write_key_value(&self, kv: KeyValue, stats: &mut WriteMetrics) {
        let ts = kv.timestamp();
        let sequence = kv.sequence();
        let op_type = kv.op_type();
        let mut series = self.series.write().unwrap();
        let size = series.push(ts, sequence, op_type, kv.fields());
        stats.value_bytes += size;
        // safety: timestamp of kv must be both present and a valid timestamp value.
        let ts = kv
            .timestamp()
            .try_into_timestamp()
            .unwrap()
            .unwrap()
            .value();
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
        series.extend(ts, OpType::Put as u8, sequence, fields)?;
        extend_timer.observe_duration();

        self.update_stats(WriteMetrics {
            key_bytes: 0,
            value_bytes: part.estimated_size(),
            min_ts: part.min_timestamp,
            max_ts: part.max_timestamp,
            num_rows: part.num_rows(),
            max_sequence: sequence,
        });
        Ok(())
    }

    #[cfg(any(test, feature = "test"))]
    fn iter(
        &self,
        projection: Option<&[ColumnId]>,
        _predicate: Option<table::predicate::Predicate>,
        sequence: Option<store_api::storage::SequenceRange>,
    ) -> error::Result<BoxedBatchIterator> {
        let iter = self.create_iter(projection, sequence)?.build(None)?;
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
        options: RangesOptions,
    ) -> error::Result<MemtableRanges> {
        let predicate = options.predicate;
        let sequence = options.sequence;
        let start_time = Instant::now();
        let projection = Arc::new(self.build_projection(projection));

        // Use the memtable's overall time range and max sequence for all ranges
        let max_sequence = self.max_sequence.load(Ordering::Relaxed);
        let time_range = {
            let num_rows = self.num_rows.load(Ordering::Relaxed);
            if num_rows > 0 {
                let ts_type = self.region_metadata.time_index_type();
                let max_timestamp =
                    ts_type.create_timestamp(self.max_timestamp.load(Ordering::Relaxed));
                let min_timestamp =
                    ts_type.create_timestamp(self.min_timestamp.load(Ordering::Relaxed));
                Some((min_timestamp, max_timestamp))
            } else {
                None
            }
        };

        let values = self.series.read().unwrap().read_to_values();
        let contexts = values
            .into_par_iter()
            .filter_map(|v| {
                let filtered = match v
                    .to_batch(
                        &[],
                        &self.region_metadata,
                        &projection,
                        self.dedup,
                        self.merge_mode,
                    )
                    .and_then(|mut b| {
                        b.filter_by_sequence(sequence)?;
                        Ok(b)
                    }) {
                    Ok(filtered) => filtered,
                    Err(e) => {
                        return Some(Err(e));
                    }
                };
                if filtered.is_empty() {
                    None
                } else {
                    Some(Ok(filtered))
                }
            })
            .map(|result| {
                result.map(|batch| {
                    let num_rows = batch.num_rows();
                    let estimated_bytes = batch.memory_size();

                    let range_stats = MemtableStats {
                        estimated_bytes,
                        time_range,
                        num_rows,
                        num_ranges: 1,
                        max_sequence,
                        series_count: 1,
                    };

                    let builder = BatchRangeBuilder {
                        batch,
                        merge_mode: self.merge_mode,
                        scan_cost: start_time.elapsed(),
                    };
                    (
                        range_stats,
                        Arc::new(MemtableRangeContext::new(
                            self.id,
                            Box::new(builder),
                            predicate.clone(),
                        )),
                    )
                })
            })
            .collect::<error::Result<Vec<_>>>()?;

        let ranges = contexts
            .into_iter()
            .enumerate()
            .map(|(idx, (range_stats, context))| (idx, MemtableRange::new(context, range_stats)))
            .collect();

        Ok(MemtableRanges { ranges })
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
                series_count: 0,
            };
        }
        let ts_type = self.region_metadata.time_index_type();
        let max_timestamp = ts_type.create_timestamp(self.max_timestamp.load(Ordering::Relaxed));
        let min_timestamp = ts_type.create_timestamp(self.min_timestamp.load(Ordering::Relaxed));
        MemtableStats {
            estimated_bytes,
            time_range: Some((min_timestamp, max_timestamp)),
            num_rows,
            num_ranges: 1,
            max_sequence: self.max_sequence.load(Ordering::Relaxed),
            series_count: 1,
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
pub struct BatchRangeBuilder {
    pub batch: Batch,
    pub merge_mode: MergeMode,
    scan_cost: Duration,
}

impl IterBuilder for BatchRangeBuilder {
    fn build(&self, metrics: Option<MemScanMetrics>) -> error::Result<BoxedBatchIterator> {
        let batch = self.batch.clone();
        if let Some(metrics) = metrics {
            let inner = crate::memtable::MemScanMetricsData {
                total_series: 1,
                num_rows: batch.num_rows(),
                num_batches: 1,
                scan_cost: self.scan_cost,
            };
            metrics.merge_inner(&inner);
        }

        let iter = Iter {
            batch: Some(Ok(batch)),
        };

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

    use api::v1::helper::row;
    use api::v1::value::ValueData;
    use api::v1::{Mutation, OpType, Rows, SemanticType};
    use common_recordbatch::DfRecordBatch;
    use common_time::Timestamp;
    use datatypes::arrow::array::{ArrayRef, Float64Array, RecordBatch, TimestampMillisecondArray};
    use datatypes::arrow_array::StringArray;
    use datatypes::data_type::ConcreteDataType;
    use datatypes::prelude::{ScalarVector, Vector};
    use datatypes::schema::ColumnSchema;
    use datatypes::value::Value;
    use datatypes::vectors::TimestampMillisecondVector;
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::{RegionId, SequenceNumber, SequenceRange};

    use super::*;
    use crate::read;
    use crate::read::dedup::DedupReader;
    use crate::read::merge::MergeReaderBuilder;
    use crate::read::{BatchReader, Source};
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
        op_type: OpType,
    ) -> KeyValues {
        let column_schemas: Vec<_> = metadata
            .column_metadatas
            .iter()
            .map(column_metadata_to_column_schema)
            .collect();

        let rows: Vec<_> = row_values
            .iter()
            .map(|(ts, f1, f2)| {
                row(vec![
                    ValueData::TimestampMillisecondValue(*ts),
                    ValueData::F64Value(*f1),
                    ValueData::StringValue(f2.clone()),
                ])
            })
            .collect();
        let mutation = Mutation {
            op_type: op_type as i32,
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
                OpType::Put,
            ))
            .unwrap();
        memtable
            .write(&build_key_values(
                &memtable.region_metadata,
                1,
                &[(2, 2.0, "b".to_string())],
                OpType::Put,
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
                OpType::Put,
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
                OpType::Put,
            ))
            .unwrap();
        memtable
            .write(&build_key_values(
                &memtable.region_metadata,
                1,
                &[(1, 2.0, "b".to_string())],
                OpType::Put,
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
        let kvs = build_key_values(
            &memtable.region_metadata,
            0,
            &[(1, 1.0, "a".to_string())],
            OpType::Put,
        );
        let kv = kvs.iter().next().unwrap();
        memtable.write_one(kv).unwrap();

        let mut iter = memtable.iter(None, None, None).unwrap();
        let batch = iter.next().unwrap().unwrap();
        assert_eq!(1, batch.num_rows());
    }

    #[tokio::test]
    async fn test_write_dedup() {
        let memtable = new_test_memtable(true, MergeMode::LastRow);
        let kvs = build_key_values(
            &memtable.region_metadata,
            0,
            &[(1, 1.0, "a".to_string())],
            OpType::Put,
        );
        let kv = kvs.iter().next().unwrap();
        memtable.write_one(kv).unwrap();
        memtable.freeze().unwrap();

        let kvs = build_key_values(
            &memtable.region_metadata,
            1,
            &[(1, 1.0, "a".to_string())],
            OpType::Delete,
        );
        let kv = kvs.iter().next().unwrap();
        memtable.write_one(kv).unwrap();

        let ranges = memtable.ranges(None, RangesOptions::default()).unwrap();
        let mut source = vec![];
        for r in ranges.ranges.values() {
            source.push(Source::Iter(r.build_iter().unwrap()));
        }

        let reader = MergeReaderBuilder::from_sources(source)
            .build()
            .await
            .unwrap();

        let mut reader = DedupReader::new(reader, read::dedup::LastRow::new(false), None);
        let mut num_rows = 0;
        while let Some(b) = reader.next_batch().await.unwrap() {
            num_rows += b.num_rows();
        }
        assert_eq!(num_rows, 1);
    }

    #[tokio::test]
    async fn test_delete_only() {
        let memtable = new_test_memtable(true, MergeMode::LastRow);
        let kvs = build_key_values(
            &memtable.region_metadata,
            0,
            &[(1, 1.0, "a".to_string())],
            OpType::Delete,
        );
        let kv = kvs.iter().next().unwrap();
        memtable.write_one(kv).unwrap();
        memtable.freeze().unwrap();

        let ranges = memtable.ranges(None, RangesOptions::default()).unwrap();
        let mut source = vec![];
        for r in ranges.ranges.values() {
            source.push(Source::Iter(r.build_iter().unwrap()));
        }

        let reader = MergeReaderBuilder::from_sources(source)
            .build()
            .await
            .unwrap();

        let mut reader = DedupReader::new(reader, read::dedup::LastRow::new(false), None);
        let mut num_rows = 0;
        while let Some(b) = reader.next_batch().await.unwrap() {
            num_rows += b.num_rows();
            assert_eq!(b.num_rows(), 1);
            assert_eq!(b.op_types().get_data(0).unwrap(), OpType::Delete as u8);
        }
        assert_eq!(num_rows, 1);
    }

    #[tokio::test]
    async fn test_single_range() {
        let memtable = new_test_memtable(true, MergeMode::LastRow);
        let kvs = build_key_values(
            &memtable.region_metadata,
            0,
            &[(1, 1.0, "a".to_string())],
            OpType::Put,
        );
        memtable.write_one(kvs.iter().next().unwrap()).unwrap();

        let kvs = build_key_values(
            &memtable.region_metadata,
            1,
            &[(1, 2.0, "b".to_string())],
            OpType::Put,
        );
        memtable.write_one(kvs.iter().next().unwrap()).unwrap();
        memtable.freeze().unwrap();

        let ranges = memtable.ranges(None, RangesOptions::default()).unwrap();
        assert_eq!(ranges.ranges.len(), 1);
        let range = ranges.ranges.into_values().next().unwrap();
        let mut reader = range.context.builder.build(None).unwrap();

        let mut num_rows = 0;
        while let Some(b) = reader.next().transpose().unwrap() {
            num_rows += b.num_rows();
            assert_eq!(b.fields()[1].data.get(0).as_string(), Some("b".to_string()));
        }
        assert_eq!(num_rows, 1);
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
            min_timestamp: 1,
            max_timestamp: 2,
            timestamp_index: 0,
            raw_data: None,
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

        let kvs = build_key_values(
            &memtable.region_metadata,
            2,
            &[(3, 3.0, "c".to_string())],
            OpType::Put,
        );
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
                OpType::Put,
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
                OpType::Put,
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
                OpType::Put,
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
                OpType::Put,
            ))
            .unwrap();
        memtable
            .write(&build_key_values(
                &memtable.region_metadata,
                1,
                &[(2, 2.0, "b".to_string())],
                OpType::Put,
            ))
            .unwrap();

        // Filter with sequence 0 should only return first write
        let mut iter = memtable
            .iter(None, None, Some(SequenceRange::LtEq { max: 0 }))
            .unwrap();
        let batch = iter.next().unwrap().unwrap();
        assert_eq!(1, batch.num_rows());
        assert_eq!(1.0, batch.fields()[0].data.get(0).as_f64_lossy().unwrap());
    }

    fn rb_with_large_string(
        ts: i64,
        string_len: i32,
        region_meta: &RegionMetadataRef,
    ) -> RecordBatch {
        let schema = region_meta.schema.arrow_schema().clone();
        RecordBatch::try_new(
            schema,
            vec![
                Arc::new(StringArray::from_iter_values(
                    ["a".repeat(string_len as usize).clone()].into_iter(),
                )) as ArrayRef,
                Arc::new(TimestampMillisecondArray::from_iter_values(
                    [ts].into_iter(),
                )) as ArrayRef,
            ],
        )
        .unwrap()
    }

    #[tokio::test]
    async fn test_write_read_large_string() {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(123, 456));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("k0", ConcreteDataType::string_datatype(), false),
                semantic_type: SemanticType::Field,
                column_id: 0,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 1,
            })
            .primary_key(vec![]);
        let region_meta = Arc::new(builder.build().unwrap());
        let memtable =
            SimpleBulkMemtable::new(0, region_meta.clone(), None, true, MergeMode::LastRow);
        memtable
            .write_bulk(BulkPart {
                batch: rb_with_large_string(0, i32::MAX, &region_meta),
                max_timestamp: 0,
                min_timestamp: 0,
                sequence: 0,
                timestamp_index: 1,
                raw_data: None,
            })
            .unwrap();

        memtable.freeze().unwrap();
        memtable
            .write_bulk(BulkPart {
                batch: rb_with_large_string(1, 3, &region_meta),
                max_timestamp: 1,
                min_timestamp: 1,
                sequence: 1,
                timestamp_index: 1,
                raw_data: None,
            })
            .unwrap();
        let MemtableRanges { ranges, .. } =
            memtable.ranges(None, RangesOptions::default()).unwrap();
        let mut source = if ranges.len() == 1 {
            let only_range = ranges.into_values().next().unwrap();
            Source::Iter(only_range.build_iter().unwrap())
        } else {
            let sources = ranges
                .into_values()
                .map(|r| r.build_iter().map(Source::Iter))
                .collect::<error::Result<Vec<_>>>()
                .unwrap();
            let merge_reader = MergeReaderBuilder::from_sources(sources)
                .build()
                .await
                .unwrap();
            Source::Reader(Box::new(merge_reader))
        };

        let mut rows = 0;
        while let Some(b) = source.next_batch().await.unwrap() {
            rows += b.num_rows();
        }
        assert_eq!(rows, 2);
    }
}
