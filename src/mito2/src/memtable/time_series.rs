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

use std::collections::btree_map::Entry;
use std::collections::{BTreeMap, Bound, HashSet};
use std::fmt::{Debug, Formatter};
use std::iter;
use std::sync::atomic::{AtomicI64, AtomicU64, AtomicUsize, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use api::v1::OpType;
use common_recordbatch::filter::SimpleFilterEvaluator;
use common_telemetry::{debug, error};
use common_time::Timestamp;
use datatypes::arrow;
use datatypes::arrow::array::ArrayRef;
use datatypes::arrow_array::StringArray;
use datatypes::data_type::{ConcreteDataType, DataType};
use datatypes::prelude::{ScalarVector, Vector, VectorRef};
use datatypes::types::TimestampType;
use datatypes::value::{Value, ValueRef};
use datatypes::vectors::{
    Helper, TimestampMicrosecondVector, TimestampMillisecondVector, TimestampNanosecondVector,
    TimestampSecondVector, UInt64Vector, UInt8Vector,
};
use snafu::{ensure, OptionExt, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{ColumnId, SequenceNumber};
use table::predicate::Predicate;

use crate::error;
use crate::error::{ComputeArrowSnafu, ConvertVectorSnafu, PrimaryKeyLengthMismatchSnafu, Result};
use crate::flush::WriteBufferManagerRef;
use crate::memtable::builder::{FieldBuilder, StringBuilder};
use crate::memtable::bulk::part::BulkPart;
use crate::memtable::key_values::KeyValue;
use crate::memtable::simple_bulk_memtable::SimpleBulkMemtable;
use crate::memtable::stats::WriteMetrics;
use crate::memtable::{
    AllocTracker, BoxedBatchIterator, IterBuilder, KeyValues, Memtable, MemtableBuilder,
    MemtableId, MemtableRange, MemtableRangeContext, MemtableRanges, MemtableRef, MemtableStats,
    PredicateGroup,
};
use crate::metrics::{READ_ROWS_TOTAL, READ_STAGE_ELAPSED};
use crate::read::dedup::LastNonNullIter;
use crate::read::{Batch, BatchBuilder, BatchColumn};
use crate::region::options::MergeMode;
use crate::row_converter::{DensePrimaryKeyCodec, PrimaryKeyCodecExt};

/// Initial vector builder capacity.
const INITIAL_BUILDER_CAPACITY: usize = 1024 * 8;

/// Vector builder capacity.
const BUILDER_CAPACITY: usize = 512;

/// Builder to build [TimeSeriesMemtable].
#[derive(Debug, Default)]
pub struct TimeSeriesMemtableBuilder {
    write_buffer_manager: Option<WriteBufferManagerRef>,
    dedup: bool,
    merge_mode: MergeMode,
}

impl TimeSeriesMemtableBuilder {
    /// Creates a new builder with specific `write_buffer_manager`.
    pub fn new(
        write_buffer_manager: Option<WriteBufferManagerRef>,
        dedup: bool,
        merge_mode: MergeMode,
    ) -> Self {
        Self {
            write_buffer_manager,
            dedup,
            merge_mode,
        }
    }
}

impl MemtableBuilder for TimeSeriesMemtableBuilder {
    fn build(&self, id: MemtableId, metadata: &RegionMetadataRef) -> MemtableRef {
        if metadata.primary_key.is_empty() {
            Arc::new(SimpleBulkMemtable::new(
                id,
                metadata.clone(),
                self.write_buffer_manager.clone(),
                self.dedup,
                self.merge_mode,
            ))
        } else {
            Arc::new(TimeSeriesMemtable::new(
                metadata.clone(),
                id,
                self.write_buffer_manager.clone(),
                self.dedup,
                self.merge_mode,
            ))
        }
    }
}

/// Memtable implementation that groups rows by their primary key.
pub struct TimeSeriesMemtable {
    id: MemtableId,
    region_metadata: RegionMetadataRef,
    row_codec: Arc<DensePrimaryKeyCodec>,
    series_set: SeriesSet,
    alloc_tracker: AllocTracker,
    max_timestamp: AtomicI64,
    min_timestamp: AtomicI64,
    max_sequence: AtomicU64,
    dedup: bool,
    merge_mode: MergeMode,
    /// Total written rows in memtable. This also includes deleted and duplicated rows.
    num_rows: AtomicUsize,
}

impl TimeSeriesMemtable {
    pub fn new(
        region_metadata: RegionMetadataRef,
        id: MemtableId,
        write_buffer_manager: Option<WriteBufferManagerRef>,
        dedup: bool,
        merge_mode: MergeMode,
    ) -> Self {
        let row_codec = Arc::new(DensePrimaryKeyCodec::new(&region_metadata));
        let series_set = SeriesSet::new(region_metadata.clone(), row_codec.clone());
        let dedup = if merge_mode == MergeMode::LastNonNull {
            false
        } else {
            dedup
        };
        Self {
            id,
            region_metadata,
            series_set,
            row_codec,
            alloc_tracker: AllocTracker::new(write_buffer_manager),
            max_timestamp: AtomicI64::new(i64::MIN),
            min_timestamp: AtomicI64::new(i64::MAX),
            max_sequence: AtomicU64::new(0),
            dedup,
            merge_mode,
            num_rows: Default::default(),
        }
    }

    /// Updates memtable stats.
    fn update_stats(&self, stats: WriteMetrics) {
        self.alloc_tracker
            .on_allocation(stats.key_bytes + stats.value_bytes);
        self.max_timestamp.fetch_max(stats.max_ts, Ordering::SeqCst);
        self.min_timestamp.fetch_min(stats.min_ts, Ordering::SeqCst);
        self.max_sequence
            .fetch_max(stats.max_sequence, Ordering::SeqCst);
        self.num_rows.fetch_add(stats.num_rows, Ordering::SeqCst);
    }

    fn write_key_value(&self, kv: KeyValue, stats: &mut WriteMetrics) -> Result<()> {
        ensure!(
            self.row_codec.num_fields() == kv.num_primary_keys(),
            PrimaryKeyLengthMismatchSnafu {
                expect: self.row_codec.num_fields(),
                actual: kv.num_primary_keys(),
            }
        );

        let primary_key_encoded = self.row_codec.encode(kv.primary_keys())?;

        let (key_allocated, value_allocated) =
            self.series_set.push_to_series(primary_key_encoded, &kv);
        stats.key_bytes += key_allocated;
        stats.value_bytes += value_allocated;

        // safety: timestamp of kv must be both present and a valid timestamp value.
        let ts = kv.timestamp().as_timestamp().unwrap().unwrap().value();
        stats.min_ts = stats.min_ts.min(ts);
        stats.max_ts = stats.max_ts.max(ts);
        Ok(())
    }
}

impl Debug for TimeSeriesMemtable {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("TimeSeriesMemtable").finish()
    }
}

impl Memtable for TimeSeriesMemtable {
    fn id(&self) -> MemtableId {
        self.id
    }

    fn write(&self, kvs: &KeyValues) -> Result<()> {
        if kvs.is_empty() {
            return Ok(());
        }

        let mut local_stats = WriteMetrics::default();

        for kv in kvs.iter() {
            self.write_key_value(kv, &mut local_stats)?;
        }
        local_stats.value_bytes += kvs.num_rows() * std::mem::size_of::<Timestamp>();
        local_stats.value_bytes += kvs.num_rows() * std::mem::size_of::<OpType>();
        local_stats.max_sequence = kvs.max_sequence();
        local_stats.num_rows = kvs.num_rows();
        // TODO(hl): this maybe inaccurate since for-iteration may return early.
        // We may lift the primary key length check out of Memtable::write
        // so that we can ensure writing to memtable will succeed.
        self.update_stats(local_stats);
        Ok(())
    }

    fn write_one(&self, key_value: KeyValue) -> Result<()> {
        let mut metrics = WriteMetrics::default();
        let res = self.write_key_value(key_value, &mut metrics);
        metrics.value_bytes += std::mem::size_of::<Timestamp>() + std::mem::size_of::<OpType>();
        metrics.max_sequence = key_value.sequence();
        metrics.num_rows = 1;

        if res.is_ok() {
            self.update_stats(metrics);
        }
        res
    }

    fn write_bulk(&self, part: BulkPart) -> Result<()> {
        // Default implementation fallback to row iteration.
        let mutation = part.to_mutation(&self.region_metadata)?;
        let mut metrics = WriteMetrics::default();
        if let Some(key_values) = KeyValues::new(&self.region_metadata, mutation) {
            for kv in key_values.iter() {
                self.write_key_value(kv, &mut metrics)?
            }
        }

        metrics.max_sequence = part.sequence;
        metrics.max_ts = part.max_ts;
        metrics.min_ts = part.min_ts;
        metrics.num_rows = part.num_rows;
        self.update_stats(metrics);
        Ok(())
    }

    fn iter(
        &self,
        projection: Option<&[ColumnId]>,
        filters: Option<Predicate>,
        sequence: Option<SequenceNumber>,
    ) -> Result<BoxedBatchIterator> {
        let projection = if let Some(projection) = projection {
            projection.iter().copied().collect()
        } else {
            self.region_metadata
                .field_columns()
                .map(|c| c.column_id)
                .collect()
        };

        let iter = self
            .series_set
            .iter_series(projection, filters, self.dedup, sequence)?;

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
    ) -> Result<MemtableRanges> {
        let projection = if let Some(projection) = projection {
            projection.iter().copied().collect()
        } else {
            self.region_metadata
                .field_columns()
                .map(|c| c.column_id)
                .collect()
        };
        let builder = Box::new(TimeSeriesIterBuilder {
            series_set: self.series_set.clone(),
            projection,
            predicate: predicate.predicate().cloned(),
            dedup: self.dedup,
            merge_mode: self.merge_mode,
            sequence,
        });
        let context = Arc::new(MemtableRangeContext::new(self.id, builder, predicate));

        Ok(MemtableRanges {
            ranges: [(0, MemtableRange::new(context))].into(),
            stats: self.stats(),
        })
    }

    fn is_empty(&self) -> bool {
        self.series_set.series.read().unwrap().is_empty()
    }

    fn freeze(&self) -> Result<()> {
        self.alloc_tracker.done_allocating();

        Ok(())
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
            num_rows: self.num_rows.load(Ordering::Relaxed),
            num_ranges: 1,
            max_sequence: self.max_sequence.load(Ordering::Relaxed),
        }
    }

    fn fork(&self, id: MemtableId, metadata: &RegionMetadataRef) -> MemtableRef {
        Arc::new(TimeSeriesMemtable::new(
            metadata.clone(),
            id,
            self.alloc_tracker.write_buffer_manager(),
            self.dedup,
            self.merge_mode,
        ))
    }
}

type SeriesRwLockMap = RwLock<BTreeMap<Vec<u8>, Arc<RwLock<Series>>>>;

#[derive(Clone)]
pub(crate) struct SeriesSet {
    pub(crate) region_metadata: RegionMetadataRef,
    pub(crate) series: Arc<SeriesRwLockMap>,
    pub(crate) codec: Arc<DensePrimaryKeyCodec>,
}

impl SeriesSet {
    fn new(region_metadata: RegionMetadataRef, codec: Arc<DensePrimaryKeyCodec>) -> Self {
        Self {
            region_metadata,
            series: Default::default(),
            codec,
        }
    }
}

impl SeriesSet {
    /// Push [KeyValue] to SeriesSet with given primary key and return key/value allocated memory size.
    fn push_to_series(&self, primary_key: Vec<u8>, kv: &KeyValue) -> (usize, usize) {
        if let Some(series) = self.series.read().unwrap().get(&primary_key) {
            let value_allocated = series.write().unwrap().push(
                kv.timestamp(),
                kv.sequence(),
                kv.op_type(),
                kv.fields(),
            );
            return (0, value_allocated);
        };

        let mut indices = self.series.write().unwrap();
        match indices.entry(primary_key) {
            Entry::Vacant(v) => {
                let key_len = v.key().len();
                let mut series = Series::new(&self.region_metadata);
                let value_allocated =
                    series.push(kv.timestamp(), kv.sequence(), kv.op_type(), kv.fields());
                v.insert(Arc::new(RwLock::new(series)));
                (key_len, value_allocated)
            }
            // safety: series must exist at given index.
            Entry::Occupied(v) => {
                let value_allocated = v.get().write().unwrap().push(
                    kv.timestamp(),
                    kv.sequence(),
                    kv.op_type(),
                    kv.fields(),
                );
                (0, value_allocated)
            }
        }
    }

    #[cfg(test)]
    fn get_series(&self, primary_key: &[u8]) -> Option<Arc<RwLock<Series>>> {
        self.series.read().unwrap().get(primary_key).cloned()
    }

    /// Iterates all series in [SeriesSet].
    fn iter_series(
        &self,
        projection: HashSet<ColumnId>,
        predicate: Option<Predicate>,
        dedup: bool,
        sequence: Option<SequenceNumber>,
    ) -> Result<Iter> {
        let primary_key_schema = primary_key_schema(&self.region_metadata);
        let primary_key_datatypes = self
            .region_metadata
            .primary_key_columns()
            .map(|pk| pk.column_schema.data_type.clone())
            .collect();

        Iter::try_new(
            self.region_metadata.clone(),
            self.series.clone(),
            projection,
            predicate,
            primary_key_schema,
            primary_key_datatypes,
            self.codec.clone(),
            dedup,
            sequence,
        )
    }
}

/// Creates an arrow [SchemaRef](arrow::datatypes::SchemaRef) that only contains primary keys
/// of given region schema
pub(crate) fn primary_key_schema(
    region_metadata: &RegionMetadataRef,
) -> arrow::datatypes::SchemaRef {
    let fields = region_metadata
        .primary_key_columns()
        .map(|pk| {
            arrow::datatypes::Field::new(
                pk.column_schema.name.clone(),
                pk.column_schema.data_type.as_arrow_type(),
                pk.column_schema.is_nullable(),
            )
        })
        .collect::<Vec<_>>();
    Arc::new(arrow::datatypes::Schema::new(fields))
}

/// Metrics for reading the memtable.
#[derive(Debug, Default)]
struct Metrics {
    /// Total series in the memtable.
    total_series: usize,
    /// Number of series pruned.
    num_pruned_series: usize,
    /// Number of rows read.
    num_rows: usize,
    /// Number of batch read.
    num_batches: usize,
    /// Duration to scan the memtable.
    scan_cost: Duration,
}

struct Iter {
    metadata: RegionMetadataRef,
    series: Arc<SeriesRwLockMap>,
    projection: HashSet<ColumnId>,
    last_key: Option<Vec<u8>>,
    predicate: Vec<SimpleFilterEvaluator>,
    pk_schema: arrow::datatypes::SchemaRef,
    pk_datatypes: Vec<ConcreteDataType>,
    codec: Arc<DensePrimaryKeyCodec>,
    dedup: bool,
    sequence: Option<SequenceNumber>,
    metrics: Metrics,
}

impl Iter {
    #[allow(clippy::too_many_arguments)]
    pub(crate) fn try_new(
        metadata: RegionMetadataRef,
        series: Arc<SeriesRwLockMap>,
        projection: HashSet<ColumnId>,
        predicate: Option<Predicate>,
        pk_schema: arrow::datatypes::SchemaRef,
        pk_datatypes: Vec<ConcreteDataType>,
        codec: Arc<DensePrimaryKeyCodec>,
        dedup: bool,
        sequence: Option<SequenceNumber>,
    ) -> Result<Self> {
        let predicate = predicate
            .map(|predicate| {
                predicate
                    .exprs()
                    .iter()
                    .filter_map(SimpleFilterEvaluator::try_new)
                    .collect::<Vec<_>>()
            })
            .unwrap_or_default();
        Ok(Self {
            metadata,
            series,
            projection,
            last_key: None,
            predicate,
            pk_schema,
            pk_datatypes,
            codec,
            dedup,
            sequence,
            metrics: Metrics::default(),
        })
    }
}

impl Drop for Iter {
    fn drop(&mut self) {
        debug!(
            "Iter {} time series memtable, metrics: {:?}",
            self.metadata.region_id, self.metrics
        );

        READ_ROWS_TOTAL
            .with_label_values(&["time_series_memtable"])
            .inc_by(self.metrics.num_rows as u64);
        READ_STAGE_ELAPSED
            .with_label_values(&["scan_memtable"])
            .observe(self.metrics.scan_cost.as_secs_f64());
    }
}

impl Iterator for Iter {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        let start = Instant::now();
        let map = self.series.read().unwrap();
        let range = match &self.last_key {
            None => map.range::<Vec<u8>, _>(..),
            Some(last_key) => {
                map.range::<Vec<u8>, _>((Bound::Excluded(last_key), Bound::Unbounded))
            }
        };

        // TODO(hl): maybe yield more than one time series to amortize range overhead.
        for (primary_key, series) in range {
            self.metrics.total_series += 1;

            let mut series = series.write().unwrap();
            if !self.predicate.is_empty()
                && !prune_primary_key(
                    &self.codec,
                    primary_key.as_slice(),
                    &mut series,
                    &self.pk_datatypes,
                    self.pk_schema.clone(),
                    &self.predicate,
                )
            {
                // read next series
                self.metrics.num_pruned_series += 1;
                continue;
            }
            self.last_key = Some(primary_key.clone());

            let values = series.compact(&self.metadata);
            let batch = values.and_then(|v| {
                v.to_batch(primary_key, &self.metadata, &self.projection, self.dedup)
            });

            // Update metrics.
            self.metrics.num_batches += 1;
            self.metrics.num_rows += batch.as_ref().map(|b| b.num_rows()).unwrap_or(0);
            self.metrics.scan_cost += start.elapsed();

            let mut batch = batch;
            batch = batch.and_then(|mut batch| {
                batch.filter_by_sequence(self.sequence)?;
                Ok(batch)
            });
            return Some(batch);
        }
        self.metrics.scan_cost += start.elapsed();

        None
    }
}

fn prune_primary_key(
    codec: &Arc<DensePrimaryKeyCodec>,
    pk: &[u8],
    series: &mut Series,
    datatypes: &[ConcreteDataType],
    pk_schema: arrow::datatypes::SchemaRef,
    predicates: &[SimpleFilterEvaluator],
) -> bool {
    // no primary key, we simply return true.
    if pk_schema.fields().is_empty() {
        return true;
    }

    // retrieve primary key values from cache or decode from bytes.
    let pk_values = if let Some(pk_values) = series.pk_cache.as_ref() {
        pk_values
    } else {
        let pk_values = codec.decode_dense_without_column_id(pk);
        if let Err(e) = pk_values {
            error!(e; "Failed to decode primary key");
            return true;
        }
        series.update_pk_cache(pk_values.unwrap());
        series.pk_cache.as_ref().unwrap()
    };

    // evaluate predicates against primary key values
    let mut result = true;
    for predicate in predicates {
        // ignore predicates that are not referencing primary key columns
        let Ok(index) = pk_schema.index_of(predicate.column_name()) else {
            continue;
        };
        // Safety: arrow schema and datatypes are constructed from the same source.
        let scalar_value = pk_values[index]
            .try_to_scalar_value(&datatypes[index])
            .unwrap();
        result &= predicate.evaluate_scalar(&scalar_value).unwrap_or(true);
    }

    result
}

/// A `Series` holds a list of field values of some given primary key.
pub(crate) struct Series {
    pk_cache: Option<Vec<Value>>,
    active: ValueBuilder,
    frozen: Vec<Values>,
    region_metadata: RegionMetadataRef,
}

impl Series {
    pub(crate) fn new(region_metadata: &RegionMetadataRef) -> Self {
        Self {
            pk_cache: None,
            active: ValueBuilder::new(region_metadata, INITIAL_BUILDER_CAPACITY),
            frozen: vec![],
            region_metadata: region_metadata.clone(),
        }
    }

    pub fn is_empty(&self) -> bool {
        self.active.len() == 0 && self.frozen.is_empty()
    }

    /// Pushes a row of values into Series. Return the size of values.
    pub(crate) fn push<'a>(
        &mut self,
        ts: ValueRef<'a>,
        sequence: u64,
        op_type: OpType,
        values: impl Iterator<Item = ValueRef<'a>>,
    ) -> usize {
        // + 10 to avoid potential reallocation.
        if self.active.len() + 10 > BUILDER_CAPACITY {
            let region_metadata = self.region_metadata.clone();
            self.freeze(&region_metadata);
        }
        self.active.push(ts, sequence, op_type as u8, values)
    }

    fn update_pk_cache(&mut self, pk_values: Vec<Value>) {
        self.pk_cache = Some(pk_values);
    }

    /// Freezes the active part and push it to `frozen`.
    pub(crate) fn freeze(&mut self, region_metadata: &RegionMetadataRef) {
        if self.active.len() != 0 {
            let mut builder = ValueBuilder::new(region_metadata, INITIAL_BUILDER_CAPACITY);
            std::mem::swap(&mut self.active, &mut builder);
            self.frozen.push(Values::from(builder));
        }
    }

    pub(crate) fn extend(
        &mut self,
        ts_v: VectorRef,
        op_type_v: u8,
        sequence_v: u64,
        fields: impl Iterator<Item = VectorRef>,
    ) -> Result<()> {
        self.active.extend(ts_v, op_type_v, sequence_v, fields)
    }

    /// Freezes active part to frozen part and compact frozen part to reduce memory fragmentation.
    /// Returns the frozen and compacted values.
    pub(crate) fn compact(&mut self, region_metadata: &RegionMetadataRef) -> Result<&Values> {
        self.freeze(region_metadata);

        let frozen = &self.frozen;

        // Each series must contain at least one row
        debug_assert!(!frozen.is_empty());

        if frozen.len() > 1 {
            // TODO(hl): We should keep track of min/max timestamps for each values and avoid
            // cloning and sorting when values do not overlap with each other.

            let column_size = frozen[0].fields.len() + 3;

            if cfg!(debug_assertions) {
                debug_assert!(frozen
                    .iter()
                    .zip(frozen.iter().skip(1))
                    .all(|(prev, next)| { prev.fields.len() == next.fields.len() }));
            }

            let arrays = frozen.iter().map(|v| v.columns()).collect::<Vec<_>>();
            let concatenated = (0..column_size)
                .map(|i| {
                    let to_concat = arrays.iter().map(|a| a[i].as_ref()).collect::<Vec<_>>();
                    arrow::compute::concat(&to_concat)
                })
                .collect::<std::result::Result<Vec<_>, _>>()
                .context(ComputeArrowSnafu)?;

            debug_assert_eq!(concatenated.len(), column_size);
            let values = Values::from_columns(&concatenated)?;
            self.frozen = vec![values];
        };
        Ok(&self.frozen[0])
    }
}

/// `ValueBuilder` holds all the vector builders for field columns.
struct ValueBuilder {
    timestamp: Vec<i64>,
    timestamp_type: ConcreteDataType,
    sequence: Vec<u64>,
    op_type: Vec<u8>,
    fields: Vec<Option<FieldBuilder>>,
    field_types: Vec<ConcreteDataType>,
}

impl ValueBuilder {
    pub(crate) fn new(region_metadata: &RegionMetadataRef, capacity: usize) -> Self {
        let timestamp_type = region_metadata
            .time_index_column()
            .column_schema
            .data_type
            .clone();
        let sequence = Vec::with_capacity(capacity);
        let op_type = Vec::with_capacity(capacity);

        let field_types = region_metadata
            .field_columns()
            .map(|c| c.column_schema.data_type.clone())
            .collect::<Vec<_>>();
        let fields = (0..field_types.len()).map(|_| None).collect();

        Self {
            timestamp: Vec::with_capacity(capacity),
            timestamp_type,
            sequence,
            op_type,
            fields,
            field_types,
        }
    }

    /// Pushes a new row to `ValueBuilder`.
    /// We don't need primary keys since they've already be encoded.
    /// Returns the size of field values.
    ///
    /// In this method, we don't check the data type of the value, because it is already checked in the caller.
    fn push<'a>(
        &mut self,
        ts: ValueRef,
        sequence: u64,
        op_type: u8,
        fields: impl Iterator<Item = ValueRef<'a>>,
    ) -> usize {
        #[cfg(debug_assertions)]
        let fields = {
            let field_vec = fields.collect::<Vec<_>>();
            debug_assert_eq!(field_vec.len(), self.fields.len());
            field_vec.into_iter()
        };

        self.timestamp
            .push(ts.as_timestamp().unwrap().unwrap().value());
        self.sequence.push(sequence);
        self.op_type.push(op_type);
        let num_rows = self.timestamp.len();
        let mut size = 0;
        for (idx, field_value) in fields.enumerate() {
            size += field_value.data_size();
            if !field_value.is_null() || self.fields[idx].is_some() {
                if let Some(field) = self.fields[idx].as_mut() {
                    let _ = field.push(field_value);
                } else {
                    let mut mutable_vector =
                        if let ConcreteDataType::String(_) = &self.field_types[idx] {
                            FieldBuilder::String(StringBuilder::with_capacity(256, 4096))
                        } else {
                            FieldBuilder::Other(
                                self.field_types[idx]
                                    .create_mutable_vector(num_rows.max(INITIAL_BUILDER_CAPACITY)),
                            )
                        };
                    mutable_vector.push_nulls(num_rows - 1);
                    let _ = mutable_vector.push(field_value);
                    self.fields[idx] = Some(mutable_vector);
                }
            }
        }

        size
    }

    pub(crate) fn extend(
        &mut self,
        ts_v: VectorRef,
        op_type: u8,
        sequence: u64,
        fields: impl Iterator<Item = VectorRef>,
    ) -> error::Result<()> {
        let num_rows_before = self.timestamp.len();
        let num_rows_to_write = ts_v.len();
        self.timestamp.reserve(num_rows_to_write);
        match self.timestamp_type {
            ConcreteDataType::Timestamp(TimestampType::Second(_)) => {
                self.timestamp.extend(
                    ts_v.as_any()
                        .downcast_ref::<TimestampSecondVector>()
                        .unwrap()
                        .iter_data()
                        .map(|v| v.unwrap().0.value()),
                );
            }
            ConcreteDataType::Timestamp(TimestampType::Millisecond(_)) => {
                self.timestamp.extend(
                    ts_v.as_any()
                        .downcast_ref::<TimestampMillisecondVector>()
                        .unwrap()
                        .iter_data()
                        .map(|v| v.unwrap().0.value()),
                );
            }
            ConcreteDataType::Timestamp(TimestampType::Microsecond(_)) => {
                self.timestamp.extend(
                    ts_v.as_any()
                        .downcast_ref::<TimestampMicrosecondVector>()
                        .unwrap()
                        .iter_data()
                        .map(|v| v.unwrap().0.value()),
                );
            }
            ConcreteDataType::Timestamp(TimestampType::Nanosecond(_)) => {
                self.timestamp.extend(
                    ts_v.as_any()
                        .downcast_ref::<TimestampNanosecondVector>()
                        .unwrap()
                        .iter_data()
                        .map(|v| v.unwrap().0.value()),
                );
            }
            _ => unreachable!(),
        };

        self.op_type.reserve(num_rows_to_write);
        self.op_type
            .extend(iter::repeat_n(op_type, num_rows_to_write));
        self.sequence.reserve(num_rows_to_write);
        self.sequence
            .extend(iter::repeat_n(sequence, num_rows_to_write));

        for (field_idx, (field_src, field_dest)) in fields.zip(self.fields.iter_mut()).enumerate() {
            let builder = field_dest.get_or_insert_with(|| {
                let mut field_builder =
                    FieldBuilder::create(&self.field_types[field_idx], INITIAL_BUILDER_CAPACITY);
                field_builder.push_nulls(num_rows_before);
                field_builder
            });
            match builder {
                FieldBuilder::String(builder) => {
                    let array = field_src.to_arrow_array();
                    let string_array =
                        array
                            .as_any()
                            .downcast_ref::<StringArray>()
                            .with_context(|| error::InvalidBatchSnafu {
                                reason: format!(
                                    "Field type mismatch, expecting String, given: {}",
                                    field_src.data_type()
                                ),
                            })?;
                    builder.append_array(string_array);
                }
                FieldBuilder::Other(builder) => {
                    let len = field_src.len();
                    builder
                        .extend_slice_of(&*field_src, 0, len)
                        .context(error::ComputeVectorSnafu)?;
                }
            }
        }
        Ok(())
    }

    /// Returns the length of [ValueBuilder]
    fn len(&self) -> usize {
        let sequence_len = self.sequence.len();
        debug_assert_eq!(sequence_len, self.op_type.len());
        debug_assert_eq!(sequence_len, self.timestamp.len());
        sequence_len
    }
}

/// [Values] holds an immutable vectors of field columns, including `sequence` and `op_type`.
#[derive(Clone)]
pub(crate) struct Values {
    timestamp: VectorRef,
    sequence: Arc<UInt64Vector>,
    op_type: Arc<UInt8Vector>,
    fields: Vec<VectorRef>,
}

impl Values {
    /// Converts [Values] to `Batch`, sorts the batch according to `timestamp, sequence` desc and
    /// keeps only the latest row for the same timestamp.
    pub fn to_batch(
        &self,
        primary_key: &[u8],
        metadata: &RegionMetadataRef,
        projection: &HashSet<ColumnId>,
        dedup: bool,
    ) -> Result<Batch> {
        let builder = BatchBuilder::with_required_columns(
            primary_key.to_vec(),
            self.timestamp.clone(),
            self.sequence.clone(),
            self.op_type.clone(),
        );

        let fields = metadata
            .field_columns()
            .zip(self.fields.iter())
            .filter_map(|(c, f)| {
                projection.get(&c.column_id).map(|c| BatchColumn {
                    column_id: *c,
                    data: f.clone(),
                })
            })
            .collect();

        let mut batch = builder.with_fields(fields).build()?;
        batch.sort(dedup)?;
        Ok(batch)
    }

    /// Returns a vector of all columns converted to arrow [Array](datatypes::arrow::array::Array) in [Values].
    fn columns(&self) -> Vec<ArrayRef> {
        let mut res = Vec::with_capacity(3 + self.fields.len());
        res.push(self.timestamp.to_arrow_array());
        res.push(self.sequence.to_arrow_array());
        res.push(self.op_type.to_arrow_array());
        res.extend(self.fields.iter().map(|f| f.to_arrow_array()));
        res
    }

    /// Builds a new [Values] instance from columns.
    fn from_columns(cols: &[ArrayRef]) -> Result<Self> {
        debug_assert!(cols.len() >= 3);
        let timestamp = Helper::try_into_vector(&cols[0]).context(ConvertVectorSnafu)?;
        let sequence =
            Arc::new(UInt64Vector::try_from_arrow_array(&cols[1]).context(ConvertVectorSnafu)?);
        let op_type =
            Arc::new(UInt8Vector::try_from_arrow_array(&cols[2]).context(ConvertVectorSnafu)?);
        let fields = Helper::try_into_vectors(&cols[3..]).context(ConvertVectorSnafu)?;

        Ok(Self {
            timestamp,
            sequence,
            op_type,
            fields,
        })
    }
}

impl From<ValueBuilder> for Values {
    fn from(mut value: ValueBuilder) -> Self {
        let num_rows = value.len();
        let fields = value
            .fields
            .iter_mut()
            .enumerate()
            .map(|(i, v)| {
                if let Some(v) = v {
                    v.finish()
                } else {
                    let mut single_null = value.field_types[i].create_mutable_vector(num_rows);
                    single_null.push_nulls(num_rows);
                    single_null.to_vector()
                }
            })
            .collect::<Vec<_>>();

        let sequence = Arc::new(UInt64Vector::from_vec(value.sequence));
        let op_type = Arc::new(UInt8Vector::from_vec(value.op_type));
        let timestamp: VectorRef = match value.timestamp_type {
            ConcreteDataType::Timestamp(TimestampType::Second(_)) => {
                Arc::new(TimestampSecondVector::from_vec(value.timestamp))
            }
            ConcreteDataType::Timestamp(TimestampType::Millisecond(_)) => {
                Arc::new(TimestampMillisecondVector::from_vec(value.timestamp))
            }
            ConcreteDataType::Timestamp(TimestampType::Microsecond(_)) => {
                Arc::new(TimestampMicrosecondVector::from_vec(value.timestamp))
            }
            ConcreteDataType::Timestamp(TimestampType::Nanosecond(_)) => {
                Arc::new(TimestampNanosecondVector::from_vec(value.timestamp))
            }
            _ => unreachable!(),
        };

        if cfg!(debug_assertions) {
            debug_assert_eq!(timestamp.len(), sequence.len());
            debug_assert_eq!(timestamp.len(), op_type.len());
            for field in &fields {
                debug_assert_eq!(timestamp.len(), field.len());
            }
        }

        Self {
            timestamp,
            sequence,
            op_type,
            fields,
        }
    }
}

struct TimeSeriesIterBuilder {
    series_set: SeriesSet,
    projection: HashSet<ColumnId>,
    predicate: Option<Predicate>,
    dedup: bool,
    sequence: Option<SequenceNumber>,
    merge_mode: MergeMode,
}

impl IterBuilder for TimeSeriesIterBuilder {
    fn build(&self) -> Result<BoxedBatchIterator> {
        let iter = self.series_set.iter_series(
            self.projection.clone(),
            self.predicate.clone(),
            self.dedup,
            self.sequence,
        )?;

        if self.merge_mode == MergeMode::LastNonNull {
            let iter = LastNonNullIter::new(iter);
            Ok(Box::new(iter))
        } else {
            Ok(Box::new(iter))
        }
    }
}

#[cfg(test)]
mod tests {
    use std::collections::{HashMap, HashSet};

    use api::helper::ColumnDataTypeWrapper;
    use api::v1::value::ValueData;
    use api::v1::{Mutation, Row, Rows, SemanticType};
    use common_time::Timestamp;
    use datatypes::prelude::{ConcreteDataType, ScalarVector};
    use datatypes::schema::ColumnSchema;
    use datatypes::value::{OrderedFloat, Value};
    use datatypes::vectors::{Float64Vector, Int64Vector, TimestampMillisecondVector};
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::RegionId;

    use super::*;
    use crate::row_converter::SortField;
    use crate::test_util::column_metadata_to_column_schema;

    fn schema_for_test() -> RegionMetadataRef {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(123, 456));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("k0", ConcreteDataType::string_datatype(), false),
                semantic_type: SemanticType::Tag,
                column_id: 0,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("k1", ConcreteDataType::int64_datatype(), false),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "ts",
                    ConcreteDataType::timestamp_millisecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("v0", ConcreteDataType::int64_datatype(), true),
                semantic_type: SemanticType::Field,
                column_id: 3,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("v1", ConcreteDataType::float64_datatype(), true),
                semantic_type: SemanticType::Field,
                column_id: 4,
            })
            .primary_key(vec![0, 1]);
        let region_metadata = builder.build().unwrap();
        Arc::new(region_metadata)
    }

    fn ts_value_ref(val: i64) -> ValueRef<'static> {
        ValueRef::Timestamp(Timestamp::new_millisecond(val))
    }

    fn field_value_ref(v0: i64, v1: f64) -> impl Iterator<Item = ValueRef<'static>> {
        vec![ValueRef::Int64(v0), ValueRef::Float64(OrderedFloat(v1))].into_iter()
    }

    fn check_values(values: &Values, expect: &[(i64, u64, u8, i64, f64)]) {
        let ts = values
            .timestamp
            .as_any()
            .downcast_ref::<TimestampMillisecondVector>()
            .unwrap();

        let v0 = values.fields[0]
            .as_any()
            .downcast_ref::<Int64Vector>()
            .unwrap();
        let v1 = values.fields[1]
            .as_any()
            .downcast_ref::<Float64Vector>()
            .unwrap();
        let read = ts
            .iter_data()
            .zip(values.sequence.iter_data())
            .zip(values.op_type.iter_data())
            .zip(v0.iter_data())
            .zip(v1.iter_data())
            .map(|((((ts, sequence), op_type), v0), v1)| {
                (
                    ts.unwrap().0.value(),
                    sequence.unwrap(),
                    op_type.unwrap(),
                    v0.unwrap(),
                    v1.unwrap(),
                )
            })
            .collect::<Vec<_>>();
        assert_eq!(expect, &read);
    }

    #[test]
    fn test_series() {
        let region_metadata = schema_for_test();
        let mut series = Series::new(&region_metadata);
        series.push(ts_value_ref(1), 0, OpType::Put, field_value_ref(1, 10.1));
        series.push(ts_value_ref(2), 0, OpType::Put, field_value_ref(2, 10.2));
        assert_eq!(2, series.active.timestamp.len());
        assert_eq!(0, series.frozen.len());

        let values = series.compact(&region_metadata).unwrap();
        check_values(values, &[(1, 0, 1, 1, 10.1), (2, 0, 1, 2, 10.2)]);
        assert_eq!(0, series.active.timestamp.len());
        assert_eq!(1, series.frozen.len());
    }

    #[test]
    fn test_series_with_nulls() {
        let region_metadata = schema_for_test();
        let mut series = Series::new(&region_metadata);
        // col1: NULL 1 2 3
        // col2: NULL NULL 10.2 NULL
        series.push(
            ts_value_ref(1),
            0,
            OpType::Put,
            vec![ValueRef::Null, ValueRef::Null].into_iter(),
        );
        series.push(
            ts_value_ref(1),
            0,
            OpType::Put,
            vec![ValueRef::Int64(1), ValueRef::Null].into_iter(),
        );
        series.push(ts_value_ref(1), 2, OpType::Put, field_value_ref(2, 10.2));
        series.push(
            ts_value_ref(1),
            3,
            OpType::Put,
            vec![ValueRef::Int64(2), ValueRef::Null].into_iter(),
        );
        assert_eq!(4, series.active.timestamp.len());
        assert_eq!(0, series.frozen.len());

        let values = series.compact(&region_metadata).unwrap();
        assert_eq!(values.fields[0].null_count(), 1);
        assert_eq!(values.fields[1].null_count(), 3);
        assert_eq!(0, series.active.timestamp.len());
        assert_eq!(1, series.frozen.len());
    }

    fn check_value(batch: &Batch, expect: Vec<Vec<Value>>) {
        let ts_len = batch.timestamps().len();
        assert_eq!(batch.sequences().len(), ts_len);
        assert_eq!(batch.op_types().len(), ts_len);
        for f in batch.fields() {
            assert_eq!(f.data.len(), ts_len);
        }

        let mut rows = vec![];
        for idx in 0..ts_len {
            let mut row = Vec::with_capacity(batch.fields().len() + 3);
            row.push(batch.timestamps().get(idx));
            row.push(batch.sequences().get(idx));
            row.push(batch.op_types().get(idx));
            row.extend(batch.fields().iter().map(|f| f.data.get(idx)));
            rows.push(row);
        }

        assert_eq!(expect.len(), rows.len());
        for (idx, row) in rows.iter().enumerate() {
            assert_eq!(&expect[idx], row);
        }
    }

    #[test]
    fn test_values_sort() {
        let schema = schema_for_test();
        let timestamp = Arc::new(TimestampMillisecondVector::from_vec(vec![1, 2, 3, 4, 3]));
        let sequence = Arc::new(UInt64Vector::from_vec(vec![1, 1, 1, 1, 2]));
        let op_type = Arc::new(UInt8Vector::from_vec(vec![1, 1, 1, 1, 0]));

        let fields = vec![
            Arc::new(Int64Vector::from_vec(vec![4, 3, 2, 1, 2])) as Arc<_>,
            Arc::new(Float64Vector::from_vec(vec![1.1, 2.1, 4.2, 3.3, 4.2])) as Arc<_>,
        ];
        let values = Values {
            timestamp: timestamp as Arc<_>,
            sequence,
            op_type,
            fields,
        };

        let batch = values
            .to_batch(
                b"test",
                &schema,
                &[0, 1, 2, 3, 4].into_iter().collect(),
                true,
            )
            .unwrap();
        check_value(
            &batch,
            vec![
                vec![
                    Value::Timestamp(Timestamp::new_millisecond(1)),
                    Value::UInt64(1),
                    Value::UInt8(1),
                    Value::Int64(4),
                    Value::Float64(OrderedFloat(1.1)),
                ],
                vec![
                    Value::Timestamp(Timestamp::new_millisecond(2)),
                    Value::UInt64(1),
                    Value::UInt8(1),
                    Value::Int64(3),
                    Value::Float64(OrderedFloat(2.1)),
                ],
                vec![
                    Value::Timestamp(Timestamp::new_millisecond(3)),
                    Value::UInt64(2),
                    Value::UInt8(0),
                    Value::Int64(2),
                    Value::Float64(OrderedFloat(4.2)),
                ],
                vec![
                    Value::Timestamp(Timestamp::new_millisecond(4)),
                    Value::UInt64(1),
                    Value::UInt8(1),
                    Value::Int64(1),
                    Value::Float64(OrderedFloat(3.3)),
                ],
            ],
        )
    }

    fn build_key_values(schema: &RegionMetadataRef, k0: String, k1: i64, len: usize) -> KeyValues {
        let column_schema = schema
            .column_metadatas
            .iter()
            .map(|c| api::v1::ColumnSchema {
                column_name: c.column_schema.name.clone(),
                datatype: ColumnDataTypeWrapper::try_from(c.column_schema.data_type.clone())
                    .unwrap()
                    .datatype() as i32,
                semantic_type: c.semantic_type as i32,
                ..Default::default()
            })
            .collect();

        let rows = (0..len)
            .map(|i| Row {
                values: vec![
                    api::v1::Value {
                        value_data: Some(ValueData::StringValue(k0.clone())),
                    },
                    api::v1::Value {
                        value_data: Some(ValueData::I64Value(k1)),
                    },
                    api::v1::Value {
                        value_data: Some(ValueData::TimestampMillisecondValue(i as i64)),
                    },
                    api::v1::Value {
                        value_data: Some(ValueData::I64Value(i as i64)),
                    },
                    api::v1::Value {
                        value_data: Some(ValueData::F64Value(i as f64)),
                    },
                ],
            })
            .collect();
        let mutation = api::v1::Mutation {
            op_type: 1,
            sequence: 0,
            rows: Some(Rows {
                schema: column_schema,
                rows,
            }),
            write_hint: None,
        };
        KeyValues::new(schema.as_ref(), mutation).unwrap()
    }

    #[test]
    fn test_series_set_concurrency() {
        let schema = schema_for_test();
        let row_codec = Arc::new(DensePrimaryKeyCodec::with_fields(
            schema
                .primary_key_columns()
                .map(|c| {
                    (
                        c.column_id,
                        SortField::new(c.column_schema.data_type.clone()),
                    )
                })
                .collect(),
        ));
        let set = Arc::new(SeriesSet::new(schema.clone(), row_codec));

        let concurrency = 32;
        let pk_num = concurrency * 2;
        let mut handles = Vec::with_capacity(concurrency);
        for i in 0..concurrency {
            let set = set.clone();
            let schema = schema.clone();
            let column_schemas = schema
                .column_metadatas
                .iter()
                .map(column_metadata_to_column_schema)
                .collect::<Vec<_>>();
            let handle = std::thread::spawn(move || {
                for j in i * 100..(i + 1) * 100 {
                    let pk = j % pk_num;
                    let primary_key = format!("pk-{}", pk).as_bytes().to_vec();

                    let kvs = KeyValues::new(
                        &schema,
                        Mutation {
                            op_type: OpType::Put as i32,
                            sequence: j as u64,
                            rows: Some(Rows {
                                schema: column_schemas.clone(),
                                rows: vec![Row {
                                    values: vec![
                                        api::v1::Value {
                                            value_data: Some(ValueData::StringValue(format!(
                                                "{}",
                                                j
                                            ))),
                                        },
                                        api::v1::Value {
                                            value_data: Some(ValueData::I64Value(j as i64)),
                                        },
                                        api::v1::Value {
                                            value_data: Some(ValueData::TimestampMillisecondValue(
                                                j as i64,
                                            )),
                                        },
                                        api::v1::Value {
                                            value_data: Some(ValueData::I64Value(j as i64)),
                                        },
                                        api::v1::Value {
                                            value_data: Some(ValueData::F64Value(j as f64)),
                                        },
                                    ],
                                }],
                            }),
                            write_hint: None,
                        },
                    )
                    .unwrap();
                    set.push_to_series(primary_key, &kvs.iter().next().unwrap());
                }
            });
            handles.push(handle);
        }
        for h in handles {
            h.join().unwrap();
        }

        let mut timestamps = Vec::with_capacity(concurrency * 100);
        let mut sequences = Vec::with_capacity(concurrency * 100);
        let mut op_types = Vec::with_capacity(concurrency * 100);
        let mut v0 = Vec::with_capacity(concurrency * 100);

        for i in 0..pk_num {
            let pk = format!("pk-{}", i).as_bytes().to_vec();
            let series = set.get_series(&pk).unwrap();
            let mut guard = series.write().unwrap();
            let values = guard.compact(&schema).unwrap();
            timestamps.extend(values.sequence.iter_data().map(|v| v.unwrap() as i64));
            sequences.extend(values.sequence.iter_data().map(|v| v.unwrap() as i64));
            op_types.extend(values.op_type.iter_data().map(|v| v.unwrap()));
            v0.extend(
                values
                    .fields
                    .first()
                    .unwrap()
                    .as_any()
                    .downcast_ref::<Int64Vector>()
                    .unwrap()
                    .iter_data()
                    .map(|v| v.unwrap()),
            );
        }

        let expected_sequence = (0..(concurrency * 100) as i64).collect::<HashSet<_>>();
        assert_eq!(
            expected_sequence,
            sequences.iter().copied().collect::<HashSet<_>>()
        );

        op_types.iter().all(|op| *op == OpType::Put as u8);
        assert_eq!(
            expected_sequence,
            timestamps.iter().copied().collect::<HashSet<_>>()
        );

        assert_eq!(timestamps, sequences);
        assert_eq!(v0, timestamps);
    }

    #[test]
    fn test_memtable() {
        common_telemetry::init_default_ut_logging();
        check_memtable_dedup(true);
        check_memtable_dedup(false);
    }

    fn check_memtable_dedup(dedup: bool) {
        let schema = schema_for_test();
        let kvs = build_key_values(&schema, "hello".to_string(), 42, 100);
        let memtable = TimeSeriesMemtable::new(schema, 42, None, dedup, MergeMode::LastRow);
        memtable.write(&kvs).unwrap();
        memtable.write(&kvs).unwrap();

        let mut expected_ts: HashMap<i64, usize> = HashMap::new();
        for ts in kvs
            .iter()
            .map(|kv| kv.timestamp().as_timestamp().unwrap().unwrap().value())
        {
            *expected_ts.entry(ts).or_default() += if dedup { 1 } else { 2 };
        }

        let iter = memtable.iter(None, None, None).unwrap();
        let mut read = HashMap::new();

        for ts in iter
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
        {
            *read.entry(ts).or_default() += 1;
        }
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
    fn test_memtable_projection() {
        common_telemetry::init_default_ut_logging();
        let schema = schema_for_test();
        let kvs = build_key_values(&schema, "hello".to_string(), 42, 100);
        let memtable = TimeSeriesMemtable::new(schema, 42, None, true, MergeMode::LastRow);
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
        assert_eq!((0..100i64).collect::<Vec<_>>(), v0_all);
    }

    #[test]
    fn test_memtable_concurrent_write_read() {
        common_telemetry::init_default_ut_logging();
        let schema = schema_for_test();
        let memtable = Arc::new(TimeSeriesMemtable::new(
            schema.clone(),
            42,
            None,
            true,
            MergeMode::LastRow,
        ));

        // Number of writer threads
        let num_writers = 10;
        // Number of reader threads
        let num_readers = 5;
        // Number of series per writer
        let series_per_writer = 100;
        // Number of rows per series
        let rows_per_series = 10;
        // Total number of series
        let total_series = num_writers * series_per_writer;

        // Create a barrier to synchronize the start of all threads
        let barrier = Arc::new(std::sync::Barrier::new(num_writers + num_readers + 1));

        // Spawn writer threads
        let mut writer_handles = Vec::with_capacity(num_writers);
        for writer_id in 0..num_writers {
            let memtable = memtable.clone();
            let schema = schema.clone();
            let barrier = barrier.clone();

            let handle = std::thread::spawn(move || {
                // Wait for all threads to be ready
                barrier.wait();

                // Create and write series
                for series_id in 0..series_per_writer {
                    let series_key = format!("writer-{}-series-{}", writer_id, series_id);
                    let kvs =
                        build_key_values(&schema, series_key, series_id as i64, rows_per_series);
                    memtable.write(&kvs).unwrap();
                }
            });

            writer_handles.push(handle);
        }

        // Spawn reader threads
        let mut reader_handles = Vec::with_capacity(num_readers);
        for _ in 0..num_readers {
            let memtable = memtable.clone();
            let barrier = barrier.clone();

            let handle = std::thread::spawn(move || {
                barrier.wait();

                for _ in 0..10 {
                    let iter = memtable.iter(None, None, None).unwrap();
                    for batch_result in iter {
                        let _ = batch_result.unwrap();
                    }
                }
            });

            reader_handles.push(handle);
        }

        barrier.wait();

        for handle in writer_handles {
            handle.join().unwrap();
        }
        for handle in reader_handles {
            handle.join().unwrap();
        }

        let iter = memtable.iter(None, None, None).unwrap();
        let mut series_count = 0;
        let mut row_count = 0;

        for batch_result in iter {
            let batch = batch_result.unwrap();
            series_count += 1;
            row_count += batch.num_rows();
        }
        assert_eq!(total_series, series_count);
        assert_eq!(total_series * rows_per_series, row_count);
    }
}
