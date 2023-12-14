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
use std::sync::atomic::{AtomicI64, AtomicU32, Ordering};
use std::sync::{Arc, RwLock};
use std::time::{Duration, Instant};

use api::v1::OpType;
use common_telemetry::{debug, error, trace};
use common_time::Timestamp;
use datafusion::physical_plan::PhysicalExpr;
use datafusion_common::ScalarValue;
use datafusion_expr::ColumnarValue;
use datatypes::arrow;
use datatypes::arrow::array::{ArrayRef, BooleanArray};
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::data_type::DataType;
use datatypes::prelude::{MutableVector, ScalarVectorBuilder, Vector, VectorRef};
use datatypes::value::ValueRef;
use datatypes::vectors::{
    Helper, UInt64Vector, UInt64VectorBuilder, UInt8Vector, UInt8VectorBuilder,
};
use snafu::{ensure, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;
use table::predicate::Predicate;

use crate::error::{
    ComputeArrowSnafu, ConvertVectorSnafu, NewRecordBatchSnafu, PrimaryKeyLengthMismatchSnafu,
    Result,
};
use crate::flush::WriteBufferManagerRef;
use crate::memtable::{
    AllocTracker, BoxedBatchIterator, KeyValues, Memtable, MemtableBuilder, MemtableId,
    MemtableRef, MemtableStats,
};
use crate::metrics::{READ_ROWS_TOTAL, READ_STAGE_ELAPSED};
use crate::read::{Batch, BatchBuilder, BatchColumn};
use crate::row_converter::{McmpRowCodec, RowCodec, SortField};

/// Initial vector builder capacity.
const INITIAL_BUILDER_CAPACITY: usize = 32;

/// Builder to build [TimeSeriesMemtable].
#[derive(Debug, Default)]
pub struct TimeSeriesMemtableBuilder {
    id: AtomicU32,
    write_buffer_manager: Option<WriteBufferManagerRef>,
}

impl TimeSeriesMemtableBuilder {
    /// Creates a new builder with specific `write_buffer_manager`.
    pub fn new(write_buffer_manager: Option<WriteBufferManagerRef>) -> Self {
        Self {
            id: AtomicU32::new(0),
            write_buffer_manager,
        }
    }
}

impl MemtableBuilder for TimeSeriesMemtableBuilder {
    fn build(&self, metadata: &RegionMetadataRef) -> MemtableRef {
        let id = self.id.fetch_add(1, Ordering::Relaxed);
        Arc::new(TimeSeriesMemtable::new(
            metadata.clone(),
            id,
            self.write_buffer_manager.clone(),
        ))
    }
}

/// Memtable implementation that groups rows by their primary key.
pub struct TimeSeriesMemtable {
    id: MemtableId,
    region_metadata: RegionMetadataRef,
    row_codec: Arc<McmpRowCodec>,
    series_set: SeriesSet,
    alloc_tracker: AllocTracker,
    max_timestamp: AtomicI64,
    min_timestamp: AtomicI64,
}

impl TimeSeriesMemtable {
    pub fn new(
        region_metadata: RegionMetadataRef,
        id: MemtableId,
        write_buffer_manager: Option<WriteBufferManagerRef>,
    ) -> Self {
        let row_codec = Arc::new(McmpRowCodec::new(
            region_metadata
                .primary_key_columns()
                .map(|c| SortField::new(c.column_schema.data_type.clone()))
                .collect(),
        ));
        let series_set = SeriesSet::new(region_metadata.clone(), row_codec.clone());
        Self {
            id,
            region_metadata,
            series_set,
            row_codec,
            alloc_tracker: AllocTracker::new(write_buffer_manager),
            max_timestamp: AtomicI64::new(i64::MIN),
            min_timestamp: AtomicI64::new(i64::MAX),
        }
    }

    /// Updates memtable stats.
    fn update_stats(&self, request_size: usize, min: i64, max: i64) {
        self.alloc_tracker.on_allocation(request_size);

        loop {
            let current_min = self.min_timestamp.load(Ordering::Relaxed);
            if min >= current_min {
                break;
            }

            let Err(updated) = self.min_timestamp.compare_exchange(
                current_min,
                min,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) else {
                break;
            };

            if updated == min {
                break;
            }
        }

        loop {
            let current_max = self.max_timestamp.load(Ordering::Relaxed);
            if max <= current_max {
                break;
            }

            let Err(updated) = self.max_timestamp.compare_exchange(
                current_max,
                max,
                Ordering::Relaxed,
                Ordering::Relaxed,
            ) else {
                break;
            };

            if updated == max {
                break;
            }
        }
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
        let mut allocated = 0;
        let mut min_ts = i64::MAX;
        let mut max_ts = i64::MIN;

        for kv in kvs.iter() {
            ensure!(
                kv.num_primary_keys() == self.row_codec.num_fields(),
                PrimaryKeyLengthMismatchSnafu {
                    expect: self.row_codec.num_fields(),
                    actual: kv.num_primary_keys()
                }
            );
            let primary_key_encoded = self.row_codec.encode(kv.primary_keys())?;
            let fields = kv.fields().collect::<Vec<_>>();

            allocated += fields.iter().map(|v| v.data_size()).sum::<usize>();
            let (series, series_allocated) = self.series_set.get_or_add_series(primary_key_encoded);
            allocated += series_allocated;

            // safety: timestamp of kv must be both present and a valid timestamp value.
            let ts = kv.timestamp().as_timestamp().unwrap().unwrap().value();
            min_ts = min_ts.min(ts);
            max_ts = max_ts.max(ts);

            let mut guard = series.write().unwrap();
            guard.push(kv.timestamp(), kv.sequence(), kv.op_type(), fields);
        }
        allocated += kvs.num_rows() * std::mem::size_of::<Timestamp>();
        allocated += kvs.num_rows() * std::mem::size_of::<OpType>();

        // TODO(hl): this maybe inaccurate since for-iteration may return early.
        // We may lift the primary key length check out of Memtable::write
        // so that we can ensure writing to memtable will succeed.
        self.update_stats(allocated, min_ts, max_ts);
        Ok(())
    }

    fn iter(
        &self,
        projection: Option<&[ColumnId]>,
        filters: Option<Predicate>,
    ) -> BoxedBatchIterator {
        let projection = if let Some(projection) = projection {
            projection.iter().copied().collect()
        } else {
            self.region_metadata
                .field_columns()
                .map(|c| c.column_id)
                .collect()
        };

        Box::new(self.series_set.iter_series(projection, filters))
    }

    fn is_empty(&self) -> bool {
        self.series_set.series.read().unwrap().is_empty()
    }

    fn mark_immutable(&self) {
        self.alloc_tracker.done_allocating();
    }

    fn stats(&self) -> MemtableStats {
        let estimated_bytes = self.alloc_tracker.bytes_allocated();

        if estimated_bytes == 0 {
            // no rows ever written
            return MemtableStats {
                estimated_bytes,
                time_range: None,
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
        }
    }
}

type SeriesRwLockMap = RwLock<BTreeMap<Vec<u8>, Arc<RwLock<Series>>>>;

struct SeriesSet {
    region_metadata: RegionMetadataRef,
    series: Arc<SeriesRwLockMap>,
    codec: Arc<McmpRowCodec>,
}

impl SeriesSet {
    fn new(region_metadata: RegionMetadataRef, codec: Arc<McmpRowCodec>) -> Self {
        Self {
            region_metadata,
            series: Default::default(),
            codec,
        }
    }
}

impl SeriesSet {
    /// Returns the series for given primary key, or create a new series if not already exist,
    /// along with the allocated memory footprint for primary keys.
    fn get_or_add_series(&self, primary_key: Vec<u8>) -> (Arc<RwLock<Series>>, usize) {
        if let Some(series) = self.series.read().unwrap().get(&primary_key) {
            return (series.clone(), 0);
        };
        let s = Arc::new(RwLock::new(Series::new(&self.region_metadata)));
        let mut indices = self.series.write().unwrap();
        match indices.entry(primary_key) {
            Entry::Vacant(v) => {
                let key_len = v.key().len();
                v.insert(s.clone());
                (s, key_len)
            }
            // safety: series must exist at given index.
            Entry::Occupied(v) => (v.get().clone(), 0),
        }
    }

    /// Iterates all series in [SeriesSet].
    fn iter_series(&self, projection: HashSet<ColumnId>, predicate: Option<Predicate>) -> Iter {
        let (primary_key_builders, primary_key_schema) =
            primary_key_builders(&self.region_metadata, 1);

        let physical_exprs: Vec<_> = predicate
            .and_then(|p| p.to_physical_exprs(&primary_key_schema).ok())
            .unwrap_or_default();

        Iter {
            metadata: self.region_metadata.clone(),
            series: self.series.clone(),
            projection,
            last_key: None,
            predicate: physical_exprs,
            pk_schema: primary_key_schema,
            primary_key_builders,
            codec: self.codec.clone(),
            metrics: Metrics::default(),
        }
    }
}

/// Creates primary key array builders and arrow's schema for primary keys of given region schema.
fn primary_key_builders(
    region_metadata: &RegionMetadataRef,
    num_pk_rows: usize,
) -> (Vec<Box<dyn MutableVector>>, arrow::datatypes::SchemaRef) {
    let (builders, fields): (_, Vec<_>) = region_metadata
        .primary_key_columns()
        .map(|pk| {
            (
                pk.column_schema
                    .data_type
                    .create_mutable_vector(num_pk_rows),
                arrow::datatypes::Field::new(
                    pk.column_schema.name.clone(),
                    pk.column_schema.data_type.as_arrow_type(),
                    pk.column_schema.is_nullable(),
                ),
            )
        })
        .unzip();
    (builders, Arc::new(arrow::datatypes::Schema::new(fields)))
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
    predicate: Vec<Arc<dyn PhysicalExpr>>,
    pk_schema: arrow::datatypes::SchemaRef,
    primary_key_builders: Vec<Box<dyn MutableVector>>,
    codec: Arc<McmpRowCodec>,
    metrics: Metrics,
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
                    &mut self.primary_key_builders,
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
            let batch =
                values.and_then(|v| v.to_batch(primary_key, &self.metadata, &self.projection));

            // Update metrics.
            self.metrics.num_batches += 1;
            self.metrics.num_rows += batch.as_ref().map(|b| b.num_rows()).unwrap_or(0);
            self.metrics.scan_cost += start.elapsed();
            return Some(batch);
        }
        self.metrics.scan_cost += start.elapsed();

        None
    }
}

fn prune_primary_key(
    codec: &Arc<McmpRowCodec>,
    pk: &[u8],
    series: &mut Series,
    builders: &mut [Box<dyn MutableVector>],
    pk_schema: arrow::datatypes::SchemaRef,
    predicate: &[Arc<dyn PhysicalExpr>],
) -> bool {
    // no primary key, we simply return true.
    if pk_schema.fields().is_empty() {
        return true;
    }

    if let Some(rb) = series.pk_cache.as_ref() {
        prune_inner(predicate, rb).unwrap_or(true)
    } else {
        let rb = match pk_to_record_batch(codec, pk, builders, pk_schema) {
            Ok(rb) => rb,
            Err(e) => {
                error!(e; "Failed to build record batch from primary keys");
                return true;
            }
        };
        let res = prune_inner(predicate, &rb).unwrap_or(true);
        series.update_pk_cache(rb);
        res
    }
}

fn prune_inner(predicates: &[Arc<dyn PhysicalExpr>], primary_key: &RecordBatch) -> Result<bool> {
    for expr in predicates {
        // evaluate every filter against primary key
        let Ok(eva) = expr.evaluate(primary_key) else {
            continue;
        };
        let result = match eva {
            ColumnarValue::Array(array) => {
                let predicate_array = array.as_any().downcast_ref::<BooleanArray>().unwrap();
                predicate_array
                    .into_iter()
                    .map(|x| x.unwrap_or(true))
                    .next()
                    .unwrap_or(true)
            }
            // result was a column
            ColumnarValue::Scalar(ScalarValue::Boolean(v)) => v.unwrap_or(true),
            _ => {
                unreachable!("Unexpected primary key record batch evaluation result: {:?}, primary key: {:?}", eva, primary_key);
            }
        };
        trace!(
            "Evaluate primary key {:?} against filter: {:?}, result: {:?}",
            primary_key,
            expr,
            result
        );
        if !result {
            return Ok(false);
        }
    }
    Ok(true)
}

fn pk_to_record_batch(
    codec: &Arc<McmpRowCodec>,
    bytes: &[u8],
    builders: &mut [Box<dyn MutableVector>],
    pk_schema: arrow::datatypes::SchemaRef,
) -> Result<RecordBatch> {
    let pk_values = codec.decode(bytes).unwrap();

    let arrays = builders
        .iter_mut()
        .zip(pk_values.iter())
        .map(|(builder, pk_value)| {
            builder.push_value_ref(pk_value.as_value_ref());
            builder.to_vector().to_arrow_array()
        })
        .collect();

    RecordBatch::try_new(pk_schema, arrays).context(NewRecordBatchSnafu)
}

/// A `Series` holds a list of field values of some given primary key.
struct Series {
    pk_cache: Option<RecordBatch>,
    active: ValueBuilder,
    frozen: Vec<Values>,
}

impl Series {
    fn new(region_metadata: &RegionMetadataRef) -> Self {
        Self {
            pk_cache: None,
            active: ValueBuilder::new(region_metadata, INITIAL_BUILDER_CAPACITY),
            frozen: vec![],
        }
    }

    /// Pushes a row of values into Series.
    fn push(&mut self, ts: ValueRef, sequence: u64, op_type: OpType, values: Vec<ValueRef>) {
        self.active.push(ts, sequence, op_type as u8, values);
    }

    fn update_pk_cache(&mut self, pk_batch: RecordBatch) {
        self.pk_cache = Some(pk_batch);
    }

    /// Freezes the active part and push it to `frozen`.
    fn freeze(&mut self, region_metadata: &RegionMetadataRef) {
        if self.active.len() != 0 {
            let mut builder = ValueBuilder::new(region_metadata, INITIAL_BUILDER_CAPACITY);
            std::mem::swap(&mut self.active, &mut builder);
            self.frozen.push(Values::from(builder));
        }
    }

    /// Freezes active part to frozen part and compact frozen part to reduce memory fragmentation.
    /// Returns the frozen and compacted values.
    fn compact(&mut self, region_metadata: &RegionMetadataRef) -> Result<Values> {
        self.freeze(region_metadata);

        let mut frozen = self.frozen.clone();

        // Each series must contain at least one row
        debug_assert!(!frozen.is_empty());

        let values = if frozen.len() == 1 {
            frozen.pop().unwrap()
        } else {
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
            self.frozen = vec![values.clone()];
            values
        };
        Ok(values)
    }
}

/// `ValueBuilder` holds all the vector builders for field columns.
struct ValueBuilder {
    timestamp: Box<dyn MutableVector>,
    sequence: UInt64VectorBuilder,
    op_type: UInt8VectorBuilder,
    fields: Vec<Box<dyn MutableVector>>,
}

impl ValueBuilder {
    fn new(region_metadata: &RegionMetadataRef, capacity: usize) -> Self {
        let timestamp = region_metadata
            .time_index_column()
            .column_schema
            .data_type
            .create_mutable_vector(capacity);
        let sequence = UInt64VectorBuilder::with_capacity(capacity);
        let op_type = UInt8VectorBuilder::with_capacity(capacity);

        let fields = region_metadata
            .field_columns()
            .map(|c| c.column_schema.data_type.create_mutable_vector(capacity))
            .collect();

        Self {
            timestamp,
            sequence,
            op_type,
            fields,
        }
    }

    /// Pushes a new row to `ValueBuilder`.
    /// We don't need primary keys since they've already be encoded.
    fn push(&mut self, ts: ValueRef, sequence: u64, op_type: u8, fields: Vec<ValueRef>) {
        debug_assert_eq!(fields.len(), self.fields.len());
        self.timestamp.push_value_ref(ts);
        self.sequence.push_value_ref(ValueRef::UInt64(sequence));
        self.op_type.push_value_ref(ValueRef::UInt8(op_type));
        for (idx, field_value) in fields.into_iter().enumerate() {
            self.fields[idx].push_value_ref(field_value);
        }
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
struct Values {
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
        batch.sort_and_dedup()?;
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
        let fields = value
            .fields
            .iter_mut()
            .map(|v| v.to_vector())
            .collect::<Vec<_>>();
        let sequence = Arc::new(value.sequence.finish());
        let op_type = Arc::new(value.op_type.finish());
        let timestamp = value.timestamp.to_vector();

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

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use api::helper::ColumnDataTypeWrapper;
    use api::v1::value::ValueData;
    use api::v1::{Row, Rows, SemanticType};
    use common_time::Timestamp;
    use datatypes::prelude::{ConcreteDataType, ScalarVector};
    use datatypes::schema::ColumnSchema;
    use datatypes::value::{OrderedFloat, Value};
    use datatypes::vectors::{Float64Vector, Int64Vector, TimestampMillisecondVector};
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::storage::RegionId;

    use super::*;

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

    fn field_value_ref(v0: i64, v1: f64) -> Vec<ValueRef<'static>> {
        vec![ValueRef::Int64(v0), ValueRef::Float64(OrderedFloat(v1))]
    }

    fn check_values(values: Values, expect: &[(i64, u64, u8, i64, f64)]) {
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
            .to_batch(b"test", &schema, &[0, 1, 2, 3, 4].into_iter().collect())
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
        };
        KeyValues::new(schema.as_ref(), mutation).unwrap()
    }

    #[test]
    fn test_series_set_concurrency() {
        let schema = schema_for_test();
        let row_codec = Arc::new(McmpRowCodec::new(
            schema
                .primary_key_columns()
                .map(|c| SortField::new(c.column_schema.data_type.clone()))
                .collect(),
        ));
        let set = Arc::new(SeriesSet::new(schema.clone(), row_codec));

        let concurrency = 32;
        let pk_num = concurrency * 2;
        let mut handles = Vec::with_capacity(concurrency);
        for i in 0..concurrency {
            let set = set.clone();
            let handle = std::thread::spawn(move || {
                for j in i * 100..(i + 1) * 100 {
                    let pk = j % pk_num;
                    let primary_key = format!("pk-{}", pk).as_bytes().to_vec();
                    let (series, _) = set.get_or_add_series(primary_key);
                    let mut guard = series.write().unwrap();
                    guard.push(
                        ts_value_ref(j as i64),
                        j as u64,
                        OpType::Put,
                        field_value_ref(j as i64, j as f64),
                    );
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
            let (series, _) = set.get_or_add_series(pk);
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
        let schema = schema_for_test();
        let kvs = build_key_values(&schema, "hello".to_string(), 42, 100);
        let memtable = TimeSeriesMemtable::new(schema, 42, None);
        memtable.write(&kvs).unwrap();

        let expected_ts = kvs
            .iter()
            .map(|kv| kv.timestamp().as_timestamp().unwrap().unwrap().value())
            .collect::<HashSet<_>>();

        let iter = memtable.iter(None, None);
        let read = iter
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
            .collect::<HashSet<_>>();
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
        let memtable = TimeSeriesMemtable::new(schema, 42, None);
        memtable.write(&kvs).unwrap();

        let iter = memtable.iter(Some(&[3]), None);

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
}
