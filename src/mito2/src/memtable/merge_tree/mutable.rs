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

//! Mutable part of the merge tree.

use std::collections::{HashSet, VecDeque};
use std::sync::Arc;
use std::time::{Duration, Instant};

use api::v1::OpType;
use common_telemetry::{debug, warn};
use common_time::Timestamp;
use datafusion::physical_plan::PhysicalExpr;
use datafusion_expr::ColumnarValue;
use datatypes::arrow;
use datatypes::arrow::array::BooleanArray;
use datatypes::arrow::buffer::BooleanBuffer;
use datatypes::arrow::datatypes::Field;
use datatypes::arrow::record_batch::RecordBatch;
use datatypes::data_type::{ConcreteDataType, DataType};
use datatypes::scalars::{ScalarVector, ScalarVectorBuilder};
use datatypes::types::TimestampType;
use datatypes::vectors::{
    BinaryVector, BinaryVectorBuilder, BooleanVector, MutableVector, TimestampMicrosecondVector,
    TimestampMillisecondVector, TimestampNanosecondVector, TimestampSecondVector, UInt32Vector,
    UInt64Vector, UInt64VectorBuilder, UInt8Vector, UInt8VectorBuilder, Vector, VectorOp,
    VectorRef,
};
use snafu::{ensure, ResultExt};
use store_api::metadata::RegionMetadataRef;
use store_api::storage::{ColumnId, RegionId};
use table::predicate::Predicate;

use crate::error::{
    ComputeVectorSnafu, NewRecordBatchSnafu, PrimaryKeyLengthMismatchSnafu, Result,
};
use crate::memtable::key_values::KeyValue;
use crate::memtable::merge_tree::MergeTreeConfig;
use crate::memtable::{BoxedBatchIterator, KeyValues};
use crate::metrics::{READ_ROWS_TOTAL, READ_STAGE_ELAPSED};
use crate::read::{Batch, BatchBuilder, BatchColumn};
use crate::row_converter::{McmpRowCodec, RowCodec};

/// Initial capacity for builders.
// TODO(yingwen): maybe use 0 to avoid allocating spaces for empty columns.
const INITIAL_BUILDER_CAPACITY: usize = 8;

/// Mutable part that buffers input data.
pub(crate) struct MutablePart {
    /// Stores rows without interning primary keys.
    plain_block: Option<PlainBlock>,
}

impl MutablePart {
    /// Creates a new mutable part.
    pub(crate) fn new(_config: &MergeTreeConfig) -> MutablePart {
        MutablePart { plain_block: None }
    }

    /// Write key-values into the part.
    pub(crate) fn write(
        &mut self,
        metadata: &RegionMetadataRef,
        row_codec: &McmpRowCodec,
        kvs: &KeyValues,
        metrics: &mut WriteMetrics,
    ) -> Result<()> {
        let mut primary_key = Vec::new();

        for kv in kvs.iter() {
            ensure!(
                kv.num_primary_keys() == row_codec.num_fields(),
                PrimaryKeyLengthMismatchSnafu {
                    expect: row_codec.num_fields(),
                    actual: kv.num_primary_keys(),
                }
            );
            // Safety: timestamp of kv must be both present and a valid timestamp value.
            let ts = kv.timestamp().as_timestamp().unwrap().unwrap().value();
            metrics.min_ts = metrics.min_ts.min(ts);
            metrics.max_ts = metrics.max_ts.max(ts);
            metrics.value_bytes += kv.fields().map(|v| v.data_size()).sum::<usize>();

            if metadata.primary_key.is_empty() {
                // No primary key.
                self.write_plain(metadata, &kv, None);
                continue;
            }

            // Encode primary key.
            primary_key.clear();
            row_codec.encode_to_vec(kv.primary_keys(), &mut primary_key)?;

            // Add bytes used by the primary key.
            metrics.key_bytes += primary_key.len();

            // TODO(yingwen): Freeze a block if it is large enough (in bytes).
            // Write rows with primary keys.
            self.write_plain(metadata, &kv, Some(&primary_key));
        }

        metrics.value_bytes +=
            kvs.num_rows() * (std::mem::size_of::<Timestamp>() + std::mem::size_of::<OpType>());

        Ok(())
    }

    /// Returns an iter to scan the mutable part.
    pub(crate) fn scan_part(
        &self,
        metadata: &RegionMetadataRef,
        row_codec: Arc<McmpRowCodec>,
        projection: Option<&[ColumnId]>,
        predicate: Option<&Predicate>,
        dedup: bool,
    ) -> Result<BoxedBatchIterator> {
        let mut metrics = ReadMetrics::default();
        let now = Instant::now();

        let projection = if let Some(projection) = projection {
            projection.iter().copied().collect()
        } else {
            metadata.field_columns().map(|c| c.column_id).collect()
        };
        let plain_vectors = match &self.plain_block {
            Some(block) => {
                let mut vectors = block.to_vectors(&projection);

                let prune_start = Instant::now();
                vectors.prune(metadata, &row_codec, predicate, &mut metrics)?;
                metrics.prune_cost = prune_start.elapsed();

                let sort_start = Instant::now();
                vectors.sort_and_dedup(dedup)?;
                metrics.sort_dedup_cost = sort_start.elapsed();

                Some(vectors)
            }
            None => None,
        };
        let offsets = plain_vectors.as_ref().and_then(|v| v.primary_key_offsets());

        // TODO(yingwen): prune cost.
        metrics.init_cost = now.elapsed();

        let iter = Iter {
            region_id: metadata.region_id,
            plain_vectors,
            offsets,
            metrics,
        };

        Ok(Box::new(iter))
    }

    /// Returns true if the part is empty.
    pub(crate) fn is_empty(&self) -> bool {
        self.plain_block
            .as_ref()
            .map(|block| block.is_empty())
            .unwrap_or(true)
    }

    /// Writes the `key_value` and the `primary_key` to the plain block.
    fn write_plain(
        &mut self,
        metadata: &RegionMetadataRef,
        key_value: &KeyValue,
        primary_key: Option<&[u8]>,
    ) {
        let block = self
            .plain_block
            .get_or_insert_with(|| PlainBlock::new(metadata, INITIAL_BUILDER_CAPACITY));

        block.insert_by_key(primary_key, key_value);
    }
}

/// Metrics of writing the mutable part.
pub(crate) struct WriteMetrics {
    /// Size allocated by keys.
    pub(crate) key_bytes: usize,
    /// Size allocated by values.
    pub(crate) value_bytes: usize,
    /// Minimum timestamp.
    pub(crate) min_ts: i64,
    /// Maximum timestamp
    pub(crate) max_ts: i64,
}

impl Default for WriteMetrics {
    fn default() -> Self {
        Self {
            key_bytes: 0,
            value_bytes: 0,
            min_ts: i64::MAX,
            max_ts: i64::MIN,
        }
    }
}

/// Metrics of scanning the mutable part.
#[derive(Debug, Default)]
pub(crate) struct ReadMetrics {
    /// Time used to initialize the iter.
    pub(crate) init_cost: Duration,
    /// Time used to prune rows.
    pub(crate) prune_cost: Duration,
    /// Time used to sort and dedup rows.
    sort_dedup_cost: Duration,
    /// Time used to invoke next.
    pub(crate) next_cost: Duration,
    /// Number of batches returned by the iter.
    pub(crate) num_batches: usize,
    /// Number of rows before prunning.
    num_rows_before_prune: usize,
    /// Number of rows returned.
    pub(crate) num_rows_returned: usize,
    /// Failures during evaluating expressions.
    eval_failure_total: u32,
}

impl ReadMetrics {
    /// Returns the total duration to read the memtable.
    fn total_scan_cost(&self) -> Duration {
        self.init_cost + self.next_cost
    }
}

/// Iterator of the mutable part.
struct Iter {
    region_id: RegionId,
    plain_vectors: Option<PlainBlockVectors>,
    offsets: Option<VecDeque<usize>>,
    metrics: ReadMetrics,
}

impl Iterator for Iter {
    type Item = Result<Batch>;

    fn next(&mut self) -> Option<Self::Item> {
        let now = Instant::now();
        let res = self.next_batch();
        self.metrics.next_cost += now.elapsed();
        res
    }
}

impl Drop for Iter {
    fn drop(&mut self) {
        debug!(
            "Iter region {} memtable, metrics: {:?}",
            self.region_id, self.metrics
        );

        READ_ROWS_TOTAL
            .with_label_values(&["merge_tree_memtable"])
            .inc_by(self.metrics.num_rows_returned as u64);
        READ_STAGE_ELAPSED
            .with_label_values(&["init_scan_memtable"])
            .observe(self.metrics.init_cost.as_secs_f64());
        READ_STAGE_ELAPSED
            .with_label_values(&["prune_memtable"])
            .observe(self.metrics.prune_cost.as_secs_f64());
        READ_STAGE_ELAPSED
            .with_label_values(&["scan_memtable"])
            .observe(self.metrics.total_scan_cost().as_secs_f64());
    }
}

impl Iter {
    fn next_batch(&mut self) -> Option<Result<Batch>> {
        let plain_vectors = self.plain_vectors.as_ref()?;

        if let Some(offsets) = &mut self.offsets {
            let start = offsets.pop_front()?;
            let end = offsets.front().copied()?;

            let batch = plain_vectors.slice_to_batch(start, end);
            self.metrics.num_batches += 1;
            self.metrics.num_rows_returned += batch.as_ref().map(|b| b.num_rows()).unwrap_or(0);

            Some(batch)
        } else {
            let batch = plain_vectors.slice_to_batch(0, plain_vectors.len());
            self.plain_vectors = None;
            self.metrics.num_batches += 1;
            self.metrics.num_rows_returned += batch.as_ref().map(|b| b.num_rows()).unwrap_or(0);

            Some(batch)
        }
    }
}

/// A block that stores primary key directly.
struct PlainBlock {
    /// Primary keys. `None` if no primary key.
    key: Option<BinaryVectorBuilder>,
    /// Values of rows.
    value: ValueBuilder,
}

impl PlainBlock {
    /// Returns a new block.
    fn new(metadata: &RegionMetadataRef, capacity: usize) -> PlainBlock {
        let key = (!metadata.primary_key.is_empty())
            .then(|| BinaryVectorBuilder::with_capacity(capacity));
        PlainBlock {
            key,
            value: ValueBuilder::new(metadata, capacity),
        }
    }

    /// Inserts the row by key.
    fn insert_by_key(&mut self, primary_key: Option<&[u8]>, key_value: &KeyValue) {
        if primary_key.is_some() {
            // Safety: This memtable has a primary key.
            self.key.as_mut().unwrap().push(primary_key);
        }

        self.value.push(key_value);
    }

    /// Gets vectors of columns in the block.
    fn to_vectors(&self, projection: &HashSet<ColumnId>) -> PlainBlockVectors {
        let key = self.key.as_ref().map(|builder| builder.finish_cloned());
        let value = self.value.to_vectors(projection);

        PlainBlockVectors { key, value }
    }

    /// Returns true if the block is empty.
    fn is_empty(&self) -> bool {
        self.value.is_empty()
    }
}

/// Vector builder for a field.
struct FieldBuilder {
    /// Column id of the field column.
    column_id: ColumnId,
    /// Builder of the field column.
    // TODO(yingwen): Lazy initialize field builders.
    builder: Box<dyn MutableVector>,
}

/// Mutable buffer for values.
///
/// Now timestamp, sequence, op_type is not null.
struct ValueBuilder {
    timestamp: Box<dyn MutableVector>,
    sequence: UInt64VectorBuilder,
    op_type: UInt8VectorBuilder,
    fields: Vec<FieldBuilder>,
}

impl ValueBuilder {
    /// Creates a new builder with specific capacity.
    fn new(metadata: &RegionMetadataRef, capacity: usize) -> Self {
        let timestamp = metadata
            .time_index_column()
            .column_schema
            .data_type
            .create_mutable_vector(capacity);
        let sequence = UInt64VectorBuilder::with_capacity(capacity);
        let op_type = UInt8VectorBuilder::with_capacity(capacity);
        let fields = metadata
            .field_columns()
            .map(|c| FieldBuilder {
                column_id: c.column_id,
                builder: c.column_schema.data_type.create_mutable_vector(capacity),
            })
            .collect();

        Self {
            timestamp,
            sequence,
            op_type,
            fields,
        }
    }

    /// Returns true if the builder is empty.
    fn is_empty(&self) -> bool {
        self.timestamp.is_empty()
    }

    /// Pushes the value of a row into the builder.
    ///
    /// # Panics
    /// Panics if fields have unexpected types.
    fn push(&mut self, key_value: &KeyValue) {
        self.timestamp.push_value_ref(key_value.timestamp());
        self.sequence.push(Some(key_value.sequence()));
        self.op_type.push(Some(key_value.op_type() as u8));
        for (idx, value) in key_value.fields().enumerate() {
            self.fields[idx].builder.push_value_ref(value);
        }
    }

    /// Builds vectors from buffered values.
    ///
    /// Ignore field columns not in `projection`.
    fn to_vectors(&self, projection: &HashSet<ColumnId>) -> ValueVectors {
        let sequence = self.sequence.finish_cloned();
        let op_type = self.op_type.finish_cloned();
        let timestamp = self.timestamp.to_vector_cloned();
        // Iterates and filters fields. It keeps the relative order of fields.
        let fields = self
            .fields
            .iter()
            .filter_map(|builder| {
                if projection.contains(&builder.column_id) {
                    Some(BatchColumn {
                        column_id: builder.column_id,
                        data: builder.builder.to_vector_cloned(),
                    })
                } else {
                    None
                }
            })
            .collect();

        ValueVectors {
            timestamp,
            sequence,
            op_type,
            fields,
        }
    }
}

/// Projected columns in a [PlainBlock].
struct PlainBlockVectors {
    /// Primary keys.
    key: Option<BinaryVector>,
    /// Value vectors.
    value: ValueVectors,
}

impl PlainBlockVectors {
    /// Returns number of rows in vectors.
    fn len(&self) -> usize {
        self.value.timestamp.len()
    }

    /// Prune vectors by the predicate.
    ///
    /// It tolerates errors during evaluating expressions but still returns an error on other cases.
    fn prune(
        &mut self,
        metadata: &RegionMetadataRef,
        codec: &Arc<McmpRowCodec>,
        predicate: Option<&Predicate>,
        metrics: &mut ReadMetrics,
    ) -> Result<()> {
        if self.value.is_empty() {
            return Ok(());
        }
        let Some(predicate) = predicate else {
            return Ok(());
        };
        if predicate.exprs().is_empty() {
            return Ok(());
        }

        // TODO(yingwen): Build schema first, then we don't need to build record batch
        // if nothing need to prune.
        let batch = self.record_batch_to_prune(metadata, codec)?;
        metrics.num_rows_before_prune += batch.num_rows();

        let physical_exprs: Vec<_> = predicate
            .to_physical_exprs(&batch.schema())
            .unwrap_or_default();
        if physical_exprs.is_empty() {
            return Ok(());
        }
        let mut mask = BooleanArray::new(BooleanBuffer::new_set(batch.num_rows()), None);
        for expr in &physical_exprs {
            let Some(new_mask) = Self::evaluate_expr(&batch, &**expr, &mask) else {
                metrics.eval_failure_total += 1;
                continue;
            };
            mask = new_mask;
        }
        let mask = BooleanVector::from(mask);

        let pruned = self.filter(&mask)?;
        *self = pruned;

        Ok(())
    }

    /// Sort vectors by key, timestamp, seq desc.
    fn sort_and_dedup(&mut self, dedup: bool) -> Result<()> {
        // TODO(yingwen): Don't sort if vectors are already sorted.
        // Creates entries to sort.
        let indices = if self.key.is_none() {
            self.sort_and_dedup_without_key(dedup)
        } else {
            self.sort_and_dedup_with_key(dedup)
        };

        self.take_in_place(&indices)
    }

    /// Returns indices to sort vectors by pk, timestamp, seq desc.
    fn sort_and_dedup_with_key(&self, dedup: bool) -> UInt32Vector {
        // Safety: `sort_and_dedup()` ensures key exists.
        let pk_vector = self.key.as_ref().unwrap();
        // Safety: primary key is not null.
        let pk_values = pk_vector.iter_data().map(|key| key.unwrap());
        let ts_values = self.value.timestamp_values();
        let seq_values = self.value.sequence.as_arrow().values();
        debug_assert_eq!(pk_vector.len(), seq_values.len());
        debug_assert_eq!(ts_values.len(), seq_values.len());

        let mut index_and_key: Vec<_> = pk_values
            .zip(ts_values.iter())
            .zip(seq_values.iter())
            .enumerate()
            .map(|(index, key)| (index, (key.0 .0, *key.0 .1, *key.1)))
            .collect();
        index_and_key.sort_unstable_by(|(_, a), (_, b)| {
            a.0.cmp(b.0) // compare pk
                .then_with(|| a.1.cmp(&b.1)) // compare timestamp
                .then_with(|| b.2.cmp(&a.2)) // then compare seq desc
        });

        if dedup {
            // Dedup by primary key, timestamp
            index_and_key.dedup_by_key(|x| (x.1 .0, x.1 .1));
        }

        UInt32Vector::from_iter_values(index_and_key.iter().map(|(idx, _)| *idx as u32))
    }

    /// Returns indices to sort vectors by timestamp, seq desc.
    fn sort_and_dedup_without_key(&self, dedup: bool) -> UInt32Vector {
        let ts_values = self.value.timestamp_values();
        let seq_values = self.value.sequence.as_arrow().values();
        debug_assert_eq!(ts_values.len(), seq_values.len());

        let mut index_and_key: Vec<_> = ts_values
            .iter()
            .zip(seq_values.iter())
            .enumerate()
            .collect();
        index_and_key.sort_unstable_by(|(_, a), (_, b)| {
            a.0.cmp(b.0) // compare timestamp
                .then_with(|| b.1.cmp(a.1)) // then compare seq desc
        });

        if dedup {
            // Dedup by timestamp
            index_and_key.dedup_by_key(|x| x.1 .0);
        }

        UInt32Vector::from_iter_values(index_and_key.iter().map(|(idx, _)| *idx as u32))
    }

    /// Evaluate the expression and compute the new mask.
    ///
    /// Returns `None` on failure.
    fn evaluate_expr(
        batch: &RecordBatch,
        expr: &dyn PhysicalExpr,
        mask: &BooleanArray,
    ) -> Option<BooleanArray> {
        let eval = expr
            .evaluate(batch)
            .inspect_err(|e| debug!("Failed to evaluate expr, err: {e}"))
            .ok()?;
        let ColumnarValue::Array(array) = eval else {
            warn!("Unexpected eval result: {eval:?}");
            return None;
        };

        let bool_array = array.as_any().downcast_ref::<BooleanArray>()?;
        arrow::compute::and(mask, bool_array)
            .inspect_err(|e| warn!("Failed to compute mask, {e}"))
            .ok()
    }

    /// Convert vectors to an arrow [RecordBatch] to prune.
    ///
    /// The column order in the record batch is unspecific.
    fn record_batch_to_prune(
        &self,
        metadata: &RegionMetadataRef,
        codec: &Arc<McmpRowCodec>,
    ) -> Result<RecordBatch> {
        debug_assert!(!self.value.is_empty());

        // Prunes pk, fields and ts.
        let field_num = metadata.primary_key.len() + self.value.fields.len() + 1;
        let mut fields = Vec::with_capacity(field_num);
        let mut arrays = Vec::with_capacity(field_num);
        // Decode pk.
        if let Some(pk_vector) = &self.key {
            let mut pk_builders = Vec::with_capacity(metadata.primary_key.len());
            let num_pk_rows = pk_vector.len();
            for pk_col in metadata.primary_key_columns() {
                pk_builders.push(
                    pk_col
                        .column_schema
                        .data_type
                        .create_mutable_vector(num_pk_rows),
                );
                fields.push(Field::new(
                    &pk_col.column_schema.name,
                    pk_col.column_schema.data_type.as_arrow_type(),
                    pk_col.column_schema.is_nullable(),
                ));
            }

            self.decode_primary_keys_to(codec, &mut pk_builders);
            for mut pk_builder in pk_builders {
                arrays.push(pk_builder.to_vector().to_arrow_array());
            }
        }

        // Time index.
        let time_index = metadata.time_index_column();
        fields.push(Field::new(
            &time_index.column_schema.name,
            time_index.column_schema.data_type.as_arrow_type(),
            time_index.column_schema.is_nullable(),
        ));
        arrays.push(self.value.timestamp.to_arrow_array());

        // Field columns.
        for column in &self.value.fields {
            // Safety: the metadata should contain this field as the ValueBuilder always builds all fields.
            let column_meta = metadata.column_by_id(column.column_id).unwrap();
            fields.push(Field::new(
                &column_meta.column_schema.name,
                column_meta.column_schema.data_type.as_arrow_type(),
                column_meta.column_schema.is_nullable(),
            ));
            arrays.push(column.data.to_arrow_array());
        }

        let schema = Arc::new(arrow::datatypes::Schema::new(fields));
        RecordBatch::try_new(schema, arrays).context(NewRecordBatchSnafu)
    }

    /// Converts the slice to a [Batch].
    fn slice_to_batch(&self, start: usize, end: usize) -> Result<Batch> {
        let num_rows = end - start;
        let primary_key = self
            .key
            .as_ref()
            .and_then(|pk_vector| pk_vector.get_data(start))
            .map(|pk| pk.to_vec())
            .unwrap_or_default();

        let mut builder = BatchBuilder::new(primary_key);
        builder
            .timestamps_array(self.value.timestamp.slice(start, num_rows).to_arrow_array())?
            .sequences_array(
                self.value
                    .sequence
                    .get_slice(start, num_rows)
                    .to_arrow_array(),
            )?
            .op_types_array(
                self.value
                    .op_type
                    .get_slice(start, num_rows)
                    .to_arrow_array(),
            )?;
        for batch_column in &self.value.fields {
            builder.push_field(BatchColumn {
                column_id: batch_column.column_id,
                data: batch_column.data.slice(start, num_rows),
            });
        }

        builder.build()
    }

    fn decode_primary_keys_to(
        &self,
        codec: &Arc<McmpRowCodec>,
        pk_builders: &mut [Box<dyn MutableVector>],
    ) {
        // Safety: `record_batch_to_prune()` ensures key is not None.
        let pk_vector = self.key.as_ref().unwrap();
        for pk in pk_vector.iter_data() {
            // Safety: key is not null.
            let pk = pk.unwrap();
            // Safety: Pk is encoded from values. If decode returns an error, it should be a bug.
            let pk_values = codec.decode(pk).unwrap();

            for (builder, pk_value) in pk_builders.iter_mut().zip(&pk_values) {
                builder.push_value_ref(pk_value.as_value_ref());
            }
        }
    }

    fn filter(&self, predicate: &BooleanVector) -> Result<PlainBlockVectors> {
        let key = self
            .key
            .as_ref()
            .map(|pk_vector| {
                // Safety: The vector is binary type.
                let v = pk_vector
                    .filter(predicate)
                    .context(ComputeVectorSnafu)?
                    .as_any()
                    .downcast_ref::<BinaryVector>()
                    .unwrap()
                    .clone();
                Ok(v)
            })
            .transpose()?;
        let value = self.value.filter(predicate)?;

        Ok(PlainBlockVectors { key, value })
    }

    fn take_in_place(&mut self, indices: &UInt32Vector) -> Result<()> {
        // Safety: we know the vector type.
        if let Some(key_vector) = &self.key {
            let key_vector = key_vector
                .take(indices)
                .context(ComputeVectorSnafu)?
                .as_any()
                .downcast_ref::<BinaryVector>()
                .unwrap()
                .clone();
            self.key = Some(key_vector);
        }
        self.value.timestamp = self
            .value
            .timestamp
            .take(indices)
            .context(ComputeVectorSnafu)?;
        self.value.sequence = self
            .value
            .sequence
            .take(indices)
            .context(ComputeVectorSnafu)?
            .as_any()
            .downcast_ref::<UInt64Vector>()
            .unwrap()
            .clone();
        self.value.op_type = self
            .value
            .op_type
            .take(indices)
            .context(ComputeVectorSnafu)?
            .as_any()
            .downcast_ref::<UInt8Vector>()
            .unwrap()
            .clone();
        for batch_column in &mut self.value.fields {
            batch_column.data = batch_column
                .data
                .take(indices)
                .context(ComputeVectorSnafu)?;
        }

        Ok(())
    }

    /// Compute offsets of different primary keys in the array.
    fn primary_key_offsets(&self) -> Option<VecDeque<usize>> {
        let pk_vector = self.key.as_ref()?;
        if pk_vector.is_empty() {
            return Some(VecDeque::new());
        }

        // Init offsets.
        let mut offsets = VecDeque::new();
        offsets.push_back(0);
        for (i, (current, next)) in pk_vector
            .iter_data()
            .take(pk_vector.len() - 1)
            .zip(pk_vector.iter_data().skip(1))
            .enumerate()
        {
            // Safety: key is not null.
            let (current, next) = (current.unwrap(), next.unwrap());
            if current != next {
                // We meet a new key, push the next index as end offset of the current key.
                offsets.push_back(i + 1);
            }
        }
        // The last end offset.
        offsets.push_back(pk_vector.len());

        Some(offsets)
    }
}

/// [ValueVectors] holds immutable vectors of field columns, `timestamp`, `sequence` and `op_type`.
///
/// Note that fields might be projected.
struct ValueVectors {
    timestamp: VectorRef,
    sequence: UInt64Vector,
    op_type: UInt8Vector,
    fields: Vec<BatchColumn>,
}

impl ValueVectors {
    /// Returns true if it is empty.
    fn is_empty(&self) -> bool {
        self.timestamp.is_empty()
    }

    /// Returns values of the time index column.
    fn timestamp_values(&self) -> &[i64] {
        // Safety: time index always has timestamp type.
        match self.timestamp.data_type() {
            ConcreteDataType::Timestamp(t) => match t {
                TimestampType::Second(_) => self
                    .timestamp
                    .as_any()
                    .downcast_ref::<TimestampSecondVector>()
                    .unwrap()
                    .as_arrow()
                    .values(),
                TimestampType::Millisecond(_) => self
                    .timestamp
                    .as_any()
                    .downcast_ref::<TimestampMillisecondVector>()
                    .unwrap()
                    .as_arrow()
                    .values(),
                TimestampType::Microsecond(_) => self
                    .timestamp
                    .as_any()
                    .downcast_ref::<TimestampMicrosecondVector>()
                    .unwrap()
                    .as_arrow()
                    .values(),
                TimestampType::Nanosecond(_) => self
                    .timestamp
                    .as_any()
                    .downcast_ref::<TimestampNanosecondVector>()
                    .unwrap()
                    .as_arrow()
                    .values(),
            },
            other => unreachable!("Unexpected type {:?}", other),
        }
    }

    fn filter(&self, predicate: &BooleanVector) -> Result<ValueVectors> {
        let timestamp = self
            .timestamp
            .filter(predicate)
            .context(ComputeVectorSnafu)?;
        // Safety: we know the vector type.
        let sequence = self
            .sequence
            .filter(predicate)
            .context(ComputeVectorSnafu)?
            .as_any()
            .downcast_ref::<UInt64Vector>()
            .unwrap()
            .clone();
        let op_type = self
            .op_type
            .filter(predicate)
            .context(ComputeVectorSnafu)?
            .as_any()
            .downcast_ref::<UInt8Vector>()
            .unwrap()
            .clone();
        let fields = self
            .fields
            .iter()
            .map(|col| {
                Ok(BatchColumn {
                    column_id: col.column_id,
                    data: col.data.filter(predicate).context(ComputeVectorSnafu)?,
                })
            })
            .collect::<Result<Vec<_>>>()?;

        Ok(ValueVectors {
            timestamp,
            sequence,
            op_type,
            fields,
        })
    }
}
