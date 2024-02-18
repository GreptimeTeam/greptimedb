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

//! The value part of key-value separated merge-tree structure.

use std::cmp::{Ordering, Reverse};
use std::collections::BinaryHeap;
use std::fmt::{Debug, Formatter};
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use datatypes::arrow;
use datatypes::arrow::array::{Array, RecordBatch, UInt16Array, UInt32Array};
use datatypes::arrow::datatypes::{Field, Schema, SchemaRef};
use datatypes::data_type::DataType;
use datatypes::prelude::{ConcreteDataType, ScalarVectorBuilder, Vector, VectorRef};
use datatypes::schema::ColumnSchema;
use datatypes::types::TimestampType;
use datatypes::vectors::{
    MutableVector, TimestampMicrosecondVector, TimestampMillisecondVector,
    TimestampNanosecondVector, TimestampSecondVector, UInt16Vector, UInt16VectorBuilder,
    UInt64Vector, UInt64VectorBuilder, UInt8VectorBuilder,
};
use parquet::arrow::arrow_reader::{ParquetRecordBatchReader, ParquetRecordBatchReaderBuilder};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::consts::{OP_TYPE_COLUMN_NAME, SEQUENCE_COLUMN_NAME};

use crate::error;
use crate::error::Result;
use crate::memtable::key_values::KeyValue;
use crate::memtable::merge_tree::{PkId, PkIndex, ShardId};

pub const PK_INDEX_COLUMN_NAME: &str = "pk_index";

/// Data part batches returns by `DataParts::read`.
#[derive(Debug, Clone)]
pub struct DataBatch {
    /// Primary key index of this batch.
    pk_index: PkIndex,
    /// Record batch of data.
    rb: RecordBatch,
    /// Range of current primary key inside record batch
    range: Range<usize>,
}

impl DataBatch {
    pub(crate) fn pk_index(&self) -> PkIndex {
        self.pk_index
    }

    pub(crate) fn record_batch(&self) -> &RecordBatch {
        &self.rb
    }

    pub(crate) fn range(&self) -> Range<usize> {
        self.range.clone()
    }

    pub(crate) fn as_record_batch(&self) -> RecordBatch {
        self.rb.slice(self.range.start, self.range.len())
    }
}

/// Data parts including an active writing part and several frozen parts.
pub struct DataParts {
    pub(crate) active: DataBuffer,
    pub(crate) frozen: Vec<DataPart>, // todo(hl): merge all frozen parts into one parquet-encoded bytes.
}

impl DataParts {
    pub fn with_capacity(
        meta: RegionMetadataRef,
        init_capacity: usize,
        freeze_threshold: usize,
    ) -> Self {
        Self {
            active: DataBuffer::with_capacity(meta, init_capacity, freeze_threshold),
            frozen: vec![],
        }
    }
}

pub struct HeapNode {
    pk_weights: Arc<Vec<u16>>,
    source: Source,
}

impl Debug for HeapNode {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        let mut debug_struct = f.debug_struct("HeapNode");
        let valid = self.source.is_valid();
        debug_struct.field("valid", &valid);
        if valid {
            debug_struct.field("current_pk_index", &self.source.current_pk_index());
        }
        debug_struct.finish()
    }
}

#[derive(Debug)]
enum Source {
    Active(DataBufferIter),
    Frozen(DataPartIter),
}

impl Source {
    /// Returns current pk index of node.
    /// # Panics
    /// If current node is already exhausted.
    fn current_pk_index(&self) -> PkIndex {
        match self {
            Source::Active(i) => i.current_pk_index(),
            Source::Frozen(i) => i.current_pk_index(),
        }
    }

    fn next(&mut self) -> Result<()> {
        match self {
            Source::Active(i) => i.next(),
            Source::Frozen(i) => i.next(),
        }
    }

    fn current_batch(&self) -> DataBatch {
        match self {
            Source::Active(i) => i.current_data_batch(),
            Source::Frozen(i) => i.current_data_batch(),
        }
    }

    fn is_valid(&self) -> bool {
        match self {
            Source::Active(i) => i.is_valid(),
            Source::Frozen(i) => i.is_valid(),
        }
    }
}

impl Eq for HeapNode {}

impl PartialEq<Self> for HeapNode {
    fn eq(&self, other: &Self) -> bool {
        self.source
            .current_pk_index()
            .eq(&other.source.current_pk_index())
    }
}

impl PartialOrd<Self> for HeapNode {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for HeapNode {
    fn cmp(&self, other: &Self) -> Ordering {
        let self_weight = self.pk_weights[self.source.current_pk_index() as usize];
        let other_weight = self.pk_weights[other.source.current_pk_index() as usize];
        other_weight.cmp(&self_weight)
    }
}

/// Iterator for iterating data in `DataParts`
pub struct Iter {
    heap: BinaryHeap<HeapNode>,
}

impl Iter {
    fn new(parts: &mut DataParts, pk_weights: Vec<u16>) -> Result<Iter> {
        let pk_weights = Arc::new(pk_weights);
        let mut heap = BinaryHeap::with_capacity(1 + parts.frozen.len());
        let active_iter = parts.active.iter(pk_weights.as_slice())?;
        if active_iter.is_valid() {
            heap.push(HeapNode {
                pk_weights: pk_weights.clone(),
                source: Source::Active(active_iter),
            });
        }

        for p in &mut parts.frozen {
            let iter = p.iter(&pk_weights)?;
            if iter.is_valid() {
                heap.push(HeapNode {
                    pk_weights: pk_weights.clone(),
                    source: Source::Frozen(iter),
                });
            }
        }
        Ok(Self { heap })
    }
}

impl Iterator for Iter {
    type Item = Result<DataBatch>;

    fn next(&mut self) -> Option<Self::Item> {
        while let Some(mut top) = self.heap.pop() {
            if top.source.is_valid() {
                let top_batch = top.source.current_batch();
                if let Err(e) = top.source.next() {
                    return Some(Err(e));
                }
                if top.source.is_valid() {
                    self.heap.push(top);
                }
                return Some(Ok(top_batch));
            }
        }
        None
    }
}

impl DataParts {
    pub(crate) fn new(
        metadata: RegionMetadataRef,
        capacity: usize,
        freeze_threshold: usize,
    ) -> Self {
        Self {
            active: DataBuffer::with_capacity(metadata, capacity, freeze_threshold),
            frozen: Vec::new(),
        }
    }

    /// Writes one row into active part.
    pub fn write_row(&mut self, pk_id: PkId, kv: KeyValue) -> bool {
        self.active.write_row(pk_id, kv)
    }

    /// Freezes the active data buffer into frozen data parts.
    pub fn freeze(&mut self, pk_weights: &[u16]) -> Result<()> {
        let part = self.active.freeze(pk_weights)?;
        self.frozen.push(part);
        Ok(())
    }

    /// Reads data from all parts including active and frozen parts.
    /// The returned iterator yields a record batch of one primary key at a time.
    /// The order of yielding primary keys is determined by provided weights.
    pub fn iter(&mut self, pk_weights: Vec<u16>) -> Result<Iter> {
        Iter::new(self, pk_weights)
    }

    pub(crate) fn is_empty(&self) -> bool {
        unimplemented!()
    }
}

/// Buffer for the value part (TSID, ts, field columns) in a shard.
pub struct DataBuffer {
    shard_id: ShardId,
    metadata: RegionMetadataRef,
    data_part_schema: SchemaRef,
    /// Data types for field columns.
    field_types: Vec<ConcreteDataType>,
    /// Builder for primary key index.
    pk_index_builder: UInt16VectorBuilder,
    ts_builder: Box<dyn MutableVector>,
    sequence_builder: UInt64VectorBuilder,
    op_type_builder: UInt8VectorBuilder,
    /// Builders for field columns.
    field_builders: Vec<Option<Box<dyn MutableVector>>>,

    freeze_threshold: usize,
}

impl DataBuffer {
    pub fn with_capacity(
        metadata: RegionMetadataRef,
        init_capacity: usize,
        freeze_threshold: usize,
    ) -> Self {
        let ts_builder = metadata
            .time_index_column()
            .column_schema
            .data_type
            .create_mutable_vector(init_capacity);

        let pk_id_builder = UInt16VectorBuilder::with_capacity(init_capacity);
        let sequence_builder = UInt64VectorBuilder::with_capacity(init_capacity);
        let op_type_builder = UInt8VectorBuilder::with_capacity(init_capacity);

        let field_types = metadata
            .field_columns()
            .map(|c| c.column_schema.data_type.clone())
            .collect::<Vec<_>>();
        let field_builders = (0..field_types.len()).map(|_| None).collect();

        let data_part_schema = memtable_schema_to_encoded_schema(&metadata);
        Self {
            shard_id: 0,
            metadata,
            data_part_schema,
            field_types,
            pk_index_builder: pk_id_builder,
            ts_builder,
            sequence_builder,
            op_type_builder,
            field_builders,
            freeze_threshold,
        }
    }

    /// Writes a row to data buffer.
    pub fn write_row(&mut self, pk_id: PkId, kv: KeyValue) -> bool {
        self.ts_builder.push_value_ref(kv.timestamp());
        self.pk_index_builder.push(Some(pk_id.pk_index));
        self.sequence_builder.push(Some(kv.sequence()));
        self.op_type_builder.push(Some(kv.op_type() as u8));

        debug_assert_eq!(self.field_builders.len(), kv.num_fields());

        for (idx, field) in kv.fields().enumerate() {
            self.field_builders[idx]
                .get_or_insert_with(|| {
                    let mut builder =
                        self.field_types[idx].create_mutable_vector(self.ts_builder.len());
                    builder.push_nulls(self.ts_builder.len() - 1);
                    builder
                })
                .push_value_ref(field);
        }

        self.ts_builder.len() >= self.freeze_threshold
    }

    /// Freezes `DataBuffer` to bytes. Use `pk_weights` to convert pk_id to pk sort order.
    /// `freeze` clears the buffers of builders.
    pub fn freeze(&mut self, pk_weights: &[u16]) -> Result<DataPart> {
        let encoder = DataPartEncoder::new(&self.metadata, pk_weights, None);
        let encoded = encoder.write(self)?;
        Ok(DataPart::Parquet(encoded))
    }

    /// Reads batches from data buffer without resetting builder's buffers.
    pub fn iter(&mut self, pk_weights: &[u16]) -> Result<DataBufferIter> {
        let batch =
            data_buffer_to_record_batches(self.data_part_schema.clone(), self, pk_weights, true)?;
        Ok(DataBufferIter::new(batch))
    }

    /// Returns num of rows in data buffer.
    pub fn num_rows(&self) -> usize {
        self.ts_builder.len()
    }

    /// Returns whether the buffer is empty.
    pub fn is_empty(&self) -> bool {
        self.num_rows() == 0
    }
}

#[derive(Debug)]
pub(crate) struct DataBufferIter {
    batch: RecordBatch,
    offset: usize,
    current_data_batch: Option<DataBatch>,
}

impl DataBufferIter {
    pub(crate) fn new(batch: RecordBatch) -> Self {
        let mut iter = Self {
            batch,
            offset: 0,
            current_data_batch: None,
        };
        iter.next(); // fill data batch for comparison and merge.
        iter
    }

    pub(crate) fn is_valid(&self) -> bool {
        self.current_data_batch.is_some()
    }

    /// # Panics
    /// If Current iterator is not exhausted.
    pub(crate) fn current_data_batch(&self) -> DataBatch {
        self.current_data_batch.as_ref().unwrap().clone()
    }

    /// # Panics
    /// If Current iterator is not exhausted.
    pub(crate) fn current_pk_index(&self) -> PkIndex {
        self.current_data_batch.as_ref().unwrap().pk_index
    }

    /// Advances iterator to next data batch.
    pub(crate) fn next(&mut self) -> Result<()> {
        if self.offset >= self.batch.num_rows() {
            self.current_data_batch = None;
            return Ok(());
        }
        let pk_index_array = pk_index_array(&self.batch);
        if let Some((next_pk, range)) = search_next_pk_range(pk_index_array, self.offset) {
            self.offset = range.end;
            self.current_data_batch = Some(DataBatch {
                pk_index: next_pk,
                rb: self.batch.clone(),
                range,
            })
        } else {
            self.current_data_batch = None;
        }
        Ok(())
    }
}

struct DataPartEncoder<'a> {
    schema: SchemaRef,
    pk_weights: &'a [u16],
    row_group_size: Option<usize>,
}

impl<'a> DataPartEncoder<'a> {
    pub fn new(
        metadata: &RegionMetadataRef,
        pk_weights: &'a [u16],
        row_group_size: Option<usize>,
    ) -> DataPartEncoder<'a> {
        let schema = memtable_schema_to_encoded_schema(metadata);
        Self {
            schema,
            pk_weights,
            row_group_size,
        }
    }

    fn writer_props(&self) -> Option<WriterProperties> {
        self.row_group_size.map(|size| {
            WriterProperties::builder()
                .set_max_row_group_size(size)
                .build()
        })
    }
    pub fn write(&self, source: &mut DataBuffer) -> Result<Bytes> {
        let mut bytes = Vec::with_capacity(1024);
        let mut writer = ArrowWriter::try_new(&mut bytes, self.schema.clone(), self.writer_props())
            .context(error::EncodeMemtableSnafu)?;
        let rb =
            data_buffer_to_record_batches(self.schema.clone(), source, self.pk_weights, false)?;
        writer.write(&rb).context(error::EncodeMemtableSnafu)?;
        let _file_meta = writer.close().context(error::EncodeMemtableSnafu)?;
        Ok(Bytes::from(bytes))
    }
}

fn memtable_schema_to_encoded_schema(schema: &RegionMetadataRef) -> SchemaRef {
    use datatypes::arrow::datatypes::DataType;
    let ColumnSchema {
        name: ts_name,
        data_type: ts_type,
        ..
    } = &schema.time_index_column().column_schema;

    let mut fields = vec![
        Field::new(PK_INDEX_COLUMN_NAME, DataType::UInt16, false),
        Field::new(ts_name, ts_type.as_arrow_type(), false),
        Field::new(SEQUENCE_COLUMN_NAME, DataType::UInt64, false),
        Field::new(OP_TYPE_COLUMN_NAME, DataType::UInt8, false),
    ];

    fields.extend(schema.field_columns().map(|c| {
        Field::new(
            &c.column_schema.name,
            c.column_schema.data_type.as_arrow_type(),
            c.column_schema.is_nullable(),
        )
    }));

    Arc::new(Schema::new(fields))
}

#[derive(Eq, PartialEq)]
struct InnerKey {
    pk_weight: u16,
    timestamp: i64,
    sequence: u64,
}

impl PartialOrd for InnerKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for InnerKey {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.pk_weight, self.timestamp, Reverse(self.sequence)).cmp(&(
            other.pk_weight,
            other.timestamp,
            Reverse(other.sequence),
        ))
    }
}

/// Converts `DataBuffer` to record batches, with rows sorted according to pk_weights.
fn data_buffer_to_record_batches(
    schema: SchemaRef,
    buffer: &mut DataBuffer,
    pk_weights: &[u16],
    keep_data: bool,
) -> Result<RecordBatch> {
    let num_rows = buffer.ts_builder.len();

    let (pk_index_v, ts_v, sequence_v, op_type_v) = if keep_data {
        (
            buffer.pk_index_builder.finish_cloned(),
            buffer.ts_builder.to_vector_cloned(),
            buffer.sequence_builder.finish_cloned(),
            buffer.op_type_builder.finish_cloned(),
        )
    } else {
        (
            buffer.pk_index_builder.finish(),
            buffer.ts_builder.to_vector(),
            buffer.sequence_builder.finish(),
            buffer.op_type_builder.finish(),
        )
    };

    let mut rows = build_rows_to_sort(pk_weights, &pk_index_v, &ts_v, &sequence_v);

    // sort and dedup
    rows.sort_unstable_by(|l, r| l.1.cmp(&r.1));
    rows.dedup_by(|l, r| l.1.timestamp == r.1.timestamp);
    let indices_to_take = UInt32Array::from_iter_values(rows.into_iter().map(|v| v.0 as u32));

    let mut columns = Vec::with_capacity(4 + buffer.field_builders.len());

    columns.push(
        arrow::compute::take(&pk_index_v.as_arrow(), &indices_to_take, None)
            .context(error::ComputeArrowSnafu)?,
    );

    columns.push(
        arrow::compute::take(&ts_v.to_arrow_array(), &indices_to_take, None)
            .context(error::ComputeArrowSnafu)?,
    );

    columns.push(
        arrow::compute::take(&sequence_v.as_arrow(), &indices_to_take, None)
            .context(error::ComputeArrowSnafu)?,
    );

    columns.push(
        arrow::compute::take(&op_type_v.as_arrow(), &indices_to_take, None)
            .context(error::ComputeArrowSnafu)?,
    );

    for (idx, c) in buffer.field_builders.iter_mut().enumerate() {
        let array = match c {
            None => {
                let mut single_null = buffer.field_types[idx].create_mutable_vector(num_rows);
                single_null.push_nulls(num_rows);
                single_null.to_vector().to_arrow_array()
            }
            Some(v) => {
                if keep_data {
                    v.to_vector_cloned().to_arrow_array()
                } else {
                    v.to_vector().to_arrow_array()
                }
            }
        };

        columns.push(
            arrow::compute::take(&array, &indices_to_take, None)
                .context(error::ComputeArrowSnafu)?,
        );
    }

    RecordBatch::try_new(schema, columns).context(error::NewRecordBatchSnafu)
}

fn build_rows_to_sort(
    pk_weights: &[u16],
    pk_index: &UInt16Vector,
    ts: &VectorRef,
    sequence: &UInt64Vector,
) -> Vec<(usize, InnerKey)> {
    let ts_values = match ts.data_type() {
        ConcreteDataType::Timestamp(t) => match t {
            TimestampType::Second(_) => ts
                .as_any()
                .downcast_ref::<TimestampSecondVector>()
                .unwrap()
                .as_arrow()
                .values(),
            TimestampType::Millisecond(_) => ts
                .as_any()
                .downcast_ref::<TimestampMillisecondVector>()
                .unwrap()
                .as_arrow()
                .values(),
            TimestampType::Microsecond(_) => ts
                .as_any()
                .downcast_ref::<TimestampMicrosecondVector>()
                .unwrap()
                .as_arrow()
                .values(),
            TimestampType::Nanosecond(_) => ts
                .as_any()
                .downcast_ref::<TimestampNanosecondVector>()
                .unwrap()
                .as_arrow()
                .values(),
        },
        other => unreachable!("Unexpected type {:?}", other),
    };
    let pk_index_values = pk_index.as_arrow().values();
    let sequence_values = sequence.as_arrow().values();
    debug_assert_eq!(ts_values.len(), pk_index_values.len());
    debug_assert_eq!(ts_values.len(), sequence_values.len());

    ts_values
        .iter()
        .zip(pk_index_values.iter())
        .zip(sequence_values.iter())
        .enumerate()
        .map(|(idx, ((timestamp, pk_index), sequence))| {
            (
                idx,
                InnerKey {
                    timestamp: *timestamp,
                    pk_weight: pk_weights[*pk_index as usize],
                    sequence: *sequence,
                },
            )
        })
        .collect()
}

/// Format of immutable data part.
pub enum DataPart {
    Parquet(Bytes),
}

pub struct DataPartIter {
    inner: ParquetRecordBatchReader,
    current_range: Range<usize>,
    current_pk_index: Option<PkIndex>,
    current_batch: Option<RecordBatch>,
}

impl Debug for DataPartIter {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataPartIter")
            .field("current_range", &self.current_range)
            .field("current_pk_index", &self.current_pk_index)
            .finish()
    }
}

impl DataPartIter {
    pub fn new(data: Bytes, batch_size: Option<usize>) -> Result<Self> {
        let mut builder =
            ParquetRecordBatchReaderBuilder::try_new(data).context(error::ReadDataPartSnafu)?;
        if let Some(batch_size) = batch_size {
            builder = builder.with_batch_size(batch_size);
        }
        let mut reader = builder.build().context(error::ReadDataPartSnafu)?;
        let mut iter = Self {
            inner: reader,
            current_pk_index: None,
            current_range: 0..0,
            current_batch: None,
        };
        iter.next()?;
        Ok(iter)
    }

    /// Returns false if current iter is exhausted.
    pub(crate) fn is_valid(&self) -> bool {
        self.current_pk_index.is_some()
    }

    /// Returns current pk index.
    ///
    /// # Panics
    /// If iterator is exhausted.
    pub(crate) fn current_pk_index(&self) -> PkIndex {
        self.current_pk_index.expect("DataPartIter is exhausted")
    }

    /// Returns current data batch of iterator.
    /// # Panics
    /// If iterator is exhausted.
    pub(crate) fn current_data_batch(&self) -> DataBatch {
        let rb = self.current_batch.as_ref().unwrap().clone();
        let pk_index = self.current_pk_index.unwrap();
        let range = self.current_range.clone();
        DataBatch {
            pk_index,
            rb,
            range,
        }
    }

    pub(crate) fn next(&mut self) -> Result<()> {
        if let Some((next_pk, range)) = self.search_next_pk_range() {
            // first try to search next pk in current record batch.
            self.current_pk_index = Some(next_pk);
            self.current_range = range;
        } else {
            // current record batch reaches eof, fetch next record batch from parquet reader.
            if let Some(rb) = self.inner.next() {
                let rb = rb.context(error::ComputeArrowSnafu)?;
                self.current_range = 0..0;
                self.current_batch = Some(rb);
                return self.next();
            } else {
                // parquet is also exhausted
                self.current_pk_index = None;
                self.current_batch = None;
            }
        }

        Ok(())
    }

    /// Searches next primary key along with it's offset range inside record batch.
    fn search_next_pk_range(&self) -> Option<(PkIndex, Range<usize>)> {
        self.current_batch.as_ref().and_then(|b| {
            // safety: PK_INDEX_COLUMN_NAME must present in record batch yielded by data part.
            let pk_array = pk_index_array(b);
            search_next_pk_range(pk_array, self.current_range.end)
        })
    }
}

impl DataPart {
    /// Iterates frozen data parts and yields record batches.
    /// Returned record batches are ga
    pub fn iter(&self, _pk_weights: &[u16]) -> Result<DataPartIter> {
        match self {
            DataPart::Parquet(data_bytes) => DataPartIter::new(data_bytes.clone(), None),
        }
    }
}

/// Searches for next pk index and it's offset range in a sorted `UInt16Array`.
fn search_next_pk_range(array: &UInt16Array, start: usize) -> Option<(PkIndex, Range<usize>)> {
    let num_rows = array.len();
    if start >= num_rows {
        return None;
    }

    let next_pk = array.value(start);
    for idx in start..num_rows {
        if array.value(idx) != next_pk {
            return Some((next_pk, start..idx));
        }
    }
    Some((next_pk, start..num_rows))
}

/// Gets `pk_index` array from record batch.
/// # Panics
/// If pk index column is not the first column or the type is not `UInt16Array`.
fn pk_index_array(batch: &RecordBatch) -> &UInt16Array {
    batch
        .column(0)
        .as_any()
        .downcast_ref::<UInt16Array>()
        .unwrap()
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Float64Array;
    use datatypes::arrow::array::{TimestampMillisecondArray, UInt16Array};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::data_type::AsBytes;

    use super::*;
    use crate::test_util::memtable_util::{build_key_values_with_ts_seq_values, metadata_for_test};

    fn check_test_data_buffer_to_record_batches(keep_data: bool) {
        let meta = metadata_for_test();
        let mut buffer = DataBuffer::with_capacity(meta.clone(), 10, usize::MAX);

        write_rows_to_buffer(&mut buffer, &meta, 0, vec![1, 2], vec![Some(0.1), None], 1);
        write_rows_to_buffer(&mut buffer, &meta, 1, vec![1, 2], vec![Some(1.1), None], 2);
        write_rows_to_buffer(&mut buffer, &meta, 0, vec![2], vec![Some(1.1)], 3);
        assert_eq!(5, buffer.num_rows());
        let schema = memtable_schema_to_encoded_schema(&meta);
        let batch = data_buffer_to_record_batches(schema, &mut buffer, &[3, 1], keep_data).unwrap();

        assert_eq!(
            vec![1, 2, 1, 2],
            batch
                .column_by_name("ts")
                .unwrap()
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .iter()
                .map(|v| v.unwrap())
                .collect::<Vec<_>>()
        );

        assert_eq!(
            vec![1, 1, 0, 0],
            batch
                .column_by_name(PK_INDEX_COLUMN_NAME)
                .unwrap()
                .as_any()
                .downcast_ref::<UInt16Array>()
                .unwrap()
                .iter()
                .map(|v| v.unwrap())
                .collect::<Vec<_>>()
        );

        assert_eq!(
            vec![Some(1.1), None, Some(0.1), Some(1.1)],
            batch
                .column_by_name("v1")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .iter()
                .collect::<Vec<_>>()
        );

        if keep_data {
            assert_eq!(5, buffer.num_rows());
        } else {
            assert_eq!(0, buffer.num_rows());
        }
    }

    #[test]
    fn test_data_buffer_to_record_batches() {
        check_test_data_buffer_to_record_batches(true);
        check_test_data_buffer_to_record_batches(false);
    }

    fn write_rows_to_buffer(
        buffer: &mut DataBuffer,
        schema: &RegionMetadataRef,
        pk_index: u16,
        ts: Vec<i64>,
        v0: Vec<Option<f64>>,
        sequence: u64,
    ) {
        let kvs = build_key_values_with_ts_seq_values(
            schema,
            "whatever".to_string(),
            1,
            ts.into_iter(),
            v0.into_iter(),
            sequence,
        );

        for kv in kvs.iter() {
            buffer.write_row(
                PkId {
                    shard_id: 0,
                    pk_index,
                },
                kv,
            );
        }
    }

    #[test]
    fn test_encode_data_buffer() {
        let meta = metadata_for_test();
        let mut buffer = DataBuffer::with_capacity(meta.clone(), 10, usize::MAX);

        // write rows with null values.
        write_rows_to_buffer(
            &mut buffer,
            &meta,
            2,
            vec![0, 1, 2],
            vec![Some(1.0), None, Some(3.0)],
            2,
        );

        assert_eq!(3, buffer.num_rows());

        write_rows_to_buffer(&mut buffer, &meta, 2, vec![1], vec![Some(2.0)], 3);

        assert_eq!(4, buffer.num_rows());

        let mut encoder = DataPartEncoder::new(&meta, &[0, 1, 2], None);
        let encoded = encoder.write(&mut buffer).unwrap();
        let s = String::from_utf8_lossy(encoded.as_bytes());
        assert!(s.starts_with("PAR1"));
        assert!(s.ends_with("PAR1"));

        let mut builder = ParquetRecordBatchReaderBuilder::try_new(encoded).unwrap();
        let mut reader = builder.build().unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(3, batch.num_rows());
    }

    fn check_buffer_values_equal(iter: &mut DataBufferIter, expected_values: &[Vec<f64>]) {
        let mut output = Vec::with_capacity(expected_values.len());
        while iter.is_valid() {
            let batch = iter.current_data_batch().as_record_batch();
            let values = batch
                .column_by_name("v1")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .iter()
                .map(|v| v.unwrap())
                .collect::<Vec<_>>();
            output.push(values);
            iter.next().unwrap();
        }
        assert_eq!(expected_values, output);
    }

    fn check_part_values_equal(iter: &mut DataPartIter, expected_values: &[Vec<f64>]) {
        let mut output = Vec::with_capacity(expected_values.len());
        while iter.is_valid() {
            let batch = iter.current_data_batch().as_record_batch();
            let values = batch
                .column_by_name("v1")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .iter()
                .map(|v| v.unwrap())
                .collect::<Vec<_>>();
            output.push(values);
            iter.next().unwrap();
        }
        assert_eq!(expected_values, output);
    }

    fn check_iter_data_part(weights: &[u16], expected_values: &[Vec<f64>]) {
        let meta = metadata_for_test();
        let mut buffer = DataBuffer::with_capacity(meta.clone(), 10, usize::MAX);

        // write rows with null values.
        write_rows_to_buffer(
            &mut buffer,
            &meta,
            2,
            vec![0, 1, 2],
            vec![Some(1.0), Some(2.0), Some(3.0)],
            2,
        );

        // write rows with null values.
        write_rows_to_buffer(
            &mut buffer,
            &meta,
            3,
            vec![1, 2, 3],
            vec![Some(1.1), Some(2.1), Some(3.1)],
            3,
        );

        let mut encoder = DataPartEncoder::new(&meta, weights, Some(4));
        let encoded = encoder.write(&mut buffer).unwrap();

        let mut iter = DataPartIter::new(encoded, Some(4)).unwrap();
        check_part_values_equal(&mut iter, expected_values);
    }

    #[test]
    fn test_iter_data_part() {
        check_iter_data_part(
            &[0, 1, 2, 3],
            &[vec![1.0, 2.0, 3.0], vec![1.1], vec![2.1, 3.1]],
        );
        check_iter_data_part(
            &[3, 2, 1, 0],
            &[vec![1.1, 2.1, 3.1], vec![1.0], vec![2.0, 3.0]],
        );
    }

    #[test]
    fn test_search_next_pk_range() {
        let a = UInt16Array::from_iter_values([1, 1, 3, 3, 4, 6]);
        assert_eq!((1, 0..2), search_next_pk_range(&a, 0).unwrap());
        assert_eq!((3, 2..4), search_next_pk_range(&a, 2).unwrap());
        assert_eq!((4, 4..5), search_next_pk_range(&a, 4).unwrap());
        assert_eq!((6, 5..6), search_next_pk_range(&a, 5).unwrap());

        assert_eq!(None, search_next_pk_range(&a, 6));
    }

    #[test]
    fn test_iter_data_buffer() {
        let meta = metadata_for_test();
        let mut buffer = DataBuffer::with_capacity(meta.clone(), 10, usize::MAX);

        write_rows_to_buffer(
            &mut buffer,
            &meta,
            3,
            vec![1, 2, 3],
            vec![Some(1.1), Some(2.1), Some(3.1)],
            3,
        );

        write_rows_to_buffer(
            &mut buffer,
            &meta,
            2,
            vec![0, 1, 2],
            vec![Some(1.0), Some(2.0), Some(3.0)],
            2,
        );

        let mut iter = buffer.iter(&[0, 1, 3, 2]).unwrap();
        check_buffer_values_equal(&mut iter, &[vec![1.1, 2.1, 3.1], vec![1.0, 2.0, 3.0]]);
    }

    #[test]
    fn test_iter_empty_data_buffer() {
        let meta = metadata_for_test();
        let mut buffer = DataBuffer::with_capacity(meta.clone(), 10, usize::MAX);
        let mut iter = buffer.iter(&[0, 1, 3, 2]).unwrap();
        check_buffer_values_equal(&mut iter, &[]);
    }

    #[test]
    fn test_iter_empty_data_part() {
        let meta = metadata_for_test();
        let mut buffer = DataBuffer::with_capacity(meta.clone(), 10, usize::MAX);
        let data_part = buffer.freeze(&[]).unwrap();
        let mut iter = data_part.iter(&[]).unwrap();
        check_part_values_equal(&mut iter, &[]);
    }

    fn test_data_parts_iter_with_weight(pk_weights: &[u16], expected_values: &[Vec<f64>]) {
        let meta = metadata_for_test();
        let mut buffer = DataBuffer::with_capacity(meta.clone(), 10, usize::MAX);

        write_rows_to_buffer(
            &mut buffer,
            &meta,
            3,
            vec![1, 2, 3],
            vec![Some(1.3), Some(2.3), Some(3.3)],
            1,
        );
        write_rows_to_buffer(
            &mut buffer,
            &meta,
            0,
            vec![1, 2, 3],
            vec![Some(1.0), Some(2.0), Some(3.0)],
            2,
        );

        let part_0 = buffer.freeze(pk_weights).unwrap();
        write_rows_to_buffer(
            &mut buffer,
            &meta,
            1,
            vec![1, 2, 3],
            vec![Some(1.1), Some(2.1), Some(3.1)],
            3,
        );
        let part_1 = buffer.freeze(pk_weights).unwrap();

        write_rows_to_buffer(
            &mut buffer,
            &meta,
            2,
            vec![1, 2, 3],
            vec![Some(1.2), Some(2.2), Some(3.2)],
            4,
        );

        let mut parts = DataParts {
            active: buffer,
            frozen: vec![part_0, part_1],
        };
        let mut iter = parts.iter(pk_weights.to_vec()).unwrap();
        let mut res = Vec::with_capacity(expected_values.len());
        for b in iter {
            let batch = b.unwrap().as_record_batch();
            let values = batch
                .column_by_name("v1")
                .unwrap()
                .as_any()
                .downcast_ref::<Float64Array>()
                .unwrap()
                .iter()
                .map(|v| v.unwrap())
                .collect::<Vec<_>>();
            res.push(values);
        }
        assert_eq!(expected_values, &res);
    }

    #[test]
    fn test_data_parts_iter() {
        test_data_parts_iter_with_weight(
            &[8, 7, 6, 5, 4, 3, 2, 1],
            &[
                vec![1.3, 2.3, 3.3],
                vec![1.2, 2.2, 3.2],
                vec![1.1, 2.1, 3.1],
                vec![1.0, 2.0, 3.0],
            ],
        );
        test_data_parts_iter_with_weight(
            &[1, 2, 3, 3, 4, 5, 6, 7],
            &[
                vec![1.0, 2.0, 3.0],
                vec![1.1, 2.1, 3.1],
                vec![1.2, 2.2, 3.2],
                vec![1.3, 2.3, 3.3],
            ],
        );
    }
}
