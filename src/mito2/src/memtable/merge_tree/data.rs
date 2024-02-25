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

//! Data part of a shard.

use std::cmp::{Ordering, Reverse};
use std::fmt::{Debug, Formatter};
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use datatypes::arrow;
use datatypes::arrow::array::{ArrayRef, RecordBatch, UInt16Array, UInt32Array, UInt64Array};
use datatypes::arrow::datatypes::{Field, Schema, SchemaRef};
use datatypes::data_type::DataType;
use datatypes::prelude::{ConcreteDataType, MutableVector, ScalarVectorBuilder, Vector, VectorRef};
use datatypes::schema::ColumnSchema;
use datatypes::types::TimestampType;
use datatypes::vectors::{
    TimestampMicrosecondVector, TimestampMillisecondVector, TimestampNanosecondVector,
    TimestampSecondVector, UInt16Vector, UInt16VectorBuilder, UInt64Vector, UInt64VectorBuilder,
    UInt8VectorBuilder,
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
use crate::memtable::merge_tree::merger::{DataBatchKey, DataNode, DataSource, Merger};
use crate::memtable::merge_tree::PkIndex;

const PK_INDEX_COLUMN_NAME: &str = "__pk_index";

/// Initial capacity for the data buffer.
pub(crate) const DATA_INIT_CAP: usize = 8;

/// Range of a data batch.
#[derive(Debug, Clone, Copy)]
pub(crate) struct DataBatchRange {
    /// Primary key index of this batch.
    pub(crate) pk_index: PkIndex,
    /// Start of current primary key inside record batch.
    pub(crate) start: usize,
    /// End of current primary key inside record batch.
    pub(crate) end: usize,
}

impl DataBatchRange {
    pub(crate) fn len(&self) -> usize {
        self.end - self.start
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.len() == 0
    }
}

/// Data part batches returns by `DataParts::read`.
#[derive(Debug, Clone, Copy)]
pub struct DataBatch<'a> {
    /// Record batch of data.
    rb: &'a RecordBatch,
    /// Range of current primary key inside record batch
    range: DataBatchRange,
}

impl<'a> DataBatch<'a> {
    pub(crate) fn pk_index(&self) -> PkIndex {
        self.range.pk_index
    }

    pub(crate) fn range(&self) -> DataBatchRange {
        self.range
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.range.is_empty()
    }

    pub(crate) fn slice_record_batch(&self) -> RecordBatch {
        self.rb.slice(self.range.start, self.range.len())
    }

    pub(crate) fn first_row(&self) -> (i64, u64) {
        let ts_values = timestamp_array_to_i64_slice(self.rb.column(1));
        let sequence_values = self
            .rb
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .values();
        (
            ts_values[self.range.start],
            sequence_values[self.range.start],
        )
    }

    pub(crate) fn last_row(&self) -> (i64, u64) {
        let ts_values = timestamp_array_to_i64_slice(self.rb.column(1));
        let sequence_values = self
            .rb
            .column(2)
            .as_any()
            .downcast_ref::<UInt64Array>()
            .unwrap()
            .values();
        (
            ts_values[self.range.end - 1],
            sequence_values[self.range.end - 1],
        )
    }

    pub(crate) fn first_key(&self) -> DataBatchKey {
        let pk_index = self.pk_index();
        let ts_array = self.rb.column(1);

        // maybe safe the result somewhere.
        let ts_values = timestamp_array_to_i64_slice(ts_array);
        let timestamp = ts_values[self.range.start];
        DataBatchKey {
            pk_index,
            timestamp,
        }
    }

    pub(crate) fn search_key(&self, key: &DataBatchKey) -> Result<usize, usize> {
        let DataBatchKey {
            pk_index,
            timestamp,
        } = key;
        assert_eq!(*pk_index, self.range.pk_index);
        let ts_values = timestamp_array_to_i64_slice(self.rb.column(1));
        let ts_values = &ts_values[self.range.start..self.range.end];
        ts_values.binary_search(timestamp)
    }

    pub(crate) fn slice(self, offset: usize, length: usize) -> DataBatch<'a> {
        let start = self.range.start + offset;
        let end = start + length;
        DataBatch {
            rb: self.rb,
            range: DataBatchRange {
                pk_index: self.range.pk_index,
                start,
                end,
            },
        }
    }

    pub(crate) fn num_rows(&self) -> usize {
        self.range.len()
    }
}

/// Buffer for the value part (pk_index, ts, sequence, op_type, field columns) in a shard.
pub struct DataBuffer {
    metadata: RegionMetadataRef,
    /// Schema for data part (primary keys are replaced with pk_index)
    data_part_schema: SchemaRef,
    /// Builder for primary key index.
    pk_index_builder: UInt16VectorBuilder,
    /// Builder for timestamp column.
    ts_builder: Box<dyn MutableVector>,
    /// Builder for sequence column.
    sequence_builder: UInt64VectorBuilder,
    /// Builder for op_type column.
    op_type_builder: UInt8VectorBuilder,
    /// Builders for field columns.
    field_builders: Vec<LazyMutableVectorBuilder>,

    dedup: bool,
}

impl DataBuffer {
    /// Creates a `DataBuffer` instance with given schema and capacity.
    pub fn with_capacity(metadata: RegionMetadataRef, init_capacity: usize, dedup: bool) -> Self {
        let ts_builder = metadata
            .time_index_column()
            .column_schema
            .data_type
            .create_mutable_vector(init_capacity);

        let pk_id_builder = UInt16VectorBuilder::with_capacity(init_capacity);
        let sequence_builder = UInt64VectorBuilder::with_capacity(init_capacity);
        let op_type_builder = UInt8VectorBuilder::with_capacity(init_capacity);

        let field_builders = metadata
            .field_columns()
            .map(|c| LazyMutableVectorBuilder::new(c.column_schema.data_type.clone()))
            .collect::<Vec<_>>();

        let data_part_schema = memtable_schema_to_encoded_schema(&metadata);
        Self {
            metadata,
            data_part_schema,
            pk_index_builder: pk_id_builder,
            ts_builder,
            sequence_builder,
            op_type_builder,
            field_builders,
            dedup,
        }
    }

    /// Writes a row to data buffer.
    pub fn write_row(&mut self, pk_index: PkIndex, kv: KeyValue) {
        self.ts_builder.push_value_ref(kv.timestamp());
        self.pk_index_builder.push(Some(pk_index));
        self.sequence_builder.push(Some(kv.sequence()));
        self.op_type_builder.push(Some(kv.op_type() as u8));

        debug_assert_eq!(self.field_builders.len(), kv.num_fields());

        for (idx, field) in kv.fields().enumerate() {
            self.field_builders[idx]
                .get_or_create_builder(self.ts_builder.len())
                .push_value_ref(field);
        }
    }

    /// Freezes `DataBuffer` to bytes.
    /// If `pk_weights` is present, it will be used to sort rows.
    ///
    /// `freeze` clears the buffers of builders.
    pub fn freeze(
        &mut self,
        pk_weights: Option<&[u16]>,
        replace_pk_index: bool,
    ) -> Result<DataPart> {
        let encoder = DataPartEncoder::new(
            &self.metadata,
            pk_weights,
            None,
            replace_pk_index,
            self.dedup,
        );
        let parts = encoder.write(self)?;
        Ok(parts)
    }

    /// Reads batches from data buffer without resetting builder's buffers.
    /// If pk_weights is present, yielded rows are sorted according to weights,
    /// otherwise rows are sorted by "pk_weights" values as they are actually weights.
    pub fn read(&mut self, pk_weights: Option<&[u16]>) -> Result<DataBufferReader> {
        let batch = data_buffer_to_record_batches(
            self.data_part_schema.clone(),
            self,
            pk_weights,
            true,
            self.dedup,
            // replace_pk_index is always set to false since:
            // - for DataBuffer in ShardBuilder, pk dict is not frozen
            // - for DataBuffer in Shard, values in pk_index column has already been replaced during `freeze`.
            false,
        )?;
        DataBufferReader::new(batch)
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

enum LazyMutableVectorBuilder {
    Type(ConcreteDataType),
    Builder(Box<dyn MutableVector>),
}

impl LazyMutableVectorBuilder {
    fn new(ty: ConcreteDataType) -> Self {
        Self::Type(ty)
    }

    fn get_or_create_builder(&mut self, init_capacity: usize) -> &mut Box<dyn MutableVector> {
        match self {
            LazyMutableVectorBuilder::Type(ty) => {
                let builder = ty.create_mutable_vector(init_capacity);
                *self = LazyMutableVectorBuilder::Builder(builder);
                self.get_or_create_builder(init_capacity)
            }
            LazyMutableVectorBuilder::Builder(builder) => builder,
        }
    }
}

/// Converts `DataBuffer` to record batches, with rows sorted according to pk_weights.
/// `keep_data`: whether to keep the original data inside `DataBuffer`.
/// `dedup`: whether to true to remove the duplicated rows inside `DataBuffer`.
/// `replace_pk_index`: whether to replace the pk_index values with corresponding pk weight.
fn data_buffer_to_record_batches(
    schema: SchemaRef,
    buffer: &mut DataBuffer,
    pk_weights: Option<&[u16]>,
    keep_data: bool,
    dedup: bool,
    replace_pk_index: bool,
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

    let pk_array = if replace_pk_index {
        // replace pk index values with pk weights.
        Arc::new(UInt16Array::from_iter_values(
            rows.iter().map(|(_, key)| key.pk_weight),
        )) as Arc<_>
    } else {
        pk_index_v.to_arrow_array()
    };

    // sort and dedup
    rows.sort_unstable_by(|l, r| l.1.cmp(&r.1));
    if dedup {
        rows.dedup_by(|l, r| l.1.pk_weight == r.1.pk_weight && l.1.timestamp == r.1.timestamp);
    }

    let indices_to_take = UInt32Array::from_iter_values(rows.iter().map(|(idx, _)| *idx as u32));

    let mut columns = Vec::with_capacity(4 + buffer.field_builders.len());

    columns.push(
        arrow::compute::take(&pk_array, &indices_to_take, None)
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

    for b in buffer.field_builders.iter_mut() {
        let array = match b {
            LazyMutableVectorBuilder::Type(ty) => {
                let mut single_null = ty.create_mutable_vector(num_rows);
                single_null.push_nulls(num_rows);
                single_null.to_vector().to_arrow_array()
            }
            LazyMutableVectorBuilder::Builder(builder) => {
                if keep_data {
                    builder.to_vector_cloned().to_arrow_array()
                } else {
                    builder.to_vector().to_arrow_array()
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

pub(crate) fn timestamp_array_to_i64_slice(arr: &ArrayRef) -> &[i64] {
    use datatypes::arrow::array::{
        TimestampMicrosecondArray, TimestampMillisecondArray, TimestampNanosecondArray,
        TimestampSecondArray,
    };
    use datatypes::arrow::datatypes::{DataType, TimeUnit};

    match arr.data_type() {
        DataType::Timestamp(t, _) => match t {
            TimeUnit::Second => arr
                .as_any()
                .downcast_ref::<TimestampSecondArray>()
                .unwrap()
                .values(),
            TimeUnit::Millisecond => arr
                .as_any()
                .downcast_ref::<TimestampMillisecondArray>()
                .unwrap()
                .values(),
            TimeUnit::Microsecond => arr
                .as_any()
                .downcast_ref::<TimestampMicrosecondArray>()
                .unwrap()
                .values(),
            TimeUnit::Nanosecond => arr
                .as_any()
                .downcast_ref::<TimestampNanosecondArray>()
                .unwrap()
                .values(),
        },
        _ => unreachable!(),
    }
}

#[derive(Debug)]
pub(crate) struct DataBufferReader {
    batch: RecordBatch,
    offset: usize,
    current_range: Option<DataBatchRange>,
}

impl DataBufferReader {
    pub(crate) fn new(batch: RecordBatch) -> Result<Self> {
        let mut reader = Self {
            batch,
            offset: 0,
            current_range: None,
        };
        reader.next()?; // fill data batch for comparison and merge.
        Ok(reader)
    }

    pub(crate) fn is_valid(&self) -> bool {
        self.current_range.is_some()
    }

    /// Returns current data batch.
    /// # Panics
    /// If Current reader is exhausted.
    pub(crate) fn current_data_batch(&self) -> DataBatch {
        let range = self.current_range.unwrap();
        DataBatch {
            rb: &self.batch,
            range,
        }
    }

    /// # Panics
    /// If Current reader is exhausted.
    pub(crate) fn current_pk_index(&self) -> PkIndex {
        self.current_range.as_ref().unwrap().pk_index
    }

    /// Advances reader to next data batch.
    pub(crate) fn next(&mut self) -> Result<()> {
        if self.offset >= self.batch.num_rows() {
            self.current_range = None;
            return Ok(());
        }
        let pk_index_array = pk_index_array(&self.batch);
        if let Some((next_pk, range)) = search_next_pk_range(pk_index_array, self.offset) {
            self.offset = range.end;
            self.current_range = Some(DataBatchRange {
                pk_index: next_pk,
                start: range.start,
                end: range.end,
            });
        } else {
            self.current_range = None;
        }
        Ok(())
    }
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

/// Searches for next pk index, and it's offset range in a sorted `UInt16Array`.
fn search_next_pk_range(array: &UInt16Array, start: usize) -> Option<(PkIndex, Range<usize>)> {
    let num_rows = array.len();
    if start >= num_rows {
        return None;
    }

    let values = array.values();
    let next_pk = values[start];

    for idx in start..num_rows {
        if values[idx] != next_pk {
            return Some((next_pk, start..idx));
        }
    }
    Some((next_pk, start..num_rows))
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

fn build_rows_to_sort(
    pk_weights: Option<&[u16]>,
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
            let pk_weight = if let Some(weights) = pk_weights {
                weights[*pk_index as usize] // if pk_weights is present, sort according to weight.
            } else {
                *pk_index // otherwise pk_index has already been replaced by weights.
            };
            (
                idx,
                InnerKey {
                    timestamp: *timestamp,
                    pk_weight,
                    sequence: *sequence,
                },
            )
        })
        .collect()
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

struct DataPartEncoder<'a> {
    schema: SchemaRef,
    pk_weights: Option<&'a [u16]>,
    row_group_size: Option<usize>,
    replace_pk_index: bool,
    dedup: bool,
}

impl<'a> DataPartEncoder<'a> {
    pub fn new(
        metadata: &RegionMetadataRef,
        pk_weights: Option<&'a [u16]>,
        row_group_size: Option<usize>,
        replace_pk_index: bool,
        dedup: bool,
    ) -> DataPartEncoder<'a> {
        let schema = memtable_schema_to_encoded_schema(metadata);
        Self {
            schema,
            pk_weights,
            row_group_size,
            replace_pk_index,
            dedup,
        }
    }

    fn writer_props(&self) -> Option<WriterProperties> {
        self.row_group_size.map(|size| {
            WriterProperties::builder()
                .set_max_row_group_size(size)
                .build()
        })
    }
    pub fn write(&self, source: &mut DataBuffer) -> Result<DataPart> {
        let mut bytes = Vec::with_capacity(1024);
        let mut writer = ArrowWriter::try_new(&mut bytes, self.schema.clone(), self.writer_props())
            .context(error::EncodeMemtableSnafu)?;
        let rb = data_buffer_to_record_batches(
            self.schema.clone(),
            source,
            self.pk_weights,
            false,
            self.dedup,
            self.replace_pk_index,
        )?;
        writer.write(&rb).context(error::EncodeMemtableSnafu)?;
        let _metadata = writer.close().context(error::EncodeMemtableSnafu)?;
        Ok(DataPart::Parquet(ParquetPart {
            data: Bytes::from(bytes),
        }))
    }
}

/// Format of immutable data part.
pub enum DataPart {
    Parquet(ParquetPart),
}

impl DataPart {
    fn is_empty(&self) -> bool {
        match self {
            DataPart::Parquet(p) => p.data.is_empty(),
        }
    }

    /// Reads frozen data part and yields [DataBatch]es.
    pub fn read(&self) -> Result<DataPartReader> {
        match self {
            DataPart::Parquet(data_bytes) => DataPartReader::new(data_bytes.data.clone(), None),
        }
    }
}

pub struct DataPartReader {
    inner: ParquetRecordBatchReader,
    current_batch: Option<RecordBatch>,
    current_range: Option<DataBatchRange>,
}

impl Debug for DataPartReader {
    fn fmt(&self, f: &mut Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("DataPartReader")
            .field("current_range", &self.current_range)
            .finish()
    }
}

impl DataPartReader {
    pub fn new(data: Bytes, batch_size: Option<usize>) -> Result<Self> {
        let mut builder =
            ParquetRecordBatchReaderBuilder::try_new(data).context(error::ReadDataPartSnafu)?;
        if let Some(batch_size) = batch_size {
            builder = builder.with_batch_size(batch_size);
        }
        let parquet_reader = builder.build().context(error::ReadDataPartSnafu)?;
        let mut reader = Self {
            inner: parquet_reader,
            current_batch: None,
            current_range: None,
        };
        reader.next()?;
        Ok(reader)
    }

    /// Returns false if current reader is exhausted.
    pub(crate) fn is_valid(&self) -> bool {
        self.current_range.is_some()
    }

    /// Returns current pk index.
    ///
    /// # Panics
    /// If reader is exhausted.
    pub(crate) fn current_pk_index(&self) -> PkIndex {
        self.current_range.as_ref().unwrap().pk_index
    }

    /// Returns current data batch of reader.
    /// # Panics
    /// If reader is exhausted.
    pub(crate) fn current_data_batch(&self) -> DataBatch {
        let range = self.current_range.unwrap();
        DataBatch {
            rb: self.current_batch.as_ref().unwrap(),
            range,
        }
    }

    pub(crate) fn next(&mut self) -> Result<()> {
        if let Some((next_pk, range)) = self.search_next_pk_range() {
            // first try to search next pk in current record batch.
            self.current_range = Some(DataBatchRange {
                pk_index: next_pk,
                start: range.start,
                end: range.end,
            });
        } else {
            // current record batch reaches eof, fetch next record batch from parquet reader.
            if let Some(rb) = self.inner.next() {
                let rb = rb.context(error::ComputeArrowSnafu)?;
                self.current_batch = Some(rb);
                self.current_range = None;
                return self.next();
            } else {
                // parquet is also exhausted
                self.current_batch = None;
                self.current_range = None;
            }
        }

        Ok(())
    }

    /// Searches next primary key along with it's offset range inside record batch.
    fn search_next_pk_range(&self) -> Option<(PkIndex, Range<usize>)> {
        self.current_batch.as_ref().and_then(|b| {
            // safety: PK_INDEX_COLUMN_NAME must present in record batch yielded by data part.
            let pk_array = pk_index_array(b);
            let start = self
                .current_range
                .as_ref()
                .map(|range| range.end)
                .unwrap_or(0);
            search_next_pk_range(pk_array, start)
        })
    }
}

/// Parquet-encoded `DataPart`.
pub struct ParquetPart {
    data: Bytes,
}

/// Data parts under a shard.
pub struct DataParts {
    /// The active writing buffer.
    active: DataBuffer,
    /// immutable (encoded) parts.
    frozen: Vec<DataPart>,
}

impl DataParts {
    pub(crate) fn new(metadata: RegionMetadataRef, capacity: usize, dedup: bool) -> Self {
        Self {
            active: DataBuffer::with_capacity(metadata, capacity, dedup),
            frozen: Vec::new(),
        }
    }

    pub(crate) fn with_frozen(mut self, frozen: Vec<DataPart>) -> Self {
        self.frozen = frozen;
        self
    }

    /// Writes a row into parts.
    pub fn write_row(&mut self, pk_index: PkIndex, kv: KeyValue) {
        self.active.write_row(pk_index, kv)
    }

    /// Freezes the active data buffer into frozen data parts.
    pub fn freeze(&mut self) -> Result<()> {
        self.frozen.push(self.active.freeze(None, false)?);
        Ok(())
    }

    /// Reads data from all parts including active and frozen parts.
    /// The returned iterator yields a record batch of one primary key at a time.
    /// The order of yielding primary keys is determined by provided weights.
    pub fn read(&mut self) -> Result<DataPartsReader> {
        let mut nodes = Vec::with_capacity(self.frozen.len() + 1);
        nodes.push(DataNode::new(DataSource::Buffer(
            // `DataPars::read` ensures that all pk_index inside `DataBuffer` are replaced by weights.
            // then we pass None to sort rows directly according to pk_index.
            self.active.read(None)?,
        )));
        for p in &self.frozen {
            nodes.push(DataNode::new(DataSource::Part(p.read()?)));
        }
        let merger = Merger::try_new(nodes)?;
        Ok(DataPartsReader { merger })
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.active.is_empty() && self.frozen.iter().all(|part| part.is_empty())
    }
}

/// Reader for all parts inside a `DataParts`.
pub struct DataPartsReader {
    merger: Merger<DataNode>,
}

impl DataPartsReader {
    pub(crate) fn current_data_batch(&self) -> DataBatch {
        let batch = self.merger.current_node().current_data_batch();
        batch.slice(0, self.merger.current_rows())
    }

    pub(crate) fn next(&mut self) -> Result<()> {
        self.merger.next()
    }

    pub(crate) fn is_valid(&self) -> bool {
        self.merger.is_valid()
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Float64Array;
    use datatypes::arrow::array::{TimestampMillisecondArray, UInt16Array, UInt64Array};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::data_type::AsBytes;

    use super::*;
    use crate::test_util::memtable_util::{
        extract_data_batch, metadata_for_test, write_rows_to_buffer,
    };

    #[test]
    fn test_lazy_mutable_vector_builder() {
        let mut builder = LazyMutableVectorBuilder::new(ConcreteDataType::boolean_datatype());
        match builder {
            LazyMutableVectorBuilder::Type(ref t) => {
                assert_eq!(&ConcreteDataType::boolean_datatype(), t);
            }
            LazyMutableVectorBuilder::Builder(_) => {
                unreachable!()
            }
        }
        builder.get_or_create_builder(1);
        match builder {
            LazyMutableVectorBuilder::Type(_) => {
                unreachable!()
            }
            LazyMutableVectorBuilder::Builder(_) => {}
        }
    }

    fn check_test_data_buffer_to_record_batches(keep_data: bool) {
        let meta = metadata_for_test();
        let mut buffer = DataBuffer::with_capacity(meta.clone(), 10, true);

        write_rows_to_buffer(&mut buffer, &meta, 0, vec![1, 2], vec![Some(0.1), None], 1);
        write_rows_to_buffer(&mut buffer, &meta, 1, vec![1, 2], vec![Some(1.1), None], 2);
        write_rows_to_buffer(&mut buffer, &meta, 0, vec![2], vec![Some(1.1)], 3);
        assert_eq!(5, buffer.num_rows());
        let schema = memtable_schema_to_encoded_schema(&meta);
        let batch = data_buffer_to_record_batches(
            schema,
            &mut buffer,
            Some(&[3, 1]),
            keep_data,
            true,
            true,
        )
        .unwrap();

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
            vec![1, 1, 3, 3],
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

    fn check_data_buffer_dedup(dedup: bool) {
        let metadata = metadata_for_test();
        let mut buffer = DataBuffer::with_capacity(metadata.clone(), 10, dedup);
        write_rows_to_buffer(
            &mut buffer,
            &metadata,
            0,
            vec![2, 3],
            vec![Some(1.0), Some(2.0)],
            0,
        );
        write_rows_to_buffer(
            &mut buffer,
            &metadata,
            0,
            vec![1, 2],
            vec![Some(1.1), Some(2.1)],
            2,
        );

        let mut reader = buffer.read(Some(&[0])).unwrap();
        let mut res = vec![];
        while reader.is_valid() {
            let batch = reader.current_data_batch();
            res.push(extract_data_batch(&batch));
            reader.next().unwrap();
        }
        if dedup {
            assert_eq!(vec![(0, vec![(1, 2), (2, 3), (3, 1)])], res);
        } else {
            assert_eq!(vec![(0, vec![(1, 2), (2, 3), (2, 0), (3, 1)])], res);
        }
    }

    #[test]
    fn test_data_buffer_dedup() {
        check_data_buffer_dedup(true);
        check_data_buffer_dedup(false);
    }

    #[test]
    fn test_data_buffer_to_record_batches_with_dedup() {
        let meta = metadata_for_test();
        let mut buffer = DataBuffer::with_capacity(meta.clone(), 10, true);

        write_rows_to_buffer(&mut buffer, &meta, 0, vec![1, 2], vec![Some(0.1), None], 1);
        write_rows_to_buffer(&mut buffer, &meta, 1, vec![2], vec![Some(1.1)], 2);
        write_rows_to_buffer(&mut buffer, &meta, 0, vec![2], vec![Some(1.1)], 3);
        assert_eq!(4, buffer.num_rows());
        let schema = memtable_schema_to_encoded_schema(&meta);
        let batch =
            data_buffer_to_record_batches(schema, &mut buffer, Some(&[0, 1]), true, true, true)
                .unwrap();

        assert_eq!(3, batch.num_rows());
        assert_eq!(
            vec![0, 0, 1],
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
            vec![1, 2, 2],
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
            vec![1, 3, 2],
            batch
                .column_by_name(SEQUENCE_COLUMN_NAME)
                .unwrap()
                .as_any()
                .downcast_ref::<UInt64Array>()
                .unwrap()
                .iter()
                .map(|v| v.unwrap())
                .collect::<Vec<_>>()
        );
    }

    #[test]
    fn test_data_buffer_to_record_batches_without_dedup() {
        let meta = metadata_for_test();
        let mut buffer = DataBuffer::with_capacity(meta.clone(), 10, true);

        write_rows_to_buffer(&mut buffer, &meta, 0, vec![1, 2], vec![Some(0.1), None], 1);
        write_rows_to_buffer(&mut buffer, &meta, 1, vec![1, 2], vec![Some(1.1), None], 2);
        write_rows_to_buffer(&mut buffer, &meta, 0, vec![2], vec![Some(1.1)], 3);
        assert_eq!(5, buffer.num_rows());
        let schema = memtable_schema_to_encoded_schema(&meta);
        let batch =
            data_buffer_to_record_batches(schema, &mut buffer, Some(&[3, 1]), true, false, true)
                .unwrap();

        assert_eq!(
            vec![1, 1, 3, 3, 3],
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
            vec![1, 2, 1, 2, 2],
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
    }

    fn check_data_buffer_freeze(
        pk_weights: Option<&[u16]>,
        replace_pk_weights: bool,
        expected: &[(u16, Vec<(i64, u64)>)],
    ) {
        let meta = metadata_for_test();
        let mut buffer = DataBuffer::with_capacity(meta.clone(), 10, true);

        // write rows with null values.
        write_rows_to_buffer(
            &mut buffer,
            &meta,
            0,
            vec![0, 1, 2],
            vec![Some(1.0), None, Some(3.0)],
            0,
        );
        write_rows_to_buffer(&mut buffer, &meta, 1, vec![1], vec![Some(2.0)], 3);

        let mut res = Vec::with_capacity(3);
        let mut reader = buffer
            .freeze(pk_weights, replace_pk_weights)
            .unwrap()
            .read()
            .unwrap();
        while reader.is_valid() {
            let batch = reader.current_data_batch();
            res.push(extract_data_batch(&batch));
            reader.next().unwrap();
        }
        assert_eq!(expected, res);
    }

    #[test]
    fn test_data_buffer_freeze() {
        check_data_buffer_freeze(
            None,
            false,
            &[(0, vec![(0, 0), (1, 1), (2, 2)]), (1, vec![(1, 3)])],
        );

        check_data_buffer_freeze(
            Some(&[1, 2]),
            false,
            &[(0, vec![(0, 0), (1, 1), (2, 2)]), (1, vec![(1, 3)])],
        );

        check_data_buffer_freeze(
            Some(&[3, 2]),
            true,
            &[(2, vec![(1, 3)]), (3, vec![(0, 0), (1, 1), (2, 2)])],
        );

        check_data_buffer_freeze(
            Some(&[3, 2]),
            false,
            &[(1, vec![(1, 3)]), (0, vec![(0, 0), (1, 1), (2, 2)])],
        );
    }

    #[test]
    fn test_encode_data_buffer() {
        let meta = metadata_for_test();
        let mut buffer = DataBuffer::with_capacity(meta.clone(), 10, true);

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

        let encoder = DataPartEncoder::new(&meta, Some(&[0, 1, 2]), None, true, true);
        let encoded = match encoder.write(&mut buffer).unwrap() {
            DataPart::Parquet(data) => data.data,
        };

        let s = String::from_utf8_lossy(encoded.as_bytes());
        assert!(s.starts_with("PAR1"));
        assert!(s.ends_with("PAR1"));

        let builder = ParquetRecordBatchReaderBuilder::try_new(encoded).unwrap();
        let mut reader = builder.build().unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(3, batch.num_rows());
    }

    fn check_buffer_values_equal(reader: &mut DataBufferReader, expected_values: &[Vec<f64>]) {
        let mut output = Vec::with_capacity(expected_values.len());
        while reader.is_valid() {
            let batch = reader.current_data_batch().slice_record_batch();
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
            reader.next().unwrap();
        }
        assert_eq!(expected_values, output);
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

    fn check_iter_data_buffer(pk_weights: Option<&[u16]>, expected: &[Vec<f64>]) {
        let meta = metadata_for_test();
        let mut buffer = DataBuffer::with_capacity(meta.clone(), 10, true);

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

        let mut iter = buffer.read(pk_weights).unwrap();
        check_buffer_values_equal(&mut iter, expected);
    }

    #[test]
    fn test_iter_data_buffer() {
        check_iter_data_buffer(None, &[vec![1.0, 2.0, 3.0], vec![1.1, 2.1, 3.1]]);
        check_iter_data_buffer(
            Some(&[0, 1, 2, 3]),
            &[vec![1.0, 2.0, 3.0], vec![1.1, 2.1, 3.1]],
        );
        check_iter_data_buffer(
            Some(&[3, 2, 1, 0]),
            &[vec![1.1, 2.1, 3.1], vec![1.0, 2.0, 3.0]],
        );
    }

    #[test]
    fn test_iter_empty_data_buffer() {
        let meta = metadata_for_test();
        let mut buffer = DataBuffer::with_capacity(meta.clone(), 10, true);
        let mut iter = buffer.read(Some(&[0, 1, 3, 2])).unwrap();
        check_buffer_values_equal(&mut iter, &[]);
    }

    fn check_part_values_equal(iter: &mut DataPartReader, expected_values: &[Vec<f64>]) {
        let mut output = Vec::with_capacity(expected_values.len());
        while iter.is_valid() {
            let batch = iter.current_data_batch().slice_record_batch();
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
        let mut buffer = DataBuffer::with_capacity(meta.clone(), 10, true);

        write_rows_to_buffer(
            &mut buffer,
            &meta,
            2,
            vec![0, 1, 2],
            vec![Some(1.0), Some(2.0), Some(3.0)],
            2,
        );

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
            vec![2, 3],
            vec![Some(2.2), Some(2.3)],
            4,
        );

        let encoder = DataPartEncoder::new(&meta, Some(weights), Some(4), true, true);
        let encoded = encoder.write(&mut buffer).unwrap();

        let mut iter = encoded.read().unwrap();
        check_part_values_equal(&mut iter, expected_values);
    }

    #[test]
    fn test_iter_data_part() {
        check_iter_data_part(
            &[0, 1, 2, 3],
            &[vec![1.0, 2.0, 3.0, 2.3], vec![1.1, 2.1, 3.1]],
        );

        check_iter_data_part(
            &[3, 2, 1, 0],
            &[vec![1.1, 2.1, 3.1], vec![1.0, 2.0, 3.0, 2.3]],
        );
    }
}
