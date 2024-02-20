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
use std::ops::Range;
use std::sync::Arc;

use bytes::Bytes;
use datatypes::arrow;
use datatypes::arrow::array::{RecordBatch, UInt16Array, UInt32Array};
use datatypes::arrow::datatypes::{Field, Schema, SchemaRef};
use datatypes::data_type::DataType;
use datatypes::prelude::{ConcreteDataType, MutableVector, ScalarVectorBuilder, VectorRef};
use datatypes::schema::ColumnSchema;
use datatypes::types::TimestampType;
use datatypes::vectors::{
    TimestampMicrosecondVector, TimestampMillisecondVector, TimestampNanosecondVector,
    TimestampSecondVector, UInt16Vector, UInt16VectorBuilder, UInt64Vector, UInt64VectorBuilder,
    UInt8VectorBuilder,
};
use parquet::arrow::ArrowWriter;
use parquet::file::properties::WriterProperties;
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::consts::{OP_TYPE_COLUMN_NAME, SEQUENCE_COLUMN_NAME};

use crate::error;
use crate::error::Result;
use crate::memtable::key_values::KeyValue;
use crate::memtable::merge_tree::{PkId, PkIndex};

const PK_INDEX_COLUMN_NAME: &str = "__pk_index";

/// Data part batches returns by `DataParts::read`.
#[derive(Debug, Clone)]
pub struct DataBatch {
    /// Primary key index of this batch.
    pk_index: PkIndex,
    /// Record batch of data.
    rb: Arc<RecordBatch>,
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

    pub(crate) fn slice_record_batch(&self) -> RecordBatch {
        self.rb.slice(self.range.start, self.range.len())
    }
}

/// Buffer for the value part (pk_index, ts, sequence, op_type, field columns) in a shard.
pub struct DataBuffer {
    metadata: RegionMetadataRef,
    /// Schema for data part (primary keys are replaced with pk_index)
    data_part_schema: SchemaRef,
    /// Data types for field columns.
    field_types: Vec<ConcreteDataType>,
    /// Builder for primary key index.
    pk_index_builder: UInt16VectorBuilder,
    /// Builder for timestamp column.
    ts_builder: Box<dyn MutableVector>,
    /// Builder for sequence column.
    sequence_builder: UInt64VectorBuilder,
    /// Builder for op_type column.
    op_type_builder: UInt8VectorBuilder,
    /// Builders for field columns.
    field_builders: Vec<Option<Box<dyn MutableVector>>>,
}

impl DataBuffer {
    /// Creates a `DataBuffer` instance with given schema and capacity.
    pub fn with_capacity(metadata: RegionMetadataRef, init_capacity: usize) -> Self {
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
            metadata,
            data_part_schema,
            field_types,
            pk_index_builder: pk_id_builder,
            ts_builder,
            sequence_builder,
            op_type_builder,
            field_builders,
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
    pub fn freeze(&mut self, _pk_weights: &[u16]) -> Result<DataPart> {
        // we need distinguish between `freeze` in `ShardWriter` And `Shard`.
        todo!()
    }

    /// Reads batches from data buffer without resetting builder's buffers.
    pub fn iter(&mut self, pk_weights: &[u16]) -> Result<DataBufferIter> {
        let batch =
            data_buffer_to_record_batches(self.data_part_schema.clone(), self, pk_weights, true)?;
        DataBufferIter::new(batch)
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

#[derive(Debug)]
pub(crate) struct DataBufferIter {
    batch: Arc<RecordBatch>,
    offset: usize,
    current_data_batch: Option<DataBatch>,
}

impl DataBufferIter {
    pub(crate) fn new(batch: RecordBatch) -> Result<Self> {
        let mut iter = Self {
            batch: Arc::new(batch),
            offset: 0,
            current_data_batch: None,
        };
        iter.next()?; // fill data batch for comparison and merge.
        Ok(iter)
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

/// Format of immutable data part.
pub enum DataPart {
    Parquet(Bytes),
}

impl DataPart {
    fn is_empty(&self) -> bool {
        match self {
            DataPart::Parquet(data) => data.is_empty(),
        }
    }
}

/// Data parts under a shard.
pub struct DataParts {}

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
        let mut buffer = DataBuffer::with_capacity(meta.clone(), 10);

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
        let mut buffer = DataBuffer::with_capacity(meta.clone(), 10);

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

        let encoder = DataPartEncoder::new(&meta, &[0, 1, 2], None);
        let encoded = encoder.write(&mut buffer).unwrap();
        let s = String::from_utf8_lossy(encoded.as_bytes());
        assert!(s.starts_with("PAR1"));
        assert!(s.ends_with("PAR1"));

        let builder = ParquetRecordBatchReaderBuilder::try_new(encoded).unwrap();
        let mut reader = builder.build().unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(3, batch.num_rows());
    }

    fn check_buffer_values_equal(iter: &mut DataBufferIter, expected_values: &[Vec<f64>]) {
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
        let mut buffer = DataBuffer::with_capacity(meta.clone(), 10);

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
        let mut buffer = DataBuffer::with_capacity(meta.clone(), 10);
        let mut iter = buffer.iter(&[0, 1, 3, 2]).unwrap();
        check_buffer_values_equal(&mut iter, &[]);
    }
}
