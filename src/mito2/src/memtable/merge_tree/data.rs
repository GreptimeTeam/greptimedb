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
use std::sync::Arc;

use bytes::Bytes;
use datatypes::arrow;
use datatypes::arrow::array::{RecordBatch, UInt32Array};
use datatypes::arrow::datatypes::{Field, Schema, SchemaRef};
use datatypes::data_type::DataType;
use datatypes::prelude::{ConcreteDataType, ScalarVectorBuilder, Value, Vector, VectorRef};
use datatypes::schema::ColumnSchema;
use datatypes::types::TimestampType;
use datatypes::vectors::{
    MutableVector, TimestampMicrosecondVector, TimestampMillisecondVector,
    TimestampNanosecondVector, TimestampSecondVector, UInt16Vector, UInt16VectorBuilder,
    UInt64Vector, UInt64VectorBuilder, UInt8VectorBuilder,
};
use parquet::arrow::ArrowWriter;
use snafu::ResultExt;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::consts::{OP_TYPE_COLUMN_NAME, SEQUENCE_COLUMN_NAME};

use crate::error;
use crate::error::Result;
use crate::memtable::key_values::KeyValue;
use crate::memtable::merge_tree::{PkId, ShardId};
use crate::memtable::BoxedBatchIterator;

pub const PK_INDEX_COLUMN_NAME: &str = "pk_index";

/// Buffer for the value part (TSID, ts, field columns) in a shard.
pub struct DataBuffer {
    shard_id: ShardId,
    metadata: RegionMetadataRef,
    /// Data types for field columns.
    field_types: Vec<ConcreteDataType>,
    /// Builder for primary key index.
    pk_index_builder: UInt16VectorBuilder,
    ts_builder: Box<dyn MutableVector>,
    sequence_builder: UInt64VectorBuilder,
    op_type_builder: UInt8VectorBuilder,
    /// Builders for field columns.
    field_builders: Vec<Option<Box<dyn MutableVector>>>,
}

impl DataBuffer {
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

        Self {
            shard_id: 0,
            metadata,
            field_types,
            pk_index_builder: pk_id_builder,
            ts_builder,
            sequence_builder,
            op_type_builder,
            field_builders,
        }
    }

    /// Writes a row to data buffer.
    pub fn write_row(&mut self, pk_id: PkId, kv: KeyValue) {
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
    }

    /// Freezes `DataBuffer` to bytes. Use `pk_weights` to convert pk_id to pk sort order.
    pub fn freeze(&mut self, pk_weights: &[u16]) -> Result<DataPart> {
        let encoder = DataPartEncoder::new(&self.metadata, pk_weights);
        let encoded = encoder.write(self)?;
        Ok(DataPart::Parquet(encoded))
    }

    pub fn iter(&self) -> Result<impl Iterator<Item = RecordBatch>> {
        todo!();
        Ok(std::iter::empty())
    }

    /// Returns num of rows in data buffer.
    pub fn num_rows(&self) -> usize {
        self.ts_builder.len()
    }
}

struct DataPartEncoder<'a> {
    schema: SchemaRef,
    pk_weights: &'a [u16],
}

impl<'a> DataPartEncoder<'a> {
    pub fn new(metadata: &RegionMetadataRef, pk_weights: &'a [u16]) -> DataPartEncoder<'a> {
        let schema = memtable_schema_to_encoded_schema(metadata);
        Self { schema, pk_weights }
    }

    pub fn write(&self, source: &DataBuffer) -> Result<Bytes> {
        let mut bytes = Vec::with_capacity(1024);
        let mut writer = ArrowWriter::try_new(&mut bytes, self.schema.clone(), None)
            .context(error::EncodeMemtableSnafu)?;
        let batches = data_buffer_to_record_batches(self.schema.clone(), source, self.pk_weights)?;
        for rb in batches {
            writer.write(&rb).context(error::EncodeMemtableSnafu)?;
        }
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

/// Converts `DataBuffer` to record batches.
fn data_buffer_to_record_batches(
    schema: SchemaRef,
    buffer: &DataBuffer,
    pk_weights: &[u16],
) -> Result<Vec<RecordBatch>> {
    let num_rows = buffer.ts_builder.len();

    let pk_index_v = buffer.pk_index_builder.finish_cloned();
    let ts_v = buffer.ts_builder.to_vector_cloned();
    let sequence_v = buffer.sequence_builder.finish_cloned();
    let op_type_v = buffer.op_type_builder.finish_cloned();

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

    for (idx, c) in buffer.field_builders.iter().enumerate() {
        let array = match c {
            None => {
                let mut single_null = buffer.field_types[idx].create_mutable_vector(num_rows);
                single_null.push_nulls(num_rows);
                single_null.to_vector_cloned().to_arrow_array()
            }
            Some(v) => v.to_vector_cloned().to_arrow_array(),
        };

        columns.push(
            arrow::compute::take(&array, &indices_to_take, None)
                .context(error::ComputeArrowSnafu)?,
        );
    }

    Ok(vec![
        RecordBatch::try_new(schema, columns).context(error::NewRecordBatchSnafu)?
    ])
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

impl DataPart {
    pub fn iter(&self) -> Result<BoxedBatchIterator> {
        todo!()
    }
}

#[cfg(test)]
mod tests {
    use datafusion::arrow::array::Float64Array;
    use datatypes::arrow::array::{TimestampMillisecondArray, UInt16Array};
    use parquet::arrow::arrow_reader::ParquetRecordBatchReaderBuilder;
    use parquet::data_type::AsBytes;

    use super::*;
    use crate::test_util::memtable_util::{build_key_values_with_ts_seq_values, metadata_for_test};

    #[test]
    fn test_data_buffer_to_record_batches() {
        let meta = metadata_for_test();
        let mut buffer = DataBuffer::with_capacity(meta.clone(), 10);

        write_rows_to_buffer(&mut buffer, &meta, 0, vec![1, 2], vec![Some(0.1), None], 1);
        write_rows_to_buffer(&mut buffer, &meta, 1, vec![1, 2], vec![Some(1.1), None], 2);
        write_rows_to_buffer(&mut buffer, &meta, 0, vec![2], vec![Some(1.1)], 3);

        let schema = memtable_schema_to_encoded_schema(&meta);
        let mut rbs = data_buffer_to_record_batches(schema, &buffer, &[3, 1]).unwrap();
        assert_eq!(1, rbs.len());
        let batch = rbs.pop().unwrap();
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

        let mut encoder = DataPartEncoder::new(&meta, &[0, 1, 2]);
        let encoded = encoder.write(&buffer).unwrap();
        let s = String::from_utf8_lossy(encoded.as_bytes());
        assert!(s.starts_with("PAR1"));
        assert!(s.ends_with("PAR1"));

        let mut builder = ParquetRecordBatchReaderBuilder::try_new(encoded).unwrap();
        let mut reader = builder.build().unwrap();
        let batch = reader.next().unwrap().unwrap();
        assert_eq!(3, batch.num_rows());
    }
}
