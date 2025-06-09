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

//! Memtable test utilities.

use std::sync::Arc;

use api::helper::ColumnDataTypeWrapper;
use api::v1::value::ValueData;
use api::v1::{Row, Rows, SemanticType};
use common_time::Timestamp;
use datatypes::arrow::array::UInt64Array;
use datatypes::data_type::ConcreteDataType;
use datatypes::scalars::ScalarVector;
use datatypes::schema::ColumnSchema;
use datatypes::vectors::TimestampMillisecondVector;
use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder, RegionMetadataRef};
use store_api::storage::{ColumnId, RegionId, SequenceNumber};
use table::predicate::Predicate;

use crate::error::Result;
use crate::memtable::bulk::part::BulkPart;
use crate::memtable::key_values::KeyValue;
use crate::memtable::partition_tree::data::{timestamp_array_to_i64_slice, DataBatch, DataBuffer};
use crate::memtable::{
    BoxedBatchIterator, KeyValues, Memtable, MemtableBuilder, MemtableId, MemtableRanges,
    MemtableRef, MemtableStats,
};
use crate::read::scan_region::PredicateGroup;
use crate::row_converter::{DensePrimaryKeyCodec, PrimaryKeyCodecExt, SortField};

/// Empty memtable for test.
#[derive(Debug, Default)]
pub struct EmptyMemtable {
    /// Id of this memtable.
    id: MemtableId,
    /// Time range to return.
    time_range: Option<(Timestamp, Timestamp)>,
}

impl EmptyMemtable {
    /// Returns a new memtable with specific `id`.
    pub fn new(id: MemtableId) -> EmptyMemtable {
        EmptyMemtable {
            id,
            time_range: None,
        }
    }

    /// Attaches the time range to the memtable.
    pub fn with_time_range(mut self, time_range: Option<(Timestamp, Timestamp)>) -> EmptyMemtable {
        self.time_range = time_range;
        self
    }
}

impl Memtable for EmptyMemtable {
    fn id(&self) -> MemtableId {
        self.id
    }

    fn write(&self, _kvs: &KeyValues) -> Result<()> {
        Ok(())
    }

    fn write_one(&self, _key_value: KeyValue) -> Result<()> {
        Ok(())
    }

    fn write_bulk(&self, _part: BulkPart) -> Result<()> {
        Ok(())
    }

    fn iter(
        &self,
        _projection: Option<&[ColumnId]>,
        _filters: Option<Predicate>,
        _sequence: Option<SequenceNumber>,
    ) -> Result<BoxedBatchIterator> {
        Ok(Box::new(std::iter::empty()))
    }

    fn ranges(
        &self,
        _projection: Option<&[ColumnId]>,
        _predicate: PredicateGroup,
        _sequence: Option<SequenceNumber>,
    ) -> Result<MemtableRanges> {
        Ok(MemtableRanges::default())
    }

    fn is_empty(&self) -> bool {
        true
    }

    fn freeze(&self) -> Result<()> {
        Ok(())
    }

    fn stats(&self) -> MemtableStats {
        MemtableStats::default().with_time_range(self.time_range)
    }

    fn fork(&self, id: MemtableId, _metadata: &RegionMetadataRef) -> MemtableRef {
        Arc::new(EmptyMemtable::new(id))
    }
}

/// Empty memtable builder.
#[derive(Debug, Default)]
pub(crate) struct EmptyMemtableBuilder {}

impl MemtableBuilder for EmptyMemtableBuilder {
    fn build(&self, id: MemtableId, _metadata: &RegionMetadataRef) -> MemtableRef {
        Arc::new(EmptyMemtable::new(id))
    }
}

/// Creates a region metadata to test memtable with default pk.
///
/// The schema is `k0, k1, ts, v0, v1` and pk is `k0, k1`.
pub(crate) fn metadata_for_test() -> RegionMetadataRef {
    metadata_with_primary_key(vec![0, 1], false)
}

/// Creates a region metadata to test memtable and specific primary key.
///
/// If `enable_table_id` is false, the schema is `k0, k1, ts, v0, v1`.
/// If `enable_table_id` is true, the schema is `k0, __table_id, ts, v0, v1`.
pub fn metadata_with_primary_key(
    primary_key: Vec<ColumnId>,
    enable_table_id: bool,
) -> RegionMetadataRef {
    let mut builder = RegionMetadataBuilder::new(RegionId::new(123, 456));
    let maybe_table_id = if enable_table_id { "__table_id" } else { "k1" };
    builder
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("k0", ConcreteDataType::string_datatype(), false),
            semantic_type: semantic_type_of_column(0, &primary_key),
            column_id: 0,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new(
                maybe_table_id,
                ConcreteDataType::uint32_datatype(),
                false,
            ),
            semantic_type: semantic_type_of_column(1, &primary_key),
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
            semantic_type: semantic_type_of_column(3, &primary_key),
            column_id: 3,
        })
        .push_column_metadata(ColumnMetadata {
            column_schema: ColumnSchema::new("v1", ConcreteDataType::float64_datatype(), true),
            semantic_type: semantic_type_of_column(4, &primary_key),
            column_id: 4,
        })
        .primary_key(primary_key);
    let region_metadata = builder.build().unwrap();
    Arc::new(region_metadata)
}

fn semantic_type_of_column(column_id: ColumnId, primary_key: &[ColumnId]) -> SemanticType {
    if primary_key.contains(&column_id) {
        SemanticType::Tag
    } else {
        SemanticType::Field
    }
}

/// Builds key values with `len` rows for test.
pub fn build_key_values(
    schema: &RegionMetadataRef,
    k0: String,
    k1: u32,
    timestamps: &[i64],
    sequence: SequenceNumber,
) -> KeyValues {
    let values = timestamps.iter().map(|v| Some(*v as f64));

    build_key_values_with_ts_seq_values(
        schema,
        k0,
        k1,
        timestamps.iter().copied(),
        values,
        sequence,
    )
}

pub(crate) fn write_rows_to_buffer(
    buffer: &mut DataBuffer,
    schema: &RegionMetadataRef,
    pk_index: u16,
    ts: Vec<i64>,
    v0: Vec<Option<f64>>,
    sequence: u64,
) {
    let kvs = crate::test_util::memtable_util::build_key_values_with_ts_seq_values(
        schema,
        "whatever".to_string(),
        1,
        ts.into_iter(),
        v0.into_iter(),
        sequence,
    );

    for kv in kvs.iter() {
        buffer.write_row(pk_index, &kv);
    }
}

/// Extracts pk index, timestamps and sequences from [DataBatch].
pub(crate) fn extract_data_batch(batch: &DataBatch) -> (u16, Vec<(i64, u64)>) {
    let rb = batch.slice_record_batch();
    let ts = timestamp_array_to_i64_slice(rb.column(1));
    let seq = rb
        .column(2)
        .as_any()
        .downcast_ref::<UInt64Array>()
        .unwrap()
        .values();
    let ts_and_seq = ts
        .iter()
        .zip(seq.iter())
        .map(|(ts, seq)| (*ts, *seq))
        .collect::<Vec<_>>();
    (batch.pk_index(), ts_and_seq)
}

/// Builds key values with timestamps (ms) and sequences for test.
pub(crate) fn build_key_values_with_ts_seq_values(
    metadata: &RegionMetadataRef,
    k0: String,
    k1: u32,
    timestamps: impl Iterator<Item = i64>,
    values: impl Iterator<Item = Option<f64>>,
    sequence: SequenceNumber,
) -> KeyValues {
    let column_schema = region_metadata_to_row_schema(metadata);

    let rows = timestamps
        .zip(values)
        .map(|(ts, v)| Row {
            values: vec![
                api::v1::Value {
                    value_data: Some(ValueData::StringValue(k0.clone())),
                },
                api::v1::Value {
                    value_data: Some(ValueData::U32Value(k1)),
                },
                api::v1::Value {
                    value_data: Some(ValueData::TimestampMillisecondValue(ts)),
                },
                api::v1::Value {
                    value_data: Some(ValueData::I64Value(ts)),
                },
                api::v1::Value {
                    value_data: v.map(ValueData::F64Value),
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

/// Converts the region metadata to column schemas for a row.
pub fn region_metadata_to_row_schema(metadata: &RegionMetadataRef) -> Vec<api::v1::ColumnSchema> {
    metadata
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
        .collect()
}

/// Encode keys.
pub(crate) fn encode_keys(
    metadata: &RegionMetadataRef,
    key_values: &KeyValues,
    keys: &mut Vec<Vec<u8>>,
) {
    let row_codec = DensePrimaryKeyCodec::new(metadata);
    for kv in key_values.iter() {
        let key = row_codec.encode(kv.primary_keys()).unwrap();
        keys.push(key);
    }
}

/// Encode one key.
pub(crate) fn encode_key_by_kv(key_value: &KeyValue) -> Vec<u8> {
    let row_codec = DensePrimaryKeyCodec::with_fields(vec![
        (0, SortField::new(ConcreteDataType::string_datatype())),
        (1, SortField::new(ConcreteDataType::uint32_datatype())),
    ]);
    row_codec.encode(key_value.primary_keys()).unwrap()
}

/// Collects timestamps from the batch iter.
pub(crate) fn collect_iter_timestamps(iter: BoxedBatchIterator) -> Vec<i64> {
    iter.flat_map(|batch| {
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
    .collect()
}
