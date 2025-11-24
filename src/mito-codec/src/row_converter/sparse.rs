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

use std::collections::{HashMap, HashSet};
use std::sync::Arc;

use bytes::BufMut;
use common_recordbatch::filter::SimpleFilterEvaluator;
use datatypes::prelude::ConcreteDataType;
use datatypes::value::{Value, ValueRef};
use memcomparable::{Deserializer, Serializer};
use serde::{Deserialize, Serialize};
use snafu::ResultExt;
use store_api::codec::PrimaryKeyEncoding;
use store_api::metadata::RegionMetadataRef;
use store_api::storage::ColumnId;
use store_api::storage::consts::ReservedColumnId;

use crate::error::{DeserializeFieldSnafu, Result, SerializeFieldSnafu, UnsupportedOperationSnafu};
use crate::key_values::KeyValue;
use crate::primary_key_filter::SparsePrimaryKeyFilter;
use crate::row_converter::dense::SortField;
use crate::row_converter::{CompositeValues, PrimaryKeyCodec, PrimaryKeyFilter};

/// A codec for sparse key of metrics.
/// It requires the input primary key columns are sorted by the column name in lexicographical order.
/// It encodes the column id of the physical region.
#[derive(Clone, Debug)]
pub struct SparsePrimaryKeyCodec {
    inner: Arc<SparsePrimaryKeyCodecInner>,
}

#[derive(Debug)]
struct SparsePrimaryKeyCodecInner {
    // Internal fields
    table_id_field: SortField,
    // Internal fields
    tsid_field: SortField,
    // User defined label field
    label_field: SortField,
    // Columns in primary key
    //
    // None means all unknown columns is primary key(`Self::label_field`).
    columns: Option<HashSet<ColumnId>>,
}

/// Sparse values representation.
///
/// A map of [`ColumnId`] to [`Value`].
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SparseValues {
    values: HashMap<ColumnId, Value>,
}

impl SparseValues {
    /// Creates a new [`SparseValues`] instance.
    pub fn new(values: HashMap<ColumnId, Value>) -> Self {
        Self { values }
    }

    /// Returns the value of the given column, or [`Value::Null`] if the column is not present.
    pub fn get_or_null(&self, column_id: ColumnId) -> &Value {
        self.values.get(&column_id).unwrap_or(&Value::Null)
    }

    /// Returns the value of the given column, or [`None`] if the column is not present.
    pub fn get(&self, column_id: &ColumnId) -> Option<&Value> {
        self.values.get(column_id)
    }

    /// Inserts a new value into the [`SparseValues`].
    pub fn insert(&mut self, column_id: ColumnId, value: Value) {
        self.values.insert(column_id, value);
    }

    /// Returns an iterator over all stored column id/value pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&ColumnId, &Value)> {
        self.values.iter()
    }
}

/// The column id of the tsid.
pub const RESERVED_COLUMN_ID_TSID: ColumnId = ReservedColumnId::tsid();
/// The column id of the table id.
pub const RESERVED_COLUMN_ID_TABLE_ID: ColumnId = ReservedColumnId::table_id();
/// The size of the column id in the encoded sparse row.
pub const COLUMN_ID_ENCODE_SIZE: usize = 4;

impl SparsePrimaryKeyCodec {
    /// Creates a new [`SparsePrimaryKeyCodec`] instance.
    pub fn from_columns(columns_ids: impl Iterator<Item = ColumnId>) -> Self {
        let columns = columns_ids.collect();
        Self {
            inner: Arc::new(SparsePrimaryKeyCodecInner {
                table_id_field: SortField::new(ConcreteDataType::uint32_datatype()),
                tsid_field: SortField::new(ConcreteDataType::uint64_datatype()),
                label_field: SortField::new(ConcreteDataType::string_datatype()),
                columns: Some(columns),
            }),
        }
    }

    /// Creates a new [`SparsePrimaryKeyCodec`] instance.
    pub fn new(region_metadata: &RegionMetadataRef) -> Self {
        Self::from_columns(region_metadata.primary_key_columns().map(|c| c.column_id))
    }

    /// Returns a new [`SparsePrimaryKeyCodec`] instance.
    ///
    /// It treats all unknown columns as primary key(label field).
    pub fn schemaless() -> Self {
        Self {
            inner: Arc::new(SparsePrimaryKeyCodecInner {
                table_id_field: SortField::new(ConcreteDataType::uint32_datatype()),
                tsid_field: SortField::new(ConcreteDataType::uint64_datatype()),
                label_field: SortField::new(ConcreteDataType::string_datatype()),
                columns: None,
            }),
        }
    }

    /// Creates a new [`SparsePrimaryKeyCodec`] instance with additional label `fields`.
    pub fn with_fields(fields: Vec<(ColumnId, SortField)>) -> Self {
        Self {
            inner: Arc::new(SparsePrimaryKeyCodecInner {
                columns: Some(fields.iter().map(|f| f.0).collect()),
                table_id_field: SortField::new(ConcreteDataType::uint32_datatype()),
                tsid_field: SortField::new(ConcreteDataType::uint64_datatype()),
                label_field: SortField::new(ConcreteDataType::string_datatype()),
            }),
        }
    }

    /// Returns the field of the given column id.
    fn get_field(&self, column_id: ColumnId) -> Option<&SortField> {
        // if the `columns` is not specified, all unknown columns is primary key(label field).
        if let Some(columns) = &self.inner.columns
            && !columns.contains(&column_id)
        {
            return None;
        }

        match column_id {
            RESERVED_COLUMN_ID_TABLE_ID => Some(&self.inner.table_id_field),
            RESERVED_COLUMN_ID_TSID => Some(&self.inner.tsid_field),
            _ => Some(&self.inner.label_field),
        }
    }

    /// Encodes the given bytes into a [`SparseValues`].
    pub fn encode_to_vec<'a, I>(&self, row: I, buffer: &mut Vec<u8>) -> Result<()>
    where
        I: Iterator<Item = (ColumnId, ValueRef<'a>)>,
    {
        let mut serializer = Serializer::new(buffer);
        for (column_id, value) in row {
            if value.is_null() {
                continue;
            }

            if let Some(field) = self.get_field(column_id) {
                column_id
                    .serialize(&mut serializer)
                    .context(SerializeFieldSnafu)?;
                field.serialize(&mut serializer, &value)?;
            } else {
                // TODO(weny): handle the error.
                common_telemetry::warn!("Column {} is not in primary key, skipping", column_id);
            }
        }
        Ok(())
    }

    pub fn encode_raw_tag_value<'a, I>(&self, row: I, buffer: &mut Vec<u8>) -> Result<()>
    where
        I: Iterator<Item = (ColumnId, &'a [u8])>,
    {
        for (tag_column_id, tag_value) in row {
            let value_len = tag_value.len();
            buffer.reserve(6 + value_len / 8 * 9);
            buffer.put_u32(tag_column_id);
            buffer.put_u8(1);
            buffer.put_u8(!tag_value.is_empty() as u8);

            // Manual implementation of memcomparable::ser::Serializer::serialize_bytes
            // to avoid byte-by-byte put.
            let mut len = 0;
            let num_chucks = value_len / 8;
            let remainder = value_len % 8;

            for idx in 0..num_chucks {
                buffer.extend_from_slice(&tag_value[idx * 8..idx * 8 + 8]);
                len += 8;
                // append an extra byte that signals the number of significant bytes in this chunk
                // 1-8: many bytes were significant and this group is the last group
                // 9: all 8 bytes were significant and there is more data to come
                let extra = if len == value_len { 8 } else { 9 };
                buffer.put_u8(extra);
            }

            if remainder != 0 {
                buffer.extend_from_slice(&tag_value[len..value_len]);
                buffer.put_bytes(0, 8 - remainder);
                buffer.put_u8(remainder as u8);
            }
        }
        Ok(())
    }

    /// Encodes the given bytes into a [`SparseValues`].
    pub fn encode_internal(&self, table_id: u32, tsid: u64, buffer: &mut Vec<u8>) -> Result<()> {
        buffer.reserve_exact(22);
        buffer.put_u32(RESERVED_COLUMN_ID_TABLE_ID);
        buffer.put_u8(1);
        buffer.put_u32(table_id);
        buffer.put_u32(RESERVED_COLUMN_ID_TSID);
        buffer.put_u8(1);
        buffer.put_u64(tsid);
        Ok(())
    }

    /// Decodes the given bytes into a [`SparseValues`].
    fn decode_sparse(&self, bytes: &[u8]) -> Result<SparseValues> {
        let mut deserializer = Deserializer::new(bytes);
        let mut values = SparseValues::new(HashMap::new());

        let column_id = u32::deserialize(&mut deserializer).context(DeserializeFieldSnafu)?;
        let value = self.inner.table_id_field.deserialize(&mut deserializer)?;
        values.insert(column_id, value);

        let column_id = u32::deserialize(&mut deserializer).context(DeserializeFieldSnafu)?;
        let value = self.inner.tsid_field.deserialize(&mut deserializer)?;
        values.insert(column_id, value);
        while deserializer.has_remaining() {
            let column_id = u32::deserialize(&mut deserializer).context(DeserializeFieldSnafu)?;
            let value = self.inner.label_field.deserialize(&mut deserializer)?;
            values.insert(column_id, value);
        }

        Ok(values)
    }

    /// Decodes the given bytes into a [`Value`].
    fn decode_leftmost(&self, bytes: &[u8]) -> Result<Option<Value>> {
        let mut deserializer = Deserializer::new(bytes);
        // Skip the column id.
        deserializer.advance(COLUMN_ID_ENCODE_SIZE);
        let value = self.inner.table_id_field.deserialize(&mut deserializer)?;
        Ok(Some(value))
    }

    /// Returns the offset of the given column id in the given primary key.
    pub fn has_column(
        &self,
        pk: &[u8],
        offsets_map: &mut HashMap<u32, usize>,
        column_id: ColumnId,
    ) -> Option<usize> {
        if offsets_map.is_empty() {
            let mut deserializer = Deserializer::new(pk);
            let mut offset = 0;
            while deserializer.has_remaining() {
                let column_id = u32::deserialize(&mut deserializer).unwrap();
                offset += 4;
                offsets_map.insert(column_id, offset);
                let Some(field) = self.get_field(column_id) else {
                    break;
                };

                let skip = field.skip_deserialize(pk, &mut deserializer).unwrap();
                offset += skip;
            }

            offsets_map.get(&column_id).copied()
        } else {
            offsets_map.get(&column_id).copied()
        }
    }

    /// Decode value at `offset` in `pk`.
    pub fn decode_value_at(&self, pk: &[u8], offset: usize, column_id: ColumnId) -> Result<Value> {
        let mut deserializer = Deserializer::new(pk);
        deserializer.advance(offset);
        // Safety: checked by `has_column`
        let field = self.get_field(column_id).unwrap();
        field.deserialize(&mut deserializer)
    }
}

impl PrimaryKeyCodec for SparsePrimaryKeyCodec {
    fn encode_key_value(&self, _key_value: &KeyValue, _buffer: &mut Vec<u8>) -> Result<()> {
        UnsupportedOperationSnafu {
            err_msg: "The encode_key_value method is not supported in SparsePrimaryKeyCodec.",
        }
        .fail()
    }

    fn encode_values(&self, values: &[(ColumnId, Value)], buffer: &mut Vec<u8>) -> Result<()> {
        self.encode_to_vec(values.iter().map(|v| (v.0, v.1.as_value_ref())), buffer)
    }

    fn encode_value_refs(
        &self,
        values: &[(ColumnId, ValueRef)],
        buffer: &mut Vec<u8>,
    ) -> Result<()> {
        self.encode_to_vec(values.iter().map(|v| (v.0, v.1.clone())), buffer)
    }

    fn estimated_size(&self) -> Option<usize> {
        None
    }

    fn num_fields(&self) -> Option<usize> {
        None
    }

    fn encoding(&self) -> PrimaryKeyEncoding {
        PrimaryKeyEncoding::Sparse
    }

    fn primary_key_filter(
        &self,
        metadata: &RegionMetadataRef,
        filters: Arc<Vec<SimpleFilterEvaluator>>,
    ) -> Box<dyn PrimaryKeyFilter> {
        Box::new(SparsePrimaryKeyFilter::new(
            metadata.clone(),
            filters,
            self.clone(),
        ))
    }

    fn decode(&self, bytes: &[u8]) -> Result<CompositeValues> {
        Ok(CompositeValues::Sparse(self.decode_sparse(bytes)?))
    }

    fn decode_leftmost(&self, bytes: &[u8]) -> Result<Option<Value>> {
        self.decode_leftmost(bytes)
    }
}

/// Field with column id.
pub struct FieldWithId {
    pub field: SortField,
    pub column_id: ColumnId,
}

/// A special encoder for memtable.
pub struct SparseEncoder {
    fields: Vec<FieldWithId>,
}

impl SparseEncoder {
    pub fn new(fields: Vec<FieldWithId>) -> Self {
        Self { fields }
    }

    pub fn encode_to_vec<'a, I>(&self, row: I, buffer: &mut Vec<u8>) -> Result<()>
    where
        I: Iterator<Item = ValueRef<'a>>,
    {
        let mut serializer = Serializer::new(buffer);
        for (value, field) in row.zip(self.fields.iter()) {
            if !value.is_null() {
                field
                    .column_id
                    .serialize(&mut serializer)
                    .context(SerializeFieldSnafu)?;
                field.field.serialize(&mut serializer, &value)?;
            }
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::sync::Arc;

    use api::v1::SemanticType;
    use common_query::prelude::{greptime_timestamp, greptime_value};
    use common_time::Timestamp;
    use common_time::timestamp::TimeUnit;
    use datatypes::schema::ColumnSchema;
    use datatypes::value::{OrderedFloat, Value};
    use store_api::metadata::{ColumnMetadata, RegionMetadataBuilder};
    use store_api::metric_engine_consts::{
        DATA_SCHEMA_TABLE_ID_COLUMN_NAME, DATA_SCHEMA_TSID_COLUMN_NAME,
    };
    use store_api::storage::{ColumnId, RegionId};

    use super::*;

    fn test_region_metadata() -> RegionMetadataRef {
        let mut builder = RegionMetadataBuilder::new(RegionId::new(1, 1));
        builder
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    DATA_SCHEMA_TABLE_ID_COLUMN_NAME,
                    ConcreteDataType::uint32_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Tag,
                column_id: ReservedColumnId::table_id(),
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    DATA_SCHEMA_TSID_COLUMN_NAME,
                    ConcreteDataType::uint64_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Tag,
                column_id: ReservedColumnId::tsid(),
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new("pod", ConcreteDataType::string_datatype(), true),
                semantic_type: SemanticType::Tag,
                column_id: 1,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "namespace",
                    ConcreteDataType::string_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 2,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "container",
                    ConcreteDataType::string_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 3,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "pod_name",
                    ConcreteDataType::string_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 4,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    "pod_ip",
                    ConcreteDataType::string_datatype(),
                    true,
                ),
                semantic_type: SemanticType::Tag,
                column_id: 5,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    greptime_value(),
                    ConcreteDataType::float64_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Field,
                column_id: 6,
            })
            .push_column_metadata(ColumnMetadata {
                column_schema: ColumnSchema::new(
                    greptime_timestamp(),
                    ConcreteDataType::timestamp_nanosecond_datatype(),
                    false,
                ),
                semantic_type: SemanticType::Timestamp,
                column_id: 7,
            })
            .primary_key(vec![
                ReservedColumnId::table_id(),
                ReservedColumnId::tsid(),
                1,
                2,
                3,
                4,
                5,
            ]);
        let metadata = builder.build().unwrap();
        Arc::new(metadata)
    }

    #[test]
    fn test_sparse_value_new_and_get_or_null() {
        let mut values = HashMap::new();
        values.insert(1, Value::Int32(42));
        let sparse_value = SparseValues::new(values);

        assert_eq!(sparse_value.get_or_null(1), &Value::Int32(42));
        assert_eq!(sparse_value.get_or_null(2), &Value::Null);
    }

    #[test]
    fn test_sparse_value_insert() {
        let mut sparse_value = SparseValues::new(HashMap::new());
        sparse_value.insert(1, Value::Int32(42));

        assert_eq!(sparse_value.get_or_null(1), &Value::Int32(42));
    }

    fn test_row() -> Vec<(ColumnId, ValueRef<'static>)> {
        vec![
            (RESERVED_COLUMN_ID_TABLE_ID, ValueRef::UInt32(42)),
            (
                RESERVED_COLUMN_ID_TSID,
                ValueRef::UInt64(123843349035232323),
            ),
            // label: pod
            (1, ValueRef::String("greptime-frontend-6989d9899-22222")),
            // label: namespace
            (2, ValueRef::String("greptime-cluster")),
            // label: container
            (3, ValueRef::String("greptime-frontend-6989d9899-22222")),
            // label: pod_name
            (4, ValueRef::String("greptime-frontend-6989d9899-22222")),
            // label: pod_ip
            (5, ValueRef::String("10.10.10.10")),
            // field: greptime_value
            (6, ValueRef::Float64(OrderedFloat(1.0))),
            // field: greptime_timestamp
            (
                7,
                ValueRef::Timestamp(Timestamp::new(1618876800000000000, TimeUnit::Nanosecond)),
            ),
        ]
    }

    #[test]
    fn test_encode_by_short_cuts() {
        let region_metadata = test_region_metadata();
        let codec = SparsePrimaryKeyCodec::new(&region_metadata);
        let mut buffer = Vec::new();
        let internal_columns = [
            (RESERVED_COLUMN_ID_TABLE_ID, ValueRef::UInt32(1024)),
            (RESERVED_COLUMN_ID_TSID, ValueRef::UInt64(42)),
        ];
        let tags = [
            (1, "greptime-frontend-6989d9899-22222"),
            (2, "greptime-cluster"),
            (3, "greptime-frontend-6989d9899-22222"),
            (4, "greptime-frontend-6989d9899-22222"),
            (5, "10.10.10.10"),
        ];
        codec
            .encode_to_vec(internal_columns.into_iter(), &mut buffer)
            .unwrap();
        codec
            .encode_to_vec(
                tags.iter()
                    .map(|(col_id, tag_value)| (*col_id, ValueRef::String(tag_value))),
                &mut buffer,
            )
            .unwrap();

        let mut buffer_by_raw_encoding = Vec::new();
        codec
            .encode_internal(1024, 42, &mut buffer_by_raw_encoding)
            .unwrap();
        let tags: Vec<_> = tags
            .into_iter()
            .map(|(col_id, tag_value)| (col_id, tag_value.as_bytes()))
            .collect();
        codec
            .encode_raw_tag_value(
                tags.iter().map(|(c, b)| (*c, *b)),
                &mut buffer_by_raw_encoding,
            )
            .unwrap();
        assert_eq!(buffer, buffer_by_raw_encoding);
    }

    #[test]
    fn test_encode_to_vec() {
        let region_metadata = test_region_metadata();
        let codec = SparsePrimaryKeyCodec::new(&region_metadata);
        let mut buffer = Vec::new();

        let row = test_row();
        codec.encode_to_vec(row.into_iter(), &mut buffer).unwrap();
        assert!(!buffer.is_empty());
        let sparse_value = codec.decode_sparse(&buffer).unwrap();
        assert_eq!(
            sparse_value.get_or_null(RESERVED_COLUMN_ID_TABLE_ID),
            &Value::UInt32(42)
        );
        assert_eq!(
            sparse_value.get_or_null(1),
            &Value::String("greptime-frontend-6989d9899-22222".into())
        );
        assert_eq!(
            sparse_value.get_or_null(2),
            &Value::String("greptime-cluster".into())
        );
        assert_eq!(
            sparse_value.get_or_null(3),
            &Value::String("greptime-frontend-6989d9899-22222".into())
        );
        assert_eq!(
            sparse_value.get_or_null(4),
            &Value::String("greptime-frontend-6989d9899-22222".into())
        );
        assert_eq!(
            sparse_value.get_or_null(5),
            &Value::String("10.10.10.10".into())
        );
    }

    #[test]
    fn test_decode_leftmost() {
        let region_metadata = test_region_metadata();
        let codec = SparsePrimaryKeyCodec::new(&region_metadata);
        let mut buffer = Vec::new();
        let row = test_row();
        codec.encode_to_vec(row.into_iter(), &mut buffer).unwrap();
        assert!(!buffer.is_empty());
        let result = codec.decode_leftmost(&buffer).unwrap().unwrap();
        assert_eq!(result, Value::UInt32(42));
    }

    #[test]
    fn test_has_column() {
        let region_metadata = test_region_metadata();
        let codec = SparsePrimaryKeyCodec::new(&region_metadata);
        let mut buffer = Vec::new();
        let row = test_row();
        codec.encode_to_vec(row.into_iter(), &mut buffer).unwrap();
        assert!(!buffer.is_empty());

        let mut offsets_map = HashMap::new();
        for column_id in [
            RESERVED_COLUMN_ID_TABLE_ID,
            RESERVED_COLUMN_ID_TSID,
            1,
            2,
            3,
            4,
            5,
        ] {
            let offset = codec.has_column(&buffer, &mut offsets_map, column_id);
            assert!(offset.is_some());
        }

        let offset = codec.has_column(&buffer, &mut offsets_map, 6);
        assert!(offset.is_none());
    }

    #[test]
    fn test_decode_value_at() {
        let region_metadata = test_region_metadata();
        let codec = SparsePrimaryKeyCodec::new(&region_metadata);
        let mut buffer = Vec::new();
        let row = test_row();
        codec.encode_to_vec(row.into_iter(), &mut buffer).unwrap();
        assert!(!buffer.is_empty());

        let row = test_row();
        let mut offsets_map = HashMap::new();
        for column_id in [
            RESERVED_COLUMN_ID_TABLE_ID,
            RESERVED_COLUMN_ID_TSID,
            1,
            2,
            3,
            4,
            5,
        ] {
            let offset = codec
                .has_column(&buffer, &mut offsets_map, column_id)
                .unwrap();
            let value = codec.decode_value_at(&buffer, offset, column_id).unwrap();
            let expected_value = row
                .iter()
                .find(|(id, _)| *id == column_id)
                .unwrap()
                .1
                .clone();
            assert_eq!(value.as_value_ref(), expected_value);
        }
    }
}
