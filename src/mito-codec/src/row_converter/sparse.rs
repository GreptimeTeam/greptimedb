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
///
/// ## Encoding format
/// Each primary key is encoded as a sequence of `(column_id, value)` pairs:
/// - The first two fields are always the reserved `table_id` (uint32) and `tsid` (uint64).
/// - User-defined labels follow, sorted by **column name** in lexicographical order.
/// - Null values are omitted (not encoded).
///
/// The `column_id` is encoded as a 4-byte big-endian integer, and the value is encoded
/// using memcomparable serialization.
///
/// `decode_leftmost` always decodes the first value from the encoded bytes (i.e., the
/// `table_id` field).
///
/// ## Requirements
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
/// callers must not insert a column id that is already present,
/// otherwise the duplicate will shadow the later value on lookup.
#[derive(Debug, Clone, PartialEq, Eq, Default)]
pub struct SparseValues {
    values: Vec<(ColumnId, Value)>,
}

impl SparseValues {
    /// Creates an empty [`SparseValues`].
    pub fn new() -> Self {
        Self { values: Vec::new() }
    }

    /// Creates an empty [`SparseValues`] with space reserved for `cap` entries.
    pub fn with_capacity(cap: usize) -> Self {
        Self {
            values: Vec::with_capacity(cap),
        }
    }

    /// Returns the value of the given column, or [`Value::Null`] if the column is not present.
    pub fn get_or_null(&self, column_id: ColumnId) -> &Value {
        for (id, value) in &self.values {
            if *id == column_id {
                return value;
            }
        }
        &Value::Null
    }

    /// Returns the value of the given column, or [`None`] if the column is not present.
    pub fn get(&self, column_id: &ColumnId) -> Option<&Value> {
        for (id, value) in &self.values {
            if id == column_id {
                return Some(value);
            }
        }
        None
    }

    /// Appends a new `(column_id, value)` pair.
    ///
    /// Append-only: the caller must ensure `column_id` is not already present.
    pub fn insert(&mut self, column_id: ColumnId, value: Value) {
        self.values.push((column_id, value));
    }

    /// Returns an iterator over all stored column id/value pairs.
    pub fn iter(&self) -> impl Iterator<Item = (&ColumnId, &Value)> {
        self.values.iter().map(|(id, value)| (id, value))
    }
}

/// The column id of the tsid.
pub const RESERVED_COLUMN_ID_TSID: ColumnId = ReservedColumnId::tsid();
/// The column id of the table id.
pub const RESERVED_COLUMN_ID_TABLE_ID: ColumnId = ReservedColumnId::table_id();
/// The size of the column id in the encoded sparse row.
pub const COLUMN_ID_ENCODE_SIZE: usize = 4;

// Fixed byte offsets for reserved columns in the sparse encoding.
// Layout: [table_id_col_id: 4B][marker: 1B][table_id: 4B][tsid_col_id: 4B][marker: 1B][tsid: 8B]
/// Byte offset to the table_id value (after its 4-byte column id).
const TABLE_ID_VALUE_OFFSET: usize = COLUMN_ID_ENCODE_SIZE;
/// Byte offset to the tsid value (after 9-byte table_id entry + 4-byte tsid column id).
const TSID_VALUE_OFFSET: usize = COLUMN_ID_ENCODE_SIZE + 5 + COLUMN_ID_ENCODE_SIZE;
/// Byte offset where tag columns start (after 9-byte table_id + 13-byte tsid entries).
const TAGS_START_OFFSET: usize = COLUMN_ID_ENCODE_SIZE + 5 + COLUMN_ID_ENCODE_SIZE + 9;

/// Inline capacity for the small-vec fast path of [`SparseOffsetsCache`].
///
/// Most sparse primary keys carry only a handful of tags; a linear scan over
/// a small pre-reserved `Vec` beats a `HashMap` lookup in that regime (no
/// hash, better cache behavior). Primary keys with more than this many tags
/// spill the remainder into a `HashMap`.
const SPARSE_OFFSETS_INLINE_CAP: usize = 32;

/// A lazily populated cache of tag column offsets inside a sparse primary key.
#[derive(Debug, Clone)]
pub struct SparseOffsetsCache {
    /// Small-vec fast path, pre-reserved to [`SPARSE_OFFSETS_INLINE_CAP`] so
    /// the common case never reallocates.
    inline: Vec<(ColumnId, usize)>,
    /// Overflow for columns beyond the inline capacity. Lazily allocated.
    overflow: HashMap<ColumnId, usize>,
    /// Next byte position in the pk to resume parsing from.
    cursor: usize,
    /// True once the decoder has walked past the last tag column (or stopped
    /// on an unknown column id); no further offsets can be discovered.
    finished: bool,
}

impl Default for SparseOffsetsCache {
    fn default() -> Self {
        Self::new()
    }
}

impl SparseOffsetsCache {
    pub fn new() -> Self {
        Self {
            inline: Vec::new(),
            overflow: HashMap::new(),
            cursor: TAGS_START_OFFSET,
            finished: false,
        }
    }

    pub fn clear(&mut self) {
        self.inline.clear();
        self.overflow.clear();
        self.cursor = TAGS_START_OFFSET;
        self.finished = false;
    }

    /// Returns the cached offset for `column_id`, if any.
    fn get(&self, column_id: ColumnId) -> Option<usize> {
        for entry in &self.inline {
            if entry.0 == column_id {
                return Some(entry.1);
            }
        }
        if self.overflow.is_empty() {
            return None;
        }
        self.overflow.get(&column_id).copied()
    }

    /// Records a new `(column_id, offset)` entry.
    fn insert(&mut self, column_id: ColumnId, offset: usize) {
        if self.inline.len() < SPARSE_OFFSETS_INLINE_CAP {
            if self.inline.capacity() == 0 {
                self.inline.reserve_exact(SPARSE_OFFSETS_INLINE_CAP);
            }
            self.inline.push((column_id, offset));
        } else {
            self.overflow.insert(column_id, offset);
        }
    }

    #[cfg(test)]
    fn contains(&self, column_id: ColumnId) -> bool {
        self.get(column_id).is_some()
    }
}

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
        let mut values = SparseValues::with_capacity(16);

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
    ///
    /// The pk must start with the table_id + tsid prefix written by
    /// `encode_internal`.
    pub fn has_column(
        &self,
        pk: &[u8],
        cache: &mut SparseOffsetsCache,
        column_id: ColumnId,
    ) -> Option<usize> {
        // Decoding is lazy: on each call we only advance the cache's cursor as
        // far as needed to answer the query. A column that has already been
        // seen returns immediately; a column we haven't reached yet causes the
        // parser to resume from `cache.cursor` and stop as soon as the column
        // is located. Once the cursor walks off the end (or hits an unknown
        // column id) the cache is marked finished, so subsequent misses are
        // O(1).
        // table_id and tsid are at fixed offsets.
        match column_id {
            RESERVED_COLUMN_ID_TABLE_ID => return Some(TABLE_ID_VALUE_OFFSET),
            RESERVED_COLUMN_ID_TSID => return Some(TSID_VALUE_OFFSET),
            _ => {}
        }

        if let Some(offset) = cache.get(column_id) {
            return Some(offset);
        }
        if cache.finished {
            return None;
        }

        let mut deserializer = Deserializer::new(pk);
        deserializer.advance(cache.cursor);
        let mut offset = cache.cursor;
        while deserializer.has_remaining() {
            let col = u32::deserialize(&mut deserializer).unwrap();
            offset += COLUMN_ID_ENCODE_SIZE;
            let value_offset = offset;
            cache.insert(col, value_offset);
            let Some(field) = self.get_field(col) else {
                cache.finished = true;
                cache.cursor = offset;
                return None;
            };

            let skip = field.skip_deserialize(pk, &mut deserializer).unwrap();
            offset += skip;
            cache.cursor = offset;
            if col == column_id {
                return Some(value_offset);
            }
        }

        cache.finished = true;
        None
    }

    /// Decode value at `offset` in `pk`.
    pub fn decode_value_at(&self, pk: &[u8], offset: usize, column_id: ColumnId) -> Result<Value> {
        let mut deserializer = Deserializer::new(pk);
        deserializer.advance(offset);
        // Safety: checked by `has_column`
        let field = self.get_field(column_id).unwrap();
        field.deserialize(&mut deserializer)
    }

    /// Returns the encoded bytes of the given `column_id` in `pk`.
    ///
    /// Returns `Ok(None)` if the `column_id` is missing in `pk`.
    pub fn encoded_value_for_column<'a>(
        &self,
        pk: &'a [u8],
        cache: &mut SparseOffsetsCache,
        column_id: ColumnId,
    ) -> Result<Option<&'a [u8]>> {
        let Some(offset) = self.has_column(pk, cache, column_id) else {
            return Ok(None);
        };

        let Some(field) = self.get_field(column_id) else {
            return Ok(None);
        };

        let mut deserializer = Deserializer::new(pk);
        deserializer.advance(offset);
        let len = field.skip_deserialize(pk, &mut deserializer)?;
        Ok(Some(&pk[offset..offset + len]))
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
        skip_partition_column: bool,
    ) -> Box<dyn PrimaryKeyFilter> {
        Box::new(SparsePrimaryKeyFilter::new(
            metadata.clone(),
            filters,
            self.clone(),
            skip_partition_column,
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
        let mut sparse_value = SparseValues::new();
        sparse_value.insert(1, Value::Int32(42));

        assert_eq!(sparse_value.get_or_null(1), &Value::Int32(42));
        assert_eq!(sparse_value.get_or_null(2), &Value::Null);
    }

    #[test]
    fn test_sparse_value_insert() {
        let mut sparse_value = SparseValues::new();
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

        let mut offsets_map = SparseOffsetsCache::new();
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
    fn test_has_column_lazy_resume() {
        let region_metadata = test_region_metadata();
        let codec = SparsePrimaryKeyCodec::new(&region_metadata);
        let mut buffer = Vec::new();
        codec
            .encode_to_vec(test_row().into_iter(), &mut buffer)
            .unwrap();

        let mut cache = SparseOffsetsCache::new();
        // Look up an early column: only a prefix of tags is decoded.
        assert!(codec.has_column(&buffer, &mut cache, 1).is_some());
        assert!(!cache.finished);
        assert!(cache.contains(1));
        assert!(!cache.contains(5));

        // A later column resumes from the cursor.
        assert!(codec.has_column(&buffer, &mut cache, 5).is_some());
        assert!(cache.contains(5));

        // An earlier column that was already cached still resolves.
        assert!(codec.has_column(&buffer, &mut cache, 2).is_some());

        // A non-existent column walks off the end and marks the cache finished.
        assert!(codec.has_column(&buffer, &mut cache, 999).is_none());
        assert!(cache.finished);
        // Further misses are O(1).
        assert!(codec.has_column(&buffer, &mut cache, 998).is_none());
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
        let mut offsets_map = SparseOffsetsCache::new();
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

    #[test]
    fn test_encoded_value_for_column() {
        let region_metadata = test_region_metadata();
        let codec = SparsePrimaryKeyCodec::new(&region_metadata);
        let mut buffer = Vec::new();
        let row = test_row();
        codec
            .encode_to_vec(row.clone().into_iter(), &mut buffer)
            .unwrap();
        assert!(!buffer.is_empty());

        let mut offsets_map = SparseOffsetsCache::new();
        for column_id in [
            RESERVED_COLUMN_ID_TABLE_ID,
            RESERVED_COLUMN_ID_TSID,
            1,
            2,
            3,
            4,
            5,
        ] {
            let encoded_value = codec
                .encoded_value_for_column(&buffer, &mut offsets_map, column_id)
                .unwrap()
                .unwrap();
            let expected_value = row
                .iter()
                .find(|(id, _)| *id == column_id)
                .unwrap()
                .1
                .clone();
            let data_type = match column_id {
                RESERVED_COLUMN_ID_TABLE_ID => ConcreteDataType::uint32_datatype(),
                RESERVED_COLUMN_ID_TSID => ConcreteDataType::uint64_datatype(),
                _ => ConcreteDataType::string_datatype(),
            };
            let field = SortField::new(data_type);
            let mut expected_encoded = Vec::new();
            let mut serializer = Serializer::new(&mut expected_encoded);
            field.serialize(&mut serializer, &expected_value).unwrap();
            assert_eq!(encoded_value, expected_encoded.as_slice());
        }

        for column_id in [6_u32, 7_u32, 999_u32] {
            let encoded_value = codec
                .encoded_value_for_column(&buffer, &mut offsets_map, column_id)
                .unwrap();
            assert!(encoded_value.is_none());
        }
    }
}
